from __future__ import annotations

import hashlib
import os
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse, urlunparse
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from fastapi.responses import HTMLResponse
from icalendar import Calendar, Event
from pydantic import BaseModel, Field, field_validator


DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "calendar-relay.sqlite3"
API_KEY = os.getenv("API_KEY", "")
CALENDAR_NAME = os.getenv("CALENDAR_NAME", "Calendar Relay")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))
WEBCAL_URLS = [url.strip() for url in os.getenv("WEBCAL_URLS", "").split(",") if url.strip()]

app = FastAPI(title="Calendar Relay", version="0.1.0")


class EventCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    start: datetime
    end: datetime
    timezone: str | None = Field(default=None, description="Used when start/end are naive datetimes.")
    description: str | None = None
    location: str | None = None
    uid: str | None = None

    @field_validator("end")
    @classmethod
    def end_must_follow_start(cls, end: datetime, info):
        start = info.data.get("start")
        if start and end <= start:
            raise ValueError("end must be after start")
        return end


class EventOut(BaseModel):
    uid: str
    title: str
    start: datetime
    end: datetime
    status: str
    location: str | None = None
    description: str | None = None


class WebcalUrlsUpdate(BaseModel):
    urls: list[str] = Field(default_factory=list)

    @field_validator("urls")
    @classmethod
    def validate_urls(cls, urls: list[str]):
        normalized_urls: list[str] = []
        seen: set[str] = set()
        for url in urls:
            url = url.strip()
            if not url:
                continue
            normalized = normalize_feed_url(url)
            if normalized in seen:
                continue
            seen.add(normalized)
            normalized_urls.append(normalized)
        return normalized_urls


@contextmanager
def db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                uid TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                start TEXT NOT NULL,
                end TEXT NOT NULL,
                description TEXT,
                location TEXT,
                status TEXT NOT NULL DEFAULT 'CONFIRMED',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS feed_cache (
                url TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        has_webcal_urls = conn.execute("SELECT 1 FROM settings WHERE key = 'webcal_urls'").fetchone()
        if not has_webcal_urls and WEBCAL_URLS:
            conn.execute(
                "INSERT INTO settings (key, value) VALUES ('webcal_urls', ?)",
                ("\n".join(normalize_feed_url(url) for url in WEBCAL_URLS),),
            )


@app.on_event("startup")
def startup() -> None:
    init_db()


def require_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    bearer = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer = authorization[7:]
    provided = x_api_key or bearer
    if not API_KEY or provided != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid api key")


def normalize_feed_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "webcal":
        parsed = parsed._replace(scheme="https")
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported calendar URL scheme: {parsed.scheme}")
    return urlunparse(parsed)


def get_webcal_urls() -> list[str]:
    with db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = 'webcal_urls'").fetchone()
    if not row:
        return []
    return [url for url in row["value"].splitlines() if url]


def set_webcal_urls(urls: list[str]) -> list[str]:
    value = "\n".join(urls)
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('webcal_urls', ?)",
            (value,),
        )
    return urls


def as_aware(value: datetime, timezone: str | None) -> datetime:
    if value.tzinfo:
        return value
    if timezone:
        try:
            return value.replace(tzinfo=ZoneInfo(timezone))
        except ZoneInfoNotFoundError as exc:
            raise HTTPException(status_code=400, detail=f"unknown timezone: {timezone}") from exc
    return value.replace(tzinfo=UTC)


def row_to_event(row: sqlite3.Row) -> EventOut:
    return EventOut(
        uid=row["uid"],
        title=row["title"],
        start=datetime.fromisoformat(row["start"]),
        end=datetime.fromisoformat(row["end"]),
        status=row["status"],
        location=row["location"],
        description=row["description"],
    )


async def fetch_feed(url: str) -> str | None:
    normalized = normalize_feed_url(url)
    now = datetime.now(UTC)
    with db() as conn:
        cached = conn.execute("SELECT body, fetched_at FROM feed_cache WHERE url = ?", (normalized,)).fetchone()
        if cached:
            fetched_at = datetime.fromisoformat(cached["fetched_at"])
            if (now - fetched_at).total_seconds() < CACHE_TTL_SECONDS:
                return cached["body"]

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(normalized)
            response.raise_for_status()
    except httpx.HTTPError:
        return cached["body"] if cached else None

    body = response.text
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO feed_cache (url, body, fetched_at) VALUES (?, ?, ?)",
            (normalized, body, now.isoformat()),
        )
    return body


def build_local_event(row: sqlite3.Row) -> Event:
    event = Event()
    event.add("uid", row["uid"])
    event.add("summary", row["title"])
    event.add("dtstart", datetime.fromisoformat(row["start"]))
    event.add("dtend", datetime.fromisoformat(row["end"]))
    event.add("dtstamp", datetime.now(UTC))
    event.add("created", datetime.fromisoformat(row["created_at"]))
    event.add("last-modified", datetime.fromisoformat(row["updated_at"]))
    event.add("status", row["status"])
    if row["description"]:
        event.add("description", row["description"])
    if row["location"]:
        event.add("location", row["location"])
    return event


async def build_calendar() -> bytes:
    calendar = Calendar()
    calendar.add("prodid", "-//calendar-relay//calendar-relay 0.1//EN")
    calendar.add("version", "2.0")
    calendar.add("calscale", "GREGORIAN")
    calendar.add("method", "PUBLISH")
    calendar.add("x-wr-calname", CALENDAR_NAME)

    seen_uids: set[str] = set()
    with db() as conn:
        rows = conn.execute("SELECT * FROM events ORDER BY start").fetchall()
    for row in rows:
        seen_uids.add(row["uid"])
        calendar.add_component(build_local_event(row))

    for source_url in get_webcal_urls():
        body = await fetch_feed(source_url)
        if not body:
            continue
        source = Calendar.from_ical(body)
        for component in source.walk("VEVENT"):
            uid = str(component.get("uid", ""))
            if not uid:
                uid = hashlib.sha256(component.to_ical()).hexdigest()
                component.add("uid", uid)
            if uid in seen_uids:
                continue
            seen_uids.add(uid)
            calendar.add_component(component)

    return calendar.to_ical()


@app.get("/")
def root():
    calendar_url = f"{PUBLIC_BASE_URL}/calendar.ics" if PUBLIC_BASE_URL else "/calendar.ics"
    return {
        "service": "calendar-relay",
        "calendar_url": calendar_url,
        "webcal_url": calendar_url.replace("https://", "webcal://", 1),
    }


@app.get("/admin", response_class=HTMLResponse)
def admin():
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Calendar Relay</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #f7f8f5;
      --panel: #ffffff;
      --text: #20231f;
      --muted: #666f61;
      --border: #d9ded2;
      --accent: #286b55;
      --surface: #eef1ea;
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --bg: #10130f;
        --panel: #191d17;
        --text: #edf1e8;
        --muted: #aeb7a8;
        --border: #353d31;
        --accent: #74c7a5;
        --surface: #23291f;
      }
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font: 15px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    main {
      width: min(960px, calc(100vw - 32px));
      margin: 32px auto;
      display: grid;
      gap: 18px;
    }
    header {
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }
    h1, h2 { margin: 0; line-height: 1.15; }
    h1 { font-size: clamp(28px, 5vw, 44px); }
    h2 { font-size: 18px; }
    a { color: var(--accent); }
    section {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 18px;
      display: grid;
      gap: 14px;
    }
    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 650;
    }
    input, textarea {
      width: 100%;
      min-height: 40px;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 9px 10px;
      background: var(--surface);
      color: var(--text);
      font: inherit;
    }
    textarea {
      min-height: 130px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 13px;
    }
    button {
      min-height: 40px;
      border: 0;
      border-radius: 6px;
      padding: 0 14px;
      background: var(--accent);
      color: white;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary {
      background: var(--surface);
      color: var(--text);
      border: 1px solid var(--border);
    }
    .row {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .status {
      min-height: 22px;
      color: var(--muted);
      font-size: 13px;
    }
    .meta {
      color: var(--muted);
      font-size: 13px;
    }
    .hidden { display: none; }
    pre {
      margin: 0;
      max-height: min(58vh, 620px);
      overflow: auto;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 12px;
      background: var(--surface);
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    @media (max-width: 720px) {
      main { width: min(100vw - 20px, 960px); margin: 16px auto; }
      section { padding: 14px; }
      button { width: 100%; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Calendar Relay</h1>
        <div class="meta"><a href="/calendar.ics">calendar.ics</a></div>
      </div>
      <button class="secondary hidden" id="changeKey" type="button">Change key</button>
    </header>

    <section id="loginPanel">
      <h2>Login</h2>
      <label>API key
        <input id="apiKey" type="password" autocomplete="off">
      </label>
      <div class="row">
        <button id="login" type="button">Continue</button>
      </div>
      <div class="status" id="loginStatus"></div>
    </section>

    <section id="sourcesPanel" class="hidden">
      <h2>Webcal Sources</h2>
      <label>One URL per line
        <textarea id="webcalUrls" spellcheck="false"></textarea>
      </label>
      <div class="row">
        <button id="saveWebcals" type="button">Save sources</button>
      </div>
      <div class="status" id="webcalStatus"></div>
    </section>

    <section id="schemaPanel" class="hidden">
      <h2>OpenAPI Schema</h2>
      <div class="row">
        <button id="copySchema" type="button">Copy schema</button>
        <button class="secondary" id="refresh" type="button">Refresh</button>
      </div>
      <pre id="openapiSchema"></pre>
      <div class="status" id="schemaStatus"></div>
    </section>
  </main>

  <script>
    const keyInput = document.querySelector("#apiKey");
    const loginPanel = document.querySelector("#loginPanel");
    const sourcesPanel = document.querySelector("#sourcesPanel");
    const schemaPanel = document.querySelector("#schemaPanel");
    const changeKeyButton = document.querySelector("#changeKey");
    const urlsInput = document.querySelector("#webcalUrls");
    const loginStatus = document.querySelector("#loginStatus");
    const webcalStatus = document.querySelector("#webcalStatus");
    const schemaStatus = document.querySelector("#schemaStatus");
    const schemaOutput = document.querySelector("#openapiSchema");

    keyInput.value = localStorage.getItem("calendarRelayApiKey") || "";

    function headers() {
      const key = keyInput.value.trim();
      return key ? { "X-API-Key": key } : {};
    }

    function setLoggedIn(loggedIn) {
      loginPanel.classList.toggle("hidden", loggedIn);
      sourcesPanel.classList.toggle("hidden", !loggedIn);
      schemaPanel.classList.toggle("hidden", !loggedIn);
      changeKeyButton.classList.toggle("hidden", !loggedIn);
    }

    async function api(path, options = {}) {
      const response = await fetch(path, {
        ...options,
        headers: {
          ...headers(),
          ...(options.body ? { "Content-Type": "application/json" } : {}),
          ...(options.headers || {})
        }
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || response.statusText);
      }
      if (response.status === 204) return null;
      return response.json();
    }

    async function loadWebcals() {
      const data = await api("/api/webcal-urls");
      urlsInput.value = data.urls.join("\\n");
      webcalStatus.textContent = `${data.urls.length} source${data.urls.length === 1 ? "" : "s"}`;
    }

    async function loadSchema() {
      const response = await fetch("/openapi.json");
      if (!response.ok) throw new Error(await response.text() || response.statusText);
      const schema = await response.json();
      schemaOutput.textContent = JSON.stringify(schema, null, 2);
      schemaStatus.textContent = "Loaded";
    }

    async function refreshAll() {
      try {
        await Promise.all([loadWebcals(), loadSchema()]);
        setLoggedIn(true);
        loginStatus.textContent = "";
      } catch (error) {
        setLoggedIn(false);
        loginStatus.textContent = error.message;
      }
    }

    document.querySelector("#login").onclick = async () => {
      localStorage.setItem("calendarRelayApiKey", keyInput.value.trim());
      await refreshAll();
    };
    changeKeyButton.onclick = () => {
      localStorage.removeItem("calendarRelayApiKey");
      keyInput.value = "";
      setLoggedIn(false);
    };
    document.querySelector("#refresh").onclick = refreshAll;
    document.querySelector("#saveWebcals").onclick = async () => {
      const urls = urlsInput.value.split("\\n").map((url) => url.trim()).filter(Boolean);
      const data = await api("/api/webcal-urls", {
        method: "PUT",
        body: JSON.stringify({ urls })
      });
      urlsInput.value = data.urls.join("\\n");
      webcalStatus.textContent = "Saved";
    };
    document.querySelector("#copySchema").onclick = async () => {
      await navigator.clipboard.writeText(schemaOutput.textContent);
      schemaStatus.textContent = "Copied";
    };

    if (keyInput.value) refreshAll();
  </script>
</body>
</html>
"""


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/events", response_model=list[EventOut])
def list_events(_: Annotated[None, Depends(require_api_key)]):
    with db() as conn:
        rows = conn.execute("SELECT * FROM events ORDER BY start").fetchall()
    return [row_to_event(row) for row in rows]


@app.get("/api/webcal-urls")
def list_webcal_urls(_: Annotated[None, Depends(require_api_key)]):
    return {"urls": get_webcal_urls()}


@app.put("/api/webcal-urls")
def replace_webcal_urls(payload: WebcalUrlsUpdate, _: Annotated[None, Depends(require_api_key)]):
    return {"urls": set_webcal_urls(payload.urls)}


@app.post("/api/events", response_model=EventOut, status_code=status.HTTP_201_CREATED)
def create_event(payload: EventCreate, _: Annotated[None, Depends(require_api_key)]):
    now = datetime.now(UTC).isoformat()
    uid = payload.uid or f"{uuid4()}@calendar-relay"
    start = as_aware(payload.start, payload.timezone).astimezone(UTC)
    end = as_aware(payload.end, payload.timezone).astimezone(UTC)
    with db() as conn:
        try:
            conn.execute(
                """
                INSERT INTO events (uid, title, start, end, description, location, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'CONFIRMED', ?, ?)
                """,
                (
                    uid,
                    payload.title,
                    start.isoformat(),
                    end.isoformat(),
                    payload.description,
                    payload.location,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="event uid already exists") from exc
        row = conn.execute("SELECT * FROM events WHERE uid = ?", (uid,)).fetchone()
    return row_to_event(row)


@app.post("/api/events/{uid}/cancel", response_model=EventOut)
def cancel_event(uid: str, _: Annotated[None, Depends(require_api_key)]):
    now = datetime.now(UTC).isoformat()
    with db() as conn:
        row = conn.execute("SELECT * FROM events WHERE uid = ?", (uid,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="event not found")
        conn.execute("UPDATE events SET status = 'CANCELLED', updated_at = ? WHERE uid = ?", (now, uid))
        row = conn.execute("SELECT * FROM events WHERE uid = ?", (uid,)).fetchone()
    return row_to_event(row)


@app.delete("/api/events/{uid}", status_code=status.HTTP_204_NO_CONTENT)
def delete_event(uid: str, _: Annotated[None, Depends(require_api_key)]):
    with db() as conn:
        result = conn.execute("DELETE FROM events WHERE uid = ?", (uid,))
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="event not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/calendar.ics")
async def calendar_feed():
    return Response(
        content=await build_calendar(),
        media_type="text/calendar; charset=utf-8",
        headers={"Content-Disposition": 'inline; filename="calendar-relay.ics"'},
    )
