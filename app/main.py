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
            normalized = normalize_feed_url(url.strip())
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
