# Calendar Relay

Calendar Relay publishes one subscription calendar from two sources:

- upstream `webcal://` or `https://` iCalendar feeds, managed through the HTTP API
- appointments created by producers through the small HTTP API

Apple Calendar can subscribe to the combined calendar:

```text
webcal://calendar-relay.misei.dev/calendar.ics
```

Or only the events created through this API:

```text
webcal://calendar-relay.misei.dev/created.ics
```

## Deploy

This compose file follows the `iac-cloud` service template and expects the platform Valkey secret resolver:

```sh
docker platform compose.yml up --build -d
```

Create the API key manually in the Valkey UI before deployment. The compose file reads it with:

```yaml
API_KEY: "{&calendar_relay_api_key!}"
```

Persistent data lives at:

```text
/vol/calendar-relay/data
```

## Configuration

Set these in `compose.yml`:

- `PUBLIC_BASE_URL`: base URL shown by `/`
- `WEBCAL_URLS`: optional comma-separated upstream calendars used only to seed an empty database on first startup
- `CACHE_TTL_SECONDS`: upstream fetch cache TTL
- `CALENDAR_NAME`: display name for the generated calendar

## API

Send the API key with `X-API-Key` or `Authorization: Bearer`.

A small browser UI is available at:

```text
https://calendar-relay.misei.dev/admin
```

It asks for the API key, then shows subscription links, upstream webcal URL editing, and copyable implementation instructions for another agent/service.

Create an appointment:

```sh
curl -X POST https://calendar-relay.misei.dev/api/events \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Dentist",
    "start": "2026-07-06T09:00:00",
    "end": "2026-07-06T09:30:00",
    "timezone": "Europe/Berlin",
    "location": "Berlin"
  }'
```

List upstream calendar URLs:

```sh
curl https://calendar-relay.misei.dev/api/webcal-urls \
  -H "X-API-Key: $API_KEY"
```

Replace upstream calendar URLs online:

```sh
curl -X PUT https://calendar-relay.misei.dev/api/webcal-urls \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "urls": [
      "webcal://example.com/team.ics",
      "https://example.com/holidays.ics"
    ]
  }'
```

Cancel an appointment while keeping a `STATUS:CANCELLED` entry in the feed:

```sh
curl -X POST https://calendar-relay.misei.dev/api/events/<uid>/cancel \
  -H "X-API-Key: $API_KEY"
```

Remove an appointment from the feed:

```sh
curl -X DELETE https://calendar-relay.misei.dev/api/events/<uid> \
  -H "X-API-Key: $API_KEY"
```
