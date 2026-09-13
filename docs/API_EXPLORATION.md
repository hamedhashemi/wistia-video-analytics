# Phase 1 — Wistia API Exploration

## Purpose

Before building AWS resources, verify the real response structure and permissions for the two supplied media IDs. This phase answers technical questions that should not be pushed to the coach.

## Endpoints tested

- `GET /modern/medias/{mediaId}` — media metadata
- `GET /modern/stats/medias/{mediaId}` — media stats
- `GET /modern/stats/medias/{mediaId}/by_date` — date-bounded stats
- `GET /modern/stats/medias/{mediaId}/engagement` — engagement data
- `GET /modern/stats/events` — visitor/event-level analytics, filtered by `media_id` and date
- `GET /modern/stats/visitors/{visitorKey}` — visitor detail when a visitor key is found in sampled events

## Security

The token must be provided through `.env` locally and later through AWS Secrets Manager. Never hard-code it, print it, or commit `.env`.

The exploration script writes a **schema-only** report. It does not write response values such as IP address, email, or visitor identifiers.

## Run

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
# edit .env and set WISTIA_API_TOKEN
python scripts/api_exploration.py
pytest -q
```

### Windows PowerShell

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
Copy-Item .env.example .env
# edit .env and set WISTIA_API_TOKEN
python scripts/api_exploration.py
pytest -q
```

## What to inspect in the report

1. Which endpoints return HTTP 200 vs 401/403/404.
2. Actual field names and nested structures.
3. Whether event results expose a visitor key.
4. Whether date-bounded endpoints return records for the selected range.
5. Fields needed for the Bronze, Silver, and Gold schemas.

Do not design final transformations until this report is reviewed.
