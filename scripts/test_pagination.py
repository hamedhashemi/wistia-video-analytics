from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.wistia_client import WistiaClient


def load_config() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "media_config.json"
    return json.loads(path.read_text(encoding="utf-8"))


def record_count(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in ("events", "visitors", "data", "results", "items"):
            nested = value.get(key)
            if isinstance(nested, list):
                return len(nested)
    return 0


def parse_args() -> argparse.Namespace:
    today = datetime.now(timezone.utc).date()
    parser = argparse.ArgumentParser(
        description="Safely verify Wistia event pagination without printing event values or PII."
    )
    parser.add_argument(
        "--start-date",
        default=(today - timedelta(days=90)).isoformat(),
        help="YYYY-MM-DD (default: 90 days ago)",
    )
    parser.add_argument(
        "--end-date", default=today.isoformat(), help="YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=2,
        choices=range(1, 101),
        metavar="1-100",
        help="Small default intentionally forces pagination when enough events exist.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Safety cap for exploration only (default: 10 pages)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(PROJECT_ROOT / ".env")
    token = os.getenv("WISTIA_API_TOKEN", "").strip()
    if not token:
        print("ERROR: WISTIA_API_TOKEN is not set in .env")
        return 2

    config = load_config()
    client = WistiaClient(token=token, api_version=config.get("api_version", "2026-07"))

    print("Wistia Pagination Verification")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"per_page={args.per_page}; max_pages={args.max_pages}")
    print("No event values, visitor keys, IPs, emails, or token will be printed.\n")

    for media_id in config["media_ids"]:
        print(f"--- Media: {media_id} ---")
        total = 0
        pages_with_data = 0

        for page in range(1, args.max_pages + 1):
            response = client.events(
                media_id=media_id,
                start_date=args.start_date,
                end_date=args.end_date,
                page=page,
                per_page=args.per_page,
            )

            if response.error is not None:
                print(f"page {page}: FAILED -> HTTP {response.status_code} ({response.error})")
                break

            count = record_count(response.data)
            print(f"page {page}: HTTP {response.status_code}, records={count}")
            total += count

            if count > 0:
                pages_with_data += 1

            # Standard page-number pagination termination condition.
            if count < args.per_page:
                break

        if pages_with_data >= 2:
            print(f"RESULT: pagination CONFIRMED; records observed across {pages_with_data} pages = {total}")
        elif total > 0:
            print(f"RESULT: endpoint works, but this sample did not require a second populated page; records observed = {total}")
        else:
            print("RESULT: no events found in this date range; pagination cannot be demonstrated for this media.")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
