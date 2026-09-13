from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.wistia_client import WistiaClient, WistiaResponse  # noqa: E402

SENSITIVE_KEY_FRAGMENTS = (
    "ip",
    "email",
    "name",
    "token",
    "authorization",
    "visitor_key",
    "visitorkey",
)


def load_config() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "media_config.json"
    return json.loads(path.read_text(encoding="utf-8"))


def type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def schema_paths(value: Any, prefix: str = "$", max_array_items: int = 3) -> dict[str, str]:
    """Return field paths and types only; no response values are exposed."""
    result: dict[str, str] = {prefix: type_name(value)}
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{prefix}.{key}"
            result.update(schema_paths(child, child_path, max_array_items))
    elif isinstance(value, list):
        for index, child in enumerate(value[:max_array_items]):
            result.update(schema_paths(child, f"{prefix}[]", max_array_items))
            if index == 0:
                break
    return result


def record_count(value: Any) -> int | None:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in ("events", "visitors", "data", "results", "items"):
            nested = value.get(key)
            if isinstance(nested, list):
                return len(nested)
    return None


def find_first_value(value: Any, candidate_keys: set[str]) -> Any | None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in candidate_keys and child not in (None, ""):
                return child
            found = find_first_value(child, candidate_keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_first_value(child, candidate_keys)
            if found is not None:
                return found
    return None


def safe_summary(label: str, response: WistiaResponse) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "endpoint": label,
        "status_code": response.status_code,
        "ok": response.error is None and 200 <= response.status_code < 300,
    }
    if response.error:
        summary["error"] = response.error
        return summary

    summary["response_type"] = type_name(response.data)
    count = record_count(response.data)
    if count is not None:
        summary["records_in_response"] = count
    summary["schema"] = schema_paths(response.data)
    return summary


def print_summary(summary: dict[str, Any]) -> None:
    status = "OK" if summary.get("ok") else "FAILED"
    print(f"[{status}] {summary['endpoint']} -> HTTP {summary['status_code']}")
    if summary.get("error"):
        print(f"       error: {summary['error']}")
        return
    if "records_in_response" in summary:
        print(f"       records in response: {summary['records_in_response']}")
    print(f"       schema fields discovered: {len(summary.get('schema', {}))}")


def parse_args() -> argparse.Namespace:
    today = date.today()
    parser = argparse.ArgumentParser(
        description="Safely explore Wistia API endpoints without printing PII or the API token."
    )
    parser.add_argument(
        "--start-date",
        default=(today - timedelta(days=7)).isoformat(),
        help="YYYY-MM-DD (default: 7 days ago)",
    )
    parser.add_argument(
        "--end-date", default=today.isoformat(), help="YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--events-per-page",
        type=int,
        default=5,
        choices=range(1, 101),
        metavar="1-100",
    )
    parser.add_argument(
        "--output",
        default="api_exploration_report.json",
        help="Safe schema-only report file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(PROJECT_ROOT / ".env")
    token = os.getenv("WISTIA_API_TOKEN", "").strip()
    if not token:
        print("ERROR: WISTIA_API_TOKEN is not set. Copy .env.example to .env and add the token.")
        return 2

    config = load_config()
    media_ids = config["media_ids"]
    client = WistiaClient(token=token, api_version=config.get("api_version", "2026-07"))

    report: dict[str, Any] = {
        "purpose": "Phase 1 API exploration; schema/types only, no API response values",
        "api_version": config.get("api_version", "2026-07"),
        "date_range": {"start_date": args.start_date, "end_date": args.end_date},
        "media": {},
    }

    print("Wistia API Exploration")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print("No token, IP address, email, visitor key, or response values will be printed.\n")

    for media_id in media_ids:
        print(f"--- Media: {media_id} ---")
        media_report: list[dict[str, Any]] = []

        calls = [
            ("media_metadata", client.media_metadata(media_id)),
            ("media_stats", client.media_stats(media_id)),
            (
                "media_stats_by_date",
                client.media_stats_by_date(media_id, args.start_date, args.end_date),
            ),
            ("media_engagement", client.media_engagement(media_id)),
            (
                "events_page_1",
                client.events(
                    media_id,
                    args.start_date,
                    args.end_date,
                    page=1,
                    per_page=args.events_per_page,
                ),
            ),
        ]

        event_response: WistiaResponse | None = None
        for label, response in calls:
            summary = safe_summary(label, response)
            media_report.append(summary)
            print_summary(summary)
            if label == "events_page_1":
                event_response = response

        if event_response and event_response.data is not None:
            visitor_key = find_first_value(
                event_response.data, {"visitor_key", "visitorkey", "visitorid", "visitor_id"}
            )
            if isinstance(visitor_key, str) and visitor_key:
                visitor_summary = safe_summary(
                    "visitor_from_event_sample", client.visitor(visitor_key)
                )
                media_report.append(visitor_summary)
                print_summary(visitor_summary)
            else:
                print("[INFO] No visitor key found in the sampled event response.")

        report["media"][media_id] = media_report
        print()

    output_path = PROJECT_ROOT / args.output
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Safe exploration report written to: {output_path}")
    print("Next: share this report (not your .env file) to finalize Bronze/Silver/Gold schemas.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
