from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

BASE_URL = "https://api.wistia.com"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

logger = logging.getLogger("wistia_bronze_ingestion")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


@dataclass(frozen=True)
class ApiResult:
    data: Any
    status_code: int


class WistiaApiError(RuntimeError):
    pass


class WistiaClient:
    """Minimal Wistia client used by the AWS Glue Python ingestion job."""

    def __init__(
        self,
        token: str,
        api_version: str,
        timeout_seconds: int = 30,
        max_retries: int = 5,
    ) -> None:
        if not token:
            raise ValueError("Wistia token is required")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "X-Wistia-API-Version": api_version,
                "User-Agent": "wistia-video-analytics-glue/1.0",
            }
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> ApiResult:
        url = f"{BASE_URL}{path}"
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise WistiaApiError(
                        f"Wistia request failed after retries: {type(exc).__name__}"
                    ) from exc
                delay = min(2**attempt, 30)
                logger.warning(
                    "Network error calling %s; retrying in %ss (attempt %s/%s)",
                    path,
                    delay,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(delay)
                continue

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt >= self.max_retries:
                    raise WistiaApiError(
                        f"Wistia endpoint {path} returned HTTP {response.status_code} after retries"
                    )
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else min(2**attempt, 30)
                except ValueError:
                    delay = min(2**attempt, 30)
                logger.warning(
                    "HTTP %s from %s; retrying in %ss (attempt %s/%s)",
                    response.status_code,
                    path,
                    delay,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(delay)
                continue

            if not response.ok:
                # Do not log the response body; it may contain sensitive values.
                raise WistiaApiError(
                    f"Wistia endpoint {path} returned HTTP {response.status_code}"
                )

            try:
                return ApiResult(data=response.json(), status_code=response.status_code)
            except ValueError as exc:
                raise WistiaApiError(
                    f"Wistia endpoint {path} returned a non-JSON response"
                ) from exc

        raise WistiaApiError("Unexpected retry loop termination")

    def media_metadata(self, media_id: str) -> ApiResult:
        return self.get(f"/modern/medias/{media_id}")

    def media_stats(self, media_id: str) -> ApiResult:
        return self.get(f"/modern/stats/medias/{media_id}")

    def media_stats_by_date(self, media_id: str, start_date: str, end_date: str) -> ApiResult:
        return self.get(
            f"/modern/stats/medias/{media_id}/by_date",
            params={"start_date": start_date, "end_date": end_date},
        )

    def media_engagement(self, media_id: str) -> ApiResult:
        return self.get(f"/modern/stats/medias/{media_id}/engagement")

    def events(
        self,
        media_id: str,
        start_date: str,
        end_date: str,
        page: int,
        per_page: int,
    ) -> ApiResult:
        return self.get(
            "/modern/stats/events",
            params={
                "media_id": media_id,
                "start_date": start_date,
                "end_date": end_date,
                "page": page,
                "per_page": per_page,
            },
        )

    def visitor(self, visitor_key: str) -> ApiResult:
        return self.get(f"/modern/stats/visitors/{visitor_key}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wistia → S3 Bronze ingestion")
    parser.add_argument("--BUCKET_NAME", required=True)
    parser.add_argument("--SECRET_NAME", required=True)
    parser.add_argument("--MEDIA_IDS", required=True, help="Comma-separated media IDs")
    parser.add_argument("--API_VERSION", default="2026-07")
    parser.add_argument("--START_DATE", default=None, help="Optional YYYY-MM-DD override")
    parser.add_argument("--END_DATE", default=None, help="Optional YYYY-MM-DD override")
    parser.add_argument("--INITIAL_LOOKBACK_DAYS", type=int, default=90)
    parser.add_argument("--OVERLAP_DAYS", type=int, default=1)
    parser.add_argument("--PER_PAGE", type=int, default=100)
    parser.add_argument("--MAX_PAGES", type=int, default=10000)
    parser.add_argument("--FETCH_VISITORS", default="true")
    parser.add_argument("--BRONZE_PREFIX", default="bronze")
    parser.add_argument("--CONTROL_KEY", default="control/watermarks.json")
    parser.add_argument("--AUDIT_PREFIX", default="audit")
    # AWS Glue injects its own runtime arguments (for example --TempDir,
    # --scriptLocation, --python-version, and --enable-glue-datacatalog).
    # Parse only the application arguments defined above and ignore Glue-managed ones.
    args, _unknown = parser.parse_known_args(argv)
    return args


def parse_media_ids(raw: str) -> list[str]:
    media_ids = [item.strip() for item in raw.split(",") if item.strip()]
    if not media_ids:
        raise ValueError("At least one MEDIA_ID is required")
    return media_ids


def parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def parse_secret_string(secret_string: str) -> str:
    """Accept either a plain-token secret or a JSON secret."""
    candidate = secret_string.strip()
    if not candidate:
        raise ValueError("Secret is empty")

    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        return candidate

    if isinstance(parsed, str) and parsed.strip():
        return parsed.strip()
    if isinstance(parsed, dict):
        for key in ("WISTIA_API_TOKEN", "wistia_api_token", "token"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    raise ValueError(
        "Secret JSON must contain WISTIA_API_TOKEN, wistia_api_token, or token"
    )


def load_wistia_token(secret_name: str) -> str:
    import boto3

    client = boto3.client("secretsmanager")
    result = client.get_secret_value(SecretId=secret_name)
    secret_string = result.get("SecretString")
    if secret_string is None:
        raise ValueError("Binary Secrets Manager values are not supported by this job")
    return parse_secret_string(secret_string)


def get_s3_client():
    import boto3

    return boto3.client("s3")


def load_watermarks(s3_client: Any, bucket: str, key: str) -> dict[str, Any]:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:  # botocore isn't required for local unit tests
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404", "NoSuchBucket"}:
            if error_code == "NoSuchBucket":
                raise
            return {}
        # Head/get failures from a missing key can also surface as ClientError NoSuchKey.
        if exc.__class__.__name__ == "NoSuchKey":
            return {}
        raise

    body = obj["Body"].read().decode("utf-8")
    if not body.strip():
        return {}
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise TypeError("Watermark object must contain a JSON object")
    return parsed


def derive_date_window(
    media_id: str,
    watermarks: dict[str, Any],
    start_override: str | None,
    end_override: str | None,
    initial_lookback_days: int,
    overlap_days: int,
    today: date | None = None,
) -> tuple[str, str]:
    today = today or datetime.now(timezone.utc).date()
    end = date.fromisoformat(end_override) if end_override else today

    if start_override:
        start = date.fromisoformat(start_override)
    else:
        last_end_raw = (
            watermarks.get("events", {})
            .get(media_id, {})
            .get("last_successful_end_date")
        )
        if last_end_raw:
            last_end = date.fromisoformat(last_end_raw)
            # OVERLAP_DAYS=1 means re-read the last successful date.
            start = last_end - timedelta(days=max(overlap_days - 1, 0))
        else:
            start = end - timedelta(days=max(initial_lookback_days - 1, 0))

    if start > end:
        raise ValueError(f"START_DATE {start} cannot be after END_DATE {end}")
    return start.isoformat(), end.isoformat()


def s3_json_put(s3_client: Any, bucket: str, key: str, data: Any) -> None:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


def visitor_filename(visitor_key: str) -> str:
    # Avoid exposing the raw visitor key in an S3 object name or operational log.
    return hashlib.sha256(visitor_key.encode("utf-8")).hexdigest()[:24]


def bronze_key(
    bronze_prefix: str,
    dataset: str,
    ingestion_date: str,
    run_id: str,
    filename: str,
    media_id: str | None = None,
) -> str:
    parts = [
        bronze_prefix.strip("/"),
        dataset,
        f"ingestion_date={ingestion_date}",
        f"run_id={run_id}",
    ]
    if media_id:
        parts.append(f"media_id={media_id}")
    parts.append(filename)
    return "/".join(parts)


def iter_event_pages(
    client: WistiaClient,
    media_id: str,
    start_date: str,
    end_date: str,
    per_page: int,
    max_pages: int,
) -> Iterable[tuple[int, list[dict[str, Any]]]]:
    for page in range(1, max_pages + 1):
        result = client.events(media_id, start_date, end_date, page, per_page)
        if not isinstance(result.data, list):
            raise WistiaApiError("Events endpoint did not return an array")
        rows = result.data
        yield page, rows
        if len(rows) < per_page:
            return

    raise WistiaApiError(
        f"Pagination safety limit ({max_pages} pages) reached for media {media_id}; "
        "increase MAX_PAGES rather than silently truncating data"
    )


def endpoint_audit(
    media_id: str,
    endpoint: str,
    start_date: str | None,
    end_date: str | None,
    pages_fetched: int,
    records_fetched: int,
    bronze_object_count: int,
) -> dict[str, Any]:
    return {
        "media_id": media_id,
        "endpoint": endpoint,
        "start_date": start_date,
        "end_date": end_date,
        "pages_fetched": pages_fetched,
        "records_fetched": records_fetched,
        "bronze_object_count": bronze_object_count,
        "status": "SUCCESS",
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    media_ids = parse_media_ids(args.MEDIA_IDS)
    fetch_visitors = parse_bool(args.FETCH_VISITORS)

    run_id = str(uuid.uuid4())
    start_ts = datetime.now(timezone.utc)
    ingestion_date = start_ts.date().isoformat()
    logger.info("Starting Wistia Bronze ingestion run_id=%s", run_id)
    logger.info("Configured media count=%s", len(media_ids))

    s3_client = get_s3_client()
    token = load_wistia_token(args.SECRET_NAME)
    client = WistiaClient(token=token, api_version=args.API_VERSION)
    watermarks = load_watermarks(s3_client, args.BUCKET_NAME, args.CONTROL_KEY)

    audit_rows: list[dict[str, Any]] = []
    candidate_events_watermarks: dict[str, dict[str, str]] = {}
    total_objects = 0
    all_visitor_keys: set[str] = set()

    try:
        for media_id in media_ids:
            start_date, end_date = derive_date_window(
                media_id=media_id,
                watermarks=watermarks,
                start_override=args.START_DATE,
                end_override=args.END_DATE,
                initial_lookback_days=args.INITIAL_LOOKBACK_DAYS,
                overlap_days=args.OVERLAP_DAYS,
            )
            logger.info(
                "Ingesting media_id=%s window=%s..%s",
                media_id,
                start_date,
                end_date,
            )

            single_endpoints = [
                ("media", "media.json", client.media_metadata(media_id).data),
                ("media_stats", "media_stats.json", client.media_stats(media_id).data),
                (
                    "media_daily_stats",
                    "media_daily_stats.json",
                    client.media_stats_by_date(media_id, start_date, end_date).data,
                ),
                (
                    "engagement",
                    "engagement.json",
                    client.media_engagement(media_id).data,
                ),
            ]

            for dataset, filename, payload in single_endpoints:
                key = bronze_key(
                    args.BRONZE_PREFIX,
                    dataset,
                    ingestion_date,
                    run_id,
                    filename,
                    media_id=media_id,
                )
                s3_json_put(s3_client, args.BUCKET_NAME, key, payload)
                total_objects += 1
                record_count = len(payload) if isinstance(payload, list) else 1
                audit_rows.append(
                    endpoint_audit(
                        media_id,
                        dataset,
                        start_date if dataset == "media_daily_stats" else None,
                        end_date if dataset == "media_daily_stats" else None,
                        1,
                        record_count,
                        1,
                    )
                )

            event_pages = 0
            event_records = 0
            event_objects = 0
            for page, events in iter_event_pages(
                client,
                media_id,
                start_date,
                end_date,
                args.PER_PAGE,
                args.MAX_PAGES,
            ):
                event_pages += 1
                event_records += len(events)
                key = bronze_key(
                    args.BRONZE_PREFIX,
                    "events",
                    ingestion_date,
                    run_id,
                    f"page={page:06d}.json",
                    media_id=media_id,
                )
                s3_json_put(s3_client, args.BUCKET_NAME, key, events)
                total_objects += 1
                event_objects += 1

                for event in events:
                    visitor_key = event.get("visitor_key") if isinstance(event, dict) else None
                    if isinstance(visitor_key, str) and visitor_key:
                        all_visitor_keys.add(visitor_key)

            audit_rows.append(
                endpoint_audit(
                    media_id,
                    "events",
                    start_date,
                    end_date,
                    event_pages,
                    event_records,
                    event_objects,
                )
            )
            candidate_events_watermarks[media_id] = {
                "last_successful_end_date": end_date
            }

        visitor_success = 0
        if fetch_visitors:
            logger.info("Fetching %s distinct visitors discovered from events", len(all_visitor_keys))
            for visitor_key in sorted(all_visitor_keys):
                payload = client.visitor(visitor_key).data
                key = bronze_key(
                    args.BRONZE_PREFIX,
                    "visitors",
                    ingestion_date,
                    run_id,
                    f"visitor={visitor_filename(visitor_key)}.json",
                )
                s3_json_put(s3_client, args.BUCKET_NAME, key, payload)
                total_objects += 1
                visitor_success += 1

        end_ts = datetime.now(timezone.utc)
        summary = {
            "run_id": run_id,
            "pipeline_stage": "BRONZE_INGESTION",
            "status": "SUCCESS",
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "media_count": len(media_ids),
            "visitor_objects_written": visitor_success,
            "bronze_object_count": total_objects,
            "endpoint_audit": audit_rows,
            "candidate_watermark": {
                "events": candidate_events_watermarks,
                "run_id": run_id,
                "created_at": end_ts.isoformat(),
            },
            "note": (
                "Candidate watermark only. The production watermark must be committed "
                "after downstream Silver/Gold validation succeeds."
            ),
        }
        summary_key = "/".join(
            [
                args.AUDIT_PREFIX.strip("/"),
                f"ingestion_date={ingestion_date}",
                f"run_id={run_id}",
                "bronze_ingestion_summary.json",
            ]
        )
        s3_json_put(s3_client, args.BUCKET_NAME, summary_key, summary)
        logger.info(
            "Bronze ingestion completed successfully run_id=%s objects=%s visitors=%s",
            run_id,
            total_objects,
            visitor_success,
        )
        return 0

    except Exception as exc:
        end_ts = datetime.now(timezone.utc)
        # Avoid serializing exception bodies that could accidentally contain sensitive data.
        failure = {
            "run_id": run_id,
            "pipeline_stage": "BRONZE_INGESTION",
            "status": "FAILED",
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:500],
        }
        failure_key = "/".join(
            [
                args.AUDIT_PREFIX.strip("/"),
                f"ingestion_date={ingestion_date}",
                f"run_id={run_id}",
                "bronze_ingestion_failure.json",
            ]
        )
        try:
            s3_json_put(s3_client, args.BUCKET_NAME, failure_key, failure)
        except Exception:
            logger.exception("Could not write failure audit object")
        logger.exception("Bronze ingestion failed run_id=%s", run_id)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
