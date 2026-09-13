from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger("wistia-watermark-commit")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Commit Wistia pipeline watermarks after Gold succeeds")
    parser.add_argument("--BUCKET_NAME", required=True)
    parser.add_argument("--CONTROL_KEY", default="control/watermarks.json")
    parser.add_argument("--AUDIT_PREFIX", default="audit")
    parser.add_argument("--MAX_AGE_HOURS", type=int, default=48)
    args, _unknown = parser.parse_known_args()
    return args


def read_json(s3: Any, bucket: str, key: str) -> dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def write_json(s3: Any, bucket: str, key: str, payload: dict[str, Any]) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=(json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        ContentType="application/json",
    )


def list_matching_keys(s3: Any, bucket: str, prefix: str, suffix: str) -> list[dict[str, Any]]:
    paginator = s3.get_paginator("list_objects_v2")
    matches: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith(suffix):
                matches.append(obj)
    return matches


def parse_iso(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def latest_successful_summary(
    s3: Any,
    bucket: str,
    prefix: str,
    suffix: str,
) -> tuple[str, dict[str, Any]]:
    objects = list_matching_keys(s3, bucket, prefix, suffix)
    if not objects:
        raise RuntimeError(f"No audit summaries found under s3://{bucket}/{prefix}")

    objects.sort(key=lambda obj: obj["LastModified"], reverse=True)
    for obj in objects:
        key = obj["Key"]
        payload = read_json(s3, bucket, key)
        if payload.get("status") == "SUCCESS":
            return key, payload
    raise RuntimeError(f"No successful audit summary found under s3://{bucket}/{prefix}")


def merge_watermarks(
    current: dict[str, Any],
    candidate: dict[str, Any],
    bronze_run_id: str,
    now_iso: str,
) -> dict[str, Any]:
    merged = dict(current or {})
    merged_events = dict(merged.get("events", {}))
    for media_id, state in candidate.get("events", {}).items():
        if not isinstance(state, dict) or not state.get("last_successful_end_date"):
            raise ValueError(f"Invalid candidate watermark for media_id={media_id}")
        merged_events[media_id] = {
            "last_successful_end_date": state["last_successful_end_date"]
        }
    merged["events"] = merged_events
    merged["last_pipeline_run_id"] = bronze_run_id
    merged["updated_at"] = now_iso
    return merged


def load_current_control(s3: Any, bucket: str, key: str) -> dict[str, Any]:
    try:
        return read_json(s3, bucket, key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            return {}
        raise


def main() -> None:
    args = parse_args()
    s3 = boto3.client("s3")
    audit_prefix = args.AUDIT_PREFIX.strip("/")

    bronze_key, bronze = latest_successful_summary(
        s3,
        args.BUCKET_NAME,
        f"{audit_prefix}/ingestion_date=",
        "bronze_ingestion_summary.json",
    )
    gold_key, gold = latest_successful_summary(
        s3,
        args.BUCKET_NAME,
        f"{audit_prefix}/gold/",
        "gold_transformation_summary.json",
    )

    bronze_end = parse_iso(bronze["end_ts"])
    gold_end = parse_iso(gold["end_ts"])
    if gold_end < bronze_end:
        raise RuntimeError(
            "Latest successful Gold audit is older than the latest successful Bronze audit; "
            "refusing to advance the watermark."
        )

    now = datetime.now(timezone.utc)
    age_hours = (now - bronze_end).total_seconds() / 3600
    if age_hours > args.MAX_AGE_HOURS:
        raise RuntimeError(
            f"Latest successful Bronze run is {age_hours:.1f} hours old, exceeding "
            f"MAX_AGE_HOURS={args.MAX_AGE_HOURS}."
        )

    candidate = bronze.get("candidate_watermark")
    if not isinstance(candidate, dict) or not candidate.get("events"):
        raise RuntimeError("Latest Bronze audit has no candidate event watermark")

    current = load_current_control(s3, args.BUCKET_NAME, args.CONTROL_KEY)
    now_iso = now.isoformat()
    merged = merge_watermarks(current, candidate, bronze["run_id"], now_iso)
    write_json(s3, args.BUCKET_NAME, args.CONTROL_KEY, merged)

    commit_summary = {
        "pipeline_stage": "WATERMARK_COMMIT",
        "status": "SUCCESS",
        "committed_at": now_iso,
        "bronze_run_id": bronze["run_id"],
        "bronze_audit_key": bronze_key,
        "gold_run_id": gold["run_id"],
        "gold_audit_key": gold_key,
        "control_key": args.CONTROL_KEY,
        "events": merged.get("events", {}),
    }
    audit_key = (
        f"{audit_prefix}/control/commit_date={now.date().isoformat()}/"
        f"bronze_run_id={bronze['run_id']}/watermark_commit_summary.json"
    )
    write_json(s3, args.BUCKET_NAME, audit_key, commit_summary)
    LOGGER.info("Watermark committed successfully to s3://%s/%s", args.BUCKET_NAME, args.CONTROL_KEY)


if __name__ == "__main__":
    main()
