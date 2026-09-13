from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger("wistia_gold_transformation")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wistia Silver → Gold PySpark transformation")
    parser.add_argument("--BUCKET_NAME", required=True)
    parser.add_argument("--SILVER_PREFIX", default="silver")
    parser.add_argument("--GOLD_PREFIX", default="gold")
    parser.add_argument("--AUDIT_PREFIX", default="audit")
    # AWS Glue injects its own runtime arguments; ignore arguments that are not ours.
    args, _unknown = parser.parse_known_args(argv)
    return args


def s3_prefix_exists(s3_client: Any, bucket: str, prefix: str) -> bool:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get("KeyCount", 0) > 0


def write_json(s3_client: Any, bucket: str, key: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


def write_parquet(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def assert_no_null_key(df: DataFrame, column: str, dataset: str) -> None:
    if df.filter(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")).limit(1).count() > 0:
        raise ValueError(f"Gold data-quality failure: {dataset}.{column} contains null/blank key values")


def assert_unique(df: DataFrame, keys: list[str], dataset: str) -> None:
    duplicate = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise ValueError(f"Gold data-quality failure: duplicate key detected in {dataset}: {keys}")


def assert_fk(fact: DataFrame, fact_col: str, dim: DataFrame, dim_col: str, dataset: str) -> None:
    missing = (
        fact.select(F.col(fact_col).alias("fk"))
        .filter(F.col("fk").isNotNull())
        .dropDuplicates()
        .join(dim.select(F.col(dim_col).alias("fk")).dropDuplicates(), on="fk", how="left_anti")
        .limit(1)
        .count()
    )
    if missing:
        raise ValueError(
            f"Gold data-quality failure: {dataset}.{fact_col} contains values missing from {dim_col} dimension"
        )


def latest_event_per_visitor(events: DataFrame) -> DataFrame:
    window = Window.partitionBy("visitor_key").orderBy(F.col("received_at").desc_nulls_last())
    return (
        events.filter(F.col("visitor_key").isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    run_id = str(uuid.uuid4())
    start_ts = datetime.now(timezone.utc)
    s3 = boto3.client("s3")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    silver_base = f"s3://{args.BUCKET_NAME}/{args.SILVER_PREFIX.strip('/')}"
    gold_base = f"s3://{args.BUCKET_NAME}/{args.GOLD_PREFIX.strip('/')}"
    counts: dict[str, int] = {}

    logger.info("Starting Gold transformation run_id=%s", run_id)

    try:
        media = spark.read.parquet(f"{silver_base}/media")
        daily = spark.read.parquet(f"{silver_base}/media_daily_stats")
        events = spark.read.parquet(f"{silver_base}/events")

        # -----------------------------
        # DIM_MEDIA: one row per media.
        # -----------------------------
        dim_media = media.select(
            "media_id",
            "wistia_numeric_id",
            "hashed_id",
            "title",
            "description",
            "duration_seconds",
            "media_type",
            "status",
            "archived",
            "created_at",
            "updated_at",
            "folder_name",
            "thumbnail_url",
        ).dropDuplicates(["media_id"])
        assert_no_null_key(dim_media, "media_id", "dim_media")
        assert_unique(dim_media, ["media_id"], "dim_media")
        counts["dim_media"] = dim_media.count()
        write_parquet(dim_media, f"{gold_base}/dim_media")

        # ----------------------------------------------------------
        # DIM_VISITOR: one row per visitor, enriched with latest geo.
        # PII is retained because the project explicitly requires
        # visitor-level analytics/IP. Restrict Gold access in IAM.
        # ----------------------------------------------------------
        latest_event = latest_event_per_visitor(events).select(
            "visitor_key",
            F.col("ip_address").alias("last_ip_address"),
            F.col("country").alias("last_country"),
            F.col("region").alias("last_region"),
            F.col("city").alias("last_city"),
            F.col("received_at").alias("last_event_at"),
        )

        visitors_prefix = f"{args.SILVER_PREFIX.strip('/')}/visitors/"
        if s3_prefix_exists(s3, args.BUCKET_NAME, visitors_prefix):
            visitors = spark.read.parquet(f"{silver_base}/visitors")
            dim_visitor = (
                visitors.select(
                    "visitor_key",
                    "created_at",
                    "last_active_at",
                    "load_count",
                    "play_count",
                    "email",
                    "visitor_name",
                    "organization_name",
                    "organization_title",
                    "browser",
                    "browser_version",
                    "is_mobile",
                    "platform",
                )
                .join(latest_event, on="visitor_key", how="left")
                .dropDuplicates(["visitor_key"])
            )
        else:
            dim_visitor = (
                latest_event.select(
                    "visitor_key",
                    "last_ip_address",
                    "last_country",
                    "last_region",
                    "last_city",
                    "last_event_at",
                )
                .withColumn("created_at", F.lit(None).cast("timestamp"))
                .withColumn("last_active_at", F.lit(None).cast("timestamp"))
                .withColumn("load_count", F.lit(None).cast("long"))
                .withColumn("play_count", F.lit(None).cast("long"))
                .withColumn("email", F.lit(None).cast("string"))
                .withColumn("visitor_name", F.lit(None).cast("string"))
                .withColumn("organization_name", F.lit(None).cast("string"))
                .withColumn("organization_title", F.lit(None).cast("string"))
                .withColumn("browser", F.lit(None).cast("string"))
                .withColumn("browser_version", F.lit(None).cast("string"))
                .withColumn("is_mobile", F.lit(None).cast("boolean"))
                .withColumn("platform", F.lit(None).cast("string"))
            )

        assert_no_null_key(dim_visitor, "visitor_key", "dim_visitor")
        assert_unique(dim_visitor, ["visitor_key"], "dim_visitor")
        counts["dim_visitor"] = dim_visitor.count()
        write_parquet(dim_visitor, f"{gold_base}/dim_visitor")

        # ---------------------------------
        # DIM_DATE: dates observed in facts.
        # ---------------------------------
        date_values = (
            daily.select(F.col("stat_date").alias("full_date"))
            .unionByName(events.select(F.col("event_date").alias("full_date")), allowMissingColumns=True)
            .filter(F.col("full_date").isNotNull())
            .dropDuplicates(["full_date"])
        )
        dim_date = date_values.select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
            "full_date",
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.dayofmonth("full_date").alias("day"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.weekofyear("full_date").alias("week_of_year"),
        )
        assert_unique(dim_date, ["date_key"], "dim_date")
        counts["dim_date"] = dim_date.count()
        write_parquet(dim_date, f"{gold_base}/dim_date")

        # -----------------------------------------------------------------
        # FACT_ENGAGEMENT_EVENT: one row per visitor-level engagement event.
        # -----------------------------------------------------------------
        fact_event = events.select(
            "event_key",
            "media_id",
            "visitor_key",
            F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
            "event_date",
            "received_at",
            "percent_viewed",
            "country",
            "region",
            "city",
            "ip_address",
            "conversion_type",
            "browser",
            "browser_version",
            "is_mobile",
            "platform",
        )
        assert_no_null_key(fact_event, "event_key", "fact_engagement_event")
        assert_no_null_key(fact_event, "media_id", "fact_engagement_event")
        assert_unique(fact_event, ["event_key"], "fact_engagement_event")
        assert_fk(fact_event, "media_id", dim_media, "media_id", "fact_engagement_event")
        counts["fact_engagement_event"] = fact_event.count()
        write_parquet(fact_event, f"{gold_base}/fact_engagement_event", ["event_date"])

        # --------------------------------------------------------------------
        # FACT_MEDIA_ENGAGEMENT: one row per media/date.
        # Combines daily media stats with event-derived visitor engagement.
        # play_rate is derived as play_count / load_count for a consistent grain.
        # watched_percent is the average event percent_viewed for that media/date.
        # --------------------------------------------------------------------
        event_daily = (
            events.groupBy("media_id", F.col("event_date").alias("stat_date"))
            .agg(
                F.count("event_key").alias("event_count"),
                F.countDistinct("visitor_key").alias("unique_visitors"),
                F.avg("percent_viewed").alias("watched_percent"),
            )
        )

        fact_media = (
            daily.select(
                "media_id",
                "stat_date",
                F.col("load_count").cast("long"),
                F.col("play_count").cast("long"),
                F.col("hours_watched").cast("double").alias("total_watch_time_hours"),
            )
            .join(event_daily, on=["media_id", "stat_date"], how="left")
            .withColumn(
                "play_rate",
                F.when(F.col("load_count") > 0, F.col("play_count") / F.col("load_count")).otherwise(F.lit(0.0)),
            )
            .withColumn("total_watch_time_seconds", F.col("total_watch_time_hours") * F.lit(3600.0))
            .withColumn("event_count", F.coalesce(F.col("event_count"), F.lit(0)).cast("long"))
            .withColumn("unique_visitors", F.coalesce(F.col("unique_visitors"), F.lit(0)).cast("long"))
            .withColumn("date_key", F.date_format("stat_date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("stat_date"))
            .withColumn("month", F.month("stat_date"))
        )
        assert_no_null_key(fact_media, "media_id", "fact_media_engagement")
        if fact_media.filter(F.col("stat_date").isNull()).limit(1).count() > 0:
            raise ValueError("Gold data-quality failure: fact_media_engagement.stat_date contains null values")
        assert_unique(fact_media, ["media_id", "stat_date"], "fact_media_engagement")
        assert_fk(fact_media, "media_id", dim_media, "media_id", "fact_media_engagement")
        counts["fact_media_engagement"] = fact_media.count()
        write_parquet(fact_media, f"{gold_base}/fact_media_engagement", ["year", "month"])

        end_ts = datetime.now(timezone.utc)
        summary = {
            "run_id": run_id,
            "pipeline_stage": "GOLD_TRANSFORMATION",
            "status": "SUCCESS",
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "row_counts": counts,
            "output_format": "parquet/snappy",
            "gold_prefix": args.GOLD_PREFIX,
            "tables": [
                "dim_media",
                "dim_visitor",
                "dim_date",
                "fact_media_engagement",
                "fact_engagement_event",
            ],
            "note": (
                "Gold uses a consistent media/date grain for fact_media_engagement and preserves "
                "visitor-level detail in fact_engagement_event."
            ),
        }
        audit_key = (
            f"{args.AUDIT_PREFIX.strip('/')}/gold/"
            f"run_id={run_id}/gold_transformation_summary.json"
        )
        write_json(s3, args.BUCKET_NAME, audit_key, summary)
        logger.info("Gold transformation completed successfully: %s", counts)
        return 0

    except Exception as exc:
        end_ts = datetime.now(timezone.utc)
        failure = {
            "run_id": run_id,
            "pipeline_stage": "GOLD_TRANSFORMATION",
            "status": "FAILED",
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:1000],
        }
        failure_key = (
            f"{args.AUDIT_PREFIX.strip('/')}/gold/"
            f"run_id={run_id}/gold_transformation_failure.json"
        )
        try:
            write_json(s3, args.BUCKET_NAME, failure_key, failure)
        except Exception:
            logger.exception("Could not write Gold failure audit object")
        logger.exception("Gold transformation failed run_id=%s", run_id)
        raise


if __name__ == "__main__":
    main()
