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
from pyspark.sql import types as T


logger = logging.getLogger("wistia_silver_transformation")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


MEDIA_SCHEMA = T.StructType(
    [
        T.StructField("archived", T.BooleanType(), True),
        T.StructField(
            "assets",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("content_type", T.StringType(), True),
                        T.StructField("file_size", T.LongType(), True),
                        T.StructField("height", T.LongType(), True),
                        T.StructField("type", T.StringType(), True),
                        T.StructField("url", T.StringType(), True),
                        T.StructField("width", T.LongType(), True),
                    ]
                )
            ),
            True,
        ),
        T.StructField("created", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("duration", T.DoubleType(), True),
        T.StructField(
            "folder",
            T.StructType(
                [
                    T.StructField("hashed_id", T.StringType(), True),
                    T.StructField("id", T.LongType(), True),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
            True,
        ),
        T.StructField("hashed_id", T.StringType(), True),
        T.StructField("id", T.LongType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("progress", T.DoubleType(), True),
        T.StructField("protected", T.BooleanType(), True),
        T.StructField("section", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField(
            "thumbnail",
            T.StructType(
                [
                    T.StructField("height", T.LongType(), True),
                    T.StructField("url", T.StringType(), True),
                    T.StructField("width", T.LongType(), True),
                ]
            ),
            True,
        ),
        T.StructField("type", T.StringType(), True),
        T.StructField("updated", T.StringType(), True),
    ]
)

MEDIA_STATS_SCHEMA = T.StructType(
    [
        T.StructField("engagement", T.DoubleType(), True),
        T.StructField("hours_watched", T.DoubleType(), True),
        T.StructField("load_count", T.LongType(), True),
        T.StructField("play_count", T.LongType(), True),
        T.StructField("play_rate", T.DoubleType(), True),
        T.StructField("visitors", T.LongType(), True),
    ]
)

MEDIA_DAILY_STATS_SCHEMA = T.StructType(
    [
        T.StructField("date", T.StringType(), True),
        T.StructField("hours_watched", T.DoubleType(), True),
        T.StructField("load_count", T.LongType(), True),
        T.StructField("play_count", T.LongType(), True),
    ]
)

ENGAGEMENT_SCHEMA = T.StructType(
    [
        T.StructField("engagement", T.DoubleType(), True),
        T.StructField("engagement_data", T.ArrayType(T.LongType()), True),
        T.StructField("rewatch_data", T.ArrayType(T.LongType()), True),
    ]
)

USER_AGENT_SCHEMA = T.StructType(
    [
        T.StructField("browser", T.StringType(), True),
        T.StructField("browser_version", T.StringType(), True),
        T.StructField("mobile", T.BooleanType(), True),
        T.StructField("platform", T.StringType(), True),
    ]
)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("city", T.StringType(), True),
        T.StructField("conversion_type", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("embed_url", T.StringType(), True),
        T.StructField("event_key", T.StringType(), True),
        T.StructField("iframe_heatmap_url", T.StringType(), True),
        T.StructField("ip", T.StringType(), True),
        T.StructField("lat", T.DoubleType(), True),
        T.StructField("lon", T.DoubleType(), True),
        T.StructField("media_id", T.StringType(), True),
        T.StructField("media_name", T.StringType(), True),
        T.StructField("media_url", T.StringType(), True),
        T.StructField("org", T.StringType(), True),
        T.StructField("percent_viewed", T.DoubleType(), True),
        T.StructField("received_at", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("user_agent_details", USER_AGENT_SCHEMA, True),
        T.StructField("visitor_key", T.StringType(), True),
    ]
)

VISITOR_SCHEMA = T.StructType(
    [
        T.StructField("created_at", T.StringType(), True),
        T.StructField("last_active_at", T.StringType(), True),
        T.StructField("last_event_key", T.StringType(), True),
        T.StructField("load_count", T.LongType(), True),
        T.StructField("play_count", T.LongType(), True),
        T.StructField("user_agent_details", USER_AGENT_SCHEMA, True),
        T.StructField(
            "visitor_identity",
            T.StructType(
                [
                    T.StructField("email", T.StringType(), True),
                    T.StructField("name", T.StringType(), True),
                    T.StructField(
                        "org",
                        T.StructType(
                            [
                                T.StructField("name", T.StringType(), True),
                                T.StructField("title", T.StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        T.StructField("visitor_key", T.StringType(), True),
    ]
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wistia Bronze → Silver PySpark transformation")
    parser.add_argument("--BUCKET_NAME", required=True)
    parser.add_argument("--BRONZE_PREFIX", default="bronze")
    parser.add_argument("--SILVER_PREFIX", default="silver")
    parser.add_argument("--AUDIT_PREFIX", default="audit")
    # Glue injects its own arguments. Ignore any that are not ours.
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


def add_lineage(df: DataFrame, audit_runs: DataFrame, has_media_in_path: bool) -> DataFrame:
    source_file = F.input_file_name()
    enriched = (
        df.withColumn("_source_file", source_file)
        .withColumn(
            "ingestion_date",
            F.to_date(F.regexp_extract(source_file, r"ingestion_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1)),
        )
        .withColumn("run_id", F.regexp_extract(source_file, r"run_id=([^/]+)", 1))
    )
    if has_media_in_path:
        enriched = enriched.withColumn(
            "media_id_from_path",
            F.regexp_extract(source_file, r"media_id=([^/]+)", 1),
        )
    return enriched.join(F.broadcast(audit_runs), on="run_id", how="inner")


def latest_by(df: DataFrame, keys: list[str], order_columns: list[F.Column]) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(*order_columns)
    return df.withColumn("_row_number", F.row_number().over(window)).filter(
        F.col("_row_number") == 1
    ).drop("_row_number")


def assert_no_null_key(df: DataFrame, column: str, dataset: str) -> None:
    if df.filter(F.col(column).isNull() | (F.trim(F.col(column)) == "")).limit(1).count() > 0:
        raise ValueError(f"Silver data-quality failure: {dataset}.{column} contains null/blank key values")


def assert_unique(df: DataFrame, keys: list[str], dataset: str) -> None:
    duplicate = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise ValueError(f"Silver data-quality failure: duplicate key detected in {dataset}: {keys}")


def write_parquet(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    silver_run_id = str(uuid.uuid4())
    start_ts = datetime.now(timezone.utc)
    s3 = boto3.client("s3")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    logger.info("Starting Silver transformation run_id=%s", silver_run_id)

    try:
        audit_glob = (
            f"s3://{args.BUCKET_NAME}/{args.AUDIT_PREFIX.strip('/')}"
            "/ingestion_date=*/run_id=*/bronze_ingestion_summary.json"
        )
        audit_raw = spark.read.option("multiLine", "true").json(audit_glob)
        audit_runs = (
            audit_raw.filter(F.col("status") == "SUCCESS")
            .select(
                F.col("run_id"),
                F.to_timestamp("start_ts").alias("ingested_at"),
            )
            .dropDuplicates(["run_id"])
        )
        successful_bronze_runs = audit_runs.count()
        if successful_bronze_runs == 0:
            raise ValueError("No successful Bronze ingestion summaries were found")

        counts: dict[str, int] = {}
        silver_base = f"s3://{args.BUCKET_NAME}/{args.SILVER_PREFIX.strip('/')}"
        bronze_base = f"s3://{args.BUCKET_NAME}/{args.BRONZE_PREFIX.strip('/')}"

        # MEDIA: one latest normalized row per Wistia media ID.
        media_raw = spark.read.schema(MEDIA_SCHEMA).option("multiLine", "true").json(
            f"{bronze_base}/media/ingestion_date=*/run_id=*/media_id=*/*.json"
        )
        media = add_lineage(media_raw, audit_runs, True).select(
            F.col("media_id_from_path").alias("media_id"),
            F.col("id").alias("wistia_numeric_id"),
            F.col("hashed_id"),
            F.col("name").alias("title"),
            F.col("description"),
            F.col("duration").cast("double").alias("duration_seconds"),
            F.col("type").alias("media_type"),
            F.col("status"),
            F.col("archived"),
            F.col("progress").cast("double"),
            F.to_timestamp("created").alias("created_at"),
            F.to_timestamp("updated").alias("updated_at"),
            F.col("folder.id").alias("folder_id"),
            F.col("folder.hashed_id").alias("folder_hashed_id"),
            F.col("folder.name").alias("folder_name"),
            F.col("thumbnail.url").alias("thumbnail_url"),
            F.col("thumbnail.width").alias("thumbnail_width"),
            F.col("thumbnail.height").alias("thumbnail_height"),
            F.size("assets").alias("asset_count"),
            F.to_json("assets").alias("assets_json"),
            F.col("ingestion_date"),
            F.col("run_id"),
            F.col("ingested_at"),
        )
        media = latest_by(
            media,
            ["media_id"],
            [F.col("updated_at").desc_nulls_last(), F.col("ingested_at").desc_nulls_last()],
        )
        assert_no_null_key(media, "media_id", "media")
        assert_unique(media, ["media_id"], "media")
        counts["media"] = media.count()
        write_parquet(media, f"{silver_base}/media")

        # MEDIA STATS: cumulative point-in-time snapshots. Preserve each successful run.
        stats_raw = spark.read.schema(MEDIA_STATS_SCHEMA).option("multiLine", "true").json(
            f"{bronze_base}/media_stats/ingestion_date=*/run_id=*/media_id=*/*.json"
        )
        media_stats = add_lineage(stats_raw, audit_runs, True).select(
            F.col("media_id_from_path").alias("media_id"),
            F.col("engagement").cast("double"),
            F.col("hours_watched").cast("double"),
            F.col("load_count").cast("long"),
            F.col("play_count").cast("long"),
            F.col("play_rate").cast("double"),
            F.col("visitors").cast("long"),
            F.col("ingested_at").alias("snapshot_at"),
            F.col("ingestion_date"),
            F.col("run_id"),
        ).dropDuplicates(["media_id", "run_id"])
        assert_no_null_key(media_stats, "media_id", "media_stats")
        assert_unique(media_stats, ["media_id", "run_id"], "media_stats")
        counts["media_stats"] = media_stats.count()
        write_parquet(media_stats, f"{silver_base}/media_stats", ["ingestion_date"])

        # DAILY STATS: latest observed value for each media/date; overlap reprocessing is deduplicated.
        daily_raw = spark.read.schema(MEDIA_DAILY_STATS_SCHEMA).option("multiLine", "true").json(
            f"{bronze_base}/media_daily_stats/ingestion_date=*/run_id=*/media_id=*/*.json"
        )
        media_daily = add_lineage(daily_raw, audit_runs, True).select(
            F.col("media_id_from_path").alias("media_id"),
            F.to_date("date").alias("stat_date"),
            F.col("hours_watched").cast("double"),
            F.col("load_count").cast("long"),
            F.col("play_count").cast("long"),
            F.col("ingested_at"),
            F.col("run_id"),
        )
        media_daily = latest_by(
            media_daily,
            ["media_id", "stat_date"],
            [F.col("ingested_at").desc_nulls_last()],
        ).withColumn("stat_year", F.year("stat_date")).withColumn("stat_month", F.month("stat_date"))
        assert_no_null_key(media_daily, "media_id", "media_daily_stats")
        if media_daily.filter(F.col("stat_date").isNull()).limit(1).count() > 0:
            raise ValueError("Silver data-quality failure: media_daily_stats.stat_date contains null values")
        assert_unique(media_daily, ["media_id", "stat_date"], "media_daily_stats")
        counts["media_daily_stats"] = media_daily.count()
        write_parquet(media_daily, f"{silver_base}/media_daily_stats", ["stat_year", "stat_month"])

        # ENGAGEMENT: preserve point-in-time engagement curve snapshots by media/run.
        engagement_raw = spark.read.schema(ENGAGEMENT_SCHEMA).option("multiLine", "true").json(
            f"{bronze_base}/engagement/ingestion_date=*/run_id=*/media_id=*/*.json"
        )
        engagement = add_lineage(engagement_raw, audit_runs, True).select(
            F.col("media_id_from_path").alias("media_id"),
            F.col("engagement").cast("double"),
            F.col("engagement_data"),
            F.col("rewatch_data"),
            F.col("ingested_at").alias("snapshot_at"),
            F.col("ingestion_date"),
            F.col("run_id"),
        ).dropDuplicates(["media_id", "run_id"])
        assert_no_null_key(engagement, "media_id", "engagement")
        assert_unique(engagement, ["media_id", "run_id"], "engagement")
        counts["engagement"] = engagement.count()
        write_parquet(engagement, f"{silver_base}/engagement", ["ingestion_date"])

        # EVENTS: flatten visitor, geography, user-agent and engagement fields; deduplicate overlap by event_key.
        events_raw = spark.read.schema(EVENT_SCHEMA).option("multiLine", "true").json(
            f"{bronze_base}/events/ingestion_date=*/run_id=*/media_id=*/page=*.json"
        )
        events = add_lineage(events_raw, audit_runs, True).select(
            F.col("event_key"),
            F.coalesce(F.col("media_id"), F.col("media_id_from_path")).alias("media_id"),
            F.col("visitor_key"),
            F.to_timestamp("received_at").alias("received_at"),
            F.col("percent_viewed").cast("double"),
            F.col("country"),
            F.col("region"),
            F.col("city"),
            F.col("lat").cast("double"),
            F.col("lon").cast("double"),
            F.col("ip").alias("ip_address"),
            F.col("email"),
            F.col("org"),
            F.col("embed_url"),
            F.col("media_name"),
            F.col("media_url"),
            F.col("conversion_type"),
            F.col("user_agent_details.browser").alias("browser"),
            F.col("user_agent_details.browser_version").alias("browser_version"),
            F.col("user_agent_details.mobile").alias("is_mobile"),
            F.col("user_agent_details.platform").alias("platform"),
            F.col("ingested_at"),
            F.col("run_id"),
        )
        events = latest_by(
            events,
            ["event_key"],
            [F.col("ingested_at").desc_nulls_last()],
        ).withColumn("event_date", F.to_date("received_at"))
        assert_no_null_key(events, "event_key", "events")
        assert_no_null_key(events, "media_id", "events")
        assert_unique(events, ["event_key"], "events")
        counts["events"] = events.count()
        write_parquet(events, f"{silver_base}/events", ["event_date"])

        # VISITORS: optional dataset. Only write if Bronze visitor objects exist.
        visitors_prefix = f"{args.BRONZE_PREFIX.strip('/')}/visitors/"
        if s3_prefix_exists(s3, args.BUCKET_NAME, visitors_prefix):
            visitors_raw = spark.read.schema(VISITOR_SCHEMA).option("multiLine", "true").json(
                f"{bronze_base}/visitors/ingestion_date=*/run_id=*/visitor=*.json"
            )
            visitors = add_lineage(visitors_raw, audit_runs, False).select(
                F.col("visitor_key"),
                F.to_timestamp("created_at").alias("created_at"),
                F.to_timestamp("last_active_at").alias("last_active_at"),
                F.col("last_event_key"),
                F.col("load_count").cast("long"),
                F.col("play_count").cast("long"),
                F.col("visitor_identity.email").alias("email"),
                F.col("visitor_identity.name").alias("visitor_name"),
                F.col("visitor_identity.org.name").alias("organization_name"),
                F.col("visitor_identity.org.title").alias("organization_title"),
                F.col("user_agent_details.browser").alias("browser"),
                F.col("user_agent_details.browser_version").alias("browser_version"),
                F.col("user_agent_details.mobile").alias("is_mobile"),
                F.col("user_agent_details.platform").alias("platform"),
                F.col("ingested_at"),
                F.col("run_id"),
            )
            visitors = latest_by(
                visitors,
                ["visitor_key"],
                [F.col("last_active_at").desc_nulls_last(), F.col("ingested_at").desc_nulls_last()],
            )
            assert_no_null_key(visitors, "visitor_key", "visitors")
            assert_unique(visitors, ["visitor_key"], "visitors")
            counts["visitors"] = visitors.count()
            write_parquet(visitors, f"{silver_base}/visitors")
        else:
            counts["visitors"] = 0
            logger.warning("No Bronze visitor objects found; Silver visitors dataset was not written")

        end_ts = datetime.now(timezone.utc)
        summary = {
            "run_id": silver_run_id,
            "pipeline_stage": "SILVER_TRANSFORMATION",
            "status": "SUCCESS",
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "successful_bronze_runs_processed": successful_bronze_runs,
            "row_counts": counts,
            "output_format": "parquet/snappy",
            "silver_prefix": args.SILVER_PREFIX,
            "note": "Silver contains normalized/deduplicated records from successful Bronze runs only.",
        }
        audit_key = (
            f"{args.AUDIT_PREFIX.strip('/')}/silver/"
            f"run_id={silver_run_id}/silver_transformation_summary.json"
        )
        write_json(s3, args.BUCKET_NAME, audit_key, summary)
        logger.info("Silver transformation completed successfully: %s", counts)
        return 0

    except Exception as exc:
        end_ts = datetime.now(timezone.utc)
        failure = {
            "run_id": silver_run_id,
            "pipeline_stage": "SILVER_TRANSFORMATION",
            "status": "FAILED",
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:1000],
        }
        failure_key = (
            f"{args.AUDIT_PREFIX.strip('/')}/silver/"
            f"run_id={silver_run_id}/silver_transformation_failure.json"
        )
        try:
            write_json(s3, args.BUCKET_NAME, failure_key, failure)
        except Exception:
            logger.exception("Could not write Silver failure audit object")
        logger.exception("Silver transformation failed run_id=%s", silver_run_id)
        raise


if __name__ == "__main__":
    main()
