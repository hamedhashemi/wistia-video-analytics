# Phase 4 — Bronze to Silver with AWS Glue PySpark

## Goal
Normalize the successful Bronze JSON objects into deduplicated, typed Silver Parquet datasets using AWS Glue PySpark.

## Job
- Name: `wistia-silver-transformation`
- Engine: Spark / PySpark
- Glue version: use the current Glue 5.x runtime available in the account
- Worker type: `G.1X`
- Number of workers: `2` for this project-sized workload
- IAM role: `AWSGlueServiceRole-WistiaVideoAnalytics`
- Script location: `s3://wistia-video-analytics-hh-2026/scripts/silver_transformation.py`
- Job bookmarks: disabled (the project uses explicit Bronze lineage + deduplication)
- Max concurrency: 1

## Job parameters

| Key | Value |
|---|---|
| `--BUCKET_NAME` | `wistia-video-analytics-hh-2026` |
| `--BRONZE_PREFIX` | `bronze` |
| `--SILVER_PREFIX` | `silver` |
| `--AUDIT_PREFIX` | `audit` |

AWS Glue injects additional runtime arguments; the script intentionally ignores unknown Glue-managed arguments.

## What the job does
1. Reads only Bronze runs whose `bronze_ingestion_summary.json` has `status=SUCCESS`.
2. Parses `ingestion_date`, `run_id`, and media ID from the S3 object path.
3. Casts timestamps and numeric types.
4. Flattens nested user-agent, folder, thumbnail, identity, and organization fields.
5. Deduplicates overlap/re-runs:
   - media: latest row per `media_id`
   - daily stats: latest row per `(media_id, stat_date)`
   - events: latest row per `event_key`
   - visitors: latest row per `visitor_key`
6. Preserves cumulative media stats and engagement curves as point-in-time snapshots by successful Bronze run.
7. Writes Snappy-compressed Parquet to the Silver layer.
8. Performs key/null and uniqueness data-quality checks.
9. Writes a Silver audit summary.

## Silver outputs

```text
silver/
├── media/
├── media_stats/
├── media_daily_stats/
├── engagement/
├── events/
└── visitors/
```

Partitioning:
- `media_stats`: `ingestion_date`
- `engagement`: `ingestion_date`
- `media_daily_stats`: `stat_year/stat_month`
- `events`: `event_date`

## Validation after the run
Check that the Glue run status is `Succeeded`, then verify these prefixes exist in S3. Also open:

```text
audit/silver/run_id=<uuid>/silver_transformation_summary.json
```

Expected:

```json
{
  "pipeline_stage": "SILVER_TRANSFORMATION",
  "status": "SUCCESS"
}
```

The summary also includes row counts for each Silver dataset.

## PII note
The Bronze layer is raw and contains Wistia visitor data such as IP/email when present. Silver currently retains these fields because the project requirement explicitly includes visitor-level/IP data. Do not log these values, expose the bucket publicly, or include them in screenshots/repositories. The Gold layer can minimize or hash PII based on the final reporting requirement.
