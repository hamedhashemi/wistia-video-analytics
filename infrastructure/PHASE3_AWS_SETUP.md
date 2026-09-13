# Phase 3 — AWS Foundation and First Bronze Ingestion

## Target for this phase

Create only the infrastructure needed to prove the first end-to-end Bronze load:

Wistia API → AWS Glue Python ingestion → Amazon S3 Bronze

Do **not** create Redshift, QuickSight, Step Functions, DynamoDB, or EMR.
Silver/Gold Spark jobs are added after Bronze ingestion is validated.

## Naming convention

Use one AWS Region for all resources. Example names below can be changed:

- S3 bucket: `wistia-video-analytics-<unique-suffix>`
- Secret: `wistia/video-analytics/api`
- Glue role: `AWSGlueServiceRole-WistiaVideoAnalytics`
- Glue job: `wistia-bronze-ingestion`

## Step 1 — S3

Create one private S3 bucket. Keep **Block all public access** enabled.
Enable default bucket encryption. S3-managed encryption (SSE-S3) is sufficient for this project.

Do not manually create folders unless you want them visible in the console; S3 prefixes are created automatically when the job writes objects.

Expected prefixes after the first run:

```text
bronze/media/
bronze/media_stats/
bronze/media_daily_stats/
bronze/engagement/
bronze/events/
bronze/visitors/
control/
audit/
scripts/
```

Upload `jobs/bronze_ingestion.py` to:

```text
s3://<bucket>/scripts/bronze_ingestion.py
```

## Step 2 — Secrets Manager

Create a secret named:

```text
wistia/video-analytics/api
```

Recommended secret value:

```json
{
  "WISTIA_API_TOKEN": "<current-token>"
}
```

Never put the token in Glue job arguments, GitHub, README files, screenshots, or CloudWatch logs.

## Step 3 — IAM role

Create an AWS Glue service role named:

```text
AWSGlueServiceRole-WistiaVideoAnalytics
```

Attach the AWS managed service-role policy used for Glue execution (`AWSGlueServiceRole`).
Then add a narrowly scoped inline policy based on:

```text
infrastructure/iam_glue_runtime_policy.template.json
```

Replace:

- `REPLACE_BUCKET_NAME` with the project bucket name.
- `REPLACE_SECRET_ARN` with the Secrets Manager secret ARN.

The role needs read/write access to the project S3 bucket and `secretsmanager:GetSecretValue` for the Wistia secret.

## Step 4 — Create the Glue Python ingestion job

Create a **Python Shell** Glue job using the latest supported Python 3 runtime available in your region/account.

Configure:

- Job name: `wistia-bronze-ingestion`
- IAM role: `AWSGlueServiceRole-WistiaVideoAnalytics`
- Script location: `s3://<bucket>/scripts/bronze_ingestion.py`
- Use the smallest Python-shell capacity suitable for the smoke test.
- Do not attach the job to a VPC for this project unless you have a specific networking reason; the job must reach the public Wistia API.

The script uses `requests`. If the selected Python Shell runtime/library set does not already provide it, add a pinned `requests` version using the job's additional Python modules setting.

### Job parameters

Add:

```text
--BUCKET_NAME        <bucket-name>
--SECRET_NAME        wistia/video-analytics/api
--MEDIA_IDS          8hunphufxp,9k4tbcdfg0
--API_VERSION        2026-07
--INITIAL_LOOKBACK_DAYS  7
--OVERLAP_DAYS       1
--PER_PAGE           100
--FETCH_VISITORS     true
```

For the first smoke test we intentionally use seven days. The initial production-history window remains configurable until the coach confirms the desired historical range.

## Step 5 — Run once manually

Run the job once on demand.

A successful run should create raw JSON under Bronze and an audit object similar to:

```text
audit/ingestion_date=YYYY-MM-DD/run_id=<uuid>/bronze_ingestion_summary.json
```

The event endpoint may legitimately produce zero rows for a media/date range. That is not a job failure.

## Step 6 — Validate before moving to Silver

Verify all of the following:

1. Glue job status is `Succeeded`.
2. CloudWatch has the job execution logs.
3. Bronze contains metadata/stats objects for both media IDs.
4. Event pages exist for both media IDs even when a page is an empty JSON array.
5. Visitor objects exist when events contained visitor keys.
6. `bronze_ingestion_summary.json` reports `SUCCESS`.
7. No API token is visible in logs or S3 object names.

Only after these checks should Phase 4/5 begin.

## Watermark behavior in this phase

The ingestion job **reads** `control/watermarks.json` if it exists, but deliberately does not advance the production watermark itself. It writes a `candidate_watermark` into the audit summary. Later, after Silver and Gold transformations plus data-quality checks succeed, the workflow will commit that candidate as the new production watermark. This prevents a failed downstream run from skipping source data on the next attempt.
