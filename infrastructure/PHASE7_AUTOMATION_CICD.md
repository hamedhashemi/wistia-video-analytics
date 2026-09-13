# Phase 7 — Automation, 7-Day Production Run, and CI/CD

## Goal

Automate the production chain so each daily run executes only after the prior stage succeeds:

`Bronze ingestion -> Silver transformation -> Gold transformation -> Watermark commit`

The Glue crawler may run after Gold to refresh Data Catalog partitions.

## A. Create the watermark commit Glue job

Upload:

`s3://wistia-video-analytics-hh-2026/scripts/commit_watermark.py`

Create a **Python Shell 3.9** Glue job:

- Job name: `wistia-commit-watermark`
- IAM role: `AWSGlueServiceRole-WistiaVideoAnalytics`
- Script location: `s3://wistia-video-analytics-hh-2026/scripts/commit_watermark.py`

Job parameters:

- `--BUCKET_NAME` = `wistia-video-analytics-hh-2026`
- `--CONTROL_KEY` = `control/watermarks.json`
- `--AUDIT_PREFIX` = `audit`
- `--MAX_AGE_HOURS` = `48`

The existing Glue runtime IAM policy must allow `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, and `s3:ListBucket` for the project bucket.

## B. Glue Workflow

Create workflow:

`wistia-video-analytics-workflow`

Create these triggers in the workflow:

1. **Scheduled trigger** — `wistia-daily-start`
   - Schedule: daily
   - Recommended project schedule: `cron(0 10 * * ? *)` (10:00 UTC each day)
   - Action: start `wistia-bronze-ingestion`

2. **Conditional trigger** — `after-bronze-success`
   - Condition: `wistia-bronze-ingestion` = SUCCEEDED
   - Action: start `wistia-silver-transformation`

3. **Conditional trigger** — `after-silver-success`
   - Condition: `wistia-silver-transformation` = SUCCEEDED
   - Action: start `wistia-gold-transformation`

4. **Conditional trigger** — `after-gold-success`
   - Condition: `wistia-gold-transformation` = SUCCEEDED
   - Action: start `wistia-commit-watermark`

5. Optional: run `wistia-gold-crawler` after Gold succeeds so Athena sees new partitions.

Set each Glue job Max concurrency to 1.

## C. Production watermark behavior

The Bronze job writes only a candidate watermark to its audit summary.

The watermark commit job runs only after Gold succeeds, verifies that the latest successful Gold run is newer than the latest successful Bronze run, then writes:

`control/watermarks.json`

This prevents a failed downstream pipeline from advancing state and skipping data.

## D. Seven-day production run

Run the workflow once daily for seven consecutive days. Do not manually advance the watermark.

For each day retain evidence of:

- Workflow run status
- Bronze Glue run = SUCCEEDED
- Silver Glue run = SUCCEEDED
- Gold Glue run = SUCCEEDED
- Watermark commit = SUCCEEDED
- Bronze audit JSON
- Silver audit JSON
- Gold audit JSON
- Watermark commit audit JSON

Use `docs/SEVEN_DAY_PRODUCTION_EVIDENCE.md` to record the results.

## E. GitHub CI/CD

### CI

`.github/workflows/ci.yml` runs on pull requests and pushes to `main`:

- install dependencies
- Ruff
- pytest

### CD

`.github/workflows/deploy.yml` deploys the Glue scripts to S3 on changes to `jobs/**`.

Configure the following GitHub repository settings:

**Repository variables**

- `AWS_REGION` = `us-east-1`
- `S3_BUCKET` = `wistia-video-analytics-hh-2026`

**Repository secret**

- `AWS_ROLE_TO_ASSUME` = ARN of a GitHub OIDC deployment role

The deployment role only needs permission to upload objects under:

`s3://wistia-video-analytics-hh-2026/scripts/*`

Do not store the Wistia API token in GitHub. The runtime token remains in AWS Secrets Manager.
