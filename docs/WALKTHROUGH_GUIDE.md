# Recorded Walkthrough Guide

Suggested length: 8–12 minutes.

## 1. Business objective — 30–45 seconds

Explain that the pipeline collects Wistia media- and visitor-level engagement data and creates an automated analytics layer for marketing analysis.

## 2. Architecture — 1–2 minutes

Show the Draw.io diagram and explain:

- Glue Python Shell for API ingestion.
- S3 Bronze/Silver/Gold Medallion layers.
- Glue PySpark for transformations.
- Glue Catalog + Athena for SQL.
- Glue Workflow for daily automation.
- Secrets Manager and CloudWatch.
- GitHub CI/CD.

Call out why Redshift/Step Functions/DynamoDB were not necessary at the current scale.

## 3. Bronze ingestion — 1–2 minutes

Show:

- `jobs/bronze_ingestion.py`.
- pagination loop.
- incremental date/watermark logic.
- S3 Bronze prefixes.
- a Bronze audit summary.

Do not display the API token or visitor PII in the recording.

## 4. Silver and Gold — 2 minutes

Show:

- PySpark transformations.
- natural-key deduplication.
- Gold tables and their grains.
- data-quality checks for null keys, uniqueness, and media foreign keys.

Emphasize why media/day and visitor/event facts are separated.

## 5. Athena validation — 1 minute

Run or show successful results for:

- `SHOW TABLES`.
- a media/day analytics query.
- duplicate check returning no rows.
- orphan media FK count returning zero.

## 6. Automation and incremental safety — 1 minute

Show the Glue Workflow graph:

```text
Bronze -> Silver -> Gold -> Watermark
```

Show `control/watermarks.json` and explain that it advances only after Gold succeeds.

## 7. GitHub CI/CD — 1 minute

Show successful GitHub Actions runs:

- CI: Ruff + pytest.
- CD: GitHub OIDC -> AWS role -> S3 script deployment.

Explain that no long-lived AWS access key is stored in GitHub.

## 8. Seven-day production evidence — 30 seconds

Show the evidence table and Glue workflow history. State clearly how many consecutive scheduled days are complete at the time of submission.

## Closing

Summarize the key engineering properties:

- automated
- incremental
- paginated
- auditable
- rerunnable
- dimensional
- SQL-queryable
- CI/CD-managed
