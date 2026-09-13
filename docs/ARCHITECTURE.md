# Architecture v2

## Coach feedback incorporated

- AWS Glue is used for API ingestion instead of adding Lambda.
- Bronze, Silver, and Gold Medallion layers are explicit.

## Proposed flow

1. **AWS Glue Python Shell ingestion job** calls Wistia APIs using Bearer authentication, handles pagination, retries, and incremental date windows.
2. **Bronze / S3** stores raw JSON responses with minimal alteration.
3. **AWS Glue PySpark Silver job** flattens, types, cleans, and deduplicates records.
4. **Silver / S3** stores clean Parquet datasets such as media, visitors, and engagement events.
5. **AWS Glue PySpark Gold job** applies business logic and creates the dimensional analytics model.
6. **Gold / S3** stores Parquet tables such as `dim_media`, `dim_visitor`, and `fact_media_engagement`.
7. **Glue Data Catalog + Athena** provide structured table metadata and SQL access.
8. **Glue Workflow + Scheduled Trigger** orchestrate the daily pipeline for the required seven-day production run.
9. **Secrets Manager** stores the Wistia token; **CloudWatch** stores operational logs and run evidence; **GitHub Actions** provides CI/CD.

## Intentionally deferred

Redshift, QuickSight, Step Functions, DynamoDB, and EMR are not included unless a confirmed requirement or actual scale/operational need justifies them.
