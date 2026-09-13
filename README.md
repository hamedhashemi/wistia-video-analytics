# Wistia Video Analytics

End-to-end AWS data engineering project for Wistia media- and visitor-level analytics.

## Target architecture

Wistia API → AWS Glue Python ingestion → S3 Bronze (raw JSON) → AWS Glue PySpark → S3 Silver (clean Parquet) → AWS Glue PySpark → S3 Gold (analytics Parquet) → Glue Data Catalog → Athena.

Orchestration and operations: Glue Workflow/Scheduled Trigger, Secrets Manager, CloudWatch, GitHub Actions.

## Project status

- **Phase 1 — API Exploration — COMPLETE**: authentication, endpoint access, schema, date filtering, event/visitor fields, and pagination verified.
- **Phase 2 — Data Model & Contracts — COMPLETE**: grains and contracts derived from the actual Wistia response schema.
- **Phase 3 — Bronze Ingestion — COMPLETE**: Glue Python Shell ingestion writes media, stats, daily stats, engagement, events, visitors, and audit metadata to S3 Bronze.
- **Phase 4 — Silver Transformation — IN PROGRESS**: Glue PySpark normalizes, casts, deduplicates, validates, and writes Snappy Parquet.
- **Phase 5 — Gold Model**: dimensional/business model and final data-quality checks.
- **Phase 6 — Catalog / Athena / Orchestration / CI-CD**.
- **Phase 7 — Seven-Day Production Run**.
- **Phase 8 — Analytics, documentation, walkthrough**.

## Current AWS jobs

- `jobs/bronze_ingestion.py` — AWS Glue Python Shell
- `jobs/silver_transformation.py` — AWS Glue Spark/PySpark

## Continue

Follow `infrastructure/PHASE4_SILVER_SETUP.md`.

## Security

Never commit the Wistia API token. Local development uses an ignored `.env`; AWS execution uses Secrets Manager. Bronze/Silver may contain visitor PII and must remain private.

## Phase 5 — Gold layer

`jobs/gold_transformation.py` builds analytics-ready Parquet tables from Silver:

- `dim_media`
- `dim_visitor`
- `dim_date`
- `fact_media_engagement` — one row per media/date
- `fact_engagement_event` — one row per Wistia event

See `infrastructure/PHASE5_GOLD_SETUP.md` for AWS Glue setup.


## Current project status

- Phase 1 — API exploration: complete
- Phase 2 — Data model/contracts: complete
- Phase 3 — Bronze ingestion: complete
- Phase 4 — Silver PySpark transformation: complete
- Phase 5 — Gold analytical model: complete
- Phase 6 — Glue Catalog + Athena validation: complete
- Phase 7 — Automation, seven-day production evidence, and CI/CD: in progress

Phase 7 files:

- `jobs/commit_watermark.py`
- `infrastructure/PHASE7_AUTOMATION_CICD.md`
- `docs/SEVEN_DAY_PRODUCTION_EVIDENCE.md`
- `.github/workflows/deploy.yml`
