# Requirements Traceability

This matrix maps the assignment requirements to the implementation and the evidence that should be shown during submission/review.

| Requirement | Implementation | Primary evidence |
|---|---|---|
| FR1 — Design architecture for ingestion, processing, storage | AWS Glue + S3 Medallion + Glue Catalog + Athena | `docs/ARCHITECTURE.md`, Draw.io diagram |
| FR2 — Authenticate to Wistia API | Token read locally from ignored env / production from Secrets Manager | `jobs/bronze_ingestion.py`, Secrets Manager configuration |
| FR3 — Extract media metadata | Bronze `media/`; Silver `media`; Gold `dim_media` | S3 objects, Glue job logs, Athena query |
| FR4 — Extract engagement metrics | media stats, daily stats, engagement, media/day Gold fact | Bronze/Silver objects and `fact_media_engagement` |
| FR5 — Extract visitor-level data | paginated events + visitor endpoint; visitor/event Gold tables | `silver/events`, `silver/visitors`, `dim_visitor`, `fact_engagement_event` |
| FR6 — Pagination | event loop continues until terminal page; safety cap fails rather than silently truncates | `jobs/bronze_ingestion.py`, pagination exploration/test |
| FR7 — Incremental ingestion | watermark + overlap + idempotent deduplication | `docs/INCREMENTAL_STRATEGY.md`, `control/watermarks.json`, commit job |
| FR8 — Production mode for 7 consecutive days | daily Glue Workflow | `docs/SEVEN_DAY_PRODUCTION_EVIDENCE.md`, Glue Workflow history |
| FR9 — GitHub CI/CD | CI = Ruff/pytest; CD = OIDC -> S3 scripts | `.github/workflows/ci.yml`, `deploy.yml`, successful Actions runs |
| FR10 — Structured data model | Gold dimensional model in Parquet, Glue Catalog, Athena | Gold S3 paths, Catalog tables, Athena validation |
| FR11 — Reports/dashboards optional | Athena analytical queries provided; dashboard intentionally deferred | `sql/athena_validation_queries.sql`, assumptions/tradeoffs |
| FR12 — Repo/docs/code/CI-CD/instructions | repository includes code, architecture, setup, runbook, SQL, evidence template | README and documentation index |

## Evaluation criteria coverage

### Architecture

- Medallion layers are explicit.
- Ingestion and transformation runtimes are separated by workload type.
- Serverless/query-on-S3 design avoids unnecessary infrastructure.

### Data quality

- Full pagination.
- Incremental overlap and watermark.
- Explicit grains and natural keys.
- Schema normalization and type casting.
- Uniqueness and foreign-key checks in Gold.

### Engineering

- HTTP retries and failure handling.
- CloudWatch logs.
- S3 audit objects for success/failure.
- Watermark advances only after successful downstream processing.
- Rerunnable/idempotent curated transformations.

### CI/CD

- Ruff and pytest run automatically.
- Production scripts deploy through GitHub Actions.
- AWS authentication uses OIDC and temporary credentials.

### Documentation

- README.
- Architecture diagram/document.
- AWS setup notes.
- Incremental strategy.
- Data model.
- Assumptions/tradeoffs.
- Operations runbook.
- Seven-day evidence template.
- Walkthrough guide.
