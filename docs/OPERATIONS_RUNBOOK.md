# Operations Runbook

## Daily production chain

```text
wistia-daily-start
  -> wistia-bronze-ingestion
  -> wistia-silver-transformation
  -> wistia-gold-transformation
  -> wistia-commit-watermark
```

Each conditional trigger should require the previous job to be `SUCCEEDED`.

## Normal daily verification

1. Open AWS Glue Workflow history.
2. Confirm the scheduled workflow started automatically.
3. Confirm Bronze, Silver, Gold, and Watermark jobs all succeeded.
4. Confirm a new Bronze audit summary exists under `audit/ingestion_date=...`.
5. Confirm new Silver and Gold audit summaries exist.
6. Confirm `control/watermarks.json` has a newer `updated_at` / successful end date.
7. Record the run IDs and status in `SEVEN_DAY_PRODUCTION_EVIDENCE.md`.
8. If desired, run a small Athena check such as a fact row count or max date.

## Failure handling

### Bronze fails

- Inspect CloudWatch error logs.
- Check Wistia authentication, endpoint response, network/retry error, and S3 permissions.
- Do not manually advance the watermark.
- Fix and rerun the workflow/job as appropriate.

### Silver fails

- Inspect PySpark exception and S3 permissions.
- Verify Bronze audit status is `SUCCESS`.
- Check schema/type assumptions and malformed records.
- The production watermark must remain unchanged.

### Gold fails

- Inspect data-quality exception first: null keys, duplicates, or missing media foreign keys.
- Verify Silver outputs are present and queryable.
- The watermark commit must not run.

### Watermark commit fails

- Confirm Bronze/Silver/Gold completed successfully.
- Check access to `control/watermarks.json` and audit objects.
- Rerun only after verifying the candidate state belongs to the successful pipeline window.

## Rerun behavior

Bronze is append-oriented and keyed by run ID/date. Silver and Gold are deterministic curated rewrites for the current small project scope. Deduplication protects against overlap and reruns.

## Deployment verification

After a GitHub CD run:

1. Confirm `Configure AWS credentials` succeeded.
2. Confirm each Glue script uploaded successfully.
3. Check S3 `scripts/` object `Last modified` timestamps if evidence is needed.
4. Do not place the Wistia token in GitHub variables or secrets; runtime token remains in AWS Secrets Manager.

## Seven-day acceptance

A day counts as production evidence when the scheduled chain is documented. If a scheduled run fails, record the failure and remediation rather than hiding it; retain the final successful retry evidence as well.
