# Phase 7 — Automation, Seven-Day Production Run, and CI/CD

## Glue Workflow

Workflow:

`wistia-video-analytics-workflow`

Production chain:

1. `wistia-daily-start` — scheduled start trigger
2. `wistia-bronze-ingestion`
3. `after-bronze-success` — requires Bronze `SUCCEEDED`
4. `wistia-silver-transformation`
5. `after-silver-success` — requires Silver `SUCCEEDED`
6. `wistia-gold-transformation`
7. `after-gold-success` — requires Gold `SUCCEEDED`
8. `wistia-commit-watermark`

Use Max concurrency = 1 for the production jobs.

## Watermark commit job

Script:

`s3://<bucket>/scripts/commit_watermark.py`

Recommended job type: Glue Python Shell 3.9.

Parameters:

- `--BUCKET_NAME`
- `--CONTROL_KEY=control/watermarks.json`
- `--AUDIT_PREFIX=audit`
- `--MAX_AGE_HOURS=48`

The Glue runtime role needs S3 read/write/delete/list access to the project bucket as required by the overwrite jobs and control/audit writes.

## CI

`.github/workflows/ci.yml` runs:

```text
ruff check .
pytest -q
```

on pull requests and pushes to `main`.

## CD

`.github/workflows/deploy.yml` deploys the four production scripts when `jobs/**` changes.

Repository variables:

- `AWS_REGION`
- `AWS_ROLE_TO_ASSUME`
- `S3_BUCKET`

The AWS role used by GitHub should have least-privilege permission to the S3 `scripts/*` prefix.

## GitHub OIDC

AWS identity provider:

```text
token.actions.githubusercontent.com
Audience: sts.amazonaws.com
```

The deployment role trust policy must match the **actual** OIDC subject (`sub`) emitted for the repository/branch. For newer GitHub repositories, the subject can include immutable owner/repository IDs, for example:

```text
repo:<owner>@<owner_id>/<repo>@<repo_id>:ref:refs/heads/main
```

Do not assume the older `repo:owner/repo:ref:...` form. If OIDC returns `Not authorized to perform sts:AssumeRoleWithWebIdentity`, inspect the token claims and align the IAM trust condition with the actual `sub`.

No long-lived AWS access key is required.

## Seven-day run

Keep the scheduled start trigger enabled. Record each scheduled workflow run in:

`docs/SEVEN_DAY_PRODUCTION_EVIDENCE.md`

The failed and retry runs should remain visible as operational evidence rather than being removed from the record.
