# Seven-Day Production Run Evidence

Pipeline: Wistia Video Analytics

Production chain:

`Wistia API -> Bronze -> Silver -> Gold -> Watermark Commit`

## Pre-production validation

An end-to-end workflow run has been manually validated successfully, including Bronze, Silver, Gold, and watermark commit. GitHub CI and OIDC-based CD have also been validated successfully.

This pre-production validation does **not** replace the requirement for seven consecutive scheduled production days.

## Scheduled production evidence

| Day | Date | Workflow Run ID | Bronze | Silver | Gold | Watermark | Notes / Evidence |
|---|---|---|---|---|---|---|---|
| 1 | | | | | | | |
| 2 | | | | | | | |
| 3 | | | | | | | |
| 4 | | | | | | | |
| 5 | | | | | | | |
| 6 | | | | | | | |
| 7 | | | | | | | |

## Evidence to retain per day

- Glue Workflow run screenshot or run ID.
- Bronze job run ID/status and Bronze audit summary.
- Silver job run ID/status and Silver audit summary.
- Gold job run ID/status and Gold audit summary.
- Watermark commit job run ID/status and control audit summary.
- `control/watermarks.json` updated after successful completion.
- Optional Athena max-date/count validation.

## Failure documentation

If a scheduled day fails, record:

- failed stage
- error category/message
- root cause
- fix
- retry run ID/status
- confirmation that the watermark did not incorrectly advance before downstream success

Do not delete failed-run evidence; it demonstrates operating/debugging the production pipeline.

## Completion criteria

Mark the assignment's seven-day production requirement complete only when seven consecutive scheduled production days are documented according to the agreed interpretation of the requirement.
