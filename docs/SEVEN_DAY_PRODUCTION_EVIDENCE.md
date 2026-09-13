# Seven-Day Production Run Evidence

Pipeline: Wistia Video Analytics

Production chain:

`Wistia API -> Bronze -> Silver -> Gold -> Watermark Commit`

| Day | Date | Bronze | Silver | Gold | Watermark | Workflow / Run Notes |
|---|---|---|---|---|---|---|
| 1 | | | | | | |
| 2 | | | | | | |
| 3 | | | | | | |
| 4 | | | | | | |
| 5 | | | | | | |
| 6 | | | | | | |
| 7 | | | | | | |

## Evidence to retain per day

- Glue workflow run screenshot or run ID
- Bronze job run ID and audit summary
- Silver job run ID and audit summary
- Gold job run ID and audit summary
- Watermark commit audit summary
- `control/watermarks.json` after completion

## Completion criteria

The seven-day requirement is complete when seven consecutive scheduled production runs have completed successfully, or when any failed day is documented together with the fix/retry and the final successful production evidence required by the project rubric.
