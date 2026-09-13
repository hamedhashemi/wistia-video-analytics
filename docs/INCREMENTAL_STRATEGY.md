# Incremental Ingestion Strategy

## Objective

Meet the project's incremental-ingestion requirement while keeping the pipeline restartable and idempotent.

## 1. Media metadata

- Natural key: `hashed_id` / configured media ID.
- Change field: `updated`.
- Strategy: fetch each configured media on every daily run (only two media IDs in the project), then upsert/overwrite the Silver record by `media_id`.
- Reason: the API request volume is negligible and this avoids unnecessary state complexity.

## 2. Daily media stats

- Natural key: (`media_id`, `stat_date`).
- Strategy: request a bounded date window and overwrite matching date partitions in Silver.
- Recommended production window: re-read the most recent 2 days on each run to protect against late-arriving adjustments, then deduplicate by (`media_id`, `stat_date`).
- For the first run: use the historical date range approved for the project.

## 3. Engagement events

- Natural key: `event_key`.
- Pagination: continue requesting pages until the response contains fewer than `per_page` records (or zero records).
- Date filtering: use `start_date` and `end_date`.
- Watermark: store `last_successful_end_date` in an S3 control JSON file.
- Restart safety: do not advance the watermark until Bronze write and downstream validation complete successfully.
- Idempotency: Silver deduplicates on `event_key`.
- Recommended overlap: re-read at least the previous day and deduplicate to protect against delayed event availability.

## 4. Visitors

- Discover visitor keys from newly ingested engagement events.
- Fetch distinct visitor keys only.
- Natural key: `visitor_key`.
- Upsert/overwrite Silver by `visitor_key`; `last_active_at` can change over time.

## 5. Watermark state

For this small project, keep state in S3 instead of adding DynamoDB.

Suggested object:

`control/watermarks.json`

Example structure:

```json
{
  "events": {
    "8hunphufxp": {"last_successful_end_date": "YYYY-MM-DD"},
    "9k4tbcdfg0": {"last_successful_end_date": "YYYY-MM-DD"}
  },
  "last_pipeline_run_id": "...",
  "updated_at": "..."
}
```

## 6. Audit metadata

Each run should record:

- `run_id`
- `start_ts`
- `end_ts`
- `status`
- `media_id`
- `endpoint`
- `start_date`
- `end_date`
- `pages_fetched`
- `records_fetched`
- `bronze_object_count`
- `error_message`

This provides evidence for the required seven-day production run.
