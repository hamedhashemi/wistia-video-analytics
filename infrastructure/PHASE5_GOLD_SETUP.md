# Phase 5 — Gold Transformation

Create a new AWS Glue **Spark / PySpark** job.

- Job name: `wistia-gold-transformation`
- IAM role: `AWSGlueServiceRole-WistiaVideoAnalytics`
- Script: `s3://wistia-video-analytics-hh-2026/scripts/gold_transformation.py`
- Glue version: 5.x or later
- Worker type: G.1X
- Workers: 2
- Bookmarks: disabled

Job parameters:

- `--BUCKET_NAME` = `wistia-video-analytics-hh-2026`
- `--SILVER_PREFIX` = `silver`
- `--GOLD_PREFIX` = `gold`
- `--AUDIT_PREFIX` = `audit`

Gold outputs:

- `gold/dim_media/`
- `gold/dim_visitor/`
- `gold/dim_date/`
- `gold/fact_media_engagement/`
- `gold/fact_engagement_event/`

The daily engagement fact has grain `(media_id, stat_date)`. Event detail remains at one row per `event_key`.
