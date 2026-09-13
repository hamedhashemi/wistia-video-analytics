# Phase 2 — Data Model Based on Actual Wistia API Schema

This model is based on the schema-only API exploration performed against the two project media IDs using Wistia API version `2026-07`.

## Key findings

1. Media metadata exposes `hashed_id`, integer `id`, `name`, `created`, `updated`, `duration`, `status`, folder data, assets, tags, and thumbnail data.
2. Aggregate media stats expose `engagement`, `hours_watched`, `load_count`, `play_count`, `play_rate`, and `visitors`.
3. Daily media stats expose only `date`, `hours_watched`, `load_count`, and `play_count` in the sampled response.
4. Event data is visitor/media-session-like data with a stable `event_key`, `visitor_key`, `media_id`, `received_at`, `percent_viewed`, geography, embed URL, conversion attributes, and user-agent attributes.
5. Visitor data exposes `visitor_key`, `created_at`, `last_active_at`, `load_count`, `play_count`, identity fields, organization fields, and user-agent fields.

## Important modeling decision

The simplified requirement model places media-level measures such as `play_rate` and `total_watch_time` in a fact table that also contains `visitor_id` and `date`. The actual API schema shows that those metrics are not all available at the same grain. To avoid mixing incompatible grains, the Gold layer uses separate facts.

## Silver layer

### silver_media
**Grain:** one row per Wistia media.

| Column | Type | Source |
|---|---|---|
| media_id | string | `hashed_id` |
| wistia_numeric_id | long | `id` |
| title | string | `name` |
| description | string | `description` |
| duration_seconds | double | `duration` |
| media_type | string | `type` |
| status | string | `status` |
| archived | boolean | `archived` |
| created_at | timestamp | `created` |
| updated_at | timestamp | `updated` |
| folder_id | long | `folder.id` |
| folder_hashed_id | string | `folder.hashed_id` |
| folder_name | string | `folder.name` |
| thumbnail_url | string | `thumbnail.url` |
| ingestion_ts | timestamp | pipeline metadata |

Notes: `assets` and `tags` are preserved in Bronze. They can be normalized later if a reporting requirement needs them.

### silver_media_daily_stats
**Grain:** one row per media per calendar date.

| Column | Type | Source |
|---|---|---|
| media_id | string | request context |
| stat_date | date | `date` |
| load_count | long | `load_count` |
| play_count | long | `play_count` |
| hours_watched | double | `hours_watched` |
| ingestion_ts | timestamp | pipeline metadata |

`hours_watched` is defined as `double` because one sampled media returned it as a JSON number while another returned an integer. A common numeric type avoids schema drift.

### silver_media_summary_stats
**Grain:** one row per media per extraction snapshot.

| Column | Type | Source |
|---|---|---|
| media_id | string | request context |
| engagement | double | `engagement` |
| hours_watched | double | `hours_watched` |
| load_count | long | `load_count` |
| play_count | long | `play_count` |
| play_rate | double | `play_rate` |
| visitors | long | `visitors` |
| snapshot_ts | timestamp | pipeline metadata |

### silver_engagement_event
**Grain:** one row per Wistia event (`event_key`).

| Column | Type | Source |
|---|---|---|
| event_key | string | `event_key` |
| media_id | string | `media_id` |
| visitor_key | string | `visitor_key` |
| received_at | timestamp | `received_at` |
| percent_viewed | double | `percent_viewed` |
| embed_url | string | `embed_url` |
| country | string | `country` |
| region | string | `region` |
| city | string | `city` |
| latitude | double | `lat` |
| longitude | double | `lon` |
| organization | string | `org` |
| conversion_type | string | `conversion_type` |
| browser | string | `user_agent_details.browser` |
| browser_version | string | `user_agent_details.browser_version` |
| platform | string | `user_agent_details.platform` |
| mobile | boolean | `user_agent_details.mobile` |
| ingestion_ts | timestamp | pipeline metadata |

PII such as raw IP and email should remain available only where required. The curated model should avoid exposing them unnecessarily.

### silver_visitor
**Grain:** one row per visitor (`visitor_key`).

| Column | Type | Source |
|---|---|---|
| visitor_key | string | `visitor_key` |
| created_at | timestamp | `created_at` |
| last_active_at | timestamp | `last_active_at` |
| last_event_key | string | `last_event_key` |
| load_count | long | `load_count` |
| play_count | long | `play_count` |
| visitor_name | string | `visitor_identity.name` |
| visitor_email | string | `visitor_identity.email` |
| org_name | string | `visitor_identity.org.name` |
| org_title | string | `visitor_identity.org.title` |
| browser | string | `user_agent_details.browser` |
| browser_version | string | `user_agent_details.browser_version` |
| platform | string | `user_agent_details.platform` |
| mobile | boolean | `user_agent_details.mobile` |
| ingestion_ts | timestamp | pipeline metadata |

## Gold layer

### dim_media
**Grain:** one row per media.

- `media_key` — surrogate key if needed; otherwise `media_id` can be used for this small project
- `media_id`
- `title`
- `duration_seconds`
- `media_type`
- `status`
- `created_at`
- `updated_at`
- `folder_name`
- `channel` — pending business clarification because the sampled API schema does not expose Facebook/YouTube as a media attribute

### dim_visitor
**Grain:** one row per visitor.

- `visitor_key`
- `created_at`
- `last_active_at`
- `country` (can be derived from most recent/known event if desired)
- device/browser attributes as needed
- raw IP/email should not be promoted to Gold unless the project explicitly requires them

### fact_media_daily
**Grain:** one row per media per date.

- `media_id`
- `stat_date`
- `load_count`
- `play_count`
- `hours_watched`
- `play_rate` — can be added only after confirming the exact business definition for daily derivation; do not invent it from the aggregate metric

### fact_engagement_event
**Grain:** one row per event.

- `event_key`
- `media_id`
- `visitor_key`
- `event_ts`
- `percent_viewed`
- conversion and device/geography attributes needed for analytics

### Optional aggregate: fact_media_snapshot
Useful only if the project needs historical snapshots of media-level aggregate KPIs.

## Why not a single fact_media_engagement table?

The sampled API proves that daily media statistics, media-level aggregate statistics, and visitor-level events are delivered at different grains. Keeping them separate prevents double counting and invalid joins. A single Gold fact can be added later only if a clearly defined aggregation rule is approved.
