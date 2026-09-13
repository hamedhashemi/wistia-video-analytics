-- Wistia Video Analytics - Phase 6 Athena validation queries
-- Database: wistia_analytics

USE wistia_analytics;

-- 1) Confirm Gold tables were cataloged
SHOW TABLES;

-- 2) Row counts across all Gold tables
SELECT 'dim_media' AS table_name, COUNT(*) AS row_count FROM dim_media
UNION ALL
SELECT 'dim_visitor', COUNT(*) FROM dim_visitor
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_media_engagement', COUNT(*) FROM fact_media_engagement
UNION ALL
SELECT 'fact_engagement_event', COUNT(*) FROM fact_engagement_event
ORDER BY table_name;

-- 3) Media dimension sanity check
SELECT
    media_id,
    title,
    duration_seconds,
    media_type,
    status,
    created_at,
    updated_at
FROM dim_media
ORDER BY media_id;

-- 4) Daily media engagement, enriched with media title
SELECT
    f.stat_date,
    f.media_id,
    m.title,
    f.load_count,
    f.play_count,
    ROUND(f.play_rate * 100, 2) AS play_rate_pct,
    ROUND(f.total_watch_time_hours, 3) AS total_watch_time_hours,
    f.event_count,
    f.unique_visitors,
    ROUND(f.watched_percent, 2) AS avg_watched_percent
FROM fact_media_engagement f
LEFT JOIN dim_media m
    ON f.media_id = m.media_id
ORDER BY f.stat_date, m.title;

-- 5) Event-level engagement by country
SELECT
    COALESCE(country, 'UNKNOWN') AS country,
    COUNT(*) AS event_count,
    COUNT(DISTINCT visitor_key) AS unique_visitors,
    ROUND(AVG(percent_viewed), 2) AS avg_percent_viewed
FROM fact_engagement_event
GROUP BY 1
ORDER BY event_count DESC;

-- 6) Media foreign-key validation: expected result is 0
SELECT COUNT(*) AS orphan_media_rows
FROM fact_media_engagement f
LEFT JOIN dim_media d
    ON f.media_id = d.media_id
WHERE d.media_id IS NULL;

-- 7) Event foreign-key validation: expected result is 0
SELECT COUNT(*) AS orphan_event_media_rows
FROM fact_engagement_event f
LEFT JOIN dim_media d
    ON f.media_id = d.media_id
WHERE d.media_id IS NULL;

-- 8) Duplicate validation for daily fact: expected result is zero rows
SELECT media_id, stat_date, COUNT(*) AS duplicate_count
FROM fact_media_engagement
GROUP BY media_id, stat_date
HAVING COUNT(*) > 1;

-- 9) Duplicate validation for events: expected result is zero rows
SELECT event_key, COUNT(*) AS duplicate_count
FROM fact_engagement_event
GROUP BY event_key
HAVING COUNT(*) > 1;

-- 10) Date dimension relationship check
SELECT
    f.stat_date,
    f.date_key,
    d.full_date,
    d.day_name,
    d.week_of_year
FROM fact_media_engagement f
LEFT JOIN dim_date d
    ON f.date_key = d.date_key
ORDER BY f.stat_date
LIMIT 50;

-- Optional: inspect registered partitions after the crawler runs
SHOW PARTITIONS fact_media_engagement;
SHOW PARTITIONS fact_engagement_event;
