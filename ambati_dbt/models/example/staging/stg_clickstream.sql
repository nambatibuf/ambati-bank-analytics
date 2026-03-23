{{
    config(
        materialized = 'view',
        description  = 'Cleaned and typed clickstream events from raw'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ambati_ops', 'clickstream_raw') }}
),

cleaned AS (
    SELECT
        -- ── Identity ──────────────────────────────────────
        event_id,
        customer_id,
        timestamp,
        SAFE_CAST(record_created_at AS TIMESTAMP)       AS record_created_at,

        -- ── Session ───────────────────────────────────────
        session_id,
        SAFE_CAST(session_start       AS TIMESTAMP)     AS session_start,
        SAFE_CAST(session_duration_sec AS INT64)        AS session_duration_sec,
        CASE WHEN LOWER(is_new_session) = 'true'
             THEN TRUE ELSE FALSE END                   AS is_new_session,
        SAFE_CAST(session_page_count AS INT64)          AS session_page_count,

        -- ── Navigation ────────────────────────────────────
        INITCAP(channel)                                AS channel,
        LOWER(page)                                     AS page,
        LOWER(previous_page)                            AS previous_page,
        LOWER(action)                                   AS action,
        LOWER(element_clicked)                          AS element_clicked,
        LOWER(feature_used)                             AS feature_used,

        -- ── Device ────────────────────────────────────────
        device,
        INITCAP(device_type)                            AS device_type,
        os,
        browser,
        app_version,
        screen_resolution,
        UPPER(network_type)                             AS network_type,
        UPPER(ip_country)                               AS ip_country,

        -- ── Performance ───────────────────────────────────
        SAFE_CAST(page_load_time_ms AS INT64)           AS page_load_time_ms,
        SAFE_CAST(time_on_page_sec  AS INT64)           AS time_on_page_sec,
        SAFE_CAST(scroll_depth_pct  AS INT64)           AS scroll_depth_pct,
        SAFE_CAST(click_count       AS INT64)           AS click_count,
        CASE WHEN LOWER(error_encountered) = 'true'
             THEN TRUE ELSE FALSE END                   AS error_encountered,
        error_message,

        -- ── Metadata ─────────────────────────────────────
        UPPER(country)                                  AS country,
        language,
        utm_source,
        utm_campaign,
        CASE WHEN LOWER(is_authenticated) = 'true'
             THEN TRUE ELSE FALSE END                   AS is_authenticated

    FROM source
    WHERE event_id    IS NOT NULL
      AND customer_id IS NOT NULL
      AND timestamp   IS NOT NULL
)

SELECT * FROM cleaned
