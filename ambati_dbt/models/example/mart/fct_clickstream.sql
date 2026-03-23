{{
    config(
        materialized  = 'incremental',
        unique_key    = 'event_id',
        on_schema_change = 'append_new_columns',
        partition_by  = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by    = ["country", "channel", "device_type"],
        incremental_strategy = 'merge',
        description   = "Incremental clickstream fact table - only processes new events each run"
    )
}}

WITH clickstream AS (
    SELECT * FROM {{ ref('stg_clickstream') }}

    {% if is_incremental() %}
        WHERE timestamp > (
            SELECT MAX(timestamp) FROM {{ this }}
        )
    {% endif %}
),

customers AS (
    SELECT * FROM {{ ref('int_customer_360') }}
),

enriched AS (
    SELECT
        -- ── Event core ────────────────────────────────────
        cl.event_id,
        cl.customer_id,
        cl.timestamp,
        DATE(cl.timestamp)                              AS event_date,
        EXTRACT(HOUR FROM cl.timestamp)                 AS event_hour,
        cl.session_id,
        cl.session_start,
        cl.session_duration_sec,
        cl.is_new_session,
        cl.session_page_count,

        -- ── Navigation ────────────────────────────────────
        cl.channel,
        cl.page,
        cl.previous_page,
        cl.action,
        cl.element_clicked,
        cl.feature_used,

        -- ── Device ───────────────────────────────────────
        cl.device,
        cl.device_type,
        cl.os,
        cl.browser,
        cl.app_version,
        cl.network_type,

        -- ── Performance ───────────────────────────────────
        cl.page_load_time_ms,
        cl.time_on_page_sec,
        cl.scroll_depth_pct,
        cl.click_count,
        cl.error_encountered,
        cl.error_message,

        -- ── Geography + Marketing ─────────────────────────
        cl.country,
        cl.language,
        cl.utm_source,
        cl.utm_campaign,
        cl.is_authenticated,

        -- ── Customer enrichment ───────────────────────────
        c.full_name                                     AS customer_name,
        c.age                                           AS customer_age,
        c.gender                                        AS customer_gender,
        c.income_group,
        c.account_status,
        c.credit_band,
        c.risk_category

    FROM clickstream cl
    LEFT JOIN customers c ON cl.customer_id = c.customer_id
)

SELECT * FROM enriched
