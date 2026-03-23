{{
    config(
        materialized  = 'incremental',
        unique_key    = 'transaction_id',
        on_schema_change = 'append_new_columns',
        partition_by  = {
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by    = ["country", "card_brand", "status"],
        incremental_strategy = 'merge',
        description   = "Incremental transactions fact table - only processes new rows each run"
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}

    {% if is_incremental() %}
        -- Only pick up rows newer than the latest timestamp already in the table
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
        -- ── Transaction core ──────────────────────────────
        t.transaction_id,
        t.customer_id,
        t.timestamp,
        DATE(t.timestamp)                               AS transaction_date,
        EXTRACT(HOUR FROM t.timestamp)                  AS transaction_hour,
        FORMAT_DATE('%A', DATE(t.timestamp))            AS transaction_day_of_week,
        t.transaction_type,
        t.channel,

        -- ── Card ─────────────────────────────────────────
        t.card_brand,
        t.card_type,
        t.card_last_four,
        t.pos_entry_mode,
        t.chip_used,
        t.contactless,
        t.auth_method,

        -- ── Merchant ─────────────────────────────────────
        t.merchant_id,
        t.merchant_name,
        t.merchant_category,
        t.merchant_city,
        t.merchant_country,

        -- ── Amount ───────────────────────────────────────
        t.amount,
        t.amount_usd,
        t.currency,
        t.cashback_amount,
        t.fee_amount,
        t.tax_amount,

        -- ── Status ───────────────────────────────────────
        t.status,
        t.decline_reason,
        t.response_code,
        t.is_international,
        t.is_online,

        -- ── Risk ─────────────────────────────────────────
        t.is_flagged,
        t.risk_score,
        t.velocity_flag,
        t.unusual_location,
        t.night_transaction,

        -- ── Geography ────────────────────────────────────
        t.country,

        -- ── Customer enrichment ───────────────────────────
        c.full_name                                     AS customer_name,
        c.age                                           AS customer_age,
        c.gender                                        AS customer_gender,
        c.income_group,
        c.employment_status,
        c.account_type,
        c.account_status,

        -- ── Credit enrichment ─────────────────────────────
        c.credit_score,
        c.credit_band,
        c.dti_ratio,
        c.dti_category,
        c.loan_risk_score,
        c.risk_category,
        c.total_debt,
        c.credit_utilization_pct,
        c.has_bureau_data,

        -- ── System ───────────────────────────────────────
        t.source_system,
        t.bank_branch_id,
        t.record_created_at

    FROM transactions t
    LEFT JOIN customers c ON t.customer_id = c.customer_id
)

SELECT * FROM enriched
