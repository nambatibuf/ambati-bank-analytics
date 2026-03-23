{{
    config(
        materialized = 'table',
        description  = "Daily transaction summary for operational dashboards"
    )
}}

WITH fact AS (
    SELECT * FROM {{ ref('fct_transactions') }}
)

SELECT
    transaction_date,
    country,
    card_brand,
    merchant_category,
    channel,
    status,

    -- ── Volume metrics ────────────────────────────────────
    COUNT(*)                                            AS total_transactions,
    COUNT(DISTINCT customer_id)                         AS unique_customers,
    COUNT(DISTINCT merchant_id)                         AS unique_merchants,

    -- ── Amount metrics ────────────────────────────────────
    SUM(amount_usd)                                     AS total_amount_usd,
    AVG(amount_usd)                                     AS avg_amount_usd,
    MIN(amount_usd)                                     AS min_amount_usd,
    MAX(amount_usd)                                     AS max_amount_usd,

    -- ── Status metrics ────────────────────────────────────
    COUNTIF(status = 'pass')                            AS passed_count,
    COUNTIF(status = 'fail')                            AS failed_count,
    ROUND(COUNTIF(status = 'pass') / COUNT(*) * 100, 2) AS pass_rate_pct,

    -- ── Risk metrics ─────────────────────────────────────
    COUNTIF(is_flagged = TRUE)                          AS flagged_count,
    AVG(risk_score)                                     AS avg_risk_score,
    COUNTIF(night_transaction = TRUE)                   AS night_txn_count,
    COUNTIF(is_international = TRUE)                    AS intl_txn_count,

    -- ── Channel metrics ───────────────────────────────────
    COUNTIF(chip_used = TRUE)                           AS chip_used_count,
    COUNTIF(contactless = TRUE)                         AS contactless_count,
    COUNTIF(is_online = TRUE)                           AS online_count

FROM fact
GROUP BY
    transaction_date,
    country,
    card_brand,
    merchant_category,
    channel,
    status
