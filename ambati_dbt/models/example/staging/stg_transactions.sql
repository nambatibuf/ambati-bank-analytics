{{
    config(
        materialized = 'view',
        description  = 'Cleaned and typed transaction events from raw'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ambati_ops', 'transactions_raw') }}
),

cleaned AS (
    SELECT
        -- ── Identity (already correct types) ──────────────
        transaction_id,
        customer_id,
        timestamp,
        SAFE_CAST(processing_date AS DATE)              AS processing_date,
        SAFE_CAST(record_created_at AS TIMESTAMP)       AS record_created_at,

        -- ── Card details ──────────────────────────────────
        UPPER(TRIM(card_brand))                         AS card_brand,
        INITCAP(TRIM(card_type))                        AS card_type,
        card_last_four,
        SAFE_CAST(card_expiry_month AS INT64)           AS card_expiry_month,
        SAFE_CAST(card_expiry_year  AS INT64)           AS card_expiry_year,
        card_network,
        LOWER(pos_entry_mode)                           AS pos_entry_mode,
        CASE WHEN LOWER(chip_used) = 'true'
             THEN TRUE ELSE FALSE END                   AS chip_used,
        CASE WHEN LOWER(contactless) = 'true'
             THEN TRUE ELSE FALSE END                   AS contactless,
        LOWER(auth_method)                              AS auth_method,

        -- ── Merchant details ──────────────────────────────
        merchant_id,
        INITCAP(TRIM(merchant_name))                    AS merchant_name,
        INITCAP(TRIM(merchant_category))                AS merchant_category,
        merchant_category_code,
        INITCAP(TRIM(merchant_city))                    AS merchant_city,
        merchant_state,
        UPPER(merchant_country)                         AS merchant_country,
        merchant_zip,

        -- ── Amount details ────────────────────────────────
        SAFE_CAST(amount          AS FLOAT64)           AS amount,
        SAFE_CAST(amount_usd      AS FLOAT64)           AS amount_usd,
        SAFE_CAST(cashback_amount AS FLOAT64)           AS cashback_amount,
        SAFE_CAST(fee_amount      AS FLOAT64)           AS fee_amount,
        SAFE_CAST(tax_amount      AS FLOAT64)           AS tax_amount,
        UPPER(currency)                                 AS currency,

        -- ── Status ───────────────────────────────────────
        LOWER(status)                                   AS status,
        LOWER(decline_reason)                           AS decline_reason,
        response_code,
        CASE WHEN LOWER(is_international) = 'true'
             THEN TRUE ELSE FALSE END                   AS is_international,
        CASE WHEN LOWER(is_online) = 'true'
             THEN TRUE ELSE FALSE END                   AS is_online,
        LOWER(channel)                                  AS channel,
        LOWER(transaction_type)                         AS transaction_type,

        -- ── Risk signals ──────────────────────────────────
        CASE WHEN LOWER(is_flagged) = 'true'
             THEN TRUE ELSE FALSE END                   AS is_flagged,
        SAFE_CAST(risk_score AS FLOAT64)                AS risk_score,
        CASE WHEN LOWER(velocity_flag) = 'true'
             THEN TRUE ELSE FALSE END                   AS velocity_flag,
        CASE WHEN LOWER(unusual_location) = 'true'
             THEN TRUE ELSE FALSE END                   AS unusual_location,
        CASE WHEN LOWER(night_transaction) = 'true'
             THEN TRUE ELSE FALSE END                   AS night_transaction,

        -- ── System metadata ───────────────────────────────
        UPPER(country)                                  AS country,
        source_system,
        bank_branch_id,
        terminal_id,
        acquirer_id,
        SAFE_CAST(batch_number    AS INT64)             AS batch_number,
        SAFE_CAST(sequence_number AS INT64)             AS sequence_number,

        -- ── Data quality flags ────────────────────────────
        CASE WHEN SAFE_CAST(amount AS FLOAT64) IS NULL
              AND amount IS NOT NULL
             THEN TRUE ELSE FALSE END                   AS amount_cast_failed,
        CASE WHEN SAFE_CAST(risk_score AS FLOAT64) IS NULL
              AND risk_score IS NOT NULL
             THEN TRUE ELSE FALSE END                   AS risk_score_cast_failed

    FROM source
    WHERE transaction_id IS NOT NULL
      AND customer_id    IS NOT NULL
      AND timestamp      IS NOT NULL
)

SELECT * FROM cleaned
