{{
    config(
        materialized = 'view',
        description  = 'Cleaned credit bureau data from Equifax/Experian batch'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ambati_ops', 'credit_bureau_raw') }}
),

-- Latest report per SSN
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ssn
        ORDER BY batch_date DESC, report_date DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- ── Identity ──────────────────────────────────────
        ssn,
        customer_id,
        SAFE_CAST(report_date AS DATE)                  AS report_date,
        bureau_name,
        score_model,

        -- ── Credit score ──────────────────────────────────
        SAFE_CAST(credit_score AS INT64)                AS credit_score,
        credit_score_band,
        SAFE_CAST(credit_score_change AS INT64)         AS credit_score_change,

        -- ── Derived credit band ───────────────────────────
        CASE
            WHEN SAFE_CAST(credit_score AS INT64) >= 800 THEN 'Excellent'
            WHEN SAFE_CAST(credit_score AS INT64) >= 740 THEN 'Very Good'
            WHEN SAFE_CAST(credit_score AS INT64) >= 670 THEN 'Good'
            WHEN SAFE_CAST(credit_score AS INT64) >= 580 THEN 'Fair'
            ELSE 'Poor'
        END                                             AS credit_band_derived,

        -- ── Debt ─────────────────────────────────────────
        SAFE_CAST(total_debt            AS FLOAT64)     AS total_debt,
        SAFE_CAST(total_credit_limit    AS FLOAT64)     AS total_credit_limit,
        SAFE_CAST(credit_utilization_pct AS FLOAT64)    AS credit_utilization_pct,
        SAFE_CAST(revolving_debt        AS FLOAT64)     AS revolving_debt,
        SAFE_CAST(installment_debt      AS FLOAT64)     AS installment_debt,
        SAFE_CAST(mortgage_debt         AS FLOAT64)     AS mortgage_debt,
        SAFE_CAST(student_loan_debt     AS FLOAT64)     AS student_loan_debt,
        SAFE_CAST(auto_loan_debt        AS FLOAT64)     AS auto_loan_debt,

        -- ── Income & DTI ──────────────────────────────────
        SAFE_CAST(stated_income   AS FLOAT64)           AS stated_income,
        SAFE_CAST(verified_income AS FLOAT64)           AS verified_income,
        SAFE_CAST(dti_ratio       AS FLOAT64)           AS dti_ratio,

        -- ── Derived DTI category ──────────────────────────
        CASE
            WHEN SAFE_CAST(dti_ratio AS FLOAT64) > 400 THEN 'Severe'
            WHEN SAFE_CAST(dti_ratio AS FLOAT64) > 200 THEN 'High Risk'
            WHEN SAFE_CAST(dti_ratio AS FLOAT64) > 100 THEN 'Risky'
            WHEN SAFE_CAST(dti_ratio AS FLOAT64) > 50  THEN 'Manageable'
            ELSE 'Low Risk'
        END                                             AS dti_category_derived,

        -- ── Risk ─────────────────────────────────────────
        SAFE_CAST(loan_risk_score AS INT64)             AS loan_risk_score,
        risk_category,

        -- ── Derived risk category ─────────────────────────
        CASE
            WHEN SAFE_CAST(loan_risk_score AS INT64) >= 65 THEN 'High Risk'
            WHEN SAFE_CAST(loan_risk_score AS INT64) >= 50 THEN 'Moderate'
            ELSE 'Low Risk'
        END                                             AS risk_category_derived,

        CASE WHEN LOWER(bankruptcy_flag)   = 'true' THEN TRUE ELSE FALSE END AS bankruptcy_flag,
        SAFE_CAST(bankruptcy_date AS DATE)              AS bankruptcy_date,
        CASE WHEN LOWER(foreclosure_flag)  = 'true' THEN TRUE ELSE FALSE END AS foreclosure_flag,
        SAFE_CAST(collections_count   AS INT64)         AS collections_count,
        SAFE_CAST(delinquency_count   AS INT64)         AS delinquency_count,
        SAFE_CAST(late_payments_30d   AS INT64)         AS late_payments_30d,
        SAFE_CAST(late_payments_60d   AS INT64)         AS late_payments_60d,
        SAFE_CAST(late_payments_90d   AS INT64)         AS late_payments_90d,

        -- ── Accounts ─────────────────────────────────────
        SAFE_CAST(open_accounts         AS INT64)       AS open_accounts,
        SAFE_CAST(closed_accounts       AS INT64)       AS closed_accounts,
        SAFE_CAST(total_accounts        AS INT64)       AS total_accounts,
        SAFE_CAST(oldest_account_years  AS INT64)       AS oldest_account_years,
        SAFE_CAST(newest_account_months AS INT64)       AS newest_account_months,
        SAFE_CAST(hard_inquiries_6m     AS INT64)       AS hard_inquiries_6m,
        SAFE_CAST(hard_inquiries_12m    AS INT64)       AS hard_inquiries_12m,

        -- ── Metadata ─────────────────────────────────────
        batch_date,
        record_created_at,
        source_file

    FROM deduped
    WHERE ssn IS NOT NULL
)

SELECT * FROM cleaned
