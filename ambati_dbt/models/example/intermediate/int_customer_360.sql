{{
    config(
        materialized = 'view',
        description  = 'Customer 360 view joining customer master + credit bureau'
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

credit AS (
    SELECT * FROM {{ ref('stg_credit_bureau') }}
),

joined AS (
    SELECT
        -- ── Customer identity ──────────────────────────────
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        c.age,
        c.gender,
        c.nationality,
        c.marital_status,
        c.dependents,

        -- ── Contact ───────────────────────────────────────
        c.email,
        c.phone_primary,
        c.address_line1,
        c.city,
        c.state,
        c.zip_code,
        c.country,

        -- ── Financial profile ─────────────────────────────
        c.annual_income,
        c.income_group,
        c.employment_status,
        c.employer_name,
        c.job_title,
        c.years_employed,

        -- ── Account info ──────────────────────────────────
        c.account_open_date,
        c.account_type,
        c.account_status,
        c.branch_id,
        c.kyc_status,

        -- ── Credit bureau data ────────────────────────────
        cr.credit_score,
        cr.credit_band_derived          AS credit_band,
        cr.credit_utilization_pct,
        cr.total_debt,
        cr.total_credit_limit,
        cr.dti_ratio,
        cr.dti_category_derived         AS dti_category,
        cr.loan_risk_score,
        cr.risk_category_derived        AS risk_category,
        cr.bankruptcy_flag,
        cr.foreclosure_flag,
        cr.collections_count,
        cr.late_payments_30d,
        cr.late_payments_60d,
        cr.late_payments_90d,
        cr.open_accounts,
        cr.hard_inquiries_6m,
        cr.report_date                  AS bureau_report_date,
        cr.bureau_name,

        -- ── Has credit bureau data flag ───────────────────
        CASE WHEN cr.ssn IS NOT NULL
             THEN TRUE ELSE FALSE END   AS has_bureau_data

    FROM customers c
    LEFT JOIN credit cr ON c.ssn = cr.ssn
)

SELECT * FROM joined
