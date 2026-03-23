{{
    config(
        materialized = 'view',
        description  = 'Cleaned customer master data from batch'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ambati_ops', 'customers_raw') }}
),

-- Get latest record per customer (in case of duplicates across batches)
deduped AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY batch_date DESC, record_updated_at DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- ── Identity ──────────────────────────────────────
        customer_id,
        ssn,
        INITCAP(TRIM(full_name))                        AS full_name,
        INITCAP(TRIM(first_name))                       AS first_name,
        INITCAP(TRIM(last_name))                        AS last_name,
        SAFE_CAST(date_of_birth AS DATE)                AS date_of_birth,
        SAFE_CAST(age AS INT64)                         AS age,
        INITCAP(gender)                                 AS gender,
        UPPER(nationality)                              AS nationality,
        INITCAP(marital_status)                         AS marital_status,
        SAFE_CAST(dependents AS INT64)                  AS dependents,

        -- ── Contact ───────────────────────────────────────
        LOWER(email)                                    AS email,
        phone_primary,
        phone_secondary,

        -- ── Address ───────────────────────────────────────
        address_line1,
        address_line2,
        INITCAP(city)                                   AS city,
        state,
        zip_code,
        UPPER(country)                                  AS country,

        -- ── Financial ─────────────────────────────────────
        SAFE_CAST(annual_income AS FLOAT64)             AS annual_income,
        INITCAP(income_group)                           AS income_group,
        INITCAP(employment_status)                      AS employment_status,
        employer_name,
        job_title,
        SAFE_CAST(years_employed AS INT64)              AS years_employed,

        -- ── Account ───────────────────────────────────────
        SAFE_CAST(account_open_date AS DATE)            AS account_open_date,
        INITCAP(account_type)                           AS account_type,
        INITCAP(account_status)                         AS account_status,
        branch_id,
        relationship_manager,
        UPPER(kyc_status)                               AS kyc_status,
        SAFE_CAST(kyc_verified_date AS DATE)            AS kyc_verified_date,

        -- ── Metadata ─────────────────────────────────────
        batch_date,
        record_created_at,
        record_updated_at,
        source_file

    FROM deduped
    WHERE customer_id IS NOT NULL
      AND ssn         IS NOT NULL
)

SELECT * FROM cleaned
