{{
    config(
        materialized = 'table',
        description  = "Customer dimension table - full 360 profile"
    )
}}

SELECT * FROM {{ ref('int_customer_360') }}
