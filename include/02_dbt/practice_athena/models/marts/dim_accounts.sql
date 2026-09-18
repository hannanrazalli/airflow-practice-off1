{{ config(materialized='table') }}

WITH stg_accounts AS (
    SELECT *
    FROM {{ ref('stg_accounts') }}
)

SELECT
    account_id,
    customer_name,
    account_status,
    account_tier,
    credit_limit,
    updated_at,
    partition_date,
    {{ audit_columns('marts') }}
FROM stg_accounts