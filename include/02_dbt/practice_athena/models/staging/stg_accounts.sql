{{ config(materialized='view') }}

WITH raw_accounts AS (
    SELECT *
    FROM {{ source('raw_sources', 'raw_accounts') }}
),

renamed_and_casted AS (
    SELECT
        cast(account_id as varchar) as account_id,
        cast(customer_name as varchar) as customer_name,
        cast(account_status as varchar) as account_status,
        cast(account_tier as varchar) as account_tier,
        cast(credit_limit as double) as credit_limit,
        cast(updated_at as timestamp) as updated_at,

        {{ get_partition_date() }} as partition_date,

        cast(current_timestamp as timestamp) as _staged_at,
        '{{ invocation_id }}' as _batch_id_stage
    FROM raw_accounts
    WHERE account_id IS NOT NULL
)

SELECT *
FROM renamed_and_casted