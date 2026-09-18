{{ config(materialized='view') }}

WITH raw_transactions AS (
    SELECT *
    FROM {{ source('raw_sources', 'raw_transactions') }}
),

renamed_and_casted AS (
    SELECT
        cast(transaction_id as varchar) as transaction_id,
        cast(account_id as varchar) as account_id,
        cast(amount as double) as amount,
        cast(currency as varchar) as currency,
        cast(transaction_type as varchar) as transaction_type,

        {{ get_partition_date() }} as partition_date,

        cast(current_timestamp as timestamp) as _staged_at,
        '{{ invocation_id }}' as _batch_id_stage
    FROM raw_transactions
    WHERE transaction_id IS NOT NULL
)

SELECT *
FROM renamed_and_casted