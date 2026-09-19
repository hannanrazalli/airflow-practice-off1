{{ config(materialized='table') }}

with stg_txn as (
    SELECT * FROM {{ ref('stg_txn') }}
),

stg_forex as (
    SELECT * FROM {{ ref('stg_forex') }}
)

joined as (
    SELECT
        t.transaction_id,
        t.account_id,
        t.transaction_type,
        t.currency as original_currency,
        t.amount as original_amount,
        
        coalesce(f.exchange_rate, 1.0) as exchange_rate_to_usd,
        
        t.partition_date,

        {{ audit_columns('marts') }}
    FROM stg_txn t
    LEFT JOIN stg_forex f
        ON t.currency = f.target_currency
        AND t.partition_date = f.partition_date
)