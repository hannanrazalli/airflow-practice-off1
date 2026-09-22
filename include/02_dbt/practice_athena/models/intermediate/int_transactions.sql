{{ config(materialized='view') }}

with stg_transactions as (
    select * from {{ ref('stg_txn') }}
),

deduplicate as (
    select
        *,
        row_number() over(
            partition by transaction_id
            order by transaction_date desc, partition_date desc
        ) as rn
    from stg_transactions
),

cleaned as (
    select
        transaction_id,
        account_id,
        amount,
        currency,
        transaction_date,

        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select *
from cleaned