{{ config(materialized='view') }}

with raw_transactions as (
    select * from {{ source('raw_sources', 'raw_transactions') }}
),

renamed_and_casted as (
    select
        cast(transaction_id as integer) as transaction_id,
        cast(account_id as integer) as account_id,
        cast(amount as double) as amount,
        cast(currency as varchar) as currency,
        cast(transaction_date as timestamp) as transaction_date,

        {{ get_partition_date() }} as partition_date,
        {{ audit_columns('staging') }}
    from raw_transactions
)

select *
from renamed_and_casted