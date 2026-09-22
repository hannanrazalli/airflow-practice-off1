{{ config(materialized='view') }}

with raw_transactions as (
    select * from {{ source('raw_sources', 'raw_transactions') }}

    where date(concat(year, '-', month, '-', day)) >= date_add('day', -7, current_date)
),

renamed_and_casted as (
    select
        cast(transaction_id as varchar) as transaction_id,
        cast(account_id as varchar) as account_id,
        cast(amount as double) as amount,
        cast(currency as varchar) as currency,
        cast(transaction_date as date) as transaction_date,

        {{ get_partition_date() }},
        {{ audit_columns('staging') }}
    from raw_transactions
    where transaction_id is not null
)

select *
from renamed_and_casted