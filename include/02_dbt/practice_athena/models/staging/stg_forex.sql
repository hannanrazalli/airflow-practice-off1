{{ config(materialized='view') }}

with raw_forex as (
    select * from {{ source('raw_sources', 'raw_forex') }}
),

casted as (
    select
        cast(forex_date as date) as forex_date,
        cast(base_currency as varchar) as base_currency,
        cast(target_currency as varchar) as target_currency,
        cast(exchange_rate as double) as exchange_rate,

        {{ get_partition_date() }},
        {{ audit_columns('staging') }}
    from raw_forex
)

select *
from casted