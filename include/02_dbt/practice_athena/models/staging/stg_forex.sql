{{ config(materialized='view') }}

with raw_forex as (
    select * from {{ source('raw_sources', 'raw_forex') }}
),

casted as (
    select
        cast(date as date) as forex_date,
        cast(base as varchar) as base_currency,
        cast(currency as varchar) as target_currency,
        cast(rate as double) as exchange_rate,

        {{ get_partition_date() }},
        {{ audit_columns('staging') }}
    from raw_forex
)

select *
from casted