{{ config(materialized='view') }}

with raw_forex as (
    select * from {{ source('raw_sources', 'raw_forex') }}
),

flattened as (
    select
        cast(item.date as date) as forex_date,
        cast(item.base as varchar) as base_currency,
        cast(item.quote as varchar) as target_currency,
        cast(item.rate as double) as exchange_rate,

        {{ get_partition_date() }} as partition_date,
        {{ audit_columns('staging') }}
    from raw_forex
    cross join unnest(array) as t(item)
)

select *
from flattened