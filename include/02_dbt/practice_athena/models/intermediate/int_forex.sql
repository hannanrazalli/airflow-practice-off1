{{ config(materialized='view') }}

with stg_forex as (
    select * from {{ ref('stg_forex') }}
),

deduplicate as (
    select
        *,
        row_number() over(
            partition by base_currency, target_currency, forex_date
            order by _staged_at desc, partition_date desc
        ) as rn
    from stg_forex
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['base_currency', 'target_currency', 'forex_date']) }} as forex_id,

        forex_date,
        base_currency,
        target_currency,
        exchange_rate,

        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select *
from cleaned