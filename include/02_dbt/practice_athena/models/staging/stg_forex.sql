{{ config(materialized='view') }}

WITH raw_forex AS (
    SELECT *
    FROM {{ source('raw_sources', 'raw_forex') }}
),

flattened AS (
    SELECT
        cast(date_parse(cast(item.date as varchar), '%Y-%m-%d') as date) as forex_date,
        cast(item.base as varchar) as base_currency,
        cast(item.quote as varchar) as target_currency,
        cast(item.rate as varchar) as exchange_rate,

        {{ get_partition_date() }} as partition_date,

        {{ audit_columns('staging') }}
    FROM raw_forex
    CROSS JOIN UNNEST(array) as t(item)
)

SELECT *
FROM flattened