{{ config(materialized='view') }}

with raw_accounts as (
    SELECT *
    FROM {{ source('raw_sources', 'raw_accounts') }}
),


renamed_and_casted as (
    select
        cast(account_id as integer) as account_id,
        cast(customer_name as varchar) as customer_name,
        cast(account_status as varchar) as account_status,
        cast(updated_at as timestamp) as updated_at,

        {{ get_partition_date() }} as partition_date,
        {{ audit_columns('staging') }}
    from raw_accounts
    where account_id is not null
)

select *
from renamed_and_casted