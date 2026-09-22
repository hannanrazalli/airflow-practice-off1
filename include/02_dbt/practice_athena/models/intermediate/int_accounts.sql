{{ config(materialized='view') }}

with stg_accounts as (
    select * from {{ ref('stg_accounts') }}
),

deduplicate as (
    select
        *,
        row_number() over(
            partition by account_id
            order by updated_at desc, partition_date desc
        ) as rn
    from stg_accounts
),

cleaned as (
    select
        account_id,
        customer_name,
        account_status,
        updated_at,

        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select *
from cleaned