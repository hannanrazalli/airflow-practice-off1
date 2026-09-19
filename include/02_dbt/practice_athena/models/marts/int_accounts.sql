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

final_int as (
    select
        account_id,
        customer_name,
        account_status,
        updated_at,
        partition_date,
        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select
    account_id,
    customer_name,
    account_status,
    updated_at,
    partition_date,
    _processed_at,
    _batch_id_int
from final_int