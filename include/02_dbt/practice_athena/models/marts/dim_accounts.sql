{{ config(materialized='table') }}

with snap_account as (
    select * from {{ ref('snap_accounts') }}
    where dbt_valid_to is null
),

final_dim as (
    select
        {{ dbt_utils.generate_surrogate_key(["'maybank'", 'account_id']) }} as account_key,

        account_id,
        customer_name,
        account_status,
        updated_at,

        {{ audit_columns('marts') }}
    from snap_account
)

select *
from final_dim