{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='transaction_id',
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

with int_txn as (
    select * from {{ ref('int_transactions') }}

    {% if is_incremental() %}
        where transaction_date >= (
            select coalesce(max(transaction_date), cast('1900-01-01' as date)) - interval '7' day
            from {{ this }}
        )
    {% endif %}
),

int_fx as (
    select * from {{ ref('int_forex') }}
),

joined as (
    select
        t.transaction_id,
        t.account_id,
        t.amount,
        t.currency,
        t.transaction_date,

        coalesce(f.exchange_rate, 1.0) as usd_rate,
        round(t.amount / coalesce(f.exchange_rate, 1.0), 2) as amount_in_usd
    from int_txn t
    left join int_fx f
        on t.currency = f.target_currency
        and t.transaction_date = f.forex_date
),

final_fact as (
    select
        {{ dbt_utils.generate_surrogate_key(["'maybank'", 'account_id']) }} as account_key,

        transaction_id,
        account_id,
        amount,
        currency,
        transaction_date,
        usd_rate,
        amount_in_usd,

        {{ audit_columns('marts') }}
    from joined
)

select *
from final_fact