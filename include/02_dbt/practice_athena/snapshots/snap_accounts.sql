{% snapshot snap_accounts %}

{{
    config(
        target_schema='practice_off_database',
        unique_key='account_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select * from {{ ref('int_accounts') }}

{% endsnapshot %}