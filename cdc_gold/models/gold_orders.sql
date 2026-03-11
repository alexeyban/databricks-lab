{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

with source_orders as (
    select
        id,
        product_id,
        product_name,
        cast(price as decimal(12,2)) as price,
        cast(created_at as timestamp) as created_at,
        cast(last_inserted_dt as timestamp) as last_inserted_dt,
        cast(last_updated_dt as timestamp) as last_updated_dt
    from {{ source('silver', 'silver_orders') }}
    where product_id is not null
),

prepared as (
    select
        id,
        product_id,
        product_name,
        price,
        case
            when price < 20 then 'low'
            when price < 60 then 'medium'
            else 'high'
        end as price_band,
        created_at,
        cast(created_at as date) as order_date,
        last_inserted_dt,
        last_updated_dt
    from source_orders
)

select *
from prepared
{% if is_incremental() %}
where last_updated_dt >= (
    select coalesce(max(last_updated_dt), cast('1900-01-01' as timestamp))
    from {{ this }}
)
{% endif %}
