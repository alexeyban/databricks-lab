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
        product_legacy,
        cast(price as decimal(12,2)) as price,
        cast(created_at as timestamp) as created_at,
        cast(last_inserted_dt as timestamp) as last_inserted_dt,
        cast(last_updated_dt as timestamp) as last_updated_dt
    from {{ source('silver', 'silver_orders') }}
    where product_id is not null
),

source_products as (
    select
        id,
        product_name,
        cast(updated_at as timestamp) as product_updated_at
    from {{ source('silver', 'silver_products') }}
),

prepared as (
    select
        o.id,
        o.product_id,
        coalesce(p.product_name, o.product_legacy) as product_name,
        o.price,
        case
            when o.price < 20 then 'low'
            when o.price < 60 then 'medium'
            else 'high'
        end as price_band,
        o.created_at,
        cast(o.created_at as date) as order_date,
        o.last_inserted_dt,
        greatest(o.last_updated_dt, coalesce(p.product_updated_at, o.last_updated_dt)) as last_updated_dt
    from source_orders o
    left join source_products p
      on o.product_id = p.id
)

select *
from prepared
{% if is_incremental() %}
where last_updated_dt >= (
    select coalesce(max(last_updated_dt), cast('1900-01-01' as timestamp))
    from {{ this }}
)
{% endif %}
