{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

with source_products as (
    select
        id,
        product_name,
        weight,
        color,
        created_at,
        updated_at
    from {{ source('silver', 'silver_products') }}
),

prepared as (
    select
        id,
        product_name,
        weight,
        color,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        case
            when weight < 0.5 then 'light'
            when weight < 1.5 then 'medium'
            else 'heavy'
        end as weight_class
    from source_products
)

select *
from prepared
{% if is_incremental() %}
where updated_at >= (
    select coalesce(max(updated_at), cast('1900-01-01' as timestamp))
    from {{ this }}
)
{% endif %}
