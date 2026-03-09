{{ config(
    materialized='incremental',
    unique_key='id'
) }}


SELECT
    id,
    product,
    price,
    created_at,
    last_inserted_dt,
    last_updated_dt
FROM {{ source('silver','silver_orders') }}
WHERE product IS NOT NULL