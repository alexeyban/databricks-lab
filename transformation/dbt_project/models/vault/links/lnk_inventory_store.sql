{{ config(
    materialized='incremental',
    unique_key='INVENTORY_STORE_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(SHA2(CAST(inventory_id AS STRING), 256) || '|' || SHA2(CAST(store_id AS STRING), 256), 256)  AS INVENTORY_STORE_HK,
    SHA2(CAST(inventory_id AS STRING), 256)  AS INVENTORY_HK,
    SHA2(CAST(store_id AS STRING), 256)  AS STORE_HK,
    CURRENT_TIMESTAMP()     AS LOAD_DATE,
    'silver.silver_inventory' AS RECORD_SOURCE,
    inventory_id,
    store_id
FROM {{ source('silver', 'silver_inventory') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
