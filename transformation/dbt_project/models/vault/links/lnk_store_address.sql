{{ config(
    materialized='incremental',
    unique_key='STORE_ADDRESS_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(SHA2(CAST(store_id AS STRING), 256) || '|' || SHA2(CAST(address_id AS STRING), 256), 256)  AS STORE_ADDRESS_HK,
    SHA2(CAST(store_id AS STRING), 256)  AS STORE_HK,
    SHA2(CAST(address_id AS STRING), 256)  AS ADDRESS_HK,
    CURRENT_TIMESTAMP()     AS LOAD_DATE,
    'silver.silver_store' AS RECORD_SOURCE,
    store_id,
    address_id
FROM {{ source('silver', 'silver_store') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
