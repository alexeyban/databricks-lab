{{ config(
    materialized='incremental',
    unique_key='ADDRESS_CITY_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(SHA2(CAST(address_id AS STRING), 256) || '|' || SHA2(CAST(city_id AS STRING), 256), 256)  AS ADDRESS_CITY_HK,
    SHA2(CAST(address_id AS STRING), 256)  AS ADDRESS_HK,
    SHA2(CAST(city_id AS STRING), 256)  AS CITY_HK,
    CURRENT_TIMESTAMP()     AS LOAD_DATE,
    'silver.silver_address' AS RECORD_SOURCE,
    address_id,
    city_id
FROM {{ source('silver', 'silver_address') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
