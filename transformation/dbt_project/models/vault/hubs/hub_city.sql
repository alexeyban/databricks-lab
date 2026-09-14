{{ config(
    materialized='incremental',
    unique_key='CITY_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(city_id AS STRING), 256)    AS CITY_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_city' AS RECORD_SOURCE,
    city_id
FROM {{ source('silver', 'silver_city') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
