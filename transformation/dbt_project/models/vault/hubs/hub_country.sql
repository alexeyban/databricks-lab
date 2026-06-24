{{ config(
    materialized='incremental',
    unique_key='COUNTRY_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(country_id AS STRING), 256)    AS COUNTRY_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_country' AS RECORD_SOURCE,
    country_id
FROM {{ source('silver', 'silver_country') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
