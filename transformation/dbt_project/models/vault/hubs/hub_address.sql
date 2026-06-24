{{ config(
    materialized='incremental',
    unique_key='ADDRESS_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(address_id AS STRING), 256)    AS ADDRESS_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_address' AS RECORD_SOURCE,
    address_id
FROM {{ source('silver', 'silver_address') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
