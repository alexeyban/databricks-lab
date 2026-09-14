{{ config(
    materialized='incremental',
    unique_key='STORE_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(store_id AS STRING), 256)    AS STORE_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_store' AS RECORD_SOURCE,
    store_id
FROM {{ source('silver', 'silver_store') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
