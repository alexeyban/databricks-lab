{{ config(
    materialized='incremental',
    unique_key='CATEGORY_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(category_id AS STRING), 256)    AS CATEGORY_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_category' AS RECORD_SOURCE,
    category_id
FROM {{ source('silver', 'silver_category') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
