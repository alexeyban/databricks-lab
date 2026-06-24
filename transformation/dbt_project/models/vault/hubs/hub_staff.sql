{{ config(
    materialized='incremental',
    unique_key='STAFF_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(staff_id AS STRING), 256)    AS STAFF_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_staff' AS RECORD_SOURCE,
    staff_id
FROM {{ source('silver', 'silver_staff') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
