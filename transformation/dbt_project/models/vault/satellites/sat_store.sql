{{ config(
    materialized='incremental',
    unique_key=['STORE_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(store_id AS STRING), 256)  AS STORE_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(last_update AS STRING), 256)           AS STORE_DIFF_HK,
        'silver.silver_store'              AS RECORD_SOURCE,
        last_update
    FROM {{ source('silver', 'silver_store') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.STORE_HK = s.STORE_HK
      AND t.STORE_DIFF_HK = s.STORE_DIFF_HK
)
{%- endif %}
