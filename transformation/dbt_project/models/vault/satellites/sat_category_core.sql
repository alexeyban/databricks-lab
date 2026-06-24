{{ config(
    materialized='incremental',
    unique_key=['CATEGORY_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(category_id AS STRING), 256)  AS CATEGORY_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(name AS STRING), 256)           AS CATEGORY_CORE_DIFF_HK,
        'silver.silver_category'              AS RECORD_SOURCE,
        name
    FROM {{ source('silver', 'silver_category') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.CATEGORY_HK = s.CATEGORY_HK
      AND t.CATEGORY_CORE_DIFF_HK = s.CATEGORY_CORE_DIFF_HK
)
{%- endif %}
