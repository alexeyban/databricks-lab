{{ config(
    materialized='incremental',
    unique_key=['CITY_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(city_id AS STRING), 256)  AS CITY_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(city AS STRING), 256)           AS CITY_CORE_DIFF_HK,
        'silver.silver_city'              AS RECORD_SOURCE,
        city
    FROM {{ source('silver', 'silver_city') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.CITY_HK = s.CITY_HK
      AND t.CITY_CORE_DIFF_HK = s.CITY_CORE_DIFF_HK
)
{%- endif %}
