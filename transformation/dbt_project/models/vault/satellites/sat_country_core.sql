{{ config(
    materialized='incremental',
    unique_key=['COUNTRY_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(country_id AS STRING), 256)  AS COUNTRY_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(country AS STRING), 256)           AS COUNTRY_CORE_DIFF_HK,
        'silver.silver_country'              AS RECORD_SOURCE,
        country
    FROM {{ source('silver', 'silver_country') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.COUNTRY_HK = s.COUNTRY_HK
      AND t.COUNTRY_CORE_DIFF_HK = s.COUNTRY_CORE_DIFF_HK
)
{%- endif %}
