{{ config(
    materialized='incremental',
    unique_key=['LANGUAGE_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(language_id AS STRING), 256)  AS LANGUAGE_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(name AS STRING), 256)           AS LANGUAGE_CORE_DIFF_HK,
        'silver.silver_language'              AS RECORD_SOURCE,
        name
    FROM {{ source('silver', 'silver_language') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.LANGUAGE_HK = s.LANGUAGE_HK
      AND t.LANGUAGE_CORE_DIFF_HK = s.LANGUAGE_CORE_DIFF_HK
)
{%- endif %}
