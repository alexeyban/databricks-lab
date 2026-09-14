{{ config(
    materialized='incremental',
    unique_key=['ADDRESS_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(address_id AS STRING), 256)  AS ADDRESS_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(address AS STRING) || '|' || CAST(address2 AS STRING) || '|' || CAST(district AS STRING) || '|' || CAST(postal_code AS STRING) || '|' || CAST(phone AS STRING), 256)           AS ADDRESS_CORE_DIFF_HK,
        'silver.silver_address'              AS RECORD_SOURCE,
        address,
        address2,
        district,
        postal_code,
        phone
    FROM {{ source('silver', 'silver_address') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.ADDRESS_HK = s.ADDRESS_HK
      AND t.ADDRESS_CORE_DIFF_HK = s.ADDRESS_CORE_DIFF_HK
)
{%- endif %}
