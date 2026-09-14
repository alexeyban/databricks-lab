{{ config(
    materialized='incremental',
    unique_key=['STAFF_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(staff_id AS STRING), 256)  AS STAFF_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(first_name AS STRING) || '|' || CAST(last_name AS STRING) || '|' || CAST(email AS STRING) || '|' || CAST(address_id AS STRING), 256)           AS STAFF_DETAILS_DIFF_HK,
        'silver.silver_staff'              AS RECORD_SOURCE,
        first_name,
        last_name,
        email,
        address_id
    FROM {{ source('silver', 'silver_staff') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.STAFF_HK = s.STAFF_HK
      AND t.STAFF_DETAILS_DIFF_HK = s.STAFF_DETAILS_DIFF_HK
)
{%- endif %}
