{{ config(
    materialized='incremental',
    unique_key=['RENTAL_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(rental_id AS STRING), 256)  AS RENTAL_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(rental_date AS STRING) || '|' || CAST(return_date AS STRING) || '|' || CAST(staff_id AS STRING), 256)           AS RENTAL_DETAILS_DIFF_HK,
        'silver.silver_rental'              AS RECORD_SOURCE,
        rental_date,
        return_date,
        staff_id
    FROM {{ source('silver', 'silver_rental') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.RENTAL_HK = s.RENTAL_HK
      AND t.RENTAL_DETAILS_DIFF_HK = s.RENTAL_DETAILS_DIFF_HK
)
{%- endif %}
