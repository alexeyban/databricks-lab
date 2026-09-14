{{ config(
    materialized='incremental',
    unique_key=['PAYMENT_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(payment_id AS STRING), 256)  AS PAYMENT_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(payment_date AS STRING), 256)           AS PAYMENT_CORE_DIFF_HK,
        'silver.silver_payment'              AS RECORD_SOURCE,
        payment_date
    FROM {{ source('silver', 'silver_payment') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.PAYMENT_HK = s.PAYMENT_HK
      AND t.PAYMENT_CORE_DIFF_HK = s.PAYMENT_CORE_DIFF_HK
)
{%- endif %}
