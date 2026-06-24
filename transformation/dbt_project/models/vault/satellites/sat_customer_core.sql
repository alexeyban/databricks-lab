{{ config(
    materialized='incremental',
    unique_key=['CUSTOMER_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(customer_id AS STRING), 256)  AS CUSTOMER_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(first_name AS STRING) || '|' || CAST(last_name AS STRING) || '|' || CAST(email AS STRING) || '|' || CAST(activebool AS STRING) || '|' || CAST(active AS STRING), 256)           AS CUSTOMER_CORE_DIFF_HK,
        'silver.silver_customer'              AS RECORD_SOURCE,
        first_name,
        last_name,
        email,
        activebool,
        active
    FROM {{ source('silver', 'silver_customer') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.CUSTOMER_HK = s.CUSTOMER_HK
      AND t.CUSTOMER_CORE_DIFF_HK = s.CUSTOMER_CORE_DIFF_HK
)
{%- endif %}
