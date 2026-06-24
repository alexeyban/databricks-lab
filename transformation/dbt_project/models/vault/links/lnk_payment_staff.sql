{{ config(
    materialized='incremental',
    unique_key='PAYMENT_STAFF_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(SHA2(CAST(payment_id AS STRING), 256) || '|' || SHA2(CAST(staff_id AS STRING), 256), 256)  AS PAYMENT_STAFF_HK,
    SHA2(CAST(payment_id AS STRING), 256)  AS PAYMENT_HK,
    SHA2(CAST(staff_id AS STRING), 256)  AS STAFF_HK,
    CURRENT_TIMESTAMP()     AS LOAD_DATE,
    'silver.silver_payment' AS RECORD_SOURCE,
    payment_id,
    staff_id
FROM {{ source('silver', 'silver_payment') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
