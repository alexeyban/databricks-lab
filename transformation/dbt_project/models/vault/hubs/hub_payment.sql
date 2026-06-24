{{ config(
    materialized='incremental',
    unique_key='PAYMENT_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(payment_id AS STRING), 256)    AS PAYMENT_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_payment' AS RECORD_SOURCE,
    payment_id
FROM {{ source('silver', 'silver_payment') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
