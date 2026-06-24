{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(customer_id AS STRING), 256)    AS CUSTOMER_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_customer' AS RECORD_SOURCE,
    customer_id
FROM {{ source('silver', 'silver_customer') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
