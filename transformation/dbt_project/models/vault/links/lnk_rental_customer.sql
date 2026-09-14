{{ config(
    materialized='incremental',
    unique_key='RENTAL_CUSTOMER_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(SHA2(CAST(rental_id AS STRING), 256) || '|' || SHA2(CAST(customer_id AS STRING), 256), 256)  AS RENTAL_CUSTOMER_HK,
    SHA2(CAST(rental_id AS STRING), 256)  AS RENTAL_HK,
    SHA2(CAST(customer_id AS STRING), 256)  AS CUSTOMER_HK,
    CURRENT_TIMESTAMP()     AS LOAD_DATE,
    'silver.silver_rental' AS RECORD_SOURCE,
    rental_id,
    customer_id
FROM {{ source('silver', 'silver_rental') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
