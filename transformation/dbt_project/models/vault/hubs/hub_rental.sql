{{ config(
    materialized='incremental',
    unique_key='RENTAL_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(rental_id AS STRING), 256)    AS RENTAL_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_rental' AS RECORD_SOURCE,
    rental_id
FROM {{ source('silver', 'silver_rental') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
