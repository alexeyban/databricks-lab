{{ config(
    materialized='incremental',
    unique_key='ACTOR_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(CAST(actor_id AS STRING), 256)    AS ACTOR_HK,
    CURRENT_TIMESTAMP()      AS LOAD_DATE,
    'silver.silver_actor' AS RECORD_SOURCE,
    actor_id
FROM {{ source('silver', 'silver_actor') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
