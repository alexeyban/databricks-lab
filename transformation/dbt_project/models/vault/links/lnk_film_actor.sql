{{ config(
    materialized='incremental',
    unique_key='FILM_ACTOR_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(SHA2(CAST(film_id AS STRING), 256) || '|' || SHA2(CAST(actor_id AS STRING), 256), 256)  AS FILM_ACTOR_HK,
    SHA2(CAST(film_id AS STRING), 256)  AS FILM_HK,
    SHA2(CAST(actor_id AS STRING), 256)  AS ACTOR_HK,
    CURRENT_TIMESTAMP()     AS LOAD_DATE,
    'silver.silver_film_actor' AS RECORD_SOURCE,
    film_id,
    actor_id
FROM {{ source('silver', 'silver_film_actor') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
