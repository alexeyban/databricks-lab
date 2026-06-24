{{ config(
    materialized='incremental',
    unique_key='FILM_CATEGORY_HK',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

SELECT
    SHA2(SHA2(CAST(film_id AS STRING), 256) || '|' || SHA2(CAST(category_id AS STRING), 256), 256)  AS FILM_CATEGORY_HK,
    SHA2(CAST(film_id AS STRING), 256)  AS FILM_HK,
    SHA2(CAST(category_id AS STRING), 256)  AS CATEGORY_HK,
    CURRENT_TIMESTAMP()     AS LOAD_DATE,
    'silver.silver_film_category' AS RECORD_SOURCE,
    film_id,
    category_id
FROM {{ source('silver', 'silver_film_category') }}
{%- if is_incremental() %}
WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{ this }})
{%- endif %}
