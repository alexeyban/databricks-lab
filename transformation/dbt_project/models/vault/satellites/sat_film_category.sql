{{ config(
    materialized='incremental',
    unique_key=['FILM_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(film_id AS STRING), 256)  AS FILM_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(category_id AS STRING), 256)           AS FILM_CATEGORY_DIFF_HK,
        'silver.silver_film_category'              AS RECORD_SOURCE,
        category_id
    FROM {{ source('silver', 'silver_film_category') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.FILM_HK = s.FILM_HK
      AND t.FILM_CATEGORY_DIFF_HK = s.FILM_CATEGORY_DIFF_HK
)
{%- endif %}
