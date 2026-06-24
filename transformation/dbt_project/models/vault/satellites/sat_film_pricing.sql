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
        SHA2(CAST(rental_duration AS STRING) || '|' || CAST(rental_rate AS STRING) || '|' || CAST(replacement_cost AS STRING), 256)           AS FILM_PRICING_DIFF_HK,
        'silver.silver_film'              AS RECORD_SOURCE,
        rental_duration,
        rental_rate,
        replacement_cost
    FROM {{ source('silver', 'silver_film') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.FILM_HK = s.FILM_HK
      AND t.FILM_PRICING_DIFF_HK = s.FILM_PRICING_DIFF_HK
)
{%- endif %}
