{{ config(
    materialized='incremental',
    unique_key=['ACTOR_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(actor_id AS STRING), 256)  AS ACTOR_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(first_name AS STRING) || '|' || CAST(last_name AS STRING), 256)           AS ACTOR_CORE_DIFF_HK,
        'silver.silver_actor'              AS RECORD_SOURCE,
        first_name,
        last_name
    FROM {{ source('silver', 'silver_actor') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.ACTOR_HK = s.ACTOR_HK
      AND t.ACTOR_CORE_DIFF_HK = s.ACTOR_CORE_DIFF_HK
)
{%- endif %}
