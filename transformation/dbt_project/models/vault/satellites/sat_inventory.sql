{{ config(
    materialized='incremental',
    unique_key=['INVENTORY_HK', 'LOAD_DATE'],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        SHA2(CAST(inventory_id AS STRING), 256)  AS INVENTORY_HK,
        CURRENT_TIMESTAMP()                    AS LOAD_DATE,
        SHA2(CAST(last_update AS STRING), 256)           AS INVENTORY_DIFF_HK,
        'silver.silver_inventory'              AS RECORD_SOURCE,
        last_update
    FROM {{ source('silver', 'silver_inventory') }}
)
SELECT s.*
FROM source s
{%- if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.INVENTORY_HK = s.INVENTORY_HK
      AND t.INVENTORY_DIFF_HK = s.INVENTORY_DIFF_HK
)
{%- endif %}
