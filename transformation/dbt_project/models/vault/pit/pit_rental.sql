{{ config(materialized='table') }}

WITH snapshot_dates AS (
    SELECT EXPLODE(SEQUENCE(
        DATE('2020-01-01'),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS snapshot_date
),
hub AS (
    SELECT DISTINCT RENTAL_HK
    FROM {{ ref('hub_rental') }}
)

SELECT
    hub.RENTAL_HK,
    snap.snapshot_date,
    CURRENT_TIMESTAMP() AS LOAD_DATE,
    s0.LOAD_DATE  AS SAT_RENTAL_CORE_LOAD_DATE,
    s0.RENTAL_CORE_DIFF_HK  AS SAT_RENTAL_CORE_DIFF_HK
FROM hub
CROSS JOIN snapshot_dates snap
LEFT JOIN {{ ref('sat_rental_core') }} s0
    ON hub.RENTAL_HK = s0.RENTAL_HK
    AND s0.LOAD_DATE = (
        SELECT MAX(LOAD_DATE) FROM {{ ref('sat_rental_core') }} s_0
        WHERE s_0.RENTAL_HK = hub.RENTAL_HK
        AND s_0.LOAD_DATE <= snap.snapshot_date
    )
