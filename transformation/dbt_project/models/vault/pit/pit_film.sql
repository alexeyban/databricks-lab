{{ config(materialized='table') }}

WITH snapshot_dates AS (
    SELECT EXPLODE(SEQUENCE(
        DATE('2020-01-01'),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS snapshot_date
),
hub AS (
    SELECT DISTINCT FILM_HK
    FROM {{ ref('hub_film') }}
)

SELECT
    hub.FILM_HK,
    snap.snapshot_date,
    CURRENT_TIMESTAMP() AS LOAD_DATE,
    s0.LOAD_DATE  AS SAT_FILM_CORE_LOAD_DATE,
    s0.FILM_CORE_DIFF_HK  AS SAT_FILM_CORE_DIFF_HK,
    s1.LOAD_DATE  AS SAT_FILM_PRICING_LOAD_DATE,
    s1.FILM_PRICING_DIFF_HK  AS SAT_FILM_PRICING_DIFF_HK
FROM hub
CROSS JOIN snapshot_dates snap
LEFT JOIN {{ ref('sat_film_core') }} s0
    ON hub.FILM_HK = s0.FILM_HK
    AND s0.LOAD_DATE = (
        SELECT MAX(LOAD_DATE) FROM {{ ref('sat_film_core') }} s_0
        WHERE s_0.FILM_HK = hub.FILM_HK
        AND s_0.LOAD_DATE <= snap.snapshot_date
    )
LEFT JOIN {{ ref('sat_film_pricing') }} s1
    ON hub.FILM_HK = s1.FILM_HK
    AND s1.LOAD_DATE = (
        SELECT MAX(LOAD_DATE) FROM {{ ref('sat_film_pricing') }} s_1
        WHERE s_1.FILM_HK = hub.FILM_HK
        AND s_1.LOAD_DATE <= snap.snapshot_date
    )
