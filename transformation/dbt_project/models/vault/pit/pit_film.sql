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
),
sat_core_ranked AS (
    SELECT
        s.FILM_HK,
        s.LOAD_DATE,
        s.FILM_CORE_DIFF_HK,
        snap.snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.FILM_HK, snap.snapshot_date
            ORDER BY s.LOAD_DATE DESC
        ) AS _rn
    FROM {{ ref('sat_film_core') }} s
    CROSS JOIN snapshot_dates snap
    WHERE s.LOAD_DATE <= snap.snapshot_date
),
sat_pricing_ranked AS (
    SELECT
        s.FILM_HK,
        s.LOAD_DATE,
        s.FILM_PRICING_DIFF_HK,
        snap.snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.FILM_HK, snap.snapshot_date
            ORDER BY s.LOAD_DATE DESC
        ) AS _rn
    FROM {{ ref('sat_film_pricing') }} s
    CROSS JOIN snapshot_dates snap
    WHERE s.LOAD_DATE <= snap.snapshot_date
)

SELECT
    hub.FILM_HK,
    snap.snapshot_date,
    CURRENT_TIMESTAMP() AS LOAD_DATE,
    r0.LOAD_DATE         AS SAT_FILM_CORE_LOAD_DATE,
    r0.FILM_CORE_DIFF_HK AS SAT_FILM_CORE_DIFF_HK,
    r1.LOAD_DATE            AS SAT_FILM_PRICING_LOAD_DATE,
    r1.FILM_PRICING_DIFF_HK AS SAT_FILM_PRICING_DIFF_HK
FROM hub
CROSS JOIN snapshot_dates snap
LEFT JOIN sat_core_ranked r0
    ON r0.FILM_HK = hub.FILM_HK
    AND r0.snapshot_date = snap.snapshot_date
    AND r0._rn = 1
LEFT JOIN sat_pricing_ranked r1
    ON r1.FILM_HK = hub.FILM_HK
    AND r1.snapshot_date = snap.snapshot_date
    AND r1._rn = 1
