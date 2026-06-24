{{ config(materialized='table') }}

WITH snapshot_dates AS (
    SELECT EXPLODE(SEQUENCE(
        DATE('2020-01-01'),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS snapshot_date
),
hub AS (
    SELECT DISTINCT CUSTOMER_HK
    FROM {{ ref('hub_customer') }}
),
sat_core_ranked AS (
    SELECT
        s.CUSTOMER_HK,
        s.LOAD_DATE,
        s.CUSTOMER_CORE_DIFF_HK,
        snap.snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.CUSTOMER_HK, snap.snapshot_date
            ORDER BY s.LOAD_DATE DESC
        ) AS _rn
    FROM {{ ref('sat_customer_core') }} s
    CROSS JOIN snapshot_dates snap
    WHERE s.LOAD_DATE <= snap.snapshot_date
)

SELECT
    hub.CUSTOMER_HK,
    snap.snapshot_date,
    CURRENT_TIMESTAMP() AS LOAD_DATE,
    r0.LOAD_DATE         AS SAT_CUSTOMER_CORE_LOAD_DATE,
    r0.CUSTOMER_CORE_DIFF_HK AS SAT_CUSTOMER_CORE_DIFF_HK
FROM hub
CROSS JOIN snapshot_dates snap
LEFT JOIN sat_core_ranked r0
    ON r0.CUSTOMER_HK = hub.CUSTOMER_HK
    AND r0.snapshot_date = snap.snapshot_date
    AND r0._rn = 1
