{{ config(materialized='table') }}

WITH snapshot_dates AS (
    SELECT EXPLODE(SEQUENCE(
        DATE('2020-01-01'),
        CURRENT_DATE(),
        INTERVAL 1 DAY
    )) AS snapshot_date
),
hub AS (
    SELECT DISTINCT PAYMENT_HK
    FROM {{ ref('hub_payment') }}
),
sat_core_ranked AS (
    SELECT
        s.PAYMENT_HK,
        s.LOAD_DATE,
        s.PAYMENT_CORE_DIFF_HK,
        snap.snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.PAYMENT_HK, snap.snapshot_date
            ORDER BY s.LOAD_DATE DESC
        ) AS _rn
    FROM {{ ref('sat_payment_core') }} s
    CROSS JOIN snapshot_dates snap
    WHERE s.LOAD_DATE <= snap.snapshot_date
),
sat_pricing_ranked AS (
    SELECT
        s.PAYMENT_HK,
        s.LOAD_DATE,
        s.PAYMENT_PRICING_DIFF_HK,
        snap.snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.PAYMENT_HK, snap.snapshot_date
            ORDER BY s.LOAD_DATE DESC
        ) AS _rn
    FROM {{ ref('sat_payment_pricing') }} s
    CROSS JOIN snapshot_dates snap
    WHERE s.LOAD_DATE <= snap.snapshot_date
)

SELECT
    hub.PAYMENT_HK,
    snap.snapshot_date,
    CURRENT_TIMESTAMP() AS LOAD_DATE,
    r0.LOAD_DATE            AS SAT_PAYMENT_CORE_LOAD_DATE,
    r0.PAYMENT_CORE_DIFF_HK AS SAT_PAYMENT_CORE_DIFF_HK,
    r1.LOAD_DATE               AS SAT_PAYMENT_PRICING_LOAD_DATE,
    r1.PAYMENT_PRICING_DIFF_HK AS SAT_PAYMENT_PRICING_DIFF_HK
FROM hub
CROSS JOIN snapshot_dates snap
LEFT JOIN sat_core_ranked r0
    ON r0.PAYMENT_HK = hub.PAYMENT_HK
    AND r0.snapshot_date = snap.snapshot_date
    AND r0._rn = 1
LEFT JOIN sat_pricing_ranked r1
    ON r1.PAYMENT_HK = hub.PAYMENT_HK
    AND r1.snapshot_date = snap.snapshot_date
    AND r1._rn = 1
