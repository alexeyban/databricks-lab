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
)

SELECT
    hub.PAYMENT_HK,
    snap.snapshot_date,
    CURRENT_TIMESTAMP() AS LOAD_DATE,
    s0.LOAD_DATE  AS SAT_PAYMENT_CORE_LOAD_DATE,
    s0.PAYMENT_CORE_DIFF_HK  AS SAT_PAYMENT_CORE_DIFF_HK,
    s1.LOAD_DATE  AS SAT_PAYMENT_PRICING_LOAD_DATE,
    s1.PAYMENT_PRICING_DIFF_HK  AS SAT_PAYMENT_PRICING_DIFF_HK
FROM hub
CROSS JOIN snapshot_dates snap
LEFT JOIN {{ ref('sat_payment_core') }} s0
    ON hub.PAYMENT_HK = s0.PAYMENT_HK
    AND s0.LOAD_DATE = (
        SELECT MAX(LOAD_DATE) FROM {{ ref('sat_payment_core') }} s_0
        WHERE s_0.PAYMENT_HK = hub.PAYMENT_HK
        AND s_0.LOAD_DATE <= snap.snapshot_date
    )
LEFT JOIN {{ ref('sat_payment_pricing') }} s1
    ON hub.PAYMENT_HK = s1.PAYMENT_HK
    AND s1.LOAD_DATE = (
        SELECT MAX(LOAD_DATE) FROM {{ ref('sat_payment_pricing') }} s_1
        WHERE s_1.PAYMENT_HK = hub.PAYMENT_HK
        AND s_1.LOAD_DATE <= snap.snapshot_date
    )
