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
)

SELECT
    hub.CUSTOMER_HK,
    snap.snapshot_date,
    CURRENT_TIMESTAMP() AS LOAD_DATE,
    s0.LOAD_DATE  AS SAT_CUSTOMER_CORE_LOAD_DATE,
    s0.CUSTOMER_CORE_DIFF_HK  AS SAT_CUSTOMER_CORE_DIFF_HK
FROM hub
CROSS JOIN snapshot_dates snap
LEFT JOIN {{ ref('sat_customer_core') }} s0
    ON hub.CUSTOMER_HK = s0.CUSTOMER_HK
    AND s0.LOAD_DATE = (
        SELECT MAX(LOAD_DATE) FROM {{ ref('sat_customer_core') }} s_0
        WHERE s_0.CUSTOMER_HK = hub.CUSTOMER_HK
        AND s_0.LOAD_DATE <= snap.snapshot_date
    )
