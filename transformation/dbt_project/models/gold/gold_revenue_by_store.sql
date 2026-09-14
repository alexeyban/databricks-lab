{{ config(materialized='table', schema='gold', tags=['gold']) }}

-- Daily revenue per store: payment totals, rental volume, average transaction.
WITH stores AS (
    SELECT * FROM {{ ref('hub_store') }}
),
payments_daily AS (
    SELECT
        DATE(spc.payment_date)      AS payment_day,
        lrs.STAFF_HK,
        spp.PAYMENT_HK,
        spp.amount
    FROM {{ ref('sat_payment_core') }} spc
    JOIN {{ ref('sat_payment_pricing') }} spp
        ON spc.PAYMENT_HK = spp.PAYMENT_HK
    JOIN {{ ref('lnk_payment_rental') }} lpr
        ON spp.PAYMENT_HK = lpr.PAYMENT_HK
    JOIN {{ ref('lnk_rental_staff') }} lrs
        ON lpr.RENTAL_HK = lrs.RENTAL_HK
    WHERE (spc.PAYMENT_HK, spc.LOAD_DATE) IN (
        SELECT PAYMENT_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_payment_core') }}
        GROUP BY PAYMENT_HK
    )
    AND (spp.PAYMENT_HK, spp.LOAD_DATE) IN (
        SELECT PAYMENT_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_payment_pricing') }}
        GROUP BY PAYMENT_HK
    )
),
staff_store AS (
    SELECT STAFF_HK, STORE_HK
    FROM {{ ref('lnk_staff_store') }}
)

SELECT
    pd.payment_day,
    ss.STORE_HK,
    COUNT(DISTINCT pd.PAYMENT_HK)               AS total_transactions,
    SUM(pd.amount)                               AS total_revenue,
    ROUND(AVG(pd.amount), 2)                     AS avg_transaction,
    COUNT(DISTINCT pd.STAFF_HK)                  AS active_staff_count,
    CURRENT_TIMESTAMP()                          AS dbt_updated_at
FROM payments_daily pd
JOIN staff_store ss ON pd.STAFF_HK = ss.STAFF_HK
GROUP BY pd.payment_day, ss.STORE_HK
