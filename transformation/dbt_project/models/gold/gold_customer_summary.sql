{{ config(materialized='table', schema='gold', tags=['gold']) }}

-- Customer lifetime value summary: total spend, rental count, active status, tier.
WITH customer AS (
    SELECT * FROM {{ ref('hub_customer') }}
),
cust_sat AS (
    SELECT * FROM {{ ref('sat_customer_core') }}
    WHERE (CUSTOMER_HK, LOAD_DATE) IN (
        SELECT CUSTOMER_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_customer_core') }}
        GROUP BY CUSTOMER_HK
    )
),
rentals AS (
    SELECT
        lrc.CUSTOMER_HK,
        COUNT(DISTINCT lrc.RENTAL_HK)       AS total_rentals,
        MIN(src.rental_date)                 AS first_rental_date,
        MAX(src.rental_date)                 AS last_rental_date
    FROM {{ ref('lnk_rental_customer') }} lrc
    JOIN {{ ref('sat_rental_core') }} src
        ON lrc.RENTAL_HK = src.RENTAL_HK
    GROUP BY lrc.CUSTOMER_HK
),
payments AS (
    SELECT
        lpc.CUSTOMER_HK,
        COUNT(*)                             AS total_payments,
        SUM(spp.amount)                      AS total_spend,
        AVG(spp.amount)                      AS avg_payment
    FROM {{ ref('lnk_payment_customer') }} lpc
    JOIN {{ ref('sat_payment_pricing') }} spp
        ON lpc.PAYMENT_HK = spp.PAYMENT_HK
    GROUP BY lpc.CUSTOMER_HK
)

SELECT
    c.CUSTOMER_HK,
    c.customer_id,
    cs.first_name,
    cs.last_name,
    cs.email,
    COALESCE(r.total_rentals, 0)                            AS total_rentals,
    COALESCE(p.total_payments, 0)                           AS total_payments,
    COALESCE(p.total_spend, 0)                              AS total_spend,
    ROUND(COALESCE(p.avg_payment, 0), 2)                    AS avg_payment,
    r.first_rental_date,
    r.last_rental_date,
    DATEDIFF(COALESCE(r.last_rental_date, CURRENT_DATE()), COALESCE(r.first_rental_date, CURRENT_DATE())) AS active_days,
    CASE
        WHEN COALESCE(p.total_spend, 0) >= 100 THEN 'Gold'
        WHEN COALESCE(p.total_spend, 0) >= 40  THEN 'Silver'
        ELSE 'Bronze'
    END                                                     AS customer_tier,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at
FROM customer c
LEFT JOIN cust_sat cs   ON c.CUSTOMER_HK = cs.CUSTOMER_HK
LEFT JOIN rentals r     ON c.CUSTOMER_HK = r.CUSTOMER_HK
LEFT JOIN payments p    ON c.CUSTOMER_HK = p.CUSTOMER_HK
