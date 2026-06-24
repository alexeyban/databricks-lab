{{ config(materialized='table', schema='gold', tags=['gold']) }}

-- Staff KPIs: total transactions, revenue driven, average payment per rental.
WITH staff AS (
    SELECT * FROM {{ ref('hub_staff') }}
),
staff_info AS (
    SELECT si.*
    FROM {{ ref('sat_staff_core') }} si
    WHERE (STAFF_HK, LOAD_DATE) IN (
        SELECT STAFF_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_staff_core') }}
        GROUP BY STAFF_HK
    )
),
staff_store AS (
    SELECT STAFF_HK, STORE_HK
    FROM {{ ref('lnk_staff_store') }}
),
rentals_handled AS (
    SELECT
        lrs.STAFF_HK,
        COUNT(DISTINCT lrs.RENTAL_HK)           AS total_rentals_handled,
        MIN(src.rental_date)                     AS first_rental,
        MAX(src.rental_date)                     AS last_rental
    FROM {{ ref('lnk_rental_staff') }} lrs
    JOIN {{ ref('sat_rental_core') }} src
        ON lrs.RENTAL_HK = src.RENTAL_HK
    WHERE (src.RENTAL_HK, src.LOAD_DATE) IN (
        SELECT RENTAL_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_rental_core') }}
        GROUP BY RENTAL_HK
    )
    GROUP BY lrs.STAFF_HK
),
payments_processed AS (
    SELECT
        lps.STAFF_HK,
        COUNT(DISTINCT lps.PAYMENT_HK)          AS total_payments_processed,
        SUM(spp.amount)                          AS total_revenue_driven,
        AVG(spp.amount)                          AS avg_payment_amount
    FROM {{ ref('lnk_payment_staff') }} lps
    JOIN {{ ref('sat_payment_pricing') }} spp
        ON lps.PAYMENT_HK = spp.PAYMENT_HK
    WHERE (spp.PAYMENT_HK, spp.LOAD_DATE) IN (
        SELECT PAYMENT_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_payment_pricing') }}
        GROUP BY PAYMENT_HK
    )
    GROUP BY lps.STAFF_HK
)

SELECT
    s.STAFF_HK,
    s.staff_id,
    si.first_name,
    si.last_name,
    si.username,
    si.active,
    ss.STORE_HK,
    COALESCE(rh.total_rentals_handled, 0)           AS total_rentals_handled,
    COALESCE(pp.total_payments_processed, 0)         AS total_payments_processed,
    ROUND(COALESCE(pp.total_revenue_driven, 0), 2)   AS total_revenue_driven,
    ROUND(COALESCE(pp.avg_payment_amount, 0), 2)     AS avg_payment_amount,
    rh.first_rental,
    rh.last_rental,
    DENSE_RANK() OVER (ORDER BY COALESCE(pp.total_revenue_driven, 0) DESC) AS revenue_rank,
    CURRENT_TIMESTAMP()                              AS dbt_updated_at
FROM staff s
LEFT JOIN staff_info si         ON s.STAFF_HK = si.STAFF_HK
LEFT JOIN staff_store ss        ON s.STAFF_HK = ss.STAFF_HK
LEFT JOIN rentals_handled rh    ON s.STAFF_HK = rh.STAFF_HK
LEFT JOIN payments_processed pp ON s.STAFF_HK = pp.STAFF_HK
