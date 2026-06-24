{{ config(materialized='table', schema='gold', tags=['gold']) }}

-- Film popularity ranking: rental count, revenue, avg duration, category breakdown.
WITH films AS (
    SELECT * FROM {{ ref('hub_film') }}
),
film_details AS (
    SELECT fd.*
    FROM {{ ref('sat_film_core') }} fd
    WHERE (FILM_HK, LOAD_DATE) IN (
        SELECT FILM_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_film_core') }}
        GROUP BY FILM_HK
    )
),
film_pricing AS (
    SELECT fp.*
    FROM {{ ref('sat_film_pricing') }} fp
    WHERE (FILM_HK, LOAD_DATE) IN (
        SELECT FILM_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_film_pricing') }}
        GROUP BY FILM_HK
    )
),
inventory_links AS (
    SELECT DISTINCT FILM_HK, INVENTORY_HK
    FROM {{ ref('lnk_inventory_film') }}
),
rental_inventory AS (
    SELECT
        il.FILM_HK,
        COUNT(DISTINCT lri.RENTAL_HK)                       AS total_rentals
    FROM inventory_links il
    JOIN {{ ref('lnk_rental_inventory') }} lri
        ON il.INVENTORY_HK = lri.INVENTORY_HK
    GROUP BY il.FILM_HK
),
revenue AS (
    SELECT
        il.FILM_HK,
        SUM(spp.amount)                                     AS total_revenue
    FROM inventory_links il
    JOIN {{ ref('lnk_rental_inventory') }} lri
        ON il.INVENTORY_HK = lri.INVENTORY_HK
    JOIN {{ ref('lnk_payment_rental') }} lpr
        ON lri.RENTAL_HK = lpr.RENTAL_HK
    JOIN {{ ref('sat_payment_pricing') }} spp
        ON lpr.PAYMENT_HK = spp.PAYMENT_HK
    GROUP BY il.FILM_HK
),
film_category AS (
    SELECT
        lfc.FILM_HK,
        FIRST(sc.name)                                      AS category_name
    FROM {{ ref('lnk_film_category') }} lfc
    JOIN {{ ref('sat_category_core') }} sc
        ON lfc.CATEGORY_HK = sc.CATEGORY_HK
    WHERE (sc.CATEGORY_HK, sc.LOAD_DATE) IN (
        SELECT CATEGORY_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_category_core') }}
        GROUP BY CATEGORY_HK
    )
    GROUP BY lfc.FILM_HK
)

SELECT
    f.FILM_HK,
    f.film_id,
    fd.title,
    fd.rating,
    fp.rental_rate,
    fp.rental_duration,
    fp.replacement_cost,
    fc.category_name,
    COALESCE(ri.total_rentals, 0)                           AS total_rentals,
    ROUND(COALESCE(rev.total_revenue, 0), 2)                AS total_revenue,
    DENSE_RANK() OVER (ORDER BY COALESCE(ri.total_rentals, 0) DESC) AS popularity_rank,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at
FROM films f
LEFT JOIN film_details fd   ON f.FILM_HK = fd.FILM_HK
LEFT JOIN film_pricing fp   ON f.FILM_HK = fp.FILM_HK
LEFT JOIN rental_inventory ri ON f.FILM_HK = ri.FILM_HK
LEFT JOIN revenue rev       ON f.FILM_HK = rev.FILM_HK
LEFT JOIN film_category fc  ON f.FILM_HK = fc.FILM_HK
