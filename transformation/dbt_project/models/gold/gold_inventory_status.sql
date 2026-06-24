{{ config(materialized='table', schema='gold', tags=['gold']) }}

-- Per-film inventory status: total copies, currently rented, available, overdue.
WITH inventory AS (
    SELECT * FROM {{ ref('hub_inventory') }}
),
film_lookup AS (
    SELECT INVENTORY_HK, FILM_HK
    FROM {{ ref('lnk_inventory_film') }}
),
store_lookup AS (
    SELECT INVENTORY_HK, STORE_HK
    FROM {{ ref('lnk_inventory_store') }}
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
latest_rental_per_inventory AS (
    SELECT
        lri.INVENTORY_HK,
        MAX(src.rental_date)  AS latest_rental_date,
        MAX(src.return_date)  AS latest_return_date
    FROM {{ ref('lnk_rental_inventory') }} lri
    JOIN {{ ref('sat_rental_core') }} src ON lri.RENTAL_HK = src.RENTAL_HK
    WHERE (src.RENTAL_HK, src.LOAD_DATE) IN (
        SELECT RENTAL_HK, MAX(LOAD_DATE)
        FROM {{ ref('sat_rental_core') }}
        GROUP BY RENTAL_HK
    )
    GROUP BY lri.INVENTORY_HK
),
inventory_status AS (
    SELECT
        i.INVENTORY_HK,
        i.inventory_id,
        fl.FILM_HK,
        sl.STORE_HK,
        CASE
            WHEN lr.latest_return_date IS NULL AND lr.latest_rental_date IS NOT NULL THEN 'RENTED'
            WHEN lr.latest_rental_date IS NULL THEN 'AVAILABLE'
            ELSE 'AVAILABLE'
        END                   AS status,
        CASE
            WHEN lr.latest_return_date IS NULL
             AND lr.latest_rental_date IS NOT NULL
             AND DATEDIFF(CURRENT_DATE(), lr.latest_rental_date) > 14 THEN TRUE
            ELSE FALSE
        END                   AS is_overdue
    FROM inventory i
    LEFT JOIN film_lookup fl  ON i.INVENTORY_HK = fl.INVENTORY_HK
    LEFT JOIN store_lookup sl ON i.INVENTORY_HK = sl.INVENTORY_HK
    LEFT JOIN latest_rental_per_inventory lr ON i.INVENTORY_HK = lr.INVENTORY_HK
)

SELECT
    s.FILM_HK,
    fd.title,
    COUNT(*)                                                AS total_copies,
    SUM(CASE WHEN s.status = 'RENTED' THEN 1 ELSE 0 END)   AS copies_rented,
    SUM(CASE WHEN s.status = 'AVAILABLE' THEN 1 ELSE 0 END) AS copies_available,
    SUM(CASE WHEN s.is_overdue THEN 1 ELSE 0 END)           AS overdue_copies,
    CURRENT_TIMESTAMP()                                     AS dbt_updated_at
FROM inventory_status s
LEFT JOIN film_details fd ON s.FILM_HK = fd.FILM_HK
GROUP BY s.FILM_HK, fd.title
