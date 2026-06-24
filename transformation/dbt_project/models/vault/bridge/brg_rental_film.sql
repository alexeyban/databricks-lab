{{ config(materialized='table') }}

SELECT
    h0.RENTAL_HK,
    l1.RENTAL_INVENTORY_HK,
    h1.INVENTORY_HK,
    l3.INVENTORY_FILM_HK,
    h3.FILM_HK,
    CURRENT_TIMESTAMP() AS LOAD_DATE
FROM {{ ref('hub_rental') }} h0
JOIN {{ ref('lnk_rental_inventory') }} l1
    ON h0.RENTAL_HK = l1.RENTAL_HK
JOIN {{ ref('hub_inventory') }} h1
    ON l1.INVENTORY_HK = h1.INVENTORY_HK
JOIN {{ ref('lnk_inventory_film') }} l3
    ON h1.INVENTORY_HK = l3.INVENTORY_HK
JOIN {{ ref('hub_film') }} h3
    ON l3.FILM_HK = h3.FILM_HK
