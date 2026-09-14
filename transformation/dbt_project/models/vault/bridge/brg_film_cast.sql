{{ config(materialized='table') }}

SELECT
    h0.FILM_HK,
    l1.FILM_ACTOR_HK,
    h1.ACTOR_HK,
    CURRENT_TIMESTAMP() AS LOAD_DATE
FROM {{ ref('hub_film') }} h0
JOIN {{ ref('lnk_film_actor') }} l1
    ON h0.FILM_HK = l1.FILM_HK
JOIN {{ ref('hub_actor') }} h1
    ON l1.ACTOR_HK = h1.ACTOR_HK
