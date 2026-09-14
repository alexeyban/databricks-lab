-- DQ: film PK uniqueness
-- Expected: 0 duplicates. Any row returned = FAIL.
SELECT
    film_id,
    COUNT(*) AS cnt
FROM {{ catalog }}.{{ silver_schema }}.silver_film
GROUP BY film_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 100;
