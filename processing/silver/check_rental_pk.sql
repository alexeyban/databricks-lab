-- DQ: rental PK uniqueness
-- Expected: 0 duplicates. Any row returned = FAIL.
SELECT
    rental_id,
    COUNT(*) AS cnt
FROM {{ catalog }}.{{ silver_schema }}.silver_rental
GROUP BY rental_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 100;
