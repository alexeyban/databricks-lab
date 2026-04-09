-- DQ: customer PK uniqueness
-- Expected: 0 duplicates. Any row returned = FAIL.
SELECT
    customer_id,
    COUNT(*) AS cnt
FROM {{ catalog }}.{{ silver_schema }}.silver_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 100;
