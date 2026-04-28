-- DQ: rental.customer_id FK null rate
-- Expected: 0 nulls (customer_id is mandatory on every rental).
SELECT
    'fk_customer_null_rate'                                          AS check_name,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)            AS null_count,
    COUNT(*)                                                         AS total_rows,
    ROUND(
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0),
        6
    )                                                                AS null_rate,
    CASE
        WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARN'
    END                                                              AS status
FROM {{ catalog }}.{{ silver_schema }}.silver_rental;
