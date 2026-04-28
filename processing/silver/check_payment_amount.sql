-- DQ: payment amount range check
-- Expected: all amounts between 0.99 and 11.99 (dvdrental business rule).
SELECT
    'payment_amount_range'                                           AS check_name,
    SUM(CASE WHEN amount < 0.99 OR amount > 11.99 THEN 1 ELSE 0 END) AS out_of_range_count,
    COUNT(*)                                                          AS total_rows,
    MIN(amount)                                                       AS min_amount,
    MAX(amount)                                                       AS max_amount,
    CASE
        WHEN SUM(CASE WHEN amount < 0.99 OR amount > 11.99 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                               AS status
FROM {{ catalog }}.{{ silver_schema }}.silver_payment;
