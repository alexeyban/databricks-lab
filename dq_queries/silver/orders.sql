SELECT
  'orders_has_rows' AS check_name,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('row_count=', CAST(COUNT(*) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_orders
UNION ALL
SELECT
  'orders_id_not_null' AS check_name,
  CASE WHEN SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('null_id_count=', CAST(SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_orders
UNION ALL
SELECT
  'orders_id_unique' AS check_name,
  CASE WHEN COUNT(*) = COUNT(DISTINCT id) THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('row_count=', CAST(COUNT(*) AS STRING), ', distinct_id_count=', CAST(COUNT(DISTINCT id) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_orders
UNION ALL
SELECT
  'orders_price_not_null' AS check_name,
  CASE WHEN SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('null_price_count=', CAST(SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_orders
UNION ALL
SELECT
  'orders_product_fk' AS check_name,
  CASE WHEN SUM(CASE WHEN o.product_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('missing_product_ref_count=', CAST(SUM(CASE WHEN o.product_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_orders o
LEFT JOIN {{ catalog }}.{{ silver_schema }}.silver_products p
  ON o.product_id = p.id;
