SELECT
  'products_has_rows' AS check_name,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('row_count=', CAST(COUNT(*) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_products
UNION ALL
SELECT
  'products_id_not_null' AS check_name,
  CASE WHEN SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('null_id_count=', CAST(SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_products
UNION ALL
SELECT
  'products_id_unique' AS check_name,
  CASE WHEN COUNT(*) = COUNT(DISTINCT id) THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('row_count=', CAST(COUNT(*) AS STRING), ', distinct_id_count=', CAST(COUNT(DISTINCT id) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_products
UNION ALL
SELECT
  'products_name_not_null' AS check_name,
  CASE WHEN SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('null_product_name_count=', CAST(SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_products
UNION ALL
SELECT
  'products_weight_not_null' AS check_name,
  CASE WHEN SUM(CASE WHEN weight IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CONCAT('null_weight_count=', CAST(SUM(CASE WHEN weight IS NULL THEN 1 ELSE 0 END) AS STRING)) AS details
FROM {{ catalog }}.{{ silver_schema }}.silver_products;
