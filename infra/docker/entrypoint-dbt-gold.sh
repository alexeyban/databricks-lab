#!/usr/bin/env bash
set -e

# Validate required env vars before attempting any dbt command.
missing=()
[[ -z "${DATABRICKS_HOST}"         ]] && missing+=("DATABRICKS_HOST")
[[ -z "${DATABRICKS_TOKEN}"        ]] && missing+=("DATABRICKS_TOKEN")
[[ -z "${DATABRICKS_WAREHOUSE_ID}" ]] && missing+=("DATABRICKS_WAREHOUSE_ID")
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: required environment variable(s) not set: ${missing[*]}"
    echo "Set them in .env or pass -e VAR=value to docker run."
    exit 1
fi

# Strip protocol prefix and trailing slash from DATABRICKS_HOST so dbt-databricks
# receives a bare hostname (e.g. dbc-xxxx.cloud.databricks.com).
export DBT_DATABRICKS_HOST="${DATABRICKS_HOST#https://}"
export DBT_DATABRICKS_HOST="${DBT_DATABRICKS_HOST#http://}"
export DBT_DATABRICKS_HOST="${DBT_DATABRICKS_HOST%/}"

# Build http_path from warehouse ID
export DBT_HTTP_PATH="/sql/1.0/warehouses/${DATABRICKS_WAREHOUSE_ID}"

echo "=== dbt Gold Layer ==="
echo "  Host:      ${DBT_DATABRICKS_HOST}"
echo "  HTTP path: ${DBT_HTTP_PATH}"
echo ""

cd /app/cdc_gold

echo "--- dbt deps ---"
dbt deps

echo ""
echo "--- dbt debug ---"
dbt debug

echo ""
echo "--- dbt build ---"
dbt build --exclude total_products_order

echo ""
echo "--- dbt test ---"
dbt test --exclude total_products_order

echo ""
echo "=== dbt Gold Layer complete ==="
