#!/usr/bin/env bash
set -e

echo "Waiting for PostgreSQL at ${PGHOST}:${PGPORT}..."
until python3 - <<'EOF'
import os, psycopg2
psycopg2.connect(
    host=os.environ["PGHOST"],
    port=os.environ["PGPORT"],
    dbname=os.environ["PGDATABASE"],
    user=os.environ["PGUSER"],
    password=os.environ["PGPASSWORD"],
).close()
EOF
do
    echo "  postgres not ready, retrying in 2s..."
    sleep 2
done

echo "PostgreSQL ready. Starting generators in parallel..."
python3 generators/load_products_generator.py &
PID_PRODUCTS=$!

python3 generators/load_generator.py &
PID_RENTALS=$!

echo "  load_products_generator PID=$PID_PRODUCTS"
echo "  load_generator          PID=$PID_RENTALS"

wait $PID_PRODUCTS $PID_RENTALS
