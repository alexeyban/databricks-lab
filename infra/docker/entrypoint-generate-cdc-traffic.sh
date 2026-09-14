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

# Each generator's stdout/stderr is prefixed with its name so lines from both
# processes are distinguishable in `docker compose logs generate-cdc-traffic`.
python3 -u generators/load_products_generator.py 2>&1 | python3 -u -c "
import sys
for line in sys.stdin:
    sys.stdout.write('[products] ' + line)
    sys.stdout.flush()
" &
PID_PRODUCTS=$!

python3 -u generators/load_generator.py 2>&1 | python3 -u -c "
import sys
for line in sys.stdin:
    sys.stdout.write('[rentals]  ' + line)
    sys.stdout.flush()
" &
PID_RENTALS=$!

echo "  load_products_generator PID=$PID_PRODUCTS"
echo "  load_generator          PID=$PID_RENTALS"

wait $PID_PRODUCTS $PID_RENTALS
