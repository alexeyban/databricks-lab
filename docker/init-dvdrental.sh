#!/bin/bash
set -e

echo "=== Setting up dvdrental database ==="

# Restore dvdrental from pre-downloaded SQL dump (mounted at /tmp/dvdrental.sql)
# Strip lines not supported by PG15 (e.g. transaction_timeout from PG16+ dumps)
echo "Restoring dvdrental into database '${POSTGRES_DB}'..."
grep -v "transaction_timeout" /tmp/dvdrental.sql \
  | psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" --set ON_ERROR_STOP=off -q
echo "dvdrental restored successfully"

# Set up logical replication publication and slot
psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_slot'
  ) THEN
    PERFORM pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
    RAISE NOTICE 'Replication slot debezium_slot created';
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'
  ) THEN
    CREATE PUBLICATION dbz_publication FOR ALL TABLES;
    RAISE NOTICE 'Publication dbz_publication created';
  END IF;
END;
$$;
SQL

echo "=== dvdrental setup complete ==="
