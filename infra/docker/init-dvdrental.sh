#!/bin/bash
set -e

echo "=== Setting up dvdrental database ==="

RESTORE_FILE=/tmp/dvdrental.tar

if [ ! -f "$RESTORE_FILE" ]; then
  echo "ERROR: $RESTORE_FILE not found. Mount dvdrental.tar into the container." >&2
  exit 1
fi

echo "Restoring dvdrental into database '${POSTGRES_DB}'..."
pg_restore -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" --no-owner --no-privileges "$RESTORE_FILE" || true

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
