#!/bin/bash
set -e

echo "=== Setting up dvdrental database ==="

# Download dvdrental backup
echo "Downloading dvdrental.zip..."
apt-get install -y wget unzip 2>/dev/null || true
wget -q -O /tmp/dvdrental.zip "https://www.postgresqltutorial.com/wp-content/uploads/2019/05/dvdrental.zip"
unzip -o /tmp/dvdrental.zip -d /tmp/
echo "Restoring dvdrental into database '${POSTGRES_DB}'..."
pg_restore -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" /tmp/dvdrental.tar
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
