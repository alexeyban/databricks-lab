#!/usr/bin/env python3
"""
One-time setup for PII key store:
  - Creates 'dvdrental-dq' secret scope (idempotent)
  - Generates and stores master-kek (32-byte random, base64-encoded AES-256 key)
  - Ensures monitoring.subject_key_store Delta table exists

Run this before the first Silver job execution that uses PII encryption.
Safe to re-run — all operations are idempotent.

Usage:
    set -a && source .env && set +a
    python3 scripts/setup_pii_secrets.py
"""
import base64
import os

from databricks.sdk import WorkspaceClient

SCOPE   = "dvdrental-dq"
KEK_KEY = "master-kek"
CATALOG = "workspace"


def main() -> None:
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "53165753164ae80e")

    # ── 1. Secret scope ──────────────────────────────────────────────────────
    existing_scopes = [s.name for s in w.secrets.list_scopes()]
    if SCOPE not in existing_scopes:
        w.secrets.create_scope(scope=SCOPE)
        print(f"Created scope '{SCOPE}'")
    else:
        print(f"Scope '{SCOPE}' already exists")

    # ── 2. Master KEK ────────────────────────────────────────────────────────
    try:
        w.secrets.get_secret(scope=SCOPE, key=KEK_KEY)
        print(f"Secret '{SCOPE}/{KEK_KEY}' already exists — skipping")
    except Exception:
        kek = base64.b64encode(os.urandom(32)).decode()
        w.secrets.put_secret(scope=SCOPE, key=KEK_KEY, string_value=kek)
        print(f"Created '{SCOPE}/{KEK_KEY}' (32-byte random KEK)")

    # ── 3. monitoring.subject_key_store ──────────────────────────────────────
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.subject_key_store (
            subject_id    STRING,
            subject_type  STRING,
            encrypted_dek BINARY,
            kek_version   STRING,
            created_at    TIMESTAMP,
            shredded_at   TIMESTAMP
        ) USING DELTA
    """
    result = w.statement_execution.execute_statement(
        warehouse_id=wh_id,
        statement=ddl,
        wait_timeout="50s",
    )
    print(f"monitoring.subject_key_store: {result.status.state}")

    print("\nPII key store setup complete.")
    print(f"  Scope : {SCOPE}")
    print(f"  KEK   : {SCOPE}/{KEK_KEY}")
    print(f"  Table : {CATALOG}.monitoring.subject_key_store")


if __name__ == "__main__":
    main()
