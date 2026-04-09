#!/usr/bin/env python3
"""
One-time setup for PII key store:
  - Creates 'dvdrental-dq' secret scope for master-kek (idempotent)
  - Creates per-subject-type scopes for DEKs (avoids 1000-secret-per-scope limit):
      dvdrental-dq-customer, dvdrental-dq-staff, dvdrental-dq-address
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

SCOPE   = "dvdrental-dq"         # holds master-kek only
KEK_KEY = "master-kek"
CATALOG = "workspace"

# Per-subject-type DEK scopes: each has up to 1000 DEK slots (1 per subject)
DEK_SCOPES = [
    "dvdrental-dq-customer",
    "dvdrental-dq-staff",
    "dvdrental-dq-address",
]


def _ensure_scope(w: WorkspaceClient, scope: str, existing: list[str]) -> None:
    if scope not in existing:
        w.secrets.create_scope(scope=scope)
        print(f"  Created scope '{scope}'")
    else:
        print(f"  Scope '{scope}' already exists")


def main() -> None:
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "53165753164ae80e")

    existing_scopes = [s.name for s in w.secrets.list_scopes()]

    # ── 1. Main scope (master-kek) ───────────────────────────────────────────
    print("Secret scopes:")
    _ensure_scope(w, SCOPE, existing_scopes)

    # ── 2. Per-type DEK scopes ───────────────────────────────────────────────
    for dek_scope in DEK_SCOPES:
        _ensure_scope(w, dek_scope, existing_scopes)

    # ── 3. Master KEK ────────────────────────────────────────────────────────
    print("Master KEK:")
    try:
        w.secrets.get_secret(scope=SCOPE, key=KEK_KEY)
        print(f"  '{SCOPE}/{KEK_KEY}' already exists — skipping")
    except Exception:
        kek = base64.b64encode(os.urandom(32)).decode()
        w.secrets.put_secret(scope=SCOPE, key=KEK_KEY, string_value=kek)
        print(f"  Created '{SCOPE}/{KEK_KEY}' (32-byte random KEK)")

    # ── 4. monitoring.subject_key_store ──────────────────────────────────────
    print("Delta table:")
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
    print(f"  monitoring.subject_key_store: {result.status.state}")

    print("\nPII key store setup complete.")
    print(f"  KEK scope  : {SCOPE}/{KEK_KEY}")
    print(f"  DEK scopes : {', '.join(DEK_SCOPES)}")
    print(f"  Table      : {CATALOG}.monitoring.subject_key_store")


if __name__ == "__main__":
    main()
