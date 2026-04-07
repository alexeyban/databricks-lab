#!/usr/bin/env python3
"""
Push .env secrets to Databricks Secret Scope 'dvdrental'.

Creates the scope if it doesn't exist (idempotent), then writes each key that
has a non-empty value.  Run once after filling in .env, and again whenever
credentials change.

Key naming convention: ENV_VAR_NAME → env-var-name  (lowercase, _ → -)

Usage:
    set -a && source .env && set +a
    python3 scripts/push_secrets_to_databricks.py
"""
import os

from dotenv import dotenv_values
from databricks.sdk import WorkspaceClient

SCOPE = "dvdrental"

# Keys to push (skip DATABRICKS_HOST / DATABRICKS_TOKEN — those authenticate
# the SDK itself and are not useful inside the scope).
KEYS_TO_PUSH = [
    "DATABRICKS_WAREHOUSE_ID",
    "KAFKA_EXTERNAL_HOST",
    "KAFKA_EXTERNAL_PORT",
    "PGHOST",
    "PGPORT",
    "PGDATABASE",
    "PGUSER",
    "PGPASSWORD",
    "LLM_PROVIDER",
    "LLM_API_KEY",
    "LLM_BASE_URL",
    "LLM_MODEL",
    "LLM_MAX_TOKENS",
    "GENIE_SPACE_ID",
]


def _secret_key(env_key: str) -> str:
    """Convert ENV_VAR_NAME to secret-key-name."""
    return env_key.lower().replace("_", "-")


def main() -> None:
    env = dotenv_values(".env")
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    # Create scope (idempotent)
    existing_scopes = [s.name for s in w.secrets.list_scopes()]
    if SCOPE not in existing_scopes:
        w.secrets.create_scope(scope=SCOPE)
        print(f"Created scope '{SCOPE}'")
    else:
        print(f"Scope '{SCOPE}' already exists")

    # Push each key that has a non-empty value
    pushed, skipped = 0, 0
    for key in KEYS_TO_PUSH:
        value = env.get(key, "").strip()
        if value:
            secret_key = _secret_key(key)
            w.secrets.put_secret(scope=SCOPE, key=secret_key, string_value=value)
            print(f"  pushed  {key!s:<30} → {SCOPE}/{secret_key}")
            pushed += 1
        else:
            skipped += 1

    print(f"\nDone: {pushed} pushed, {skipped} skipped (empty or missing in .env).")
    print(f"\nUse in notebooks:")
    print(f"  dbutils.secrets.get('{SCOPE}', '<secret-key-name>')")
    print(f"\nList all secrets in this scope:")
    print(f"  dbutils.secrets.list('{SCOPE}')")


if __name__ == "__main__":
    main()
