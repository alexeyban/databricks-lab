# Plan: Push .env secrets to Databricks Secret Scope

## Context

Secrets currently live in a plain `.env` file. The goal is to push them into a
Databricks Secret Scope (`dvdrental`) so that notebooks can use `dbutils.secrets.get()`
instead of hardcoded credentials, and secrets are no longer needed at runtime outside
the local machine.

---

## What gets added

### 1. `scripts/push_secrets_to_databricks.py` — NEW

Reads `.env`, creates the `dvdrental` scope (idempotent), and writes each key as a
Databricks secret. Run once whenever `.env` changes.

```python
#!/usr/bin/env python3
"""Push .env secrets to Databricks Secret Scope 'dvdrental'."""
import os
from dotenv import dotenv_values
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.secrets import AclPermission

SCOPE = "dvdrental"
# Keys from .env to push (skip Databricks own credentials — not needed in scope)
KEYS_TO_PUSH = [
    "DATABRICKS_WAREHOUSE_ID",
    "KAFKA_EXTERNAL_HOST",
    "KAFKA_EXTERNAL_PORT",
    "PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD",
    "LLM_PROVIDER", "LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL",
    "GENIE_SPACE_ID",
]

def main():
    env = dotenv_values(".env")
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    # Create scope (idempotent)
    existing = [s.name for s in w.secrets.list_scopes()]
    if SCOPE not in existing:
        w.secrets.create_scope(scope=SCOPE)
        print(f"Created scope '{SCOPE}'")
    else:
        print(f"Scope '{SCOPE}' already exists")

    # Push each key that has a value
    pushed, skipped = 0, 0
    for key in KEYS_TO_PUSH:
        value = env.get(key, "").strip()
        if value:
            w.secrets.put_secret(scope=SCOPE, key=key.lower().replace("_", "-"), string_value=value)
            print(f"  ✓ {key}")
            pushed += 1
        else:
            skipped += 1

    print(f"\nDone: {pushed} pushed, {skipped} skipped (empty).")
    print(f"Use in notebooks: dbutils.secrets.get('{SCOPE}', 'key-name')")

if __name__ == "__main__":
    main()
```

Key name convention: `PGPASSWORD` → `postgres-password` (lowercase, underscores → hyphens).

Actually, simpler to keep the env var name style as lowercase-hyphen for all:
- `KAFKA_EXTERNAL_HOST` → `kafka-external-host`
- `PGPASSWORD` → `pgpassword`
- `LLM_API_KEY` → `llm-api-key`

### 2. `requirements.txt`

`python-dotenv` is already listed. No new dependencies needed — `databricks-sdk` already present.

### 3. `.envexample` — add usage note

Add a comment at the top explaining how to push to Databricks:
```
# After filling in values, push to Databricks Secret Scope:
#   set -a && source .env && set +a
#   python3 scripts/push_secrets_to_databricks.py
```

---

## Files to create / modify

| File | Action |
|------|--------|
| `scripts/push_secrets_to_databricks.py` | NEW — reads `.env`, creates scope, pushes secrets |
| `.envexample` | Add 2-line usage comment at top |

---

## Verification

```bash
set -a && source .env && set +a
python3 scripts/push_secrets_to_databricks.py

# Check via Databricks CLI or SDK:
databricks secrets list-secrets dvdrental
# Or in a notebook:
# dbutils.secrets.list("dvdrental")
```
