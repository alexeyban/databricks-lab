#!/usr/bin/env python3
"""
Drop all vault tables (hub_*, lnk_*, sat_*, pit_*, brg_*) in workspace.vault
so the next vault job run performs a full initial load.

Usage:
    set -a && source .env && set +a
    python3 scripts/reset_vault.py [--catalog CATALOG] [--vault-schema SCHEMA]
"""

import argparse
import json
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

MODEL_PATH = Path(__file__).parent.parent / "pipeline_configs/datavault/dv_model.json"

# Tables from old model versions (ORDERS/PRODUCTS) no longer in dv_model.json.
# Drop them explicitly so stale data doesn't remain in the vault schema.
LEGACY_TABLES = [
    "hub_orders",
    "hub_products",
    "lnk_orders_products",
    "sat_orders_core",
    "sat_orders_pricing",
    "sat_products_core",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--vault-schema", default="vault")
    args = parser.parse_args()

    catalog = args.catalog
    schema  = args.vault_schema

    model = json.loads(MODEL_PATH.read_text())
    tables = (
        [h["name"].lower() for h in model["hubs"]] +
        [l["name"].lower() for l in model["links"]] +
        [s["name"].lower() for s in model["satellites"]] +
        [p["name"].lower() for p in model.get("pit_tables", [])] +
        [b["name"].lower() for b in model.get("bridge_tables", [])] +
        LEGACY_TABLES
    )

    print(f"Will drop {len(tables)} tables from {catalog}.{schema}:")
    for t in tables:
        print(f"  {catalog}.{schema}.{t}")

    print(f"\nDropping {len(tables)} tables from {catalog}.{schema}...")

    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    dropped = 0
    skipped = 0
    for table_name in tables:
        full_name = f"{catalog}.{schema}.{table_name}"
        try:
            w.tables.delete(full_name=full_name)
            print(f"  Dropped  {full_name}")
            dropped += 1
        except Exception as e:
            msg = str(e)
            if "NOT_FOUND" in msg or "does not exist" in msg.lower() or "404" in msg:
                print(f"  Skipped  {full_name}  (not found)")
                skipped += 1
            else:
                print(f"  ERROR    {full_name}: {e}", file=sys.stderr)

    print(f"\nDone: {dropped} dropped, {skipped} skipped (not found).")
    print("Run the vault job to perform the full initial load.")


if __name__ == "__main__":
    main()
