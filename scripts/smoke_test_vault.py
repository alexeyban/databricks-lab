#!/usr/bin/env python3
"""
Vault smoke test — runs hub + satellite ingestion against existing Silver tables
(silver_film, silver_rental, silver_payment) via Databricks SQL execution API.

Usage:
    set -a && source .env && set +a
    python3 scripts/smoke_test_vault.py
"""

import json
import os
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = "workspace"
VAULT_SCHEMA = "vault"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "53165753164ae80e")
MODEL_PATH = Path(__file__).parent.parent / "pipeline_configs/datavault/dv_model.json"

# Only entities backed by existing Silver tables
ACTIVE_SILVER = {
    "silver.silver_film",
    "silver.silver_rental",
    "silver.silver_payment",
}

# Actual columns in each Silver table (verified from Databricks)
SILVER_COLUMNS = {
    "silver.silver_film": [
        "film_id", "title", "description", "release_year", "language_id",
        "rental_duration", "rental_rate", "length", "replacement_cost",
        "rating", "last_update", "last_inserted_dt", "last_updated_dt",
    ],
    "silver.silver_rental": [
        "rental_id", "rental_date", "inventory_id", "customer_id",
        "return_date", "staff_id", "last_update", "last_inserted_dt", "last_updated_dt",
    ],
    "silver.silver_payment": [
        "payment_id", "customer_id", "staff_id", "rental_id",
        "amount", "payment_date", "last_inserted_dt", "last_updated_dt",
    ],
}


def sql(client: WorkspaceClient, stmt: str, label: str = "") -> list:
    """Execute a SQL statement and return data rows."""
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=stmt,
        wait_timeout="50s",
    )
    if result.status.state != StatementState.SUCCEEDED:
        err = result.status.error
        raise RuntimeError(
            f"SQL failed [{label}]: {err.message if err else result.status.state}\n{stmt}"
        )
    return result.result.data_array or []


def hash_key_expr(cols: list[str]) -> str:
    """Spark SQL SHA-256 hash key expression for given columns."""
    parts = ", ".join(
        f"upper(trim(cast(`{c}` as string)))" for c in cols
    )
    return f"sha2(concat_ws('||', {parts}), 256)"


def create_hub(client: WorkspaceClient, hub: dict) -> None:
    """CREATE OR REPLACE TABLE for a hub."""
    name = hub["name"].lower()
    hk = f"HK_{hub['name'][4:]}"  # HUB_FILM -> HK_FILM
    bk_cols = hub["business_key_columns"]
    bk_ddl = "\n".join(f"  `{c}` STRING," for c in bk_cols)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{VAULT_SCHEMA}.{name} (
  `{hk}` STRING NOT NULL,
{bk_ddl}
  LOAD_DATE TIMESTAMP,
  RECORD_SOURCE STRING,
  CONSTRAINT pk_{name} PRIMARY KEY (`{hk}`)
) USING DELTA
"""
    sql(client, ddl, f"create_hub_{name}")


def load_hub(client: WorkspaceClient, hub: dict) -> int:
    """MERGE Silver → Hub, return inserted row count."""
    name = hub["name"].lower()
    hk = f"HK_{hub['name'][4:]}"
    bk_cols = hub["business_key_columns"]
    src = hub["source_table"]
    load_dt = hub["load_date_column"]
    rec_src = hub["record_source"]
    hk_expr = hash_key_expr(bk_cols)
    bk_select = ", ".join(f"cast(`{c}` as string) as `{c}`" for c in bk_cols)

    merge = f"""
MERGE INTO {CATALOG}.{VAULT_SCHEMA}.{name} tgt
USING (
  SELECT {hk_expr} as `{hk}`,
         {bk_select},
         cast(`{load_dt}` as timestamp) as LOAD_DATE,
         '{rec_src}' as RECORD_SOURCE
  FROM {CATALOG}.{src}
) src ON tgt.`{hk}` = src.`{hk}`
WHEN NOT MATCHED THEN INSERT *
"""
    sql(client, merge, f"load_hub_{name}")
    rows = sql(client, f"SELECT count(*) FROM {CATALOG}.{VAULT_SCHEMA}.{name}", f"count_{name}")
    return int(rows[0][0]) if rows else 0


def create_sat(client: WorkspaceClient, sat: dict) -> None:
    """CREATE TABLE IF NOT EXISTS for a satellite."""
    name = sat["name"].lower()
    parent_hub = sat["parent_hub"]
    hk = f"HK_{parent_hub[4:]}"  # HUB_FILM -> HK_FILM

    # Filter tracked cols to those that exist in the Silver table
    src = sat["source_table"]
    available = set(SILVER_COLUMNS.get(src, []))
    tracked = [c for c in sat["tracked_columns"] if c in available]
    if not tracked:
        print(f"  SKIP SAT {name}: no tracked columns available in {src}")
        return

    col_ddl = "\n".join(f"  `{c}` STRING," for c in tracked)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{VAULT_SCHEMA}.{name} (
  `{hk}` STRING NOT NULL,
  LOAD_DATE TIMESTAMP,
  DIFF_HK STRING,
{col_ddl}
  RECORD_SOURCE STRING
) USING DELTA
"""
    sql(client, ddl, f"create_sat_{name}")


def load_sat(client: WorkspaceClient, sat: dict) -> int:
    """Append-only satellite load using DIFF_HK change detection."""
    name = sat["name"].lower()
    parent_hub = sat["parent_hub"]
    hk_col = f"HK_{parent_hub[4:]}"
    bk_col = sat["hub_key_source_column"]
    src = sat["source_table"]
    load_dt = sat["load_date_column"]
    rec_src = sat["record_source"]

    available = set(SILVER_COLUMNS.get(src, []))
    tracked = [c for c in sat["tracked_columns"] if c in available]
    if not tracked:
        return 0

    hk_expr = hash_key_expr([bk_col])
    # DIFF_HK over all tracked columns
    diff_parts = ", ".join(
        f"coalesce(cast(`{c}` as string), 'NULL')" for c in tracked
    )
    diff_expr = f"sha2(concat_ws('||', {diff_parts}), 256)"
    tracked_select = ", ".join(f"cast(`{c}` as string) as `{c}`" for c in tracked)

    merge = f"""
MERGE INTO {CATALOG}.{VAULT_SCHEMA}.{name} tgt
USING (
  SELECT {hk_expr} as `{hk_col}`,
         cast(`{load_dt}` as timestamp) as LOAD_DATE,
         {diff_expr} as DIFF_HK,
         {tracked_select},
         '{rec_src}' as RECORD_SOURCE
  FROM {CATALOG}.{src}
) src
ON tgt.`{hk_col}` = src.`{hk_col}` AND tgt.DIFF_HK = src.DIFF_HK
WHEN NOT MATCHED THEN INSERT *
"""
    sql(client, merge, f"load_sat_{name}")
    rows = sql(client, f"SELECT count(*) FROM {CATALOG}.{VAULT_SCHEMA}.{name}", f"count_{name}")
    return int(rows[0][0]) if rows else 0


def run_link_test(client: WorkspaceClient, model: dict) -> None:
    """Quick test: create + load LNK_FILM_RENTAL (rental links film to rental via inventory)."""
    # Find the rental→film link if it exists
    target_link = None
    for lnk in model.get("links", []):
        if lnk["name"] == "LNK_PAYMENT_RENTAL":
            target_link = lnk
            break
    if not target_link:
        print("  SKIP link test: LNK_PAYMENT_RENTAL not found in model")
        return

    name = target_link["name"].lower()
    lhk_expr = hash_key_expr(
        [hr["source_column"] for hr in target_link["hub_references"]]
    )
    hub_fk_ddl = "\n".join(
        f"  HK_{hr['hub'][4:]} STRING," for hr in target_link["hub_references"]
    )
    ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{VAULT_SCHEMA}.{name} (
  LHK_{target_link['name'][4:]} STRING NOT NULL,
{hub_fk_ddl}
  LOAD_DATE TIMESTAMP,
  RECORD_SOURCE STRING
) USING DELTA
"""
    sql(client, ddl, f"create_{name}")

    hub_fk_select = "\n".join(
        f"sha2(concat_ws('||', upper(trim(cast(`{hr['source_column']}` as string)))), 256) as HK_{hr['hub'][4:]},"
        for hr in target_link["hub_references"]
    )
    src_tbl = target_link["source_table"]
    load_dt_col = target_link["load_date_column"]
    rec_src = target_link["record_source"]

    merge = f"""
MERGE INTO {CATALOG}.{VAULT_SCHEMA}.{name} tgt
USING (
  SELECT {lhk_expr} as LHK_{target_link['name'][4:]},
         {hub_fk_select}
         cast(`{load_dt_col}` as timestamp) as LOAD_DATE,
         '{rec_src}' as RECORD_SOURCE
  FROM {CATALOG}.{src_tbl}
) src ON tgt.LHK_{target_link['name'][4:]} = src.LHK_{target_link['name'][4:]}
WHEN NOT MATCHED THEN INSERT *
"""
    sql(client, merge, f"load_{name}")
    rows = sql(client, f"SELECT count(*) FROM {CATALOG}.{VAULT_SCHEMA}.{name}", f"count_{name}")
    count = int(rows[0][0]) if rows else 0
    print(f"  {target_link['name']}: {count:,} rows")


def main():
    print("=" * 60)
    print("Vault Smoke Test")
    print(f"Catalog: {CATALOG}  Schema: {VAULT_SCHEMA}  WH: {WAREHOUSE_ID}")
    print("=" * 60)

    client = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    model = json.loads(MODEL_PATH.read_text())

    # --- Filter active hubs / sats ---
    active_hubs = [h for h in model["hubs"] if h["source_table"] in ACTIVE_SILVER]
    active_sats = [s for s in model["satellites"] if s["source_table"] in ACTIVE_SILVER]

    print(f"\nActive hubs: {len(active_hubs)}  Active sats: {len(active_sats)}")

    # --- Hubs ---
    print("\n[1/3] Loading Hubs...")
    hub_counts = {}
    for hub in active_hubs:
        t0 = time.time()
        create_hub(client, hub)
        count = load_hub(client, hub)
        hub_counts[hub["name"]] = count
        elapsed = time.time() - t0
        print(f"  {hub['name']}: {count:,} rows  ({elapsed:.1f}s)")

    # --- Satellites ---
    print("\n[2/3] Loading Satellites...")
    sat_counts = {}
    for sat in active_sats:
        t0 = time.time()
        create_sat(client, sat)
        count = load_sat(client, sat)
        sat_counts[sat["name"]] = count
        elapsed = time.time() - t0
        if count > 0:
            print(f"  {sat['name']}: {count:,} rows  ({elapsed:.1f}s)")

    # --- Sample link ---
    print("\n[3/3] Testing one Link...")
    run_link_test(client, model)

    # --- Summary ---
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print("\nHubs:")
    for name, cnt in hub_counts.items():
        status = "✓" if cnt > 0 else "✗ EMPTY"
        print(f"  {status}  {name}: {cnt:,}")
    print("\nSatellites:")
    for name, cnt in sat_counts.items():
        status = "✓" if cnt > 0 else "✗ EMPTY"
        print(f"  {status}  {name}: {cnt:,}")

    failures = [n for n, c in {**hub_counts, **sat_counts}.items() if c == 0]
    if failures:
        print(f"\nFAILED (0 rows): {failures}")
        sys.exit(1)
    else:
        print(f"\nAll {len(hub_counts) + len(sat_counts)} vault tables loaded successfully.")


if __name__ == "__main__":
    main()
