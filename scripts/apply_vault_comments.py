#!/usr/bin/env python3
"""
Apply descriptions (COMMENT ON TABLE / ALTER COLUMN COMMENT) to all existing
vault tables using the enriched dv_model.json.

Run after any vault pipeline run to keep metadata in sync:
    set -a && source .env && set +a
    python3 scripts/apply_vault_comments.py
"""
import json
import os
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG    = "workspace"
SCHEMA     = "vault"
MODEL_PATH = Path(__file__).parent.parent / "pipeline_configs/datavault/dv_model.json"

# ── Standard column comments ─────────────────────────────────────────────────
STANDARD_COMMENTS = {
    "LOAD_DATE":      "Timestamp when this record was first loaded into the vault",
    "RECORD_SOURCE":  "Source system identifier for this record (e.g. cdc.dvdrental.table)",
    "DIFF_HK":        "SHA-256 differential hash key for satellite change detection",
    "SNAPSHOT_DATE":  "Date of this Point-in-Time snapshot row",
}


def run_sql(w: WorkspaceClient, wh_id: str, sql: str) -> bool:
    stmt = w.statement_execution.execute_statement(
        warehouse_id=wh_id, statement=sql, wait_timeout="30s"
    )
    while stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        stmt = w.statement_execution.get_statement(stmt.statement_id)
    if stmt.status.state != StatementState.SUCCEEDED:
        print(f"    WARN: {stmt.status.error}")
        return False
    return True


def apply_table_comment(w, wh_id, table_fqn: str, comment: str):
    safe = comment.replace("'", "''")
    run_sql(w, wh_id, f"COMMENT ON TABLE {table_fqn} IS '{safe}'")


def apply_column_comment(w, wh_id, table_fqn: str, col: str, comment: str):
    safe = comment.replace("'", "''")
    run_sql(w, wh_id, f"ALTER TABLE {table_fqn} ALTER COLUMN {col} COMMENT '{safe}'")


def apply_standard_columns(w, wh_id, table_fqn: str, cols_in_table: list[str]):
    for col, comment in STANDARD_COMMENTS.items():
        if col in cols_in_table:
            apply_column_comment(w, wh_id, table_fqn, col, comment)


def get_columns(w, wh_id, table_fqn: str) -> list[str]:
    stmt = w.statement_execution.execute_statement(
        warehouse_id=wh_id,
        statement=f"DESCRIBE TABLE {table_fqn}",
        wait_timeout="30s",
    )
    while stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        stmt = w.statement_execution.get_statement(stmt.statement_id)
    if stmt.status.state != StatementState.SUCCEEDED or not stmt.result:
        return []
    return [row[0] for row in (stmt.result.data_array or []) if row[0] and not row[0].startswith("#")]


def main():
    w     = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
    wh_id = next(wh.id for wh in w.warehouses.list())

    with open(MODEL_PATH) as f:
        model = json.load(f)

    # ── Hubs ─────────────────────────────────────────────────────────────────
    print("=== Hubs ===")
    for hub in model["hubs"]:
        tbl  = f"{CATALOG}.{SCHEMA}.{hub['name'].lower()}"
        cols = get_columns(w, wh_id, tbl)
        if not cols:
            print(f"  {hub['name']}: table not found, skipping")
            continue

        apply_table_comment(w, wh_id, tbl, hub.get("description", hub["name"]))

        hk = "HK_" + hub["name"][4:]
        bk = ", ".join(hub["business_key_columns"])
        apply_column_comment(w, wh_id, tbl, hk,
                             f"SHA-256 hash key derived from: {bk}")
        for c in hub["business_key_columns"]:
            if c in cols:
                apply_column_comment(w, wh_id, tbl, c,
                                     f"Business key for {hub['name']} (source column: {c})")
        apply_standard_columns(w, wh_id, tbl, cols)
        print(f"  {hub['name']} ✓")

    # ── Links ─────────────────────────────────────────────────────────────────
    print("\n=== Links ===")
    for lnk in model["links"]:
        tbl  = f"{CATALOG}.{SCHEMA}.{lnk['name'].lower()}"
        cols = get_columns(w, wh_id, tbl)
        if not cols:
            print(f"  {lnk['name']}: table not found, skipping")
            continue

        apply_table_comment(w, wh_id, tbl, lnk.get("description", lnk["name"]))

        lnk_hk  = "HK_" + lnk["name"][4:]
        fk_src  = ", ".join(r["source_column"] for r in lnk["hub_references"])
        apply_column_comment(w, wh_id, tbl, lnk_hk,
                             f"SHA-256 composite hash key derived from: {fk_src}")

        ref_descs = lnk.get("hub_ref_descriptions", {})
        for ref in lnk["hub_references"]:
            fk_col = "HK_" + ref["hub"][4:]
            desc   = ref_descs.get(ref["hub"],
                                    f"FK ref to {ref['hub']} via {ref['source_column']}")
            if fk_col in cols:
                apply_column_comment(w, wh_id, tbl, fk_col, desc)

        apply_standard_columns(w, wh_id, tbl, cols)
        print(f"  {lnk['name']} ✓")

    # ── Satellites ────────────────────────────────────────────────────────────
    print("\n=== Satellites ===")
    for sat in model["satellites"]:
        tbl  = f"{CATALOG}.{SCHEMA}.{sat['name'].lower()}"
        cols = get_columns(w, wh_id, tbl)
        if not cols:
            print(f"  {sat['name']}: table not found, skipping")
            continue

        apply_table_comment(w, wh_id, tbl, sat.get("description", sat["name"]))

        hk = "HK_" + sat["parent_hub"][4:]
        apply_column_comment(w, wh_id, tbl, hk,
                             f"Foreign hash key reference to parent hub {sat['parent_hub']}")

        tracked_list = ", ".join(sat["tracked_columns"])
        if "DIFF_HK" in cols:
            apply_column_comment(w, wh_id, tbl, "DIFF_HK",
                                 f"SHA-256 differential hash for change detection across: {tracked_list}")

        col_descs = sat.get("column_descriptions", {})
        for c in sat["tracked_columns"]:
            if c in cols:
                desc = col_descs.get(c, f"Tracked attribute: {c}")
                apply_column_comment(w, wh_id, tbl, c, desc)

        apply_standard_columns(w, wh_id, tbl, cols)
        print(f"  {sat['name']} ✓")

    # ── PIT tables ────────────────────────────────────────────────────────────
    print("\n=== PIT Tables ===")
    for pit in model["pit_tables"]:
        tbl  = f"{CATALOG}.{SCHEMA}.{pit['name'].lower()}"
        cols = get_columns(w, wh_id, tbl)
        if not cols:
            print(f"  {pit['name']}: table not found, skipping")
            continue

        apply_table_comment(w, wh_id, tbl, pit.get("description", pit["name"]))

        hk = "HK_" + pit["hub"][4:]
        if hk in cols:
            apply_column_comment(w, wh_id, tbl, hk,
                                 f"Hub hash key reference to {pit['hub']}")
        if "SNAPSHOT_DATE" in cols:
            apply_column_comment(w, wh_id, tbl, "SNAPSHOT_DATE",
                                 "Date of this Point-in-Time snapshot row")
        if "LOAD_DATE" in cols:
            apply_column_comment(w, wh_id, tbl, "LOAD_DATE",
                                 "Timestamp when this PIT row was generated")

        for sat_name in pit["satellites"]:
            ldts_col = sat_name + "_LDTS"
            hk_col   = sat_name + "_HK"
            if ldts_col in cols:
                apply_column_comment(w, wh_id, tbl, ldts_col,
                                     f"LOAD_DATE of the latest {sat_name} record as of SNAPSHOT_DATE")
            if hk_col in cols:
                apply_column_comment(w, wh_id, tbl, hk_col,
                                     f"DIFF_HK of the latest {sat_name} record as of SNAPSHOT_DATE")
        print(f"  {pit['name']} ✓")

    # ── Bridge tables ─────────────────────────────────────────────────────────
    print("\n=== Bridge Tables ===")
    for brg in model["bridge_tables"]:
        tbl  = f"{CATALOG}.{SCHEMA}.{brg['name'].lower()}"
        cols = get_columns(w, wh_id, tbl)
        if not cols:
            print(f"  {brg['name']}: table not found, skipping")
            continue

        apply_table_comment(w, wh_id, tbl, brg.get("description", brg["name"]))

        for node in brg["path"]:
            if node.startswith("HUB_"):
                hk_col = "HK_" + node[4:]
                if hk_col in cols:
                    apply_column_comment(w, wh_id, tbl, hk_col,
                                         f"Hub hash key for {node}")
        if "LOAD_DATE" in cols:
            apply_column_comment(w, wh_id, tbl, "LOAD_DATE",
                                 "Timestamp when this bridge record was generated")
        print(f"  {brg['name']} ✓")

    print("\nAll vault comments applied.")


if __name__ == "__main__":
    main()
