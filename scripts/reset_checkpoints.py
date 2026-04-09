#!/usr/bin/env python3
"""
reset_checkpoints.py — Delete Silver (and optionally Bronze) streaming checkpoints
from the Databricks Unity Catalog Volume before a test full-reload.

Why this is needed
------------------
Silver uses Spark Structured Streaming (trigger=availableNow) backed by Delta
checkpoints stored in a Unity Catalog Volume.  The checkpoint records the exact
Delta table version processed for each Bronze source table.  When Bronze tables
are truncated and reloaded (common in dev / E2E testing), the checkpoint is
*ahead* of the new data, so subsequent Silver runs see 0 new rows and Silver
tables remain empty.

Deleting the checkpoints forces the next Silver run to start from
startingVersion=0 of each Bronze table and process all rows from scratch.

Usage
-----
    # Dry-run (shows what would be deleted, no actual deletion)
    python3 scripts/reset_checkpoints.py --dry-run

    # Delete Silver checkpoints for the current checkpoint suffix (from .env or arg)
    python3 scripts/reset_checkpoints.py

    # Delete Silver checkpoints for a specific suffix
    python3 scripts/reset_checkpoints.py --checkpoint-suffix v8

    # Also delete Bronze Auto Loader checkpoints (full E2E reset)
    python3 scripts/reset_checkpoints.py --include-bronze

    # Delete ALL checkpoint suffixes found on the Volume
    python3 scripts/reset_checkpoints.py --all-suffixes

    # After reset, immediately run Silver + Vault via the Orchestrator
    python3 scripts/reset_checkpoints.py --run

Typical E2E test reset workflow
---------------------------------
    # 1. Restart Docker stack and reload Postgres / Debezium / kafka-to-volume
    docker compose down -v && docker compose up -d

    # 2. Truncate Bronze tables (if needed) and let bronze job fill them
    #    — OR — just let kafka-to-volume + Bronze job run normally

    # 3. Reset Silver checkpoints so Silver re-reads all Bronze data
    set -a && source .env && set +a
    python3 scripts/reset_checkpoints.py --checkpoint-suffix v8 --include-bronze

    # 4. Run the full pipeline
    python3 scripts/deploy_job.py --checkpoint-suffix v8
    python3 -c "
    import os; from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=os.environ['DATABRICKS_HOST'], token=os.environ['DATABRICKS_TOKEN'])
    r = w.jobs.run_now(job_id=684287727358557)  # dvdrental-orchestrator
    print('Orchestrator run:', r.run_id)
    "
"""

import argparse
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

VOLUME_BASE = "/Volumes/workspace/default/mnt"
CHECKPOINT_ROOT_PREFIX = f"{VOLUME_BASE}/checkpoints"

# Silver checkpoint names match pipeline_configs/silver/dvdrental/*.json checkpoint_name fields
SILVER_CHECKPOINT_NAMES = [
    "actor_silver_generic",
    "address_silver_generic",
    "category_silver_generic",
    "city_silver_generic",
    "country_silver_generic",
    "customer_silver_generic",
    "film_silver_generic",
    "film_actor_silver_generic",
    "film_category_silver_generic",
    "inventory_silver_generic",
    "language_silver_generic",
    "payment_silver_generic",
    "rental_silver_generic",
    "staff_silver_generic",
    "store_silver_generic",
]

# Bronze Auto Loader checkpoints
BRONZE_CHECKPOINT_NAMES = [
    "bronze_cdc",
    "bronze_autoloader_schema",
]


def _delete_dir_recursive(w: WorkspaceClient, path: str, dry_run: bool) -> int:
    """Recursively delete a Volume directory. Returns number of files deleted."""
    deleted = 0
    try:
        items = list(w.files.list_directory_contents(path))
    except NotFound:
        return 0

    for item in items:
        item_path = item.path
        if item_path.endswith("/"):
            # directory
            deleted += _delete_dir_recursive(w, item_path.rstrip("/"), dry_run)
        else:
            if dry_run:
                print(f"  [dry-run] would delete: {item_path}")
            else:
                w.files.delete(item_path)
                print(f"  deleted: {item_path}")
            deleted += 1

    return deleted


def reset_checkpoints(
    w: WorkspaceClient,
    checkpoint_suffix: str,
    include_bronze: bool = False,
    dry_run: bool = False,
) -> None:
    checkpoint_root = f"{CHECKPOINT_ROOT_PREFIX}_{checkpoint_suffix}"
    print(f"Checkpoint root: {checkpoint_root}")

    names_to_delete = list(SILVER_CHECKPOINT_NAMES)
    if include_bronze:
        names_to_delete += BRONZE_CHECKPOINT_NAMES

    total_deleted = 0
    for name in names_to_delete:
        path = f"{checkpoint_root}/{name}"
        n = _delete_dir_recursive(w, path, dry_run)
        if n:
            print(f"  {'[dry-run] ' if dry_run else ''}  {name}: {n} file(s) removed")
        else:
            print(f"  {name}: already empty / not found")
        total_deleted += n

    action = "Would delete" if dry_run else "Deleted"
    print(f"\n{action} {total_deleted} file(s) from {checkpoint_root}")


def find_all_suffixes(w: WorkspaceClient) -> list[str]:
    """Find all checkpoint_vN directories on the Volume."""
    suffixes = []
    try:
        items = list(w.files.list_directory_contents(VOLUME_BASE))
        for item in items:
            name = item.path.rstrip("/").split("/")[-1]
            if name.startswith("checkpoints_"):
                suffixes.append(name[len("checkpoints_"):])
    except NotFound:
        pass
    return suffixes


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete Silver streaming checkpoints for E2E test reset")
    parser.add_argument("--checkpoint-suffix", default=None,
                        help="Checkpoint suffix to reset (e.g. v8). Reads CHECKPOINT_SUFFIX env var if not set.")
    parser.add_argument("--all-suffixes", action="store_true",
                        help="Reset ALL checkpoint_vN directories found on the Volume")
    parser.add_argument("--include-bronze", action="store_true",
                        help="Also delete Bronze Auto Loader checkpoints (full E2E reset)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be deleted without deleting anything")
    parser.add_argument("--run", action="store_true",
                        help="After reset, trigger the dvdrental-orchestrator job")
    args = parser.parse_args()

    host  = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in the environment.")
        sys.exit(1)

    w = WorkspaceClient(host=host, token=token)

    if args.all_suffixes:
        suffixes = find_all_suffixes(w)
        if not suffixes:
            print("No checkpoint directories found.")
            return
        print(f"Found suffixes: {suffixes}")
    else:
        suffix = args.checkpoint_suffix or os.environ.get("CHECKPOINT_SUFFIX")
        if not suffix:
            print("ERROR: Provide --checkpoint-suffix or set CHECKPOINT_SUFFIX env var.")
            sys.exit(1)
        suffixes = [suffix]

    for suffix in suffixes:
        print(f"\n=== Resetting checkpoints_{suffix} ===")
        reset_checkpoints(w, suffix, include_bronze=args.include_bronze, dry_run=args.dry_run)

    if args.run and not args.dry_run:
        ORCHESTRATOR_JOB_ID = 684287727358557
        run = w.jobs.run_now(job_id=ORCHESTRATOR_JOB_ID)
        host_clean = host.rstrip("/")
        print(f"\nOrchestrator triggered: run_id={run.run_id}")
        print(f"  {host_clean}/#job/{ORCHESTRATOR_JOB_ID}/run/{run.run_id}")


if __name__ == "__main__":
    main()
