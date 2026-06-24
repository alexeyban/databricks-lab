#!/usr/bin/env python3
"""
Deploy / update Databricks pipeline jobs to point at the current main branch
and the post-refactor notebook paths.

Usage:
    set -a && source .env && set +a
    python3 scripts/deploy_jobs.py [--run-orchestrator]

Environment variables required:
    DATABRICKS_HOST   - e.g. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN  - personal access token
"""

import argparse
import json
import os
import sys
import time

import requests

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

GIT_URL = "https://github.com/alexeyban/databricks-lab"
GIT_BRANCH = "feature/dbt-datavault"  # switch to main after PRs #8, #9, #10 are merged

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "53165753164ae80e")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
DV_MODEL_PATH = f"/Volumes/{CATALOG}/default/mnt/pipeline_configs/datavault/dv_model.json"

TABLES = [
    "actor", "address", "category", "city", "country",
    "customer", "film", "film_actor", "film_category",
    "inventory", "language", "payment", "rental", "staff", "store",
]

# Existing job IDs (discovered via API)
JOB_IDS = {
    "dvdrental-bronze": 325293262130713,
    "dvdrental-silver": 1099814608698427,
    "dvdrental-vault": 950203691556666,
    "dvdrental-orchestrator": 684287727358557,
}


def api(method, path, **kwargs):
    r = requests.request(method, f"{HOST}/api/2.1{path}", headers=HEADERS, **kwargs)
    r.raise_for_status()
    return r.json()


def git_source():
    return {"git_url": GIT_URL, "git_provider": "gitHub", "git_branch": GIT_BRANCH}



def reset_job(job_id, settings):
    api("POST", f"/jobs/reset", json={"job_id": job_id, "new_settings": settings})
    print(f"  updated job {job_id}")


def build_bronze_settings():
    return {
        "name": "dvdrental-bronze",
        "git_source": git_source(),
        "tasks": [{
            "task_key": "kafka_to_bronze",
            "notebook_task": {
                "notebook_path": "ingestion/consumers/NB_ingest_to_bronze",
                "base_parameters": {"CATALOG": CATALOG},
            },
        }],
    }


def build_silver_settings():
    # Split into 3 sequential batches of 5 to stay within serverless concurrency limits.
    # Batch B waits for all of batch A; batch C waits for all of batch B.
    batches = [TABLES[i:i + 5] for i in range(0, len(TABLES), 5)]
    tasks = []
    prev_batch_keys = []
    for batch in batches:
        batch_keys = []
        for table in batch:
            task = {
                "task_key": f"silver_{table}",
                "notebook_task": {
                    "notebook_path": "processing/silver/NB_process_to_silver_generic",
                    "base_parameters": {"TABLE_ID": table, "CATALOG": CATALOG},
                },
            }
            if prev_batch_keys:
                task["depends_on"] = [{"task_key": k} for k in prev_batch_keys]
            tasks.append(task)
            batch_keys.append(f"silver_{table}")
        prev_batch_keys = batch_keys
    return {
        "name": "dvdrental-silver",
        "git_source": git_source(),
        "tasks": tasks,
    }


def build_vault_settings():
    return {
        "name": "dvdrental-vault",
        "git_source": git_source(),
        "tasks": [
            {
                "task_key": "vault_hubs",
                "notebook_task": {
                    "notebook_path": "processing/vault/NB_ingest_to_hubs",
                    "base_parameters": {"CATALOG": CATALOG, "MODEL_PATH": DV_MODEL_PATH},
                },
            },
            {
                "task_key": "vault_links",
                "depends_on": [{"task_key": "vault_hubs"}],
                "notebook_task": {
                    "notebook_path": "processing/vault/NB_ingest_to_links",
                    "base_parameters": {"CATALOG": CATALOG, "MODEL_PATH": DV_MODEL_PATH},
                },
            },
            {
                "task_key": "vault_satellites",
                "depends_on": [{"task_key": "vault_hubs"}],
                "notebook_task": {
                    "notebook_path": "processing/vault/NB_ingest_to_satellites",
                    "base_parameters": {"CATALOG": CATALOG, "MODEL_PATH": DV_MODEL_PATH},
                },
            },
            {
                "task_key": "vault_business_vault",
                "depends_on": [
                    {"task_key": "vault_links"},
                    {"task_key": "vault_satellites"},
                ],
                "notebook_task": {
                    "notebook_path": "processing/vault/NB_dv_business_vault",
                    "base_parameters": {"CATALOG": CATALOG, "MODEL_PATH": DV_MODEL_PATH},
                },
            },
        ],
    }


def ensure_vault_gold_job():
    """Create or locate the dbt vault+gold job."""
    existing = api("GET", "/jobs/list")
    for j in existing.get("jobs", []):
        if j["settings"]["name"] == "dvdrental-vault-gold":
            job_id = j["job_id"]
            print(f"  dvdrental-vault-gold exists: {job_id}, updating...")
            reset_job(job_id, build_vault_gold_settings(None, None))
            return job_id

    result = api("POST", "/jobs/create", json=build_vault_gold_settings(None, None))
    job_id = result["job_id"]
    print(f"  created dvdrental-vault-gold: {job_id}")
    return job_id


def build_vault_gold_settings(silver_id, vault_id):
    # Use a notebook task instead of dbt_task — dbt_task requires a workspace
    # feature that may not be enabled. The NB_run_dbt notebook pip-installs
    # dbt-databricks and runs dbt build via subprocess.
    return {
        "name": "dvdrental-vault-gold",
        "git_source": git_source(),
        "tasks": [
            {
                "task_key": "dbt_vault_gold",
                "notebook_task": {
                    "notebook_path": "transformation/NB_run_dbt",
                    "base_parameters": {
                        "DBT_SELECT": "vault gold",
                        "CATALOG": CATALOG,
                        "WAREHOUSE_ID": WAREHOUSE_ID,
                    },
                },
            },
        ],
    }


def build_orchestrator_settings(bronze_id, silver_id, vault_id, vault_gold_id):
    """
    Full end-to-end chain:
      Bronze (availableNow trigger: drains Kafka → Bronze Delta, then stops)
        → Silver (15 notebook tasks in 3 batches of 5)
          → Vault notebooks (hubs → links/sats → business vault)
            → dbt Vault+Gold (incremental vault models then gold data marts)
    """
    return {
        "name": "dvdrental-orchestrator",
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",
            "timezone_id": "UTC",
            "pause_status": "PAUSED",
        },
        "tasks": [
            {
                "task_key": "run_bronze",
                "description": "Kafka → Bronze Delta (availableNow trigger, terminates when caught up)",
                "run_job_task": {"job_id": bronze_id},
            },
            {
                "task_key": "run_silver",
                "description": "Bronze → Silver MERGE for all 15 dvdrental tables",
                "depends_on": [{"task_key": "run_bronze"}],
                "run_job_task": {"job_id": silver_id},
            },
            {
                "task_key": "run_vault",
                "description": "Silver → Vault hubs/links/satellites/business-vault (PySpark notebooks)",
                "depends_on": [{"task_key": "run_silver"}],
                "run_job_task": {"job_id": vault_id},
            },
            {
                "task_key": "run_vault_gold",
                "description": "Vault → dbt incremental vault models + Gold data marts",
                "depends_on": [{"task_key": "run_vault"}],
                "run_job_task": {"job_id": vault_gold_id},
            },
        ],
    }


def run_job(job_id, name):
    result = api("POST", "/jobs/run-now", json={"job_id": job_id})
    run_id = result["run_id"]
    print(f"  triggered {name} (job {job_id}) -> run_id {run_id}")
    return run_id


def wait_for_run(run_id, label, poll_secs=15):
    print(f"  waiting for {label} run {run_id}...", flush=True)
    while True:
        r = api("GET", f"/jobs/runs/get?run_id={run_id}")
        state = r["state"]["life_cycle_state"]
        result_state = r["state"].get("result_state", "")
        print(f"    {state} {result_state}", flush=True)
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result_state != "SUCCESS":
                print(f"  ERROR: {label} run {run_id} finished with {result_state}")
                sys.exit(1)
            print(f"  {label} SUCCESS")
            return
        time.sleep(poll_secs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-orchestrator", action="store_true",
                        help="Trigger the orchestrator after deploying")
    parser.add_argument("--run-silver", action="store_true",
                        help="Trigger just the silver job")
    args = parser.parse_args()

    print("=== Deploying dvdrental pipeline jobs ===")

    print("\n[bronze] updating...")
    reset_job(JOB_IDS["dvdrental-bronze"], build_bronze_settings())

    print("\n[silver] updating...")
    reset_job(JOB_IDS["dvdrental-silver"], build_silver_settings())

    print("\n[vault] updating notebooks to new paths...")
    reset_job(JOB_IDS["dvdrental-vault"], build_vault_settings())

    print("\n[vault-gold dbt] creating/updating...")
    vg_id = ensure_vault_gold_job()

    print("\n[orchestrator] updating (bronze → silver → vault → vault-gold-dbt)...")
    reset_job(
        JOB_IDS["dvdrental-orchestrator"],
        build_orchestrator_settings(
            JOB_IDS["dvdrental-bronze"],
            JOB_IDS["dvdrental-silver"],
            JOB_IDS["dvdrental-vault"],
            vg_id,
        ),
    )

    print("\n=== All jobs updated ===")
    print(f"  bronze       : {JOB_IDS['dvdrental-bronze']}")
    print(f"  silver       : {JOB_IDS['dvdrental-silver']}")
    print(f"  vault        : {JOB_IDS['dvdrental-vault']}")
    print(f"  vault-gold   : {vg_id}")
    print(f"  orchestrator : {JOB_IDS['dvdrental-orchestrator']}")
    print()
    print("  Orchestrator chain:")
    print("    run_bronze → run_silver → run_vault → run_vault_gold")

    if args.run_silver:
        print("\n[run] triggering silver job...")
        run_id = run_job(JOB_IDS["dvdrental-silver"], "dvdrental-silver")
        wait_for_run(run_id, "dvdrental-silver")

    if args.run_orchestrator:
        print("\n[run] triggering orchestrator (bronze → silver → vault → vault-gold-dbt)...")
        run_id = run_job(JOB_IDS["dvdrental-orchestrator"], "dvdrental-orchestrator")
        wait_for_run(run_id, "dvdrental-orchestrator", poll_secs=30)


if __name__ == "__main__":
    main()
