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
GIT_BRANCH = "main"

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "53165753164ae80e")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")

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
                "base_parameters": {"catalog": CATALOG},
            },
        }],
    }


def build_silver_settings():
    tasks = []
    for table in TABLES:
        tasks.append({
            "task_key": f"silver_{table}",
            "notebook_task": {
                "notebook_path": "processing/silver/NB_process_to_silver_generic",
                "base_parameters": {"table_id": table, "catalog": CATALOG},
            },
        })
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
                    "base_parameters": {"catalog": CATALOG},
                },
            },
            {
                "task_key": "vault_links",
                "depends_on": [{"task_key": "vault_hubs"}],
                "notebook_task": {
                    "notebook_path": "processing/vault/NB_ingest_to_links",
                    "base_parameters": {"catalog": CATALOG},
                },
            },
            {
                "task_key": "vault_satellites",
                "depends_on": [{"task_key": "vault_hubs"}],
                "notebook_task": {
                    "notebook_path": "processing/vault/NB_ingest_to_satellites",
                    "base_parameters": {"catalog": CATALOG},
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
                    "base_parameters": {"catalog": CATALOG},
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
    return {
        "name": "dvdrental-vault-gold",
        "git_source": git_source(),
        "environments": [
            {
                "environment_key": "dbt_env",
                "spec": {
                    "client": "1",
                    "dependencies": ["dbt-databricks>=1.6.0"],
                },
            }
        ],
        "tasks": [
            {
                "task_key": "dbt_vault",
                "environment_key": "dbt_env",
                "dbt_task": {
                    "project_directory": "transformation/dbt_project",
                    "commands": ["dbt deps", "dbt build --select vault"],
                    "warehouse_id": WAREHOUSE_ID,
                    "schema": "vault",
                },
            },
            {
                "task_key": "dbt_gold",
                "environment_key": "dbt_env",
                "depends_on": [{"task_key": "dbt_vault"}],
                "dbt_task": {
                    "project_directory": "transformation/dbt_project",
                    "commands": ["dbt build --select gold"],
                    "warehouse_id": WAREHOUSE_ID,
                    "schema": "gold",
                },
            },
        ],
    }


def build_orchestrator_settings(silver_id, vault_gold_id):
    return {
        "name": "dvdrental-orchestrator",
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",
            "timezone_id": "UTC",
            "pause_status": "PAUSED",
        },
        "tasks": [
            {
                "task_key": "run_silver",
                "run_job_task": {"job_id": silver_id},
            },
            {
                "task_key": "run_vault_gold",
                "depends_on": [{"task_key": "run_silver"}],
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

    print("\n[orchestrator] updating...")
    reset_job(
        JOB_IDS["dvdrental-orchestrator"],
        build_orchestrator_settings(JOB_IDS["dvdrental-silver"], vg_id),
    )

    print("\n=== All jobs updated ===")
    print(f"  bronze       : {JOB_IDS['dvdrental-bronze']}")
    print(f"  silver       : {JOB_IDS['dvdrental-silver']}")
    print(f"  vault        : {JOB_IDS['dvdrental-vault']}")
    print(f"  vault-gold   : {vg_id}")
    print(f"  orchestrator : {JOB_IDS['dvdrental-orchestrator']}")

    if args.run_silver:
        print("\n[run] triggering silver job...")
        run_id = run_job(JOB_IDS["dvdrental-silver"], "dvdrental-silver")
        wait_for_run(run_id, "dvdrental-silver")

    if args.run_orchestrator:
        print("\n[run] triggering orchestrator (silver -> vault+gold)...")
        run_id = run_job(JOB_IDS["dvdrental-orchestrator"], "dvdrental-orchestrator")
        wait_for_run(run_id, "dvdrental-orchestrator", poll_secs=30)


if __name__ == "__main__":
    main()
