#!/usr/bin/env python3
"""
migrate_and_run.py - Full migration from orders/products to dvdrental + E2E test.

Steps:
  1. Drop legacy tables (bronze.orders, bronze.products, silver.silver_orders,
     silver.silver_products, gold.total_products_order) via Databricks SQL API.
  2. Update the 'dvdrental ingest job' from Orders-ingest-job.yaml via Jobs API
     (creates it if not found).
  3. Reset dvdrental tables and checkpoints via NB_reset_tables notebook.
  4. Start local Docker CDC stack, ensure ngrok tunnel, register Debezium
     connector, and run data generators.
  5. Trigger the dvdrental ingest job and poll until completion.

Requires DATABRICKS_HOST and DATABRICKS_TOKEN in the environment (or .env).
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    GitProvider,
    GitSource,
    JobEnvironment,
    NotebookTask,
    QueueSettings,
    Source,
    SubmitTask,
)
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]
CONNECT_URL = "http://127.0.0.1:8083/connectors"
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR", "BLOCKED"}
JOB_NAME = "dvdrental ingest job"

LEGACY_TABLES = [
    "bronze.orders",
    "bronze.products",
    "silver.silver_orders",
    "silver.silver_products",
    "gold.total_products_order",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_env() -> tuple[str, str]:
    load_dotenv(REPO_ROOT / ".env")
    host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env or environment")
    return host, token


def build_client(host: str, token: str) -> WorkspaceClient:
    return WorkspaceClient(host=host, token=token)


def enum_value(value):
    return getattr(value, "value", value)


def api_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Step 1 — Drop legacy tables via SQL Statement Execution API
# ---------------------------------------------------------------------------

def find_warehouse(host: str, token: str) -> str:
    """Return the ID of the first running or stopped SQL warehouse."""
    resp = requests.get(
        f"{host}/api/2.0/sql/warehouses",
        headers=api_headers(token),
        timeout=30,
    )
    resp.raise_for_status()
    warehouses = resp.json().get("warehouses", [])
    if not warehouses:
        raise RuntimeError("No SQL warehouses found in workspace")
    # Prefer RUNNING, fall back to any
    for wh in warehouses:
        if wh.get("state") == "RUNNING":
            return wh["id"]
    return warehouses[0]["id"]


def execute_sql(host: str, token: str, warehouse_id: str, statement: str) -> dict:
    """Execute a SQL statement synchronously and return the result."""
    payload = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
    }
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers=api_headers(token),
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    result = resp.json()

    # Poll if still pending/running
    statement_id = result.get("statement_id")
    deadline = time.time() + 120
    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() > deadline:
            raise TimeoutError(f"SQL statement {statement_id} timed out")
        time.sleep(3)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=api_headers(token),
            timeout=30,
        )
        poll.raise_for_status()
        result = poll.json()

    return result


def drop_legacy_tables(host: str, token: str, catalog: str) -> list[dict]:
    print("\n=== Step 1: Drop legacy tables ===")
    warehouse_id = find_warehouse(host, token)
    print(f"Using SQL warehouse: {warehouse_id}")

    results = []
    for table in LEGACY_TABLES:
        full_name = f"{catalog}.{table}"
        stmt = f"DROP TABLE IF EXISTS {full_name}"
        print(f"  {stmt}")
        try:
            result = execute_sql(host, token, warehouse_id, stmt)
            state = result.get("status", {}).get("state", "UNKNOWN")
            results.append({"table": full_name, "state": state})
            print(f"    → {state}")
        except Exception as exc:
            results.append({"table": full_name, "error": str(exc)})
            print(f"    → ERROR: {exc}")

    return results


# ---------------------------------------------------------------------------
# Step 2 — Update/create dvdrental ingest job via Jobs API
# ---------------------------------------------------------------------------

# Job definition built from Orders-ingest-job.yaml
def build_job_settings(kafka_bootstrap: str) -> dict:
    return {
        "name": JOB_NAME,
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "timeout_seconds": 0,
        "schedule": {
            "quartz_cron_expression": "56 4/5 * * * ?",
            "timezone_id": "Europe/Belgrade",
            "pause_status": "UNPAUSED",
        },
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Ingest_to_Bronze",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "notebooks/bronze/NB_ingest_to_bronze",
                    "base_parameters": {"KAFKA_BOOTSTRAP": kafka_bootstrap},
                    "source": "GIT",
                },
                "timeout_seconds": 0,
                "environment_key": "Default",
            },
            {
                "task_key": "ingest_Rental_To_Silver",
                "depends_on": [{"task_key": "Ingest_to_Bronze"}],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "notebooks/silver/NB_process_to_silver",
                    "source": "GIT",
                },
                "timeout_seconds": 0,
            },
            {
                "task_key": "ingest_Film_To_Silver",
                "depends_on": [{"task_key": "Ingest_to_Bronze"}],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "notebooks/silver/NB_process_products_silver",
                    "source": "GIT",
                },
                "timeout_seconds": 0,
            },
            {
                "task_key": "ingest_Payment_To_Silver",
                "depends_on": [{"task_key": "Ingest_to_Bronze"}],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "notebooks/silver/NB_process_payment_silver",
                    "source": "GIT",
                },
                "timeout_seconds": 0,
            },
        ],
        "git_source": {
            "git_url": "https://github.com/alexeyban/databricks-lab",
            "git_provider": "gitHub",
            "git_branch": "main",
        },
        "queue": {"enabled": True},
        "environments": [
            {
                "environment_key": "Default",
                "spec": {"environment_version": "4"},
            }
        ],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }


def find_job_by_name(host: str, token: str, name: str) -> int | None:
    """Return the job_id of the first job matching name, or None."""
    resp = requests.get(
        f"{host}/api/2.1/jobs/list",
        headers=api_headers(token),
        params={"name": name, "limit": 5},
        timeout=30,
    )
    resp.raise_for_status()
    jobs = resp.json().get("jobs", [])
    for job in jobs:
        if job.get("settings", {}).get("name") == name:
            return job["job_id"]
    return None


def upsert_job(host: str, token: str, kafka_bootstrap: str) -> int:
    """Create or reset the dvdrental ingest job. Returns job_id."""
    print(f"\n=== Step 2: Update/create Databricks job '{JOB_NAME}' ===")
    settings = build_job_settings(kafka_bootstrap)

    job_id = find_job_by_name(host, token, JOB_NAME)
    if job_id:
        print(f"  Found existing job {job_id} — resetting settings")
        resp = requests.post(
            f"{host}/api/2.1/jobs/reset",
            headers=api_headers(token),
            json={"job_id": job_id, "new_settings": settings},
            timeout=30,
        )
        resp.raise_for_status()
        print(f"  Job {job_id} updated")
    else:
        print("  No existing job found — creating new job")
        resp = requests.post(
            f"{host}/api/2.1/jobs/create",
            headers=api_headers(token),
            json=settings,
            timeout=30,
        )
        resp.raise_for_status()
        job_id = resp.json()["job_id"]
        print(f"  Job created: {job_id}")

    return job_id


# ---------------------------------------------------------------------------
# Step 3 — Reset dvdrental tables via NB_reset_tables
# ---------------------------------------------------------------------------

def reset_dvdrental_tables(client: WorkspaceClient, catalog: str, git_branch: str,
                            poll_seconds: int, timeout_seconds: int) -> dict:
    print("\n=== Step 3: Reset dvdrental tables + checkpoints ===")
    submitted = client.jobs.submit(
        run_name="migrate-reset-dvdrental",
        git_source=GitSource(
            git_url="https://github.com/alexeyban/databricks-lab",
            git_provider=GitProvider.GIT_HUB,
            git_branch=git_branch,
        ),
        environments=[JobEnvironment(environment_key="Default",
                                      spec=Environment(environment_version="4"))],
        queue=QueueSettings(enabled=True),
        tasks=[
            SubmitTask(
                task_key="reset_tables",
                environment_key="Default",
                notebook_task=NotebookTask(
                    notebook_path="notebooks/helpers/NB_reset_tables",
                    source=Source.GIT,
                    base_parameters={"CATALOG": catalog, "DRY_RUN": "false"},
                ),
            )
        ],
    )
    run_id = submitted.run_id
    print(f"  Reset run submitted: {run_id}")
    result = wait_for_run(client, run_id, poll_seconds, timeout_seconds)
    print(f"  Reset result: {result['result_state']}")
    return result


# ---------------------------------------------------------------------------
# Step 4 — Docker, connector, generators
# ---------------------------------------------------------------------------

def run_cmd(cmd: list[str], *, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, cwd=REPO_ROOT, env=env, check=True, text=True, capture_output=True,
    )


def wait_for_connect(timeout_seconds: int = 90) -> None:
    print("  Waiting for Kafka Connect…")
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(CONNECT_URL, timeout=2) as response:
                if response.status == 200:
                    print("  Kafka Connect is ready")
                    return
        except urllib.error.URLError as exc:
            last_error = str(exc)
        time.sleep(3)
    raise RuntimeError(f"Kafka Connect not ready after {timeout_seconds}s: {last_error}")


def register_connector() -> str:
    connector_config = (REPO_ROOT / "postgres-connector.json").read_bytes()
    request = urllib.request.Request(
        CONNECT_URL,
        data=connector_config,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return f"registered:{response.status}"
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return "already-registered"
        raise


def start_docker_and_generate(
    kafka_bootstrap: str,
    film_iterations: int,
    rental_iterations: int,
    kafka_local_port: int,
) -> str:
    print("\n=== Step 4: Docker stack + connector + generators ===")

    host, port = kafka_bootstrap.split(":", 1)
    compose_env = os.environ.copy() | {
        "KAFKA_EXTERNAL_HOST": host,
        "KAFKA_EXTERNAL_PORT": port,
    }
    print("  docker compose up -d")
    run_cmd(["docker", "compose", "up", "-d"], env=compose_env)

    wait_for_connect()

    connector_status = register_connector()
    print(f"  Connector: {connector_status}")

    print(f"  Running film generator ({film_iterations} iterations)…")
    run_cmd(
        ["python3", "generators/load_products_generator.py"],
        env=os.environ.copy() | {"ITERATIONS": str(film_iterations)},
    )

    print(f"  Running rental/payment generator ({rental_iterations} iterations)…")
    run_cmd(
        ["python3", "generators/load_generator.py"],
        env=os.environ.copy() | {"ITERATIONS": str(rental_iterations)},
    )

    return connector_status


# ---------------------------------------------------------------------------
# Step 5 — Trigger job and monitor
# ---------------------------------------------------------------------------

def wait_for_run(client: WorkspaceClient, run_id: int,
                 poll_seconds: int, timeout_seconds: int) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        run = client.jobs.get_run(run_id=run_id)
        state = enum_value(run.state.life_cycle_state)
        if state in TERMINAL_STATES:
            return {
                "run_id": run_id,
                "life_cycle_state": state,
                "result_state": enum_value(run.state.result_state),
                "state_message": run.state.state_message,
                "tasks": [
                    {
                        "task_key": t.task_key,
                        "life_cycle_state": enum_value(t.state.life_cycle_state),
                        "result_state": enum_value(t.state.result_state),
                        "state_message": t.state.state_message,
                    }
                    for t in (run.tasks or [])
                ],
            }
        print(f"    [{state}] waiting…")
        time.sleep(poll_seconds)
    raise TimeoutError(f"Run {run_id} did not finish within {timeout_seconds}s")


def trigger_job(client: WorkspaceClient, job_id: int, kafka_bootstrap: str,
                poll_seconds: int, timeout_seconds: int) -> dict:
    print(f"\n=== Step 5: Trigger dvdrental job {job_id} ===")
    run = client.jobs.run_now(
        job_id=job_id,
        notebook_params={"KAFKA_BOOTSTRAP": kafka_bootstrap},
    )
    print(f"  Run submitted: {run.run_id}")
    result = wait_for_run(client, run.run_id, poll_seconds, timeout_seconds)
    return result


# ---------------------------------------------------------------------------
# ngrok tunnel
# ---------------------------------------------------------------------------

def ensure_ngrok(local_port: int) -> str:
    """Return kafka_bootstrap (host:port) via active ngrok tunnel."""
    sys.path.insert(0, str(Path(__file__).parent))
    from prepare_ngrok_kafka import ensure_tunnel  # noqa: PLC0415

    tunnel = ensure_tunnel(
        api_url="http://127.0.0.1:4040/api/tunnels",
        local_port=local_port,
        timeout_seconds=30,
        log_path=REPO_ROOT / "logs" / "ngrok-kafka.log",
    )
    return tunnel["public_url"].removeprefix("tcp://")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate to dvdrental schema and run E2E test")
    parser.add_argument("--catalog", default="workspace", help="Unity Catalog name (default: workspace)")
    parser.add_argument("--git-branch", default="main")
    parser.add_argument("--kafka-local-port", type=int, default=9093)
    parser.add_argument("--film-iterations", type=int, default=6)
    parser.add_argument("--rental-iterations", type=int, default=12)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument(
        "--skip-legacy-drop",
        action="store_true",
        help="Skip dropping legacy orders/products tables (if they no longer exist)",
    )
    parser.add_argument(
        "--skip-reset",
        action="store_true",
        help="Skip resetting dvdrental tables (keep existing data)",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Skip Docker startup + connector registration + generators",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        help="Override kafka bootstrap (host:port). If not set, discovers via ngrok.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    host, token = load_env()
    client = build_client(host, token)

    output: dict = {"catalog": args.catalog}

    # Step 1 — drop legacy tables
    if not args.skip_legacy_drop:
        output["legacy_drop"] = drop_legacy_tables(host, token, args.catalog)
    else:
        print("Skipping legacy table drop (--skip-legacy-drop)")

    # Resolve Kafka bootstrap (needed for job settings + run trigger)
    if args.kafka_bootstrap:
        kafka_bootstrap = args.kafka_bootstrap
    else:
        print("\nDiscovering ngrok tunnel…")
        kafka_bootstrap = ensure_ngrok(args.kafka_local_port)
    print(f"Kafka bootstrap: {kafka_bootstrap}")
    output["kafka_bootstrap"] = kafka_bootstrap

    # Step 2 — update/create job
    job_id = upsert_job(host, token, kafka_bootstrap)
    output["job_id"] = job_id

    # Step 3 — reset dvdrental tables
    if not args.skip_reset:
        output["reset_run"] = reset_dvdrental_tables(
            client, args.catalog, args.git_branch, args.poll_seconds, args.timeout_seconds
        )
    else:
        print("Skipping dvdrental table reset (--skip-reset)")

    # Step 4 — Docker + connector + generators
    if not args.skip_docker:
        output["connector_status"] = start_docker_and_generate(
            kafka_bootstrap, args.film_iterations, args.rental_iterations, args.kafka_local_port
        )
    else:
        print("Skipping Docker/connector/generators (--skip-docker)")

    # Step 5 — trigger job
    job_result = trigger_job(client, job_id, kafka_bootstrap, args.poll_seconds, args.timeout_seconds)
    output["job_run"] = job_result

    print("\n=== Final result ===")
    print(json.dumps(output, indent=2, default=str))

    return 0 if job_result["result_state"] == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, indent=2))
        sys.exit(2)
