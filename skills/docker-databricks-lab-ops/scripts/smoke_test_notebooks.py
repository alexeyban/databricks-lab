#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    GitProvider,
    GitSource,
    JobEnvironment,
    QueueSettings,
)

from prepare_ngrok_kafka import ensure_tunnel


REPO_ROOT = Path(__file__).resolve().parents[3]
CONNECT_URL = "http://127.0.0.1:8083/connectors"
DEFAULT_JOB_ID = 574281734474239
NOTEBOOKS_TO_NORMALIZE = [
    "notebooks/bronze/NB_ingest_to_bronze.ipynb",
    "notebooks/helpers/NB_catalog_helpers.ipynb",
    "notebooks/helpers/NB_schema_contracts.ipynb",
    "notebooks/helpers/NB_schema_drift_helpers.ipynb",
    "notebooks/helpers/NB_reset_tables.ipynb",
    "notebooks/silver/NB_process_to_silver.ipynb",
    "notebooks/silver/NB_process_products_silver.ipynb",
    "notebooks/silver/NB_process_payment_silver.ipynb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=int, default=DEFAULT_JOB_ID)
    parser.add_argument("--kafka-local-port", type=int, default=9093)
    parser.add_argument("--film-iterations", type=int, default=6)
    parser.add_argument("--rental-iterations", type=int, default=12)
    parser.add_argument("--reference-iterations", type=int, default=20)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--git-branch", default="main")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop all Databricks tables and clear checkpoints before running (uses NB_reset_tables)",
    )
    parser.add_argument("--cluster-id", help="Cluster ID required when --reset is passed")
    return parser.parse_args()


def enum_value(value):
    return getattr(value, "value", value)


def run_cmd(cmd: list[str], *, env: dict | None = None, capture_output: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def normalize_notebooks() -> list[str]:
    result = run_cmd(
        ["python3", "runtime/normalize_notebooks.py", *NOTEBOOKS_TO_NORMALIZE]
    )
    payload = json.loads(result.stdout)
    return payload.get("changed", [])


def wait_for_connect(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(CONNECT_URL, timeout=2) as response:
                if response.status == 200:
                    return
        except urllib.error.URLError as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f"Kafka Connect did not become ready: {last_error}")


def register_connector() -> str:
    connector_config = (REPO_ROOT / "postgres-connector.json").read_text().encode("utf-8")
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


def run_generators(film_iterations: int, rental_iterations: int, reference_iterations: int) -> None:
    env = os.environ.copy()
    run_cmd(
        ["python3", "generators/load_products_generator.py"],
        env=env | {"ITERATIONS": str(film_iterations)},
    )
    run_cmd(
        ["python3", "generators/load_generator.py"],
        env=env | {"ITERATIONS": str(rental_iterations)},
    )
    run_cmd(
        ["python3", "generators/load_reference_generator.py"],
        env=env | {"ITERATIONS": str(reference_iterations)},
    )


def build_client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    return WorkspaceClient(host=host, token=token)


def wait_for_run(client: WorkspaceClient, run_id: int, poll_seconds: int, timeout_seconds: int) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        run = client.jobs.get_run(run_id=run_id)
        life_cycle_state = enum_value(run.state.life_cycle_state)
        if life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR", "BLOCKED"}:
            return {
                "run_id": run_id,
                "life_cycle_state": life_cycle_state,
                "result_state": enum_value(run.state.result_state),
                "state_message": run.state.state_message,
                "tasks": [
                    {
                        "task_key": task.task_key,
                        "run_id": task.run_id,
                        "life_cycle_state": enum_value(task.state.life_cycle_state),
                        "result_state": enum_value(task.state.result_state),
                        "state_message": task.state.state_message,
                    }
                    for task in (run.tasks or [])
                ],
            }
        time.sleep(poll_seconds)
    raise TimeoutError(f"Run {run_id} did not finish within {timeout_seconds} seconds")


def run_dvdrental_job(
    client: WorkspaceClient, job_id: int, kafka_bootstrap: str, poll_seconds: int, timeout_seconds: int
) -> dict:
    run = client.jobs.run_now(job_id=job_id, notebook_params={"KAFKA_BOOTSTRAP": kafka_bootstrap})
    return wait_for_run(client, run.run_id, poll_seconds, timeout_seconds)


def run_reset(
    client: WorkspaceClient, catalog: str, cluster_id: str, git_branch: str, poll_seconds: int, timeout_seconds: int
) -> dict:
    notebook_params = {"CATALOG": catalog, "DRY_RUN": "false"}
    if cluster_id:
        submitted = client.jobs.submit(
            run_name="smoke-test-reset",
            tasks=[
                {
                    "task_key": "reset_tables",
                    "existing_cluster_id": cluster_id,
                    "notebook_task": {
                        "notebook_path": "notebooks/helpers/NB_reset_tables",
                        "base_parameters": notebook_params,
                    },
                }
            ],
        )
    else:
        from databricks.sdk.service.jobs import NotebookTask, Source, SubmitTask
        submitted = client.jobs.submit(
            run_name="smoke-test-reset",
            git_source=GitSource(
                git_url="https://github.com/alexeyban/databricks-lab",
                git_provider=GitProvider.GIT_HUB,
                git_branch=git_branch,
            ),
            environments=[JobEnvironment(environment_key="Default", spec=Environment(environment_version="4"))],
            queue=QueueSettings(enabled=True),
            tasks=[
                SubmitTask(
                    task_key="reset_tables",
                    environment_key="Default",
                    notebook_task=NotebookTask(
                        notebook_path="notebooks/helpers/NB_reset_tables",
                        source=Source.GIT,
                        base_parameters=notebook_params,
                    ),
                )
            ],
        )
    return wait_for_run(client, submitted.run_id, poll_seconds, timeout_seconds)


def main() -> int:
    args = parse_args()
    normalized = normalize_notebooks()

    tunnel = ensure_tunnel(
        api_url="http://127.0.0.1:4040/api/tunnels",
        local_port=args.kafka_local_port,
        timeout_seconds=30,
        log_path=REPO_ROOT / "logs" / "ngrok-kafka.log",
    )
    kafka_bootstrap = tunnel["public_url"].removeprefix("tcp://")
    host, port = kafka_bootstrap.split(":", 1)

    compose_env = os.environ.copy() | {
        "KAFKA_EXTERNAL_HOST": host,
        "KAFKA_EXTERNAL_PORT": port,
    }
    run_cmd(["docker", "compose", "up", "-d"], env=compose_env, capture_output=False)

    wait_for_connect()
    connector_status = register_connector()
    run_generators(args.film_iterations, args.rental_iterations, args.reference_iterations)

    client = build_client()

    reset_run = None
    if args.reset:
        print("Running table reset before ingestion…")
        reset_run = run_reset(
            client,
            catalog=args.catalog,
            cluster_id=args.cluster_id or "",
            git_branch=args.git_branch,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
        )

    dvdrental_run = run_dvdrental_job(
        client,
        job_id=args.job_id,
        kafka_bootstrap=kafka_bootstrap,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
    )

    output = {
        "normalized_notebooks": normalized,
        "ngrok": {
            "public_url": tunnel["public_url"],
            "kafka_bootstrap": kafka_bootstrap,
        },
        "connector_status": connector_status,
        "load_generation": {
            "film_iterations": args.film_iterations,
            "rental_iterations": args.rental_iterations,
            "reference_iterations": args.reference_iterations,
        },
        "reset_run": reset_run,
        "dvdrental_job_run": dvdrental_run,
    }
    print(json.dumps(output, indent=2, default=str))

    success = dvdrental_run["result_state"] == "SUCCESS"
    if reset_run is not None:
        success = success and reset_run["result_state"] == "SUCCESS"
    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(2)
