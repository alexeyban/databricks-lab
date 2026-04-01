#!/usr/bin/env python3
"""
Trigger the NB_reset_tables notebook on Databricks to drop all dvdrental
Bronze/Silver/Gold Delta tables and clear streaming checkpoints.

Usage:
    python3 reset_databricks_tables.py --notebook-path notebooks/helpers/NB_reset_tables \
        --cluster-id <cluster-id>

    python3 reset_databricks_tables.py --notebook-path notebooks/helpers/NB_reset_tables \
        --cluster-id <cluster-id> --dry-run

Requires DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

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


REPO_ROOT = Path(__file__).resolve().parents[3]
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR", "BLOCKED"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset dvdrental Databricks tables via NB_reset_tables notebook")
    parser.add_argument(
        "--notebook-path",
        default="notebooks/helpers/NB_reset_tables",
        help="Notebook path in the Databricks workspace or git repo (default: notebooks/helpers/NB_reset_tables)",
    )
    parser.add_argument("--cluster-id", help="Existing cluster ID for one-off submission")
    parser.add_argument("--catalog", default="workspace", help="Unity Catalog name (default: workspace)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pass DRY_RUN=true to the notebook — previews what would be dropped without dropping",
    )
    parser.add_argument(
        "--git-branch",
        default="main",
        help="Git branch to use when submitting via git source (default: main)",
    )
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    return parser.parse_args()


def build_client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    return WorkspaceClient(host=host, token=token)


def enum_value(value):
    return getattr(value, "value", value)


def submit_reset_run(client: WorkspaceClient, args: argparse.Namespace) -> int:
    notebook_params = {
        "CATALOG": args.catalog,
        "DRY_RUN": "true" if args.dry_run else "false",
    }

    if args.cluster_id:
        run = client.jobs.submit(
            run_name="reset-dvdrental-tables",
            tasks=[
                {
                    "task_key": "reset_tables",
                    "existing_cluster_id": args.cluster_id,
                    "notebook_task": {
                        "notebook_path": args.notebook_path,
                        "base_parameters": notebook_params,
                    },
                }
            ],
        )
    else:
        run = client.jobs.submit(
            run_name="reset-dvdrental-tables",
            git_source=GitSource(
                git_url="https://github.com/alexeyban/databricks-lab",
                git_provider=GitProvider.GIT_HUB,
                git_branch=args.git_branch,
            ),
            environments=[JobEnvironment(environment_key="Default", spec=Environment(environment_version="4"))],
            queue=QueueSettings(enabled=True),
            tasks=[
                SubmitTask(
                    task_key="reset_tables",
                    environment_key="Default",
                    notebook_task=NotebookTask(
                        notebook_path=args.notebook_path,
                        source=Source.GIT,
                        base_parameters=notebook_params,
                    ),
                )
            ],
        )
    return run.run_id


def wait_for_run(client: WorkspaceClient, run_id: int, poll_seconds: int, timeout_seconds: int) -> dict:
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
                        "run_id": t.run_id,
                        "life_cycle_state": enum_value(t.state.life_cycle_state),
                        "result_state": enum_value(t.state.result_state),
                        "state_message": t.state.state_message,
                    }
                    for t in (run.tasks or [])
                ],
            }
        print(f"  [{state}] waiting…")
        time.sleep(poll_seconds)
    raise TimeoutError(f"Run {run_id} did not reach terminal state within {timeout_seconds}s")


def main() -> int:
    args = parse_args()
    client = build_client()

    action = "DRY RUN" if args.dry_run else "RESET"
    print(f"Submitting {action} for catalog '{args.catalog}' via {args.notebook_path}…")

    run_id = submit_reset_run(client, args)
    print(f"Run submitted: {run_id}")

    result = wait_for_run(client, run_id, args.poll_seconds, args.timeout_seconds)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["result_state"] == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(2)
