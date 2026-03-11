#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time

from databricks.sdk import WorkspaceClient


TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR", "BLOCKED"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--notebook-path")
    parser.add_argument("--cluster-id")
    parser.add_argument("--run-name", default="skill-run")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    return parser.parse_args()


def build_client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    return WorkspaceClient(host=host, token=token)


def submit_run(client: WorkspaceClient, args: argparse.Namespace) -> int:
    if args.job_id:
        run = client.jobs.run_now(job_id=args.job_id)
        return run.run_id

    if not args.notebook_path or not args.cluster_id:
        raise RuntimeError("For one-off notebook runs, provide --notebook-path and --cluster-id")

    run = client.jobs.submit(
        run_name=args.run_name,
        tasks=[
            {
                "task_key": "skill_notebook_task",
                "existing_cluster_id": args.cluster_id,
                "notebook_task": {
                    "notebook_path": args.notebook_path,
                },
            }
        ],
    )
    return run.run_id


def wait_for_terminal_state(client: WorkspaceClient, run_id: int, poll_seconds: int, timeout_seconds: int):
    deadline = time.time() + timeout_seconds
    history = []

    while time.time() < deadline:
        run = client.jobs.get_run(run_id=run_id)
        state = run.state.life_cycle_state
        result = run.state.result_state
        message = run.state.state_message
        history.append(
            {
                "life_cycle_state": state,
                "result_state": result,
                "state_message": message,
            }
        )
        if state in TERMINAL_STATES:
            return run, history
        time.sleep(poll_seconds)

    raise TimeoutError(f"Run {run_id} did not reach terminal state within {timeout_seconds} seconds")


def main() -> int:
    args = parse_args()
    client = build_client()
    run_id = submit_run(client, args)
    run, history = wait_for_terminal_state(
        client,
        run_id=run_id,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
    )

    output = {
        "run_id": run_id,
        "life_cycle_state": run.state.life_cycle_state,
        "result_state": run.state.result_state,
        "state_message": run.state.state_message,
        "notebook_path": args.notebook_path,
        "job_id": args.job_id,
        "history": history,
    }
    print(json.dumps(output, indent=2, default=str))

    return 0 if run.state.result_state == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(2)
