#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import Any, Dict

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR", "BLOCKED"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--job-name")
    parser.add_argument("--notebook-path")
    parser.add_argument("--cluster-id")
    parser.add_argument("--run-name", default="skill-run")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument(
        "--notebook-param",
        action="append",
        default=[],
        help="Notebook parameter override in KEY=VALUE form. Can be passed multiple times.",
    )
    return parser.parse_args()


def build_client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    return WorkspaceClient(host=host, token=token)


def resolve_job_id(client: WorkspaceClient, args: argparse.Namespace) -> int | None:
    if args.job_id is not None:
        return args.job_id

    if not args.job_name:
        return None

    matching_job_ids: list[int] = []
    for job in client.jobs.list(expand_tasks=False):
        settings = getattr(job, "settings", None)
        if settings and settings.name == args.job_name and job.job_id is not None:
            matching_job_ids.append(job.job_id)

    if not matching_job_ids:
        raise RuntimeError(f"No Databricks job found with name '{args.job_name}'")
    if len(matching_job_ids) > 1:
        raise RuntimeError(
            f"Multiple Databricks jobs found with name '{args.job_name}': {matching_job_ids}"
        )
    return matching_job_ids[0]


def parse_notebook_params(values: list[str]) -> Dict[str, str]:
    params: Dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise RuntimeError(
                f"Invalid --notebook-param '{value}'. Expected KEY=VALUE."
            )
        key, raw_value = value.split("=", 1)
        params[key] = raw_value
    return params


def enum_value(value):
    return getattr(value, "value", value)


def get_run_state(run: Any) -> Any:
    if getattr(run, "state", None) is None:
        raise RuntimeError(f"Run {run.run_id} has no state information yet")
    return run.state


def submit_run(client: WorkspaceClient, args: argparse.Namespace) -> int:
    notebook_params = parse_notebook_params(args.notebook_param)
    job_id = resolve_job_id(client, args)

    if job_id:
        run = client.jobs.run_now(
            job_id=job_id,
            notebook_params=notebook_params or None,
        )
        return run.run_id

    if not args.notebook_path or not args.cluster_id:
        raise RuntimeError(
            "For one-off notebook runs, provide --notebook-path and --cluster-id"
        )

    run = client.jobs.submit(
        run_name=args.run_name,
        tasks=[
            jobs.SubmitTask(
                task_key="skill_notebook_task",
                existing_cluster_id=args.cluster_id,
                notebook_task=jobs.NotebookTask(
                    notebook_path=args.notebook_path,
                    base_parameters=notebook_params or None,
                ),
            )
        ],
    )
    return run.run_id


def wait_for_terminal_state(
    client: WorkspaceClient, run_id: int, poll_seconds: int, timeout_seconds: int
):
    deadline = time.time() + timeout_seconds
    history = []

    while time.time() < deadline:
        run = client.jobs.get_run(run_id=run_id)
        run_state = get_run_state(run)
        state = enum_value(run_state.life_cycle_state)
        result = enum_value(run_state.result_state)
        message = run_state.state_message
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

    raise TimeoutError(
        f"Run {run_id} did not reach terminal state within {timeout_seconds} seconds"
    )


def main() -> int:
    args = parse_args()
    client = build_client()
    resolved_job_id = resolve_job_id(client, args)
    if resolved_job_id is not None:
        args.job_id = resolved_job_id
    run_id = submit_run(client, args)
    run, history = wait_for_terminal_state(
        client,
        run_id=run_id,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    run_state = get_run_state(run)

    output = {
        "run_id": run_id,
        "life_cycle_state": enum_value(run_state.life_cycle_state),
        "result_state": enum_value(run_state.result_state),
        "state_message": run_state.state_message,
        "notebook_path": args.notebook_path,
        "job_id": args.job_id,
        "notebook_params": parse_notebook_params(args.notebook_param),
        "tasks": [
            {
                "task_key": task.task_key,
                "run_id": task.run_id,
                "life_cycle_state": enum_value(
                    getattr(task.state, "life_cycle_state", None)
                ),
                "result_state": enum_value(getattr(task.state, "result_state", None)),
                "state_message": getattr(task.state, "state_message", None),
            }
            for task in (run.tasks or [])
        ],
        "history": history,
    }
    print(json.dumps(output, indent=2, default=str))

    return 0 if enum_value(run_state.result_state) == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(2)
