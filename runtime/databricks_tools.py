import time
from runtime.databricks_client import get_client

w = get_client()


def upload_notebook(workspace_path, code):

    w.workspace.import_(
        path=workspace_path,
        content=code.encode(),
        format="SOURCE",
        language="PYTHON",
        overwrite=True
    )

    return workspace_path


def run_notebook(workspace_path):

    run = w.jobs.submit(
        run_name="agent-run",
        tasks=[{
            "task_key": "agent_task",
            "notebook_task": {
                "notebook_path": workspace_path
            },
            "existing_cluster_id": "YOUR_CLUSTER_ID"
        }]
    )

    return run.run_id


def wait_for_run(run_id):

    while True:

        run = w.jobs.get_run(run_id)

        state = run.state.life_cycle_state

        if state in ["TERMINATED", "INTERNAL_ERROR"]:
            return run

        time.sleep(5)


def get_error(run):

    if run.state.result_state == "SUCCESS":
        return None

    if run.state.state_message:
        return run.state.state_message

    return "Unknown error"