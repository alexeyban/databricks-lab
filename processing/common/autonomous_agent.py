import openai
from runtime.databricks_tools import upload_notebook, run_notebook, wait_for_run, get_error

SYSTEM_PROMPT = """
You are a senior Databricks Data Engineer.

Write clean PySpark notebooks that run in Databricks.

Rules:
- Use PySpark
- Print useful output
- Avoid external dependencies
"""


def generate_code(task, error=None):

    prompt = task

    if error:
        prompt += f"\nFix this error:\n{error}"

    response = openai.ChatCompletion.create(
        model="qwen-coder",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content


def run_agent(task):

    workspace_path = "/Workspace/agent/notebook"

    error = None

    for iteration in range(5):

        print("Iteration:", iteration)

        code = generate_code(task, error)

        upload_notebook(workspace_path, code)

        run_id = run_notebook(workspace_path)

        run = wait_for_run(run_id)

        error = get_error(run)

        if not error:
            print("SUCCESS")
            return

        print("Error:", error)

    print("Failed after retries")