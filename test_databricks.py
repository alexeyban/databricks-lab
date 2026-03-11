from dotenv import load_dotenv
import os
from databricks.sdk import WorkspaceClient

load_dotenv()

w = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

for obj in w.workspace.list("/"):
    print(obj.path)