from dotenv import load_dotenv
import os
from databricks.sdk import WorkspaceClient

load_dotenv()

def get_client():
    return WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )