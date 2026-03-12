#!/usr/bin/env python3
import argparse
import json
import os
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_QUERY_DIR = REPO_ROOT / "dq_queries" / "silver"
DEFAULT_WAREHOUSE_ID = "53165753164ae80e"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
    parser.add_argument("--query-dir", default=str(DEFAULT_QUERY_DIR))
    parser.add_argument("--tables", nargs="*", default=None)
    return parser.parse_args()


def build_client() -> WorkspaceClient:
    load_dotenv(REPO_ROOT / ".env")
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    return WorkspaceClient(host=host, token=token)


def render_sql(template: str, catalog: str, silver_schema: str) -> str:
    return template.replace("{{ catalog }}", catalog).replace(
        "{{ silver_schema }}", silver_schema
    )


def wait_for_statement(client: WorkspaceClient, statement_id: str) -> object:
    while True:
        response = client.statement_execution.get_statement(statement_id)
        state = response.status.state
        if state in {
            StatementState.SUCCEEDED,
            StatementState.FAILED,
            StatementState.CANCELED,
            StatementState.CLOSED,
        }:
            return response
        time.sleep(2)


def run_sql_file(
    client: WorkspaceClient,
    warehouse_id: str,
    sql_path: Path,
    catalog: str,
    silver_schema: str,
) -> dict:
    statement = render_sql(sql_path.read_text(), catalog, silver_schema)
    response = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=silver_schema,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    if response.status.state == StatementState.PENDING:
        response = wait_for_statement(client, response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"DQ query failed for {sql_path.name}: {response.status.state}"
        )

    rows = response.result.data_array or []
    checks = [
        {"check_name": row[0], "status": row[1], "details": row[2]} for row in rows
    ]
    return {
        "table": sql_path.stem,
        "query_file": str(sql_path.relative_to(REPO_ROOT)),
        "checks": checks,
        "passed": all(check["status"] == "PASS" for check in checks),
    }


def main() -> int:
    args = parse_args()
    query_dir = Path(args.query_dir)
    if not query_dir.exists():
        raise RuntimeError(f"Query directory not found: {query_dir}")

    if args.tables:
        sql_files = [query_dir / f"{table}.sql" for table in args.tables]
    else:
        sql_files = sorted(query_dir.glob("*.sql"))

    missing = [str(path) for path in sql_files if not path.exists()]
    if missing:
        raise RuntimeError(f"Missing DQ query files: {', '.join(missing)}")

    client = build_client()
    reports = [
        run_sql_file(
            client, args.warehouse_id, sql_path, args.catalog, args.silver_schema
        )
        for sql_path in sql_files
    ]

    output = {
        "catalog": args.catalog,
        "silver_schema": args.silver_schema,
        "warehouse_id": args.warehouse_id,
        "reports": reports,
        "passed": all(report["passed"] for report in reports),
    }
    print(json.dumps(output, indent=2))
    return 0 if output["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
