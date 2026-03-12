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
from prepare_ngrok_kafka import ensure_tunnel


REPO_ROOT = Path(__file__).resolve().parents[3]
CONNECT_URL = "http://127.0.0.1:8083/connectors"
DEFAULT_JOB_ID = 574281734474239
NOTEBOOKS_TO_NORMALIZE = [
    "notebooks/bronze/NB_ingest_to_bronze.ipynb",
    "notebooks/helpers/NB_catalog_helpers.ipynb",
    "notebooks/helpers/NB_silver_metadata.ipynb",
    "notebooks/helpers/NB_schema_contracts.ipynb",
    "notebooks/helpers/NB_schema_drift_helpers.ipynb",
    "notebooks/silver/NB_process_to_silver_generic.ipynb",
    "notebooks/silver/NB_process_to_silver.ipynb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=int, default=DEFAULT_JOB_ID)
    parser.add_argument("--kafka-local-port", type=int, default=9093)
    parser.add_argument("--products-iterations", type=int, default=6)
    parser.add_argument("--orders-iterations", type=int, default=12)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--warehouse-id", default="53165753164ae80e")
    parser.add_argument("--skip-dq", action="store_true")
    parser.add_argument("--skip-dbt", action="store_true")
    parser.add_argument("--reuse-existing-infra", action="store_true")
    return parser.parse_args()


def enum_value(value):
    return getattr(value, "value", value)


def run_cmd(
    cmd: list[str], *, env: dict | None = None, capture_output: bool = True
) -> subprocess.CompletedProcess:
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


def psql(sql: str) -> str:
    env = os.environ.copy()
    env["PGPASSWORD"] = env.get("PGPASSWORD", "postgres")
    result = run_cmd(
        [
            "psql",
            "-h",
            env.get("PGHOST", "localhost"),
            "-U",
            env.get("PGUSER", "postgres"),
            "-d",
            env.get("PGDATABASE", "demo"),
            "-v",
            "ON_ERROR_STOP=1",
            "-tAc",
            sql,
        ],
        env=env,
    )
    return result.stdout.strip()


def ensure_source_schema() -> None:
    env = os.environ.copy()
    env["PGPASSWORD"] = env.get("PGPASSWORD", "postgres")
    run_cmd(
        [
            "psql",
            "-h",
            env.get("PGHOST", "localhost"),
            "-U",
            env.get("PGUSER", "postgres"),
            "-d",
            env.get("PGDATABASE", "demo"),
            "-f",
            "init-db.sql",
        ],
        env=env,
    )

    columns = set(
        filter(
            None,
            psql(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'orders'"
            ).splitlines(),
        )
    )
    if "product" not in columns:
        return

    psql(
        """
        INSERT INTO products(product_name, weight, color)
        SELECT DISTINCT o.product, 1.00, 'unknown'
        FROM orders o
        LEFT JOIN products p ON p.product_name = o.product
        WHERE o.product IS NOT NULL AND p.id IS NULL
        """
    )
    psql("ALTER TABLE orders ADD COLUMN IF NOT EXISTS product_id INTEGER")
    psql(
        """
        UPDATE orders o
        SET product_id = p.id
        FROM (
            SELECT product_name, MIN(id) AS id
            FROM products
            GROUP BY product_name
        ) p
        WHERE o.product = p.product_name
          AND o.product_id IS NULL
        """
    )
    unmapped = psql("SELECT COUNT(*) FROM orders WHERE product_id IS NULL")
    if unmapped != "0":
        raise RuntimeError(
            f"Source schema repair failed, {unmapped} orders still have NULL product_id"
        )

    psql("ALTER TABLE orders ALTER COLUMN product_id SET NOT NULL")
    has_fk = psql(
        """
        SELECT COUNT(*)
        FROM information_schema.table_constraints
        WHERE table_name = 'orders'
          AND constraint_name = 'orders_product_id_fkey'
        """
    )
    if has_fk == "0":
        psql(
            """
            ALTER TABLE orders
            ADD CONSTRAINT orders_product_id_fkey
            FOREIGN KEY (product_id)
            REFERENCES products(id)
            ON DELETE RESTRICT
            """
        )
    psql("CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders(product_id)")
    psql("ALTER TABLE orders DROP COLUMN product")


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
    connector_config = (
        (REPO_ROOT / "postgres-connector.json").read_text().encode("utf-8")
    )
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


def ensure_local_stack(args: argparse.Namespace, kafka_bootstrap: str) -> str | None:
    if args.reuse_existing_infra:
        wait_for_connect()
        return "reused-existing"

    host, port = kafka_bootstrap.split(":", 1)
    compose_env = os.environ.copy() | {
        "KAFKA_EXTERNAL_HOST": host,
        "KAFKA_EXTERNAL_PORT": port,
    }
    run_cmd(["docker", "compose", "up", "-d"], env=compose_env, capture_output=False)
    wait_for_connect()
    return "started-compose"


def run_generators(products_iterations: int, orders_iterations: int) -> None:
    env = os.environ.copy()
    run_cmd(
        ["python3", "generators/load_products_generator.py"],
        env=env | {"ITERATIONS": str(products_iterations)},
    )
    run_cmd(
        ["python3", "generators/load_generator.py"],
        env=env | {"ITERATIONS": str(orders_iterations)},
    )


def build_client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    return WorkspaceClient(host=host, token=token)


def wait_for_run(
    client: WorkspaceClient, run_id: int, poll_seconds: int, timeout_seconds: int
) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        run = client.jobs.get_run(run_id=run_id)
        run_state = run.state
        life_cycle_state = enum_value(getattr(run_state, "life_cycle_state", None))
        if life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR", "BLOCKED"}:
            return {
                "run_id": run_id,
                "life_cycle_state": life_cycle_state,
                "result_state": enum_value(getattr(run_state, "result_state", None)),
                "state_message": getattr(run_state, "state_message", None),
                "tasks": [
                    {
                        "task_key": task.task_key,
                        "run_id": task.run_id,
                        "life_cycle_state": enum_value(
                            getattr(task.state, "life_cycle_state", None)
                        ),
                        "result_state": enum_value(
                            getattr(task.state, "result_state", None)
                        ),
                        "state_message": getattr(task.state, "state_message", None),
                    }
                    for task in (run.tasks or [])
                ],
            }
        time.sleep(poll_seconds)
    raise TimeoutError(f"Run {run_id} did not finish within {timeout_seconds} seconds")


def run_ingest_job(
    client: WorkspaceClient,
    job_id: int,
    kafka_bootstrap: str,
    poll_seconds: int,
    timeout_seconds: int,
) -> dict:
    run = client.jobs.run_now(
        job_id=job_id, notebook_params={"KAFKA_BOOTSTRAP": kafka_bootstrap}
    )
    return wait_for_run(client, run.run_id, poll_seconds, timeout_seconds)


def run_silver_dq(catalog: str, silver_schema: str, warehouse_id: str) -> dict:
    result = run_cmd(
        [
            "python3",
            "skills/databricks-dq-automation/scripts/run_silver_dq.py",
            "--catalog",
            catalog,
            "--silver-schema",
            silver_schema,
            "--warehouse-id",
            warehouse_id,
        ]
    )
    return json.loads(result.stdout)


def run_dbt_gold(select: str | None = None) -> dict:
    cmd = [
        "python3",
        "skills/docker-databricks-lab-ops/scripts/run_dbt_gold.py",
        "--project-dir",
        "cdc_gold",
    ]
    if select:
        cmd.extend(["--select", select])

    result = run_cmd(cmd)
    return {
        "command": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def verify_gold_output(catalog: str, gold_schema: str) -> dict:
    row_count = psql(
        f"SELECT COUNT(*) FROM {catalog}.{gold_schema}.total_products_order"
    )
    return {
        "relation": f"{catalog}.{gold_schema}.total_products_order",
        "row_count": int(row_count),
        "passed": int(row_count) > 0,
    }


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
    infra_mode = ensure_local_stack(args, kafka_bootstrap)

    ensure_source_schema()
    connector_status = register_connector()
    run_generators(args.products_iterations, args.orders_iterations)

    client = build_client()
    ingest_run = run_ingest_job(
        client,
        job_id=args.job_id,
        kafka_bootstrap=kafka_bootstrap,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
    )

    dq_report = None
    if ingest_run["result_state"] == "SUCCESS" and not args.skip_dq:
        dq_report = run_silver_dq(
            catalog=args.catalog,
            silver_schema=args.silver_schema,
            warehouse_id=args.warehouse_id,
        )

    dbt_report = None
    gold_verification = None
    if ingest_run["result_state"] == "SUCCESS" and not args.skip_dbt:
        dbt_report = run_dbt_gold(select="total_products_order")
        if dbt_report["returncode"] == 0:
            gold_verification = verify_gold_output(
                catalog=args.catalog,
                gold_schema=args.gold_schema,
            )

    output = {
        "normalized_notebooks": normalized,
        "ngrok": {
            "public_url": tunnel["public_url"],
            "kafka_bootstrap": kafka_bootstrap,
        },
        "connector_status": connector_status,
        "infra_mode": infra_mode,
        "load_generation": {
            "products_iterations": args.products_iterations,
            "orders_iterations": args.orders_iterations,
        },
        "ingest_job_run": ingest_run,
        "dq_report": dq_report,
        "dbt_report": dbt_report,
        "gold_verification": gold_verification,
    }
    print(json.dumps(output, indent=2, default=str))

    success = ingest_run["result_state"] == "SUCCESS"
    if dq_report is not None:
        success = success and dq_report["passed"]
    if dbt_report is not None:
        success = success and dbt_report["returncode"] == 0
    if gold_verification is not None:
        success = success and gold_verification["passed"]
    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(2)
