#!/usr/bin/env python3
"""
Deploy dvdrental pipeline as 4 separate Databricks jobs:

  1. dvdrental-bronze      — Kafka → Bronze Delta (streaming)
  2. dvdrental-silver      — 15 parallel Bronze → Silver tasks
  3. dvdrental-vault       — Hubs → Links+Sats → Business Vault
  4. dvdrental-orchestrator — chains jobs 1→2→3 via run_job_task

Usage:
    set -a && source .env && set +a
    python3 scripts/deploy_job.py [--kafka-bootstrap HOST:PORT] [--checkpoint-suffix SUFFIX] [--run]

Examples:
    python3 scripts/deploy_job.py --kafka-bootstrap 6.tcp.eu.ngrok.io:16223
    python3 scripts/deploy_job.py --kafka-bootstrap 6.tcp.eu.ngrok.io:16223 --run
"""

import argparse
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    GitProvider,
    GitSource,
    JobEmailNotifications,
    JobSettings,
    NotebookTask,
    QueueSettings,
    RunJobTask,
    Source,
    Task,
    TaskDependency,
    TaskEmailNotifications,
)

GIT_URL = "https://github.com/alexeyban/databricks-lab"
GIT_BRANCH = "main"
CATALOG = "workspace"
CHECKPOINT_ROOT = "/Volumes/workspace/default/mnt/checkpoints"

SILVER_TABLES = [
    "actor", "address", "category", "city", "country",
    "customer", "film", "film_actor", "film_category",
    "inventory", "language", "payment", "rental", "staff", "store",
]

GIT_SOURCE = GitSource(
    git_url=GIT_URL,
    git_provider=GitProvider.GIT_HUB,
    git_branch=GIT_BRANCH,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _nb(path: str, params: dict | None = None) -> NotebookTask:
    return NotebookTask(notebook_path=path, source=Source.GIT,
                        base_parameters=params or {})


def _task(key: str, notebook_path: str, params: dict | None = None,
          depends_on: list[str] | None = None) -> Task:
    return Task(
        task_key=key,
        notebook_task=_nb(notebook_path, params),
        depends_on=[TaskDependency(task_key=k) for k in (depends_on or [])],
        timeout_seconds=0,
        email_notifications=TaskEmailNotifications(),
    )


def _run_job_task(key: str, job_id: int, depends_on: list[str] | None = None) -> Task:
    return Task(
        task_key=key,
        run_job_task=RunJobTask(job_id=job_id),
        depends_on=[TaskDependency(task_key=k) for k in (depends_on or [])],
        timeout_seconds=0,
        email_notifications=TaskEmailNotifications(),
    )


def _upsert_job(w: WorkspaceClient, name: str, settings: JobSettings) -> int:
    """Create the job if it doesn't exist, otherwise reset it. Returns job_id."""
    existing = [j for j in w.jobs.list(name=name) if j.settings.name == name]
    if existing:
        job_id = existing[0].job_id
        w.jobs.reset(job_id=job_id, new_settings=settings)
        print(f"  Updated  '{name}'  (job_id={job_id})")
    else:
        job = w.jobs.create(**settings.__dict__)
        job_id = job.job_id
        print(f"  Created  '{name}'  (job_id={job_id})")
    return job_id


def _base_settings(name: str, tasks: list[Task]) -> JobSettings:
    return JobSettings(
        name=name,
        tasks=tasks,
        git_source=GIT_SOURCE,
        email_notifications=JobEmailNotifications(no_alert_for_skipped_runs=False),
        timeout_seconds=0,
        max_concurrent_runs=1,
        queue=QueueSettings(enabled=True),
    )


# ── job builders ─────────────────────────────────────────────────────────────

def build_bronze_job(kafka_bootstrap: str, checkpoint_root: str) -> JobSettings:
    tasks = [_task(
        key="Ingest_to_Bronze",
        notebook_path="notebooks/bronze/NB_ingest_to_bronze",
        params={
            "KAFKA_BOOTSTRAP": kafka_bootstrap,
            "TOPIC_PATTERN": "cdc.public.*",
            "CATALOG": CATALOG,
            "BRONZE_SCHEMA": "bronze",
            "CHECKPOINT_PATH": f"{checkpoint_root}/bronze_cdc",
            "CHECKPOINT_ROOT": checkpoint_root,
        },
    )]
    return _base_settings("dvdrental-bronze", tasks)


def build_silver_job(checkpoint_root: str) -> JobSettings:
    tasks = [
        _task(
            key=f"ingest_{table}_to_silver",
            notebook_path="notebooks/silver/NB_process_to_silver_generic",
            params={
                "TABLE_ID": table,
                "CATALOG": CATALOG,
                "BRONZE_SCHEMA": "bronze",
                "SILVER_SCHEMA": "silver",
                "CHECKPOINT_ROOT": checkpoint_root,
                "MONITORING_SCHEMA": "monitoring",
                "SCHEMA_POLICY": "additive_only",
            },
        )
        for table in SILVER_TABLES
    ]
    return _base_settings("dvdrental-silver", tasks)


def build_vault_job() -> JobSettings:
    tasks = [
        _task("vault_Ingest_Hubs",
              "notebooks/vault/NB_ingest_to_hubs",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault"}),
        _task("vault_Ingest_Links",
              "notebooks/vault/NB_ingest_to_links",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault"},
              depends_on=["vault_Ingest_Hubs"]),
        _task("vault_Ingest_Satellites",
              "notebooks/vault/NB_ingest_to_satellites",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault"},
              depends_on=["vault_Ingest_Hubs"]),
        _task("vault_Business_Vault",
              "notebooks/vault/NB_dv_business_vault",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault"},
              depends_on=["vault_Ingest_Links", "vault_Ingest_Satellites"]),
    ]
    return _base_settings("dvdrental-vault", tasks)


def build_orchestrator_job(bronze_id: int, silver_id: int,
                           vault_id: int) -> JobSettings:
    tasks = [
        _run_job_task("run_bronze", bronze_id),
        _run_job_task("run_silver", silver_id, depends_on=["run_bronze"]),
        _run_job_task("run_vault",  vault_id,  depends_on=["run_silver"]),
    ]
    # Orchestrator has no git source — it just triggers other jobs
    return JobSettings(
        name="dvdrental-orchestrator",
        tasks=tasks,
        email_notifications=JobEmailNotifications(no_alert_for_skipped_runs=False),
        timeout_seconds=0,
        max_concurrent_runs=1,
        queue=QueueSettings(enabled=True),
    )


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kafka-bootstrap", default="",
                        help="Kafka bootstrap servers (e.g. 6.tcp.eu.ngrok.io:16223)")
    parser.add_argument("--checkpoint-suffix", default="",
                        help="Suffix for checkpoint paths to force a fresh start (e.g. 'v3')")
    parser.add_argument("--run", action="store_true",
                        help="Trigger the orchestrator immediately after deploying")
    args = parser.parse_args()

    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    checkpoint_root = CHECKPOINT_ROOT
    if args.checkpoint_suffix:
        checkpoint_root = f"{CHECKPOINT_ROOT}_{args.checkpoint_suffix}"

    print("Deploying dvdrental pipeline jobs...")

    bronze_id = _upsert_job(w, "dvdrental-bronze",
                            build_bronze_job(args.kafka_bootstrap, checkpoint_root))
    silver_id = _upsert_job(w, "dvdrental-silver",
                            build_silver_job(checkpoint_root))
    vault_id  = _upsert_job(w, "dvdrental-vault",
                            build_vault_job())
    orch_id   = _upsert_job(w, "dvdrental-orchestrator",
                            build_orchestrator_job(bronze_id, silver_id, vault_id))

    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    print(f"\nJobs deployed:")
    print(f"  Bronze:       {host}/#job/{bronze_id}")
    print(f"  Silver:       {host}/#job/{silver_id}")
    print(f"  Vault:        {host}/#job/{vault_id}")
    print(f"  Orchestrator: {host}/#job/{orch_id}")

    if args.run:
        run = w.jobs.run_now(job_id=orch_id)
        print(f"\nOrchestrator run triggered: {run.run_id}")
        print(f"  {host}/#job/{orch_id}/run/{run.run_id}")


if __name__ == "__main__":
    main()
