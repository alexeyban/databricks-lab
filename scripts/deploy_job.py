#!/usr/bin/env python3
"""
Deploy dvdrental pipeline as 5 separate Databricks jobs:

  1. dvdrental-bronze      — Auto Loader (cloudFiles) → Bronze Delta
  2. dvdrental-silver      — 15 parallel Bronze → Silver tasks
  3. dvdrental-vault       — Hubs → Links+Sats → Business Vault
  4. dvdrental-orchestrator — chains jobs 1→2→3 via run_job_task
  5. dvdrental-dq-gdpr     — daily VACUUM, erasure processing, SLA checks

Bronze reads NDJSON files from a Unity Catalog Volume landing zone written
by scripts/kafka_to_volume.py running locally (no ngrok TCP tunnel needed).

Usage:
    set -a && source .env && set +a
    python3 scripts/kafka_to_volume.py &   # start local consumer
    python3 scripts/deploy_job.py [--checkpoint-suffix SUFFIX] [--run]
"""

import argparse
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    GitProvider,
    GitSource,
    JobEmailNotifications,
    JobSettings,
    NotebookTask,
    PauseStatus,
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
LANDING_ROOT    = "/Volumes/workspace/default/mnt/bronze-landing"
MODEL_PATH = "/Volumes/workspace/default/mnt/pipeline_configs/datavault/dv_model.json"

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

def build_bronze_job(checkpoint_root: str) -> JobSettings:
    tasks = [_task(
        key="Ingest_to_Bronze",
        notebook_path="notebooks/bronze/NB_ingest_to_bronze",
        params={
            "LANDING_PATH":    LANDING_ROOT,
            "CATALOG":         CATALOG,
            "BRONZE_SCHEMA":   "bronze",
            "CHECKPOINT_PATH": f"{checkpoint_root}/bronze_cdc",
            "CHECKPOINT_ROOT": checkpoint_root,
            "SCHEMA_LOCATION": f"{checkpoint_root}/bronze_autoloader_schema",
        },
    )]
    return _base_settings("dvdrental-bronze", tasks)


def build_silver_job(checkpoint_root: str, full_reload: bool = False) -> JobSettings:
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
                "FULL_RELOAD": "true" if full_reload else "false",
            },
        )
        for table in SILVER_TABLES
    ]
    return _base_settings("dvdrental-silver", tasks)


def build_vault_job(full_reload: bool = False) -> JobSettings:
    reload_param = "true" if full_reload else "false"
    tasks = [
        _task("vault_Ingest_Hubs",
              "notebooks/vault/NB_ingest_to_hubs",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault", "MODEL_PATH": MODEL_PATH,
               "FULL_RELOAD": reload_param}),
        _task("vault_Ingest_Links",
              "notebooks/vault/NB_ingest_to_links",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault", "MODEL_PATH": MODEL_PATH,
               "FULL_RELOAD": reload_param},
              depends_on=["vault_Ingest_Hubs"]),
        _task("vault_Ingest_Satellites",
              "notebooks/vault/NB_ingest_to_satellites",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault", "MODEL_PATH": MODEL_PATH,
               "FULL_RELOAD": reload_param},
              depends_on=["vault_Ingest_Hubs"]),
        _task("vault_Business_Vault",
              "notebooks/vault/NB_dv_business_vault",
              {"CATALOG": CATALOG, "VAULT_SCHEMA": "vault", "MODEL_PATH": MODEL_PATH},
              depends_on=["vault_Ingest_Links", "vault_Ingest_Satellites"]),
    ]
    return _base_settings("dvdrental-vault", tasks)


def build_dq_gdpr_job(webhook_url: str = "") -> JobSettings:
    """
    Task 4.5 — Daily DQ/GDPR maintenance job.

    Tasks (all independent — run in parallel by default):
    1. vacuum_bronze_tables     — VACUUM all 15 Bronze tables, RETAIN 720 hours
    2. process_erasure_requests — run NB_process_erasure for each pending request
    3. check_erasure_sla        — alert on requests older than 25 days
    """
    bronze_tables = [
        "actor", "address", "category", "city", "country",
        "customer", "film", "film_actor", "film_category",
        "inventory", "language", "payment", "rental", "staff", "store",
    ]
    vacuum_sql = " ".join(
        f"VACUUM {CATALOG}.bronze.{t} RETAIN 720 HOURS;"
        for t in bronze_tables
    )

    tasks = [
        # 1. VACUUM all Bronze tables (inline Python notebook task)
        _task(
            key="vacuum_bronze_tables",
            notebook_path="notebooks/helpers/NB_catalog_helpers",
            params={
                "CATALOG": CATALOG,
                # Execute vacuum via a widget-driven inline call
                "_INLINE_VACUUM_SQL": vacuum_sql,
            },
        ),
        # 2. Process pending erasure requests
        _task(
            key="process_erasure_requests",
            notebook_path="notebooks/helpers/NB_process_erasure",
            params={
                "REQUEST_ID": "__BATCH__",  # Notebook handles batch mode when REQUEST_ID == __BATCH__
                "CATALOG": CATALOG,
                "DRY_RUN": "false",
                "OPERATOR": "dvdrental-dq-gdpr-job",
            },
        ),
        # 3. Check erasure SLA (alert on requests > 25 days old)
        _task(
            key="check_erasure_sla",
            notebook_path="notebooks/helpers/NB_check_erasure_sla",
            params={
                "CATALOG": CATALOG,
                "SLA_WARN_DAYS": "25",
                "ALERT_CHANNEL": "webhook" if webhook_url else "log",
                "WEBHOOK_URL": webhook_url,
            },
        ),
    ]

    return JobSettings(
        name="dvdrental-dq-gdpr",
        tasks=tasks,
        git_source=GIT_SOURCE,
        schedule=CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",  # daily at 02:00 UTC
            timezone_id="UTC",
            pause_status=PauseStatus.UNPAUSED,
        ),
        email_notifications=JobEmailNotifications(no_alert_for_skipped_runs=False),
        timeout_seconds=0,
        max_concurrent_runs=1,
        queue=QueueSettings(enabled=True),
    )


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
    parser.add_argument("--checkpoint-suffix", default="",
                        help="Suffix for checkpoint paths to force a fresh start (e.g. 'v3')")
    parser.add_argument("--full-reload", action="store_true",
                        help="Deploy Silver job with FULL_RELOAD=true (batch mode, no checkpoint). "
                             "Use for E2E test resets after Bronze truncation.")
    parser.add_argument("--full-reload-vault", action="store_true",
                        help="Deploy Vault job with FULL_RELOAD=true: drops all hub/link/sat tables "
                             "before reloading. Use after LOAD_DATE logic changes.")
    parser.add_argument("--run", action="store_true",
                        help="Trigger the orchestrator immediately after deploying")
    parser.add_argument("--webhook-url", default="",
                        help="Slack/Teams webhook URL for DQ/GDPR alerts")
    args = parser.parse_args()

    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    checkpoint_root = CHECKPOINT_ROOT
    if args.checkpoint_suffix:
        checkpoint_root = f"{CHECKPOINT_ROOT}_{args.checkpoint_suffix}"

    print("Deploying dvdrental pipeline jobs...")

    bronze_id  = _upsert_job(w, "dvdrental-bronze",
                             build_bronze_job(checkpoint_root))
    silver_id  = _upsert_job(w, "dvdrental-silver",
                             build_silver_job(checkpoint_root, full_reload=args.full_reload))
    vault_id   = _upsert_job(w, "dvdrental-vault",
                             build_vault_job(full_reload=args.full_reload_vault))
    orch_id    = _upsert_job(w, "dvdrental-orchestrator",
                             build_orchestrator_job(bronze_id, silver_id, vault_id))
    dq_gdpr_id = _upsert_job(w, "dvdrental-dq-gdpr",
                              build_dq_gdpr_job(args.webhook_url))

    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    print(f"\nJobs deployed:")
    print(f"  Bronze:       {host}/#job/{bronze_id}")
    print(f"  Silver:       {host}/#job/{silver_id}")
    print(f"  Vault:        {host}/#job/{vault_id}")
    print(f"  Orchestrator: {host}/#job/{orch_id}")
    print(f"  DQ/GDPR:      {host}/#job/{dq_gdpr_id}  (daily 02:00 UTC)")

    if args.run:
        run = w.jobs.run_now(job_id=orch_id)
        print(f"\nOrchestrator run triggered: {run.run_id}")
        print(f"  {host}/#job/{orch_id}/run/{run.run_id}")


if __name__ == "__main__":
    main()
