# Quickstart Guide

This guide gets the Databricks CDC Lakehouse Lab running as quickly as possible.

---

## Prerequisites

### Required Software
- Docker and Docker Compose
- Python 3.10+
- PostgreSQL client (`psql`) — useful for inspecting the source database
- The modern unified [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) —
  required for `databricks bundle deploy`. The legacy `databricks-cli` (0.18.x)
  has no `bundle` subcommand.

### Required Accounts/Access
- Databricks workspace with Unity Catalog enabled
- GitHub repository access (the raw-API deploy path pins notebooks to `main` via `git_source`)
- ngrok account, only if you want Databricks to reach local Kafka directly

### Environment Variables
Create a `.env` file from the example at the repo root:
```bash
cp .envexample .env
```

Fill in your Databricks credentials:
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
```

---

## Step 1: Start Local Infrastructure

The compose file lives in `infra/docker/`, so run compose from there:

```bash
cd infra/docker
docker compose up -d
```

Wait ~30 seconds for Kafka Connect to be ready, then register the Debezium connector:
```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @../../ingestion/cdc/postgres-connector.json
```

Verify the connector:
```bash
curl http://localhost:8083/connectors/postgres-connector/status
```

Optional: open Kafka UI at http://localhost:8085 to monitor topics.

**What this does**: starts Zookeeper, Kafka, Schema Registry, PostgreSQL 15,
Debezium Connect and Kafka UI. On first start PostgreSQL restores the dvdrental
dataset and creates the logical replication publication.

---

## Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## Step 3: Kafka Connectivity from Databricks (optional)

Databricks Serverless cannot reach a local broker directly. If you want direct
Kafka connectivity, expose it with ngrok:

```bash
ngrok tcp 9093
# then set in .env:
KAFKA_EXTERNAL_HOST=your-ngrok-host.ngrok.io
KAFKA_EXTERNAL_PORT=12345
```

> **Note:** `docker-compose.yml` declares a `kafka-to-volume` service intended as
> the no-ngrok alternative (a local consumer that uploads CDC events to a Unity
> Catalog Volume for Auto Loader). It invokes `scripts/kafka_to_volume.py`, which
> **is not present in the repo** — the profile will fail until that script is
> restored. The same applies to the `deploy-databricks-jobs` and
> `upload-vault-config` profiles, which call
> `scripts/push_secrets_to_databricks.py`, `scripts/deploy_job.py` and
> `scripts/upload_vault_config.py` — none of which exist either.

---

## Step 4: Seed Bulk Data

Before running the pipeline for the first time, seed a realistic data volume:
```bash
python3 ingestion/load_bulk_data.py
```

This inserts 1000 customers, 1000 films, and 10000+ DML events into PostgreSQL,
which Debezium captures into Kafka.

---

## Step 5: Deploy Databricks Resources

### Option A (recommended): Databricks Asset Bundles

Covers both pipelines — dvdrental jobs plus the pump.fun pipeline and its trigger job.

```bash
cd orchestration/bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

This deploys:
- `dvdrental-bronze` — Kafka → Bronze Delta (1 task)
- `dvdrental-silver` — Bronze → Silver MERGE (15 tasks, one per table)
- `dvdrental-vault-gold` — `dbt build --select vault` then `--select gold` (2 dbt tasks)
- `dvdrental-orchestrator` — Silver → Vault+Gold (2 tasks; daily 02:00 UTC, PAUSED)
- `pumpapi-lakehouse` — Lakeflow pipeline: pump.fun Bronze + 5 Silver tables
- `pumpfun-bronze` — triggers that pipeline every 2 min, then gold_pump_token_risk

### Option B: raw Jobs API (dvdrental only, older job graph)

```bash
set -a && source .env && set +a
python3 scripts/deploy_jobs.py
```

This creates/updates 5 jobs, building the Vault layer from the
`processing/vault/` notebooks rather than the dbt vault models:
`dvdrental-bronze`, `dvdrental-silver`, `dvdrental-vault`,
`dvdrental-vault-gold` (via `NB_run_dbt`), `dvdrental-orchestrator`
(Bronze → Silver → Vault → Vault-Gold).

---

## Step 6: Generate Continuous CDC Traffic

Start the rental/payment generator:
```bash
python3 ingestion/load_generator.py
```

In another terminal, start the film update generator:
```bash
python3 ingestion/load_products_generator.py
```

Optional settings:
```bash
ITERATIONS=100 python3 ingestion/load_generator.py
SLEEP_MIN=0.5 SLEEP_MAX=2.0 python3 ingestion/load_generator.py
```

Or run both in Docker:
```bash
cd infra/docker
docker compose --profile manual up -d generate-cdc-traffic
```

---

## Step 7: Run the Pipeline

### Option A: Via Databricks Jobs UI
1. Open your Databricks workspace
2. Find the `dvdrental-orchestrator` job
3. Click "Run Now"

### Option B: Via the deploy script (raw Jobs API path only)
```bash
python3 scripts/deploy_jobs.py --run-orchestrator
```

Under the bundle the orchestrator runs **Silver → Vault+Gold (dbt)**, with Bronze
as an independent streaming job. Under `deploy_jobs.py` it runs four tasks:
**Bronze → Silver → Vault → Vault-Gold**.

---

## Step 8: Verify the Pipeline

### Bronze
```sql
SELECT * FROM workspace.bronze.public_film LIMIT 5;
```

### Silver
```sql
SELECT * FROM workspace.silver.silver_film LIMIT 5;
```

### Vault
```sql
SELECT * FROM workspace.vault.hub_film LIMIT 5;
SELECT * FROM workspace.vault.sat_film_pricing LIMIT 5;
```

### Gold
```sql
SELECT * FROM workspace.gold.gold_film LIMIT 5;
```

Or run dbt locally:
```bash
cd transformation/dbt_project
dbt build --select vault gold
```

---

## Step 9: Monitor the Pipeline

### Check Job Runs
```bash
python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for run in w.jobs.list_runs(limit=5):
    print(run.run_id, run.run_name, run.state.result_state)
"
```

### Check Logs
- Databricks notebook cell outputs
- Databricks job run logs in the UI

### Check Kafka Topics
Kafka UI at http://localhost:8085 shows topic message counts, schema evolution
and partition assignments.

---

## Common Tasks

### Reset All Tables
Run `processing/common/NB_reset_tables.ipynb` to drop Bronze/Silver/Vault/Gold
tables and clear checkpoints.

> Always delete the Silver checkpoints before any full Bronze reload, or Silver
> will skip the replayed data.

### Add a New Table
1. Ensure Debezium captures it (update `ingestion/cdc/postgres-connector.json`)
2. Add a config at `config/silver/configs/dvdrental/<table>.json`
3. Add a matching `silver_<table>` task to `orchestration/bundle/databricks.yml`
   (and to `TABLES` in `scripts/deploy_jobs.py` if you use that path)
4. Re-deploy and run the Silver job
5. Add the entity to `config/datavault/dv_model.json` and regenerate the dbt
   vault models with `python3 scripts/generate_vault_dbt_models.py`

### Add a Gold Model
1. Create the model in `transformation/dbt_project/models/gold/`
2. Add tests in `transformation/dbt_project/models/gold/<model>.yml` or a
   singular test in `transformation/dbt_project/tests/`
3. Run `cd transformation/dbt_project && dbt build --select gold`

---

## Troubleshooting

### Kafka Connect won't start
- Check logs: `cd infra/docker && docker compose logs connect`
- Ensure PostgreSQL is ready first
- Verify `wal_level=logical` in PostgreSQL

### Databricks can't connect to Kafka
- Verify the ngrok tunnel is running and `.env` matches it
- Check the `dvdrental` secret scope exists in the workspace
- For Serverless, direct broker access from the workspace is the usual blocker

### Job fails
- Check the notebook error message in the run output
- Review [troubleshooting.md](troubleshooting.md)

---

## Next Steps

1. Explore the dbt models in `transformation/dbt_project/models/`
2. Review the Data Vault design in `design/dv2/`
3. Check the agent system in `Agents/` and `skills/`
4. Read the [ROADMAP](../ROADMAP.md) for current status and next steps

---

## Full Command Reference

```bash
# Infrastructure (from infra/docker/)
docker compose up -d                                  # Start stack
docker compose down                                   # Stop stack
docker compose --profile manual up -d generate-cdc-traffic   # Continuous CDC traffic
docker compose --profile manual run --rm dbt-gold            # dbt build in a container

# Register connector
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @../../ingestion/cdc/postgres-connector.json

# Generators (from the repo root)
python3 ingestion/load_bulk_data.py            # Seed bulk data (run once)
python3 ingestion/load_generator.py            # Continuous rental/payment traffic
python3 ingestion/load_products_generator.py   # Continuous film updates
python3 ingestion/load_reference_generator.py  # Slowly-changing reference data

# Deployment
cd orchestration/bundle && databricks bundle deploy -t dev   # Everything
python3 scripts/deploy_jobs.py                               # dvdrental only (raw Jobs API)
python3 scripts/deploy_jobs.py --run-orchestrator            # Deploy + trigger

# dbt
cd transformation/dbt_project && dbt build --select vault gold

# Monitoring
curl http://localhost:8083/connectors/postgres-connector/status
cd infra/docker && docker compose logs -f kafka
```
