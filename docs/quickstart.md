# Quickstart Guide

This guide will help you get the Databricks CDC Lakehouse Lab running as quickly as possible.

---

## Prerequisites

### Required Software
- Docker and Docker Compose
- Python 3.10+
- PostgreSQL client (psql) - for initial database setup

### Required Accounts/Access
- Databricks workspace with Unity Catalog enabled
- GitHub repository access (for notebook deployment)
- ngrok account (for local Kafka → Databricks connectivity)

### Environment Variables
Create a `.env` file from the example:
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

Start the full CDC stack:
```bash
docker compose up -d
```

Wait ~30 seconds for Kafka Connect to be ready, then register the Debezium connector:
```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @postgres-connector.json
```

Verify the connector:
```bash
curl http://localhost:8083/connectors/postgres-connector/status
```

Optional: Open Kafka UI to monitor topics:
```
http://localhost:8085
```

**What this does**: Starts PostgreSQL, Kafka, Debezium, and supporting services locally.

---

## Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## Step 3: Set Up ngrok (Optional - for direct Kafka connectivity)

If using direct Kafka connectivity from Databricks (not recommended for Serverless):

```bash
# Start ngrok tunnel for Kafka
ngrok tcp 9093

# Update .env with the ngrok hostname/port
KAFKA_EXTERNAL_HOST=your-ngrok-host.ngrok.io
KAFKA_EXTERNAL_PORT=12345
```

Alternatively, use the `kafka-to-volume` profile (recommended):
```bash
docker compose --profile kafka-to-volume up -d
```

This runs a local consumer that writes CDC events to a Unity Catalog Volume, which the Bronze notebook can read via Auto Loader.

---

## Step 4: Upload Vault Configuration

The vault notebooks read configuration from a Unity Catalog Volume:

```bash
set -a && source .env && set +a
python3 scripts/upload_vault_config.py
```

---

## Step 5: Deploy Databricks Jobs

First, push Kafka secrets to Databricks:
```bash
set -a && source .env && set +a
python3 scripts/push_secrets_to_databricks.py
```

Then deploy all jobs:
```bash
python3 scripts/deploy_jobs.py
```

This creates 5 jobs:
- `dvdrental-bronze` - Kafka → Bronze Delta (availableNow trigger)
- `dvdrental-silver` - 15 parallel Bronze → Silver tasks (3 batches of 5)
- `dvdrental-vault` - Hubs → Links+Sats → Business Vault
- `dvdrental-vault-gold` - dbt build vault+gold via NB_run_dbt
- `dvdrental-orchestrator` - Chains all four in sequence

---

## Step 5b: Seed Bulk Data (Recommended)

Before running the pipeline for the first time, seed a realistic data volume:
```bash
python3 ingestion/generators/load_bulk_data.py
```

This inserts 1000 customers, 1000 films, and 10000+ DML events into PostgreSQL, which Debezium will capture into Kafka.

---

## Step 6: Generate Continuous CDC Traffic

Start the rental/payment generator:
```bash
python3 ingestion/generators/load_generator.py
```

In another terminal, start the film update generator:
```bash
python3 ingestion/generators/load_products_generator.py
```

Optional settings:
```bash
# Run for specific number of iterations
ITERATIONS=100 python3 ingestion/generators/load_generator.py

# Adjust sleep intervals (seconds)
SLEEP_MIN=0.5 SLEEP_MAX=2.0 python3 ingestion/generators/load_generator.py
```

---

## Step 7: Run the Pipeline

### Option A: Via Databricks Jobs UI
1. Open your Databricks workspace
2. Find the `dvdrental-orchestrator` job
3. Click "Run Now"

### Option B: Via deploy script
```bash
python3 scripts/deploy_jobs.py --run-orchestrator
```

The orchestrator runs 4 tasks in sequence: **Bronze → Silver → Vault → Vault-Gold (dbt)**.

---

## Step 8: Verify the Pipeline

### Check Bronze Layer
```sql
SELECT * FROM workspace.bronze.public_film LIMIT 5;
```

### Check Silver Layer
```sql
SELECT * FROM workspace.silver.silver_film LIMIT 5;
```

### Check Vault Layer
```sql
SELECT * FROM workspace.vault.hub_film LIMIT 5;
SELECT * FROM workspace.vault.sat_film_pricing LIMIT 5;
```

### Check Gold Layer (dbt)
```bash
cd transformation/dbt_project
dbt build --select vault gold
```

---

## Step 9: Monitor the Pipeline

### Check Job Runs
```bash
# List recent runs
python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for run in w.jobs.list_runs(job_name='dvdrental-orchestrator', limit=5):
    print(f'Run {run.run_id}: {run.state.result_state}')
"
```

### Check Logs
- Databricks notebook cell outputs
- Databricks job run logs in UI

### Check Kafka Topics
Open Kafka UI at http://localhost:8085 to see:
- Topic message counts
- Schema evolution
- Partition assignments

---

## Common Tasks

### Reset All Tables
Use `notebooks/helpers/NB_reset_tables.ipynb` to:
- Drop all Bronze/Silver/Vault/Gold tables
- Clear checkpoints
- Start fresh

### Add a New Table
1. Ensure Debezium captures it (update `postgres-connector.json`)
2. Add config to `config/silver/configs/dvdrental/<table>.json`
3. Re-run Silver job (or run specific task)
4. Add to Vault config (`config/datavault/dv_model.json`) and re-upload with `scripts/upload_vault_config.py`

### Add a Gold Model
1. Create model in `transformation/dbt_project/models/gold/`
2. Add tests in `transformation/dbt_project/tests/`
3. Run `cd transformation/dbt_project && dbt build`

---

## Troubleshooting

### Kafka Connect Won't Start
- Check Docker logs: `docker compose logs connect`
- Ensure PostgreSQL is ready first
- Verify WAL is enabled in PostgreSQL

### Databricks Can't Connect to Kafka
- Verify ngrok tunnel is running
- Check secrets are in Databricks scope
- For Serverless: use `kafka-to-volume` profile instead

### Job Fails
- Check notebook error messages
- Use `databricks-notebook-remediator` skill
- Review `troubleshooting.md` for common issues

---

## Next Steps

After getting the basic pipeline running:
1. Explore the dbt models in `transformation/dbt_project/models/`
2. Review the Data Vault design in `design/dv2/`
3. Check the agent system in `Agents/` and `skills/`
4. Read the ROADMAP for future work

---

## Full Command Reference

```bash
# Infrastructure
docker compose up -d                    # Start stack
docker compose down                     # Stop stack
docker compose --profile kafka-to-volume up -d kafka-to-volume  # Volume-based ingestion (no ngrok)

# Register connector
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @postgres-connector.json

# Generators
python3 ingestion/generators/load_bulk_data.py           # Seed bulk data (run once)
python3 ingestion/generators/load_generator.py           # Continuous rental/payment traffic
python3 ingestion/generators/load_products_generator.py  # Continuous film updates

# Deployment
python3 scripts/push_secrets_to_databricks.py
python3 scripts/upload_vault_config.py
python3 scripts/deploy_jobs.py
python3 scripts/deploy_jobs.py --run-orchestrator        # Deploy + trigger run

# dbt
cd transformation/dbt_project && dbt build --select vault gold

# Monitoring
curl http://localhost:8083/connectors/postgres-connector/status  # Connector status
docker compose logs -f kafka                                     # Kafka logs
```