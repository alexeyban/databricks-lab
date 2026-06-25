# Databricks CDC Lakehouse Lab

End-to-end reference implementation of a Change Data Capture pipeline from the **dvdrental** PostgreSQL sample database into a Databricks medallion lakehouse (Bronze → Silver → Vault → Gold).

Agent-system inspiration was borrowed from [agency-agents](https://github.com/msitarzewski/agency-agents/).

## Architecture

```
PostgreSQL dvdrental (WAL)
   → Debezium Connect (topics: cdc.public.* — all 15 tables)
     → Databricks Bronze (raw Debezium envelopes in Delta tables)
       → Databricks Silver (current-state via MERGE, schema evolution)
         → Databricks Vault (Data Vault 2.0: Hubs / Links / Satellites / PIT / Bridge)
           → dbt Gold (business-ready models with data quality tests)
```

### Directory Structure

```
ingestion/                  # Ingestion layer (Bronze)
├── consumers/
│   └── NB_ingest_to_bronze.ipynb   ← Bronze streaming notebook
├── cdc/
│   └── postgres-connector.json     ← Debezium connector config
└── generators/             # Data mutation scripts
    ├── load_generator.py           ← Rental/payment generator
    ├── load_products_generator.py  ← Film update generator
    └── load_bulk_data.py           ← Bulk data seeder

processing/                 # All processing logic
├── silver/
│   └── NB_process_to_silver_generic.ipynb  ← Metadata-driven Bronze → Silver
├── vault/
│   ├── NB_ingest_to_hubs.ipynb
│   ├── NB_ingest_to_links.ipynb
│   ├── NB_ingest_to_satellites.ipynb
│   └── NB_dv_business_vault.ipynb
└── common/                 # Shared helper notebooks
    ├── NB_silver_metadata.ipynb
    ├── NB_catalog_helpers.ipynb
    ├── NB_schema_contracts.ipynb
    └── NB_schema_drift_helpers.ipynb

transformation/             # dbt (vault + gold)
├── NB_run_dbt.ipynb        ← Databricks notebook that runs dbt (dbtRunner API)
└── dbt_project/            ← dbt project
    ├── dbt_project.yml
    └── models/
        ├── vault/          ← Incremental vault models (hubs/links/sats/pit/bridge)
        └── gold/           ← Gold data marts

config/                     # All configuration files
├── datavault/
│   └── dv_model.json       ← Vault config (uploaded to Unity Catalog Volume)
├── silver/
│   └── configs/dvdrental/  ← Per-table Silver configs (15 JSON files)
└── .envexample

orchestration/              # Jobs / workflows
└── bundle/                 ← Databricks Asset Bundles (future)

scripts/                    # CLI utilities
├── deploy_jobs.py          ← Deploy all 5 Databricks jobs
├── push_secrets_to_databricks.py
├── upload_vault_config.py
└── reset_checkpoints.py

infra/                      # Local environment setup
└── docker/
    ├── docker-compose.yml
    └── init-dvdrental.sh

README.md
```

### Alternative: kafka-to-volume (no ngrok required)

Databricks Serverless cannot reach a local ngrok Kafka endpoint. Use the built-in
`kafka-to-volume` Docker profile to upload CDC events to a Databricks Volume landing zone,
allowing Bronze to use Auto Loader instead of direct Kafka connectivity.

```bash
docker compose --profile kafka-to-volume up -d kafka-to-volume
```

### Source Tables (PostgreSQL dvdrental — all 15)

> **Note:** When using local ngrok for Kafka connectivity, 3 tables (`category`, `country`,
> `film_actor`) may not reach Bronze due to Databricks Serverless network limitations.
> Use the `kafka-to-volume` Docker profile as a workaround (see Architecture section).

**Reference / Dimension**

| Table | Notes |
|-------|-------|
| `public.country` | Lookup |
| `public.city` | → country |
| `public.address` | → city |
| `public.language` | Film language lookup |
| `public.category` | Film category lookup |
| `public.actor` | Actor dimension |
| `public.store` | Store dimension |
| `public.staff` | Staff dimension → address, store |
| `public.customer` | Customer dimension → address, store |

**Transaction / Fact**

| Table | Changes captured |
|-------|-----------------|
| `film` | rental_rate, rental_duration, replacement_cost |
| `film_actor` | Junction: film ↔ actor |
| `film_category` | Junction: film ↔ category |
| `inventory` | Film copies per store |
| `rental` | New rentals, return_date updates |
| `payment` | Payment inserts |

---

### Databricks Jobs

| Job | ID | Tasks | Description |
|-----|----|-------|-------------|
| `dvdrental-bronze` | 325293262130713 | 1 | Kafka → Bronze Delta (availableNow trigger) |
| `dvdrental-silver` | 1099814608698427 | 15 | Bronze → Silver MERGE, 3 batches of 5 |
| `dvdrental-vault` | 950203691556666 | 4 | Hubs → Links+Sats → Business Vault |
| `dvdrental-vault-gold` | 83436339832760 | 1 | dbt build vault+gold via NB_run_dbt |
| `dvdrental-orchestrator` | 684287727358557 | 4 | Chains: bronze → silver → vault → vault-gold |

---

## Local Infrastructure

```bash
# Start the full CDC stack (Zookeeper, Kafka, PostgreSQL 15, Debezium Connect, Schema Registry, Kafka UI)
docker compose up -d

# Register the Debezium connector (wait ~30s for Kafka Connect to be ready)
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @postgres-connector.json

# Check connector status
curl http://localhost:8083/connectors/postgres-connector/status

# Kafka UI
open http://localhost:8085

# Alternative: kafka-to-volume (no ngrok needed for Serverless workspaces)
docker compose --profile kafka-to-volume up -d kafka-to-volume
```

---

## Python Setup

```bash
pip install -r requirements.txt

# Copy and fill in Databricks credentials
cp .envexample .env
# Edit .env: set DATABRICKS_HOST and DATABRICKS_TOKEN
```

---

## Data Generators

```bash
# Bulk seed: inserts 1000 customers, 1000 films, 10000+ DML events (run once before pipeline)
python3 ingestion/generators/load_bulk_data.py

# Film generator (updates rental_rate, rental_duration, replacement_cost on existing films)
python3 ingestion/generators/load_products_generator.py

# Rental + payment generator (new rentals, film returns, payments)
python3 ingestion/generators/load_generator.py

# Optional env vars: ITERATIONS, SLEEP_MIN, SLEEP_MAX
# DB env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
```

---

## Deployment

```bash
set -a && source .env && set +a

# Push Kafka credentials to Databricks secret scope
python3 scripts/push_secrets_to_databricks.py

# Upload vault config to Unity Catalog Volume (required before first vault run)
python3 scripts/upload_vault_config.py

# Deploy all 5 Databricks jobs
python3 scripts/deploy_jobs.py

# Deploy and immediately trigger the full orchestrator run
python3 scripts/deploy_jobs.py --run-orchestrator
```

---

## dbt (Vault + Gold Layer)

```bash
# Local
cd transformation/dbt_project
dbt debug          # verify connection
dbt build --select vault gold   # run vault + gold models + tests
dbt test           # data quality tests only

# On Databricks: run transformation/NB_run_dbt.ipynb (uses dbtRunner API, no subprocess)
```

---

## Run the Pipeline

1. Start local infrastructure: `docker compose up -d`
2. Register the Debezium connector
3. Seed bulk data: `python3 ingestion/generators/load_bulk_data.py`
4. Deploy jobs: `python3 scripts/deploy_jobs.py`
5. Trigger orchestrator from the Databricks UI or:
   `python3 scripts/deploy_jobs.py --run-orchestrator`

The orchestrator runs: **Bronze → Silver → Vault → Vault-Gold (dbt)**