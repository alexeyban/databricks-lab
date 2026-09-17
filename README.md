# Databricks CDC Lakehouse Lab

End-to-end reference implementation of a Change Data Capture pipeline from the **dvdrental** PostgreSQL sample database into a Databricks medallion lakehouse (Bronze → Silver → Vault → Gold). It also includes a second, independent near-real-time pipeline that ingests live trading events from **pump.fun**, the Solana meme-coin launchpad.

Agent-system inspiration was borrowed from [agency-agents](https://github.com/msitarzewski/agency-agents/).

## Architecture

```
PostgreSQL dvdrental (WAL)
   → Debezium Connect (topics: cdc.public.* — all 15 tables)
     → Databricks Bronze (raw Debezium envelopes in Delta tables)
       → Databricks Silver (current-state via MERGE, schema evolution)
         → Vault (Data Vault 2.0: Hubs / Links / Satellites / PIT / Bridge — dbt models)
           → dbt Gold (business-ready marts with data quality tests)

pump.fun (Solana on-chain trades)
   → PumpAPI websocket → ingestion/pumpfun service (outside Databricks)
     → batch → JSONL → zstd-compress → base64-encode → Unity Catalog Volume
       → pumpapi-lakehouse (Lakeflow Declarative Pipeline: decode → decompress → parse)
         → Bronze (pump_events_raw) + 5 Silver tables
           → gold_pump_token_risk (separate notebook task)
```

See [`docs/architecture.md`](docs/architecture.md) for the full design of both pipelines.

### Directory Structure

```
pumpapi-lakehouse/           # pump.fun Bronze + Silver — one Lakeflow Declarative Pipeline
└── transformations/
    ├── bronze_pump_events.py      ← decode zstd+base64 → workspace.bronze.pump_events_raw
    ├── silver_pump_events.py      ← all events, typed   → silver.pump_events
    ├── silver_pump_tokens.py      ← action=create       → silver.pump_tokens
    ├── silver_pump_transfers.py   ← action=transfer     → silver.pump_transfers
    ├── silver_pump_pools.py       ← pool lifecycle      → silver.pump_pools
    └── silver_pump_trades.py      ← action=buy/sell     → silver.pump_trades

ingestion/                  # Ingestion layer (Bronze) + data generators
├── consumers/
│   ├── NB_ingest_to_bronze.ipynb                    ← dvdrental Bronze streaming notebook
│   ├── outdated__NB_ingest_pumpfun_to_bronze.ipynb  ← superseded, kept for reference
│   └── outdated__NB_dlt_pumpfun_bronze.ipynb        ← superseded, kept for reference
├── cdc/
│   └── postgres-connector.json     ← Debezium connector config
├── pumpfun/                 # Standalone pump.fun websocket ingestor (runs outside Databricks)
│   ├── app/                        ← websocket client → zstd+base64 batch writer → Databricks uploader
│   ├── Dockerfile / docker-compose.yml
│   ├── systemd/pumpfun-ingestor.service
│   └── README.md
├── load_bulk_data.py               ← Bulk data seeder (run once)
├── load_generator.py               ← Rental/payment generator
├── load_products_generator.py      ← Film update generator
└── load_reference_generator.py     ← Reference-data update generator

processing/                 # All processing logic
├── silver/
│   ├── NB_process_to_silver_generic.ipynb   ← Metadata-driven Bronze → Silver (dvdrental)
│   ├── dvdrental/*.json                     ← Copy of the per-table Silver configs
│   ├── check_*.sql                          ← Silver DQ assertions
│   └── outdated__*.ipynb                    ← Superseded per-table notebooks, kept for reference
├── vault/                  # Notebook vault layer — superseded by the dbt vault models
│   ├── NB_dv_metadata.ipynb
│   ├── NB_ingest_to_hubs.ipynb
│   ├── NB_ingest_to_links.ipynb
│   ├── NB_ingest_to_satellites.ipynb
│   └── NB_dv_business_vault.ipynb
├── gold/
│   └── NB_process_pump_token_risk.ipynb     ← pump.fun risk scoring → gold.gold_pump_token_risk
└── common/                 # Shared helper notebooks + Python modules
    ├── NB_silver_metadata.ipynb / NB_catalog_helpers.ipynb
    ├── NB_schema_contracts.ipynb / NB_schema_drift_helpers.ipynb
    ├── NB_pii_catalog_helpers.ipynb / NB_key_management_helpers.ipynb
    ├── NB_process_erasure.ipynb / NB_reset_tables.ipynb
    ├── NB_confluence_generator.ipynb / confluence_doc_generator.py
    └── autonomous_agent.py / databricks_client.py / databricks_tools.py

transformation/             # dbt (vault + gold)
├── NB_run_dbt.ipynb        ← Databricks notebook that runs dbt (dbtRunner API)
└── dbt_project/            ← dbt project (project name: cdc_gold)
    ├── dbt_project.yml
    ├── macros/             ← write_dq_results, suppress_erased_subjects
    ├── tests/              ← 3 singular tests
    └── models/
        ├── vault/          ← 13 hubs, 19 links, 20 satellites, 4 PITs, 2 bridges
        └── gold/           ← 7 Gold data marts

config/                     # Configuration files
├── datavault/dv_model.json     ← Vault config (also copied to processing/vault/)
└── silver/configs/dvdrental/   ← Per-table Silver configs (15 JSON files)

orchestration/              # Jobs / workflows
├── bundle/databricks.yml   ← Databricks Asset Bundles — all dvdrental + pump.fun resources
└── databricks_jobs/        ← Operational scripts and the databricks-jobs skill

scripts/                    # CLI utilities
├── deploy_jobs.py                  ← Deploy dvdrental jobs via the raw Jobs API
└── generate_vault_dbt_models.py    ← Generate the dbt vault models from dv_model.json

infra/docker/               # Local environment setup
├── docker-compose.yml      ← Zookeeper, Kafka, Schema Registry, PostgreSQL, Connect, Kafka UI
├── init-dvdrental.sh
└── entrypoint-*.sh

design/                     # Design documents and implementation logs
docs/                       # Project documentation (start at docs/index.md)
Agents/  skills/            # 26 agent definitions, 25 skill definitions
.envexample  requirements.txt  ROADMAP.md
```

### Source Tables (PostgreSQL dvdrental — all 15)

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

## Deployment: two mechanisms, two job graphs

The repo currently carries **two** deployment paths, and they do not describe
the same set of jobs. Know which one you are using.

**1. Databricks Asset Bundles — `orchestration/bundle/databricks.yml`** (covers everything)

```bash
cd orchestration/bundle
databricks bundle deploy -t dev      # requires the modern unified Databricks CLI
```

| Resource | Type | Tasks | Description |
|----------|------|-------|-------------|
| `dvdrental-bronze` | job | 1 | Kafka → Bronze Delta |
| `dvdrental-silver` | job | 15 | Bronze → Silver MERGE, one task per table |
| `dvdrental-vault-gold` | job | 2 | `dbt build --select vault`, then `--select gold` (dbt_task) |
| `dvdrental-orchestrator` | job | 2 | Silver → Vault+Gold (Bronze runs independently) |
| `pumpapi-lakehouse` | pipeline | — | Lakeflow: Bronze + 5 Silver tables |
| `pumpfun-bronze` | job | 2 | Triggers `pumpapi-lakehouse` every 2 min, then gold_pump_token_risk |

**2. Raw Jobs API — `scripts/deploy_jobs.py`** (dvdrental only, older graph)

```bash
set -a && source .env && set +a
python3 scripts/deploy_jobs.py                     # deploy
python3 scripts/deploy_jobs.py --run-orchestrator  # deploy + trigger
```

| Job | ID | Tasks | Description |
|-----|----|-------|-------------|
| `dvdrental-bronze` | 325293262130713 | 1 | Kafka → Bronze Delta |
| `dvdrental-silver` | 1099814608698427 | 15 | Bronze → Silver MERGE |
| `dvdrental-vault` | 950203691556666 | 4 | Notebook vault: Hubs → Links+Sats → Business Vault |
| `dvdrental-vault-gold` | (looked up by name) | 1 | `transformation/NB_run_dbt.ipynb` |
| `dvdrental-orchestrator` | 684287727358557 | 4 | Bronze → Silver → Vault → Vault-Gold |

The two differ in how the Vault layer is built: the bundle runs it as **dbt
models**, `deploy_jobs.py` runs the older **`processing/vault/` notebooks**.
The bundle is the only path that deploys anything for pump.fun — the Pipelines
API doesn't support the `git_source` mechanism `deploy_jobs.py` relies on. The
bundle is the direction of travel; `deploy_jobs.py` has not been retired yet.

---

## Local Infrastructure

The Docker stack lives in `infra/docker/`, so run compose from there:

```bash
cd infra/docker

# Start the CDC stack (Zookeeper, Kafka, Schema Registry, PostgreSQL 15, Debezium Connect, Kafka UI)
docker compose up -d

# Register the Debezium connector (wait ~30s for Kafka Connect to be ready)
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @../../ingestion/cdc/postgres-connector.json

# Check connector status
curl http://localhost:8083/connectors/postgres-connector/status

# Kafka UI
open http://localhost:8085
```

PostgreSQL runs `init-dvdrental.sh` on first start, restoring the full dvdrental
dataset (~1000 films, ~16k rentals, ~14k payments) and creating the logical
replication publication.

> **Known gap:** `docker-compose.yml` also declares four `manual`-profile services
> (`deploy-databricks-jobs`, `upload-vault-config`) and a `kafka-to-volume` service
> that invoke `scripts/push_secrets_to_databricks.py`, `scripts/deploy_job.py`,
> `scripts/upload_vault_config.py` and `scripts/kafka_to_volume.py`. **None of
> those scripts exist in the repo** — those profiles will fail until they are
> restored or the services removed. The working profiles are `dbt-gold` and
> `generate-cdc-traffic`.

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
# Bulk seed: 1000 customers, 1000 films, 10000+ DML events (run once before the pipeline)
python3 ingestion/load_bulk_data.py

# Film generator (updates rental_rate, rental_duration, replacement_cost)
python3 ingestion/load_generator.py

# Rental + payment generator (new rentals, film returns, payments)
python3 ingestion/load_products_generator.py

# Reference-data generator (slowly-changing lookups)
python3 ingestion/load_reference_generator.py

# Optional env vars: ITERATIONS, SLEEP_MIN, SLEEP_MAX
# DB env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

# Or run the two continuous generators in Docker:
cd infra/docker && docker compose --profile manual up -d generate-cdc-traffic
```

---

## dbt (Vault + Gold Layer)

```bash
# Local
cd transformation/dbt_project
dbt debug                        # verify connection
dbt build --select vault gold    # run vault + gold models + tests
dbt test                         # data quality tests only

# On Databricks: the dvdrental-vault-gold job runs `dbt build` as a dbt_task
# (bundle), or transformation/NB_run_dbt.ipynb via the dbtRunner API (deploy_jobs.py)
```

`dbt_packages/` is committed to the repo, so no `dbt deps` is needed at runtime.

---

## pump.fun Pipeline

Independent of the dvdrental CDC pipeline. The producer (`ingestion/pumpfun/`)
is a standalone always-on websocket service — it does not run as a Databricks
job. Each batch is zstd-compressed and base64-encoded before upload:

```bash
cd ingestion/pumpfun
cp .env.example .env   # fill in DATABRICKS_HOST / DATABRICKS_TOKEN
docker compose up -d --build   # or: python3 -m venv venv && pip install -r requirements.txt && python -m app.main
```

See [`ingestion/pumpfun/README.md`](ingestion/pumpfun/README.md) for running it
as a systemd service. Once it is landing files in the Volume, deploy the
Databricks side with `databricks bundle deploy -t dev` from
`orchestration/bundle/` — see [`pumpapi-lakehouse/README.md`](pumpapi-lakehouse/README.md).

---

## Run the Pipeline

1. Start local infrastructure: `cd infra/docker && docker compose up -d`
2. Register the Debezium connector (see above)
3. Seed bulk data: `python3 ingestion/load_bulk_data.py`
4. Deploy jobs: `cd orchestration/bundle && databricks bundle deploy -t dev`
5. Trigger `dvdrental-orchestrator` from the Databricks UI

The bundle orchestrator runs **Silver → Vault+Gold (dbt)**; Bronze is a separate
streaming job. (Under `scripts/deploy_jobs.py` the orchestrator instead chains
**Bronze → Silver → Vault → Vault-Gold**.)

---

## Documentation

| Doc | Contents |
|-----|----------|
| [`docs/index.md`](docs/index.md) | Documentation index |
| [`docs/architecture.md`](docs/architecture.md) | Data flow, technology stack, layers, both pipelines |
| [`docs/components.md`](docs/components.md) | Every component: what it does, how, and why |
| [`docs/quickstart.md`](docs/quickstart.md) | Step-by-step setup |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Common issues and fixes |
| [`docs/target_audience.md`](docs/target_audience.md) | Who this project is for |
| [`ROADMAP.md`](ROADMAP.md) | Current status and next steps |
| [`design/`](design/) | Design documents and implementation logs |
