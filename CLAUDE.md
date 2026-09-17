# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow for Changes

**Before making any changes to this repository:**

1. Create a new branch for your changes
2. Make your changes on that branch
3. Create a pull request
4. **Wait for human approval** before merging

```bash
git checkout -b feature/your-feature-name
# ... make your changes ...
git add .
git commit -m "Description of changes"
git push -u origin feature/your-feature-name
gh pr create --title "..." --body "..."
```

Do NOT commit directly to main/master.

## Project Overview

Two independent pipelines sharing one Databricks lakehouse:

1. **dvdrental CDC** — an end-to-end Change Data Capture pipeline from the
   **dvdrental** PostgreSQL sample database into a medallion lakehouse
   (Bronze → Silver → Vault → Gold).
2. **pump.fun** — a near-real-time pipeline ingesting live Solana trading
   events from the PumpAPI websocket feed.

**All 15 dvdrental tables captured via Debezium:**

Reference / Dimension: `country`, `city`, `address`, `language`, `category`, `actor`, `store`, `staff`, `customer`

Transaction / Fact: `film`, `film_actor`, `film_category`, `inventory`, `rental`, `payment`

## Repository Layout

The repo is organised by **layer**, not by technology:

| Directory | Contents |
|-----------|----------|
| `ingestion/` | Bronze consumers, Debezium connector config, data generators, the pump.fun producer |
| `processing/` | Silver / vault / gold notebooks and shared helpers |
| `transformation/` | dbt project (vault + gold models) and its notebook runner |
| `pumpapi-lakehouse/` | pump.fun Lakeflow Declarative Pipeline source |
| `config/` | Silver per-table configs, `dv_model.json` |
| `orchestration/` | Databricks Asset Bundle, operational scripts |
| `scripts/` | CLI utilities |
| `infra/docker/` | Local CDC stack |
| `design/` | Design documents and implementation logs |
| `docs/` | Project documentation |
| `Agents/`, `skills/` | 26 agent definitions, 25 skill definitions |

Anything named `outdated__*` is superseded and wired into no job — kept for
reference only.

## Local Infrastructure

The compose file lives in `infra/docker/`, so run compose from there:

```bash
cd infra/docker

# Start the CDC stack (Zookeeper, Kafka, PostgreSQL 15, Debezium Connect, Schema Registry, Kafka UI)
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

PostgreSQL runs `infra/docker/init-dvdrental.sh` on first start, restoring the
full dvdrental dataset (~1000 films, ~16k rentals, ~14k payments) and creating
the logical replication publication.

**Known gap:** the `deploy-databricks-jobs`, `upload-vault-config` and
`kafka-to-volume` compose services call `scripts/push_secrets_to_databricks.py`,
`scripts/deploy_job.py`, `scripts/upload_vault_config.py` and
`scripts/kafka_to_volume.py` — none of which exist in the repo. Those profiles
fail as written. The working profiles are `dbt-gold` and `generate-cdc-traffic`.

## Python Setup

```bash
pip install -r requirements.txt

# Copy and fill in Databricks credentials
cp .envexample .env
```

## Data Generators

```bash
# Bulk seed (run once before the pipeline)
python3 ingestion/load_bulk_data.py

# Film generator (updates rental_rate, rental_duration, replacement_cost)
python3 ingestion/load_generator.py

# Rental + payment generator (new rentals, film returns, payments)
python3 ingestion/load_products_generator.py

# Slowly-changing reference data
python3 ingestion/load_reference_generator.py

# Optional env vars: ITERATIONS, SLEEP_MIN, SLEEP_MAX
# DB env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
```

## dbt (Vault + Gold)

```bash
cd transformation/dbt_project
dbt debug                      # verify connection
dbt build --select vault gold  # run models + tests
dbt test                       # data quality tests only
```

The dbt project is named `cdc_gold`. `dbt_packages/` is committed, so no
`dbt deps` is needed at runtime.

## Architecture

### dvdrental data flow

```
PostgreSQL dvdrental (WAL)
  → Debezium Connect (Kafka topics: cdc.public.* — all 15 tables)
    → Databricks Bronze (raw Debezium envelopes in Delta tables)
      → Databricks Silver (current-state via MERGE, with schema evolution)
        → Vault (Data Vault 2.0 — dbt incremental models)
          → dbt Gold (business-ready marts with data quality tests)
```

### pump.fun data flow

```
PumpAPI websocket
  → ingestion/pumpfun (standalone always-on service, outside Databricks)
    → JSONL batch → zstd → base64 → one JSON envelope per file
      → Unity Catalog Volume (/Volumes/workspace/default/mnt/pumpapi)
        → pumpapi-lakehouse (Lakeflow Declarative Pipeline, triggered every 2 min)
          → bronze.pump_events_raw → 5 Silver tables
            → gold.gold_pump_token_risk (separate notebook task)
```

### Notebooks

- **`ingestion/consumers/NB_ingest_to_bronze.ipynb`**: Kafka → Bronze Delta tables (topic pattern `cdc.public.*`, dynamic table naming)
- **`processing/silver/NB_process_to_silver_generic.ipynb`**: Metadata-driven Bronze → Silver for all 15 tables. Reads per-table config from `config/silver/configs/dvdrental/<TABLE_ID>.json`. Handles schema validation, Debezium transforms, deduplication, PII encryption, and MERGE.
- **`processing/vault/NB_*.ipynb`**: The notebook vault layer (metadata loader, hubs, links, satellites, business vault). **Superseded by the dbt vault models**, but still deployed by `scripts/deploy_jobs.py`.
- **`processing/gold/NB_process_pump_token_risk.ipynb`**: pump.fun rug-pull risk scoring → `gold.gold_pump_token_risk`. Kept out of the Lakeflow pipeline because selective per-mint recompute needs `foreachBatch` + `MERGE`.
- **`processing/common/`**: shared helpers — `NB_catalog_helpers`, `NB_schema_contracts`, `NB_schema_drift_helpers`, `NB_silver_metadata`, `NB_pii_catalog_helpers`, `NB_key_management_helpers`, `NB_process_erasure`, `NB_reset_tables`, `NB_confluence_generator`
- **`transformation/NB_run_dbt.ipynb`**: runs dbt via the dbtRunner API (used by the `deploy_jobs.py` path only)

### Databricks Tables

| Layer | Table | Key |
|-------|-------|-----|
| Bronze | workspace.bronze.* (15 dvdrental tables) | — |
| Bronze | workspace.bronze.pump_events_raw | — |
| Silver | workspace.silver.silver_* (15 tables) | entity PK |
| Silver | workspace.silver.pump_{events,tokens,transfers,pools,trades} | — |
| Vault | workspace.vault.hub_* (13 hubs) | SHA-256 HK |
| Vault | workspace.vault.lnk_* (19 links) | composite HK |
| Vault | workspace.vault.sat_* (20 satellites) | HK + LOAD_DATE |
| Vault | workspace.vault.pit_* (4 PITs) | HK + snapshot_date |
| Vault | workspace.vault.brg_* (2 bridges) | — |
| Gold | workspace.gold.gold_* (7 dvdrental marts) | entity PK |
| Gold | workspace.gold.gold_pump_token_risk | mint |
| Monitoring | workspace.monitoring.* (drift, DQ, PII, GDPR) | — |

### Orchestration — two mechanisms, two job graphs

**Databricks Asset Bundles** (`orchestration/bundle/databricks.yml`) — covers everything:

```bash
cd orchestration/bundle
databricks bundle deploy -t dev    # needs the modern unified Databricks CLI
```

| Resource | Type | Tasks |
|----------|------|-------|
| `dvdrental-bronze` | job | 1 (Kafka → Bronze) |
| `dvdrental-silver` | job | 15 (one per table) |
| `dvdrental-vault-gold` | job | 2 dbt_tasks (vault, then gold) |
| `dvdrental-orchestrator` | job | 2 (Silver → Vault+Gold) |
| `pumpapi-lakehouse` | pipeline | Bronze + 5 Silver |
| `pumpfun-bronze` | job | 2 (trigger pipeline, then gold risk) |

**Raw Jobs API** (`scripts/deploy_jobs.py`) — dvdrental only, older graph:

```bash
set -a && source .env && set +a
python3 scripts/deploy_jobs.py [--run-orchestrator]
```

It deploys `dvdrental-bronze`, `dvdrental-silver`, `dvdrental-vault`
(the **notebook** vault layer), `dvdrental-vault-gold` (via `NB_run_dbt`)
and a 4-task orchestrator (Bronze → Silver → Vault → Vault-Gold).

**When editing job definitions, change the bundle.** `deploy_jobs.py` has not
been retired, but the bundle is the direction of travel — see `ROADMAP.md` §4.

### Schema Evolution (Silver Layer)

Silver notebooks dynamically detect new columns in Debezium events, add them to Delta tables via `.option("mergeSchema", "true")`, and rebuild MERGE statements from the current table schema. All schema changes are logged to `monitoring.schema_drift_log`.

### Numeric Decoding (Debezium)

PostgreSQL `NUMERIC` columns (`rental_rate`, `replacement_cost`, `amount`) are encoded by Debezium as `{scale: INT, value: BASE64_STRING}`. Silver notebooks decode them with:
```python
expr("cast(conv(hex(unbase64(raw_value)), 16, 10) as double) / pow(10, scale)")
```

### pump.fun Bronze decode

`pumpapi-lakehouse/transformations/bronze_pump_events.py` uses **`mapInPandas`**,
not a scalar UDF: it decompresses each envelope and yields one row per JSONL
line. The earlier scalar UDF returned one large decompressed-text column per
envelope for Spark to `split()`+`explode()`, which exhausted the reused Python
worker's memory on a real backlog. Bounding batch sizes three different ways did
not help. Serverless pipelines reject `spark.python.worker.reuse=false`, so the
worker also drops buffer references and calls `gc.collect()` per batch.

### DV 2.0 Design

Full vault layer model and generator design: `design/dv2/`
- `DV2_VAULT_LAYER_PLAN.md` — 13 hubs, 19 links, satellites, 4 PITs, 2 bridges
- `DV2_GENERATOR_DESIGN.md` — 7-step generator tool design
- `IMPLEMENTATION_LOG.md` — all 14 modules complete

The generator itself is **archived** (`archive/`, git-ignored). Its output,
`config/datavault/dv_model.json`, is the static source of truth;
`scripts/generate_vault_dbt_models.py` turns it into the dbt vault models.

### Agent System

`Agents/` holds 26 markdown files defining specialized agent personalities;
`skills/` holds 25 reusable skill definitions. The agent loop lives in
`processing/common/autonomous_agent.py` (with `databricks_client.py` and
`databricks_tools.py`) — it generates code via LLM, uploads it to Databricks,
runs it, and retries on failures. See `AGENT_PROMPT_EXAMPLES.md` for prompt
templates.

## DQ + GDPR

Full 4-phase implementation plan: `design/dq_gdpr/IMPLEMENTATION_PLAN.md`
Runbooks: `design/runbooks/DQ_INCIDENT_RUNBOOK.md`, `design/runbooks/ERASURE_SOP.md`

Two parallel workstreams sharing a common monitoring foundation:
- **Data Quality:** Bronze quarantine, Silver/Vault/Gold assertions → `monitoring.dq_results`, dashboards, Slack alerts
- **GDPR crypto-shredding:** per-subject AES-256-GCM DEKs via key vault, Gold `erasure_registry` suppression, `NB_process_erasure` 6-step erasure pipeline, SLA monitoring

**Key architectural decisions:**
- Silver encryption in `NB_process_to_silver_generic.ipynb` only, driven by `processing/vault/pii_config.json`
- Vault satellites store ciphertext as-is (crypto-shred works uniformly)
- Dev key store: Databricks secret scopes (non-production; production = Azure Key Vault / AWS KMS)
- The `dvdrental-dq-gdpr` job is **designed but not deployed** — it is in neither the bundle nor `deploy_jobs.py`
