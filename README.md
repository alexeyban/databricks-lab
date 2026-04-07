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
| `public.film` | Updates: rental_rate, rental_duration, replacement_cost |
| `public.film_actor` | Junction: film ↔ actor |
| `public.film_category` | Junction: film ↔ category |
| `public.inventory` | Film copies per store |
| `public.rental` | Inserts: new rentals; Updates: return_date set on return |
| `public.payment` | Inserts: payments for completed rentals |

### Databricks Unity Catalog Tables

| Layer | Table | Merge / hash key |
|-------|-------|-----------------|
| Bronze | `workspace.bronze.*` (15 tables) | — |
| Silver | `workspace.silver.silver_*` (15 tables) | entity PK |
| Vault | `workspace.vault.hub_*` (13 hubs) | SHA-256 hash key |
| Vault | `workspace.vault.lnk_*` (19 links) | composite hash key |
| Vault | `workspace.vault.sat_*` (15 satellites) | hub hash key + load_date |
| Vault | `workspace.vault.pit_*` (4 PIT tables) | hub hash key + snapshot_date |
| Vault | `workspace.vault.brg_*` (2 bridge tables) | — |
| Gold | `workspace.gold.gold_film` | film_id |
| Gold | `workspace.gold.gold_rental` | rental_id |
| Monitoring | `workspace.monitoring.schema_drift_log` | — |

## Quick Start

### 1. Local Infrastructure

```bash
# Start the full CDC stack (Zookeeper, Kafka, PostgreSQL 15, Debezium Connect,
# Schema Registry, Kafka UI)
docker compose up -d

# Wait ~30s for Kafka Connect, then register the Debezium connector
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @postgres-connector.json

# Check connector status
curl http://localhost:8083/connectors/postgres-connector/status

# Kafka UI
open http://localhost:8085
```

PostgreSQL initialises from `docker/init-dvdrental.sh` on first start — restores the full
dvdrental dataset (~1000 films, ~16k rentals, ~14k payments) and creates the logical
replication publication.

### 2. Python Setup

```bash
pip install -r requirements.txt

# Copy and fill in your Databricks credentials
cp .envexample .env
```

### 3. Upload Vault Config

The vault notebooks read `dv_model.json` from a Unity Catalog Volume. Upload it once:

```bash
set -a && source .env && set +a
python3 scripts/upload_vault_config.py
```

### 4. Deploy Databricks Jobs

```bash
set -a && source .env && set +a

# Deploy all 4 jobs (Bronze / Silver / Vault / Orchestrator)
python3 scripts/deploy_job.py --kafka-bootstrap <ngrok-host:port>

# Deploy and immediately trigger the full pipeline
python3 scripts/deploy_job.py --kafka-bootstrap <ngrok-host:port> --run
```

This creates/updates four Databricks jobs:

| Job | What it does |
|-----|-------------|
| `dvdrental-bronze` | Kafka → 15 Bronze Delta tables (streaming) |
| `dvdrental-silver` | 15 parallel Bronze → Silver tasks (generic notebook) |
| `dvdrental-vault` | Hubs → Links + Satellites (parallel) → Business Vault |
| `dvdrental-orchestrator` | Chains the three jobs above in sequence |

### 5. Generate CDC Traffic

```bash
# Film updates (rental_rate, rental_duration, replacement_cost)
python3 generators/load_products_generator.py

# New rentals, returns, and payments
python3 generators/load_generator.py

# Optional env vars: ITERATIONS, SLEEP_MIN, SLEEP_MAX
# DB env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
```

### 6. dbt Gold Layer

```bash
cd cdc_gold
dbt debug          # verify connection
dbt build          # run models + tests
dbt test           # data quality tests only
```

## Repository Layout

```
docker-compose.yml              Local CDC stack definition
postgres-connector.json         Debezium PostgreSQL connector config
docker/init-dvdrental.sh        PostgreSQL init: restore dvdrental, create publication

generators/
  load_generator.py             Rental inserts, returns, payment inserts
  load_products_generator.py    Film attribute updates
  dv_generator/                 DV 2.0 meta-tool (analyze → review → apply)

notebooks/
  bronze/NB_ingest_to_bronze.ipynb              Kafka → Bronze Delta (topic pattern cdc.public.*)
  silver/NB_process_to_silver_generic.ipynb     Metadata-driven Bronze → Silver (all 15 tables)
  silver/NB_process_to_silver.ipynb             Legacy: rental-specific MERGE (kept for reference)
  silver/NB_process_products_silver.ipynb       Legacy: film-specific MERGE (kept for reference)
  silver/NB_process_payment_silver.ipynb        Legacy: payment-specific MERGE (kept for reference)
  vault/NB_dv_metadata.ipynb                    DV 2.0 config parser + hash key / DDL helpers
  vault/NB_ingest_to_hubs.ipynb                 Silver → 13 Hub tables (SHA-256 insert-only)
  vault/NB_ingest_to_links.ipynb                Silver → 19 Link tables (insert-only)
  vault/NB_ingest_to_satellites.ipynb           Silver → 15 Satellites (append-only, DIFF_HK)
  vault/NB_dv_business_vault.ipynb              PIT tables + Bridge tables
  helpers/NB_catalog_helpers.ipynb              Table/schema creation utilities
  helpers/NB_schema_drift_helpers.ipynb         Schema drift detection + alerting
  helpers/NB_schema_contracts.ipynb             Expected schema definitions (Bronze/Silver/Gold)
  helpers/NB_silver_metadata.ipynb              Silver metadata loader (pipeline_configs/*.json)
  helpers/NB_reset_tables.ipynb                 Drop all tables + clear checkpoints

pipeline_configs/
  silver/dvdrental/*.json         15 Silver table configs (field mappings, PKs, transforms)
  datavault/dv_model.json         Approved DV 2.0 model (13 hubs, 19 links, 15 sats, 4 PITs, 2 bridges)

scripts/
  deploy_job.py                   Create/update all 4 Databricks jobs; optionally trigger run
  upload_vault_config.py          Upload dv_model.json to Unity Catalog Volume (run once)
  smoke_test_vault.py             Validate vault row counts via SQL execution API

cdc_gold/                         dbt project for Gold models
```

## Notebooks

### Bronze — NB_ingest_to_bronze

Structured streaming from Kafka → Bronze Delta tables.

- Topic pattern: `cdc.public.*` (dynamically names target table from topic)
- Parameters: `KAFKA_BOOTSTRAP`, `TOPIC_PATTERN`, `CATALOG`, `BRONZE_SCHEMA`, `CHECKPOINT_PATH`
- Output: one `workspace.bronze.<table>` per source table

### Silver — NB_process_to_silver_generic

Single metadata-driven notebook that handles all 15 tables. Each Silver task in the
Databricks job calls this notebook with a different `TABLE_ID`.

Parameters: `TABLE_ID`, `CATALOG`, `BRONZE_SCHEMA`, `SILVER_SCHEMA`, `CHECKPOINT_ROOT`,
`MONITORING_SCHEMA`, `SCHEMA_POLICY`, `ALERT_CHANNEL`, `WEBHOOK_URL`, `RUN_BOOTSTRAP_HOOKS`

Per-table config is loaded from `pipeline_configs/silver/dvdrental/<TABLE_ID>.json` and
defines: Bronze/Silver table names, CDC + Silver schema contracts, primary keys, dedupe
ordering, field mappings, and optional transforms.

**Transforms supported:**

| Transform | Use case |
|-----------|---------|
| `plain` | Direct column copy (coalesce after/before) |
| `epoch_micros_to_timestamp` | Debezium microsecond timestamps → Spark timestamp |
| `decimal_from_debezium_bytes` | Debezium Base64-encoded NUMERIC bytes → decimal |
| `decimal_from_json_paths` | Variable-scale NUMERIC from JSON structure |

Features:
- Schema drift detection + alerting per `SCHEMA_POLICY`
- Graceful skip when Bronze table doesn't exist yet
- Deduplication within each micro-batch via `row_number()` window
- MERGE keyed by `primary_keys` from the table config

### Vault — NB_dv_metadata

Loads `dv_model.json` from a Unity Catalog Volume and exposes shared helpers:

- `load_model(path)` — reads the DV 2.0 config JSON; accepts absolute Volume paths or relative (resolved under `/Volumes/workspace/default/mnt/`)
- `generate_hash_key(bk_cols)` — SHA-256 hash key expression
- `generate_diff_hash(tracked_cols)` — NULL-safe DIFF_HK for satellite change detection
- DDL helpers: `create_hub_table`, `create_link_table`, `create_sat_table`, `create_pit_table`, `create_bridge_table`

### Vault — NB_ingest_to_hubs

Insert-only MERGE from Silver → 13 Hub tables. Watermarked by `LOAD_DATE`.

- Pattern: `MERGE INTO vault.hub_X USING source ON HK_X = HK_X WHEN NOT MATCHED THEN INSERT`

### Vault — NB_ingest_to_links

Insert-only MERGE from Silver → 19 Link tables. Runs after Hubs are populated.

- Composite hash key from all FK columns
- Each link record references ≥ 2 hub hash keys

### Vault — NB_ingest_to_satellites

Append-only insert from Silver → 15 Satellite tables using DIFF_HK change detection.

- `LEFT JOIN` latest DIFF_HK per hub key; insert only where DIFF_HK changed or is new
- History preserved by `LOAD_DATE` — no end-dating

### Vault — NB_dv_business_vault

Builds PIT (Point-in-Time) and Bridge tables from the vault layer.

- **PIT tables** (4): daily snapshot spine for HUB_FILM, HUB_RENTAL, HUB_CUSTOMER, HUB_PAYMENT
- **Bridge tables** (2): BRG_RENTAL_FILM (rental → inventory → film), BRG_FILM_CAST (film → actor)

### Helpers

- **NB_catalog_helpers**: `ensure_schema_exists`, `build_merge_clauses`, `execute_merge`
- **NB_schema_drift_helpers**: `validate_schema_with_policy`, `SchemaDriftException`, webhook/email/log alerting
- **NB_schema_contracts**: expected Bronze/Silver schemas for all 15 tables
- **NB_silver_metadata**: `get_silver_table_config(table_id)` — loads per-table config from `pipeline_configs/silver/dvdrental/`
- **NB_reset_tables**: drops all Bronze/Silver/Gold/monitoring tables + clears checkpoints

## Schema Evolution

Silver tables support automatic schema evolution — new columns in Debezium events are detected and added:

1. `.option("mergeSchema", "true")` on streaming writes
2. MERGE statements rebuilt dynamically from the current live table schema
3. All changes logged to `workspace.monitoring.schema_drift_log`

## Schema Drift Detection

Configurable per-layer policy with alerting.

### Policies

| Policy | Behaviour | Default layer |
|--------|-----------|--------------|
| `permissive` | Log only, never block | Bronze |
| `additive_only` | Allow new columns, block removals/type changes | Silver |
| `strict` | Block on any schema change | Gold |

### Alert Channels

`log` · `webhook` (Slack/Teams) · `email` (SMTP) · `all`

### Monitoring Query

```sql
SELECT * FROM workspace.monitoring.schema_drift_log
ORDER BY detected_at DESC
LIMIT 10;
```

## Databricks Jobs

Defined and deployed via `scripts/deploy_job.py`. Four jobs, each independently runnable:

### dvdrental-bronze

Kafka → Bronze Delta streaming. Requires Kafka to be reachable (local dev: use ngrok).

**Parameters:** `KAFKA_BOOTSTRAP`, `TOPIC_PATTERN`, `CATALOG`, `BRONZE_SCHEMA`, `CHECKPOINT_PATH`

### dvdrental-silver

15 parallel tasks, each calling `NB_process_to_silver_generic` with a different `TABLE_ID`.
Gracefully skips tables whose Bronze Delta does not yet exist.

**Parameters (per task):** `TABLE_ID`, `CATALOG`, `BRONZE_SCHEMA`, `SILVER_SCHEMA`,
`CHECKPOINT_ROOT`, `MONITORING_SCHEMA`, `SCHEMA_POLICY`

### dvdrental-vault

Sequential DAG: Hubs → (Links ‖ Satellites) → Business Vault.

**Parameters (all tasks):** `CATALOG`, `VAULT_SCHEMA`, `MODEL_PATH`

`MODEL_PATH` defaults to `/Volumes/workspace/default/mnt/pipeline_configs/datavault/dv_model.json`.
Upload the model file with `scripts/upload_vault_config.py` before the first run.

### dvdrental-orchestrator

Chains the three jobs above: Bronze → Silver → Vault. No git source — uses `run_job_task`.

### Deploy / redeploy

```bash
set -a && source .env && set +a

# Deploy only (or update existing jobs)
python3 scripts/deploy_job.py --kafka-bootstrap <host:port>

# Deploy + force fresh checkpoints + run immediately
python3 scripts/deploy_job.py --kafka-bootstrap <host:port> --checkpoint-suffix v4 --run
```

## ngrok for Local Development

Databricks Serverless cannot reach a local Kafka directly. Expose it via ngrok:

```bash
ngrok tcp 9093
# → e.g. 6.tcp.eu.ngrok.io:16223
export KAFKA_EXTERNAL_HOST=6.tcp.eu.ngrok.io
export KAFKA_EXTERNAL_PORT=16223
docker compose up -d
```

Pass the ngrok address as `--kafka-bootstrap` when deploying. Note: Databricks Serverless
has outbound network restrictions that may block ngrok endpoints depending on workspace
configuration. If Bronze fails with a Kafka timeout, run Silver and Vault independently
(Bronze Delta tables from a previous run persist in Unity Catalog).

## Secret Hygiene

Do not commit real credentials. Use `.envexample` as the template:

```bash
cp .envexample .env
# edit .env with real DATABRICKS_HOST and DATABRICKS_TOKEN
```

`.env`, `.databrickscfg`, and generated logs are git-ignored.

## DV 2.0 Generator

`generators/dv_generator/` is a **meta-tool** that automates DV 2.0 model creation from any Silver layer schema. It replaces manual vault design with a 7-step pipeline:

```
step1: schema analysis    → 01_schema_analysis.json
step2: heuristic rules    → 02_classification.json
step2b: AI classifier     → 02b_merged_classification.json  (optional, needs LLM env)
step3: artifact gen       → 03_dv_model_draft.json + query_templates/
step3b: notebook gen      → notebooks/vault/*.ipynb
step4: documentation      → 04_diagram.drawio + 04_documentation.md
step5: human review       → 05_review_notebook.ipynb  ← PAUSE
step6: validation         → 06_validation_report.json
step7: apply              → pipeline_configs/datavault/dv_model.json + notebooks/vault/
```

### Quick start

```bash
# Full run — analyzes Silver configs, generates notebooks, pauses for review
python -m generators.dv_generator.main --analyze \
  --config-dir pipeline_configs/silver/dvdrental --no-ai

# After reviewing 05_review_notebook.ipynb in Jupyter:
python -m generators.dv_generator.main --resume <session_id> \
  --from-step step6_validator

# With AI semantic classifier (requires LLM env vars):
python -m generators.dv_generator.main --analyze \
  --config-dir pipeline_configs/silver/dvdrental
```

### Key files

| File | Description |
|------|-------------|
| `pipeline_configs/datavault/dv_model.json` | Final approved DV 2.0 config |
| `pipeline_configs/silver/dvdrental/*.json` | Silver table configs (15 files) |
| `design/dv2/DV2_VAULT_LAYER_PLAN.md` | Full vault design (hubs/links/sats/PITs/bridges) |
| `design/dv2/DV2_GENERATOR_DESIGN.md` | Generator architecture decisions |
| `design/dv2/IMPLEMENTATION_LOG.md` | Module completion status |

## Agent System

`/Agents/` — 24 markdown files defining specialised agent personalities.  
`/skills/` — 24 reusable skill definitions.  
`/runtime/` — Python agent loop (`autonomous_agent.py`) that generates code via LLM, uploads to Databricks, runs it, and retries on failures.

See `AGENT_PROMPT_EXAMPLES.md` for prompt templates.
