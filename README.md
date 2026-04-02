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
| Silver | `workspace.silver.silver_film` | film_id |
| Silver | `workspace.silver.silver_rental` | rental_id |
| Silver | `workspace.silver.silver_payment` | payment_id |
| Silver | `workspace.silver.silver_*` (12 ref tables) | entity PK |
| Vault | `workspace.vault.hub_*` (13 hubs) | SHA-256 hash key |
| Vault | `workspace.vault.lnk_*` (17 links) | composite hash key |
| Vault | `workspace.vault.sat_*` (14 satellites) | hub hash key + load_date |
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

PostgreSQL initialises from `docker/init-dvdrental.sh` on first start — this downloads and restores the full dvdrental dataset (~1000 films, ~16k rentals, ~14k payments) and creates the logical replication publication.

### 2. Python Setup

```bash
pip install -r requirements.txt

# Copy and fill in your Databricks credentials
cp .envexample .env
```

### 3. Generate CDC Traffic

```bash
# Film updates (rental_rate, rental_duration, replacement_cost)
python3 generators/load_products_generator.py

# New rentals, returns, and payments
python3 generators/load_generator.py

# Optional env vars: ITERATIONS, SLEEP_MIN, SLEEP_MAX
# DB env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
```

### 4. Databricks Pipeline

Run the Bronze notebook first, then Silver notebooks in parallel, then Vault notebooks:

```bash
# One-command full migration + E2E test (discovers ngrok, resets tables,
# runs generators, triggers the Databricks job, and polls to completion)
python3 skills/docker-databricks-lab-ops/scripts/migrate_and_run.py
```

Or trigger the job manually:

```bash
python3 skills/docker-databricks-lab-ops/scripts/run_databricks_notebook.py \
  --job-id 574281734474239 \
  --notebook-param KAFKA_BOOTSTRAP=<ngrok-host:port>
```

### 5. dbt Gold Layer

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
Orders-ingest-job.yaml          Databricks job definition (Bronze → Silver)
docker/init-dvdrental.sh        PostgreSQL init: download + restore dvdrental, create publication

generators/
  load_generator.py             Rental inserts, returns, payment inserts
  load_products_generator.py    Film attribute updates

notebooks/
  bronze/NB_ingest_to_bronze.ipynb              Kafka → Bronze Delta (topic pattern cdc.public.*)
  silver/NB_process_to_silver.ipynb             Bronze rental → silver.silver_rental
  silver/NB_process_products_silver.ipynb       Bronze film → silver.silver_film
  silver/NB_process_payment_silver.ipynb        Bronze payment → silver.silver_payment
  vault/NB_dv_metadata.ipynb                    DV 2.0 config parser + hash key / DDL helpers  [planned]
  vault/NB_ingest_to_hubs.ipynb                 Silver → 13 Hub tables (SHA-256 insert-only)   [planned]
  vault/NB_ingest_to_links.ipynb                Silver → 17 Link tables (insert-only)          [planned]
  vault/NB_ingest_to_satellites.ipynb           Silver → 14 Satellites (append-only, DIFF_HK)  [planned]
  vault/NB_dv_business_vault.ipynb              PIT tables + Bridge tables                      [planned]
  helpers/NB_catalog_helpers.ipynb              Table/schema creation utilities
  helpers/NB_schema_drift_helpers.ipynb         Schema drift detection + alerting
  helpers/NB_schema_contracts.ipynb             Expected schema definitions (Bronze/Silver/Gold)
  helpers/NB_reset_tables.ipynb                 Drop all tables + clear checkpoints

cdc_gold/                       dbt project for Gold models
skills/docker-databricks-lab-ops/scripts/
  migrate_and_run.py            Full migration + E2E test script
  smoke_test_notebooks.py       Smoke test: Docker + connector + generators + job trigger
  reset_databricks_tables.py    Trigger NB_reset_tables on Databricks
  run_databricks_notebook.py    Submit/trigger a Databricks job or notebook run
  prepare_ngrok_kafka.py        Discover or start ngrok TCP tunnel for Kafka
```

## Notebooks

### Bronze — NB_ingest_to_bronze

Structured streaming from Kafka → Bronze Delta tables.

- Topic pattern: `cdc.public.*` (dynamically names target table from topic)
- Parameters: `KAFKA_BOOTSTRAP`, `TOPIC_PATTERN` (default `cdc.public.*`), `CATALOG`, `BRONZE_SCHEMA`, `CHECKPOINT_PATH`
- Output: `workspace.bronze.{film, rental, payment}`
- Schema: `topic`, `partition`, `offset`, `kafka_timestamp`, `message_key`, `value`, `source_schema`, `table_name`, `ingested_at`

### Silver — per-table MERGE notebooks

Each notebook reads a Bronze table, parses the Debezium envelope, and upserts into Silver.

| Notebook | Source | Target | Merge key |
|----------|--------|--------|-----------|
| NB_process_products_silver | bronze.film | silver.silver_film | film_id |
| NB_process_to_silver | bronze.rental | silver.silver_rental | rental_id |
| NB_process_payment_silver | bronze.payment | silver.silver_payment | payment_id |

Features:
- Schema evolution via `.option("mergeSchema", "true")` — new columns are auto-added
- Dynamic MERGE rebuilt from current table schema at runtime
- Debezium `NUMERIC` decoding: `cast(conv(hex(unbase64(value)), 16, 10) as double) / pow(10, scale)`
- All schema changes logged to `workspace.monitoring.schema_drift_log`

### Helpers

- **NB_catalog_helpers**: `ensure_schema_exists`, `create_silver_table_*`, `build_merge_clauses`, `execute_merge`
- **NB_schema_drift_helpers**: `validate_schema_with_policy`, `SchemaDriftException`, webhook/email/log alerting
- **NB_schema_contracts**: expected Bronze/Silver/Gold schemas for drift detection
- **NB_reset_tables**: drops all Bronze/Silver/Gold/monitoring tables + clears checkpoints. Parameters: `CATALOG`, `DRY_RUN`

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

### Notebook Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SCHEMA_POLICY` | `additive_only` | Validation policy |
| `ALERT_CHANNEL` | `log` | Alert delivery method |
| `WEBHOOK_URL` | (empty) | Slack/Teams webhook URL |

Example — enable Slack alerts:

```python
dbutils.widgets.set("ALERT_CHANNEL", "webhook")
dbutils.widgets.set("WEBHOOK_URL", "https://hooks.slack.com/services/XXX/YYY/ZZZ")
```

### Monitoring Query

```sql
SELECT * FROM workspace.monitoring.schema_drift_log
ORDER BY detected_at DESC
LIMIT 10;
```

## Databricks Job

`Orders-ingest-job.yaml` defines the **dvdrental ingest job** (ID: `574281734474239`):

- **Schedule**: every 5 minutes (`56 4/5 * * * ?`, Europe/Belgrade)
- **Ingest_to_Bronze**: reads from Kafka, writes all 15 `bronze.*` tables
- **ingest_*_To_Silver** — all 15 Silver tasks run in parallel after Bronze succeeds
- **NB_ingest_to_hubs** ┐
- **NB_ingest_to_links** ├ Vault tasks run in sequence after Silver (planned)
- **NB_ingest_to_satellites** ┘
- **NB_dv_business_vault** — PIT + Bridge after satellites complete (planned)
- **Git source**: `https://github.com/alexeyban/databricks-lab` branch `main`

Update the job via API after changing the YAML:

```bash
python3 skills/docker-databricks-lab-ops/scripts/migrate_and_run.py \
  --skip-legacy-drop --skip-reset --skip-docker \
  --kafka-bootstrap <host:port>
```

## ngrok for Local Development

Databricks cannot reach a local Kafka directly. Expose it via ngrok:

```bash
python3 skills/docker-databricks-lab-ops/scripts/prepare_ngrok_kafka.py
```

Then start Compose with the discovered public endpoint:

```bash
export KAFKA_EXTERNAL_HOST=<ngrok-host>
export KAFKA_EXTERNAL_PORT=<ngrok-port>
docker compose up -d
```

Pass `KAFKA_BOOTSTRAP=<ngrok-host>:<ngrok-port>` when triggering the job. Do not commit ngrok values — they change on every restart.

## Full E2E Smoke Test

```bash
python3 skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py \
  --job-id 574281734474239 \
  --film-iterations 6 \
  --rental-iterations 12 \
  --reset                  # drop all tables + checkpoints before running
```

Or use `migrate_and_run.py` which also handles legacy table drops and job definition updates.

## Secret Hygiene

Do not commit real credentials. Use `.envexample` as the template:

```bash
cp .envexample .env
# edit .env with real DATABRICKS_HOST and DATABRICKS_TOKEN
```

`.env`, `.databrickscfg`, and generated logs are git-ignored.

## Agent System

`/Agents/` — 24 markdown files defining specialised agent personalities.  
`/skills/` — 24 reusable skill definitions.  
`/runtime/` — Python agent loop (`autonomous_agent.py`) that generates code via LLM, uploads to Databricks, runs it, and retries on failures.

See `AGENT_PROMPT_EXAMPLES.md` for prompt templates.
