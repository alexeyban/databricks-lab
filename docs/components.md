# Components

This document provides a detailed breakdown of all components in the Databricks CDC Lakehouse Lab, explaining what each component does, how it works, and why it was developed.

---

## Table of Contents

1. [Infrastructure](#infrastructure)
2. [Data Generation](#data-generation)
3. [Ingestion Layer](#ingestion-layer)
4. [Enterprise Layer](#enterprise-layer)
5. [Analytics Layer](#analytics-layer)
6. [Automation & Tools](#automation--tools)
7. [pump.fun Pipeline Components](#pumpfun-pipeline-components)

---

## Infrastructure

### Docker CDC Stack

**What it does**: Provides a complete local development environment with PostgreSQL, Kafka, Debezium, and supporting services.

**How it works**:
- `infra/docker/docker-compose.yml` defines 6 always-on services: Zookeeper, Kafka, Schema Registry, PostgreSQL, Debezium Connect, Kafka UI, plus opt-in `manual`/`kafka-to-volume` profile services
- PostgreSQL is configured with `wal_level=logical` for logical replication
- Debezium Connect reads PostgreSQL WAL and publishes to Kafka topics
- Schema Registry manages Avro schemas for Kafka messages

**Why it was developed**:
- Enables local development without cloud dependencies
- Provides a complete, reproducible CDC pipeline for testing
- Allows developers to iterate quickly without infrastructure costs
- Serves as a reference architecture for production deployments

**Files**:
- `infra/docker/docker-compose.yml` - Main stack definition (run `docker compose` from `infra/docker/`)
- `infra/docker/init-dvdrental.sh` - Database initialization script
- `ingestion/cdc/postgres-connector.json` - Debezium connector configuration

---

### PostgreSQL Source Database

**What it does**: The source database containing the dvdrental sample data with 15 tables.

**How it works**:
- Runs in Docker container with logical replication enabled
- On first start, restores dvdrental from SQL dump via `init-dvdrental.sh`
- Creates logical replication publication `dbz_publication` for all tables
- Creates replication slot `debezium_slot` for Debezium

**Why it was developed**:
- Provides realistic source data for testing CDC pipelines
- dvdrental is a well-known sample database (like Sakila for MySQL)
- Includes both reference/dimension tables and transaction/fact tables
- Supports various CDC patterns: inserts, updates, relationships

---

## Data Generation

### Rental + Payment Generator

**What it does**: Simulates real business activity by continuously inserting rentals, processing returns, and creating payments.

**How it works** (`ingestion/load_generator.py`):
- Connects to PostgreSQL directly
- Randomly selects actions: insert rental (40%), return rental (35%), insert payment (25%)
- Inserts new rentals with random customer/inventory/staff
- Updates return_date on existing rentals
- Inserts payments for returned rentals without payment

**Why it was developed**:
- Generates continuous CDC traffic for pipeline testing
- Simulates realistic business patterns (not just bulk loads)
- Allows testing of streaming and micro-batch processing
- Provides varied data for testing DQ checks

**Usage**:
```bash
python3 ingestion/load_generator.py
# With custom settings:
ITERATIONS=100 SLEEP_MIN=0.5 SLEEP_MAX=2.0 python3 ingestion/load_generator.py
```

---

### Bulk Data Loader

**What it does**: Seeds a realistic initial data volume into PostgreSQL before running the pipeline for the first time.

**How it works** (`ingestion/load_bulk_data.py`):
- Inserts 1000 customers and 1000 films
- Generates 10000+ DML events (rentals, returns, payments, film updates)
- Designed for one-time use before pipeline launch to ensure non-trivial data volumes

**Usage**:
```bash
python3 ingestion/load_bulk_data.py
```

---

### Film Update Generator

**What it does**: Simulates catalog changes by updating film attributes (rental rate, duration, replacement cost).

**How it works** (`ingestion/load_products_generator.py`):
- Randomly selects films and updates their pricing/duration attributes
- Waits for initial dvdrental load before starting
- Runs continuously until interrupted

**Why it was developed**:
- Tests update CDC capture (not just inserts)
- Specifically targets high-change attributes (rental_rate changes frequently)
- Helps test satellite change detection (DIFF_HASH)
- Validates that pricing changes are properly historized in Vault

**Usage**:
```bash
python3 ingestion/load_products_generator.py
```

---

### Reference Data Generator

**What it does**: Updates slowly-changing reference data (addresses, categories, etc.).

**How it works** (`ingestion/load_reference_generator.py`):
- Modifies reference tables at lower frequency
- Helps test handling of low-change vs high-change attributes

**Why it was developed**:
- Provides variety in change rates across satellites
- Tests satellite strategies (high-change vs low-change separation)
- Enables testing of PIT table functionality with real time-travel data

**Usage**:
```bash
python3 ingestion/load_reference_generator.py
```

Both continuous generators can also be run together in Docker:
```bash
cd infra/docker && docker compose --profile manual up -d generate-cdc-traffic
```

---

## Ingestion Layer

### Bronze Layer - Kafka to Delta

**What it does**: Ingests raw CDC events from Kafka into Bronze Delta tables.

**How it works** (`ingestion/consumers/NB_ingest_to_bronze.ipynb`):
- Uses Databricks Auto Loader (cloudFiles) to read from Unity Catalog Volume
- Alternatively uses structured streaming from Kafka directly
- Creates 15 Bronze tables, one per source table
- Stores full Debezium envelopes: operation type, before/after values, metadata
- Uses checkpointing for exactly-once semantics

**Why it was developed**:
- Provides immutable, raw CDC storage for audit and replay
- Captures every change event (no data loss)
- Schema evolution support via mergeSchema option
- Foundation for all downstream processing

**Key features**:
- Dynamic table naming based on Kafka topic
- Schema drift handling
- Checkpoint recovery

---

### Silver Layer - Generic Processing

**What it does**: Transforms Bronze CDC events into clean, current-state Silver tables.

**How it works** (`processing/silver/NB_process_to_silver_generic.ipynb`):
- Reads from Bronze tables
- Applies Debezium transforms (flattens payload, extracts before/after)
- Decodes PostgreSQL NUMERIC types (encoded as base64 + scale)
- Deduplicates via MERGE on entity primary key
- Driven by per-table JSON configs in `config/silver/configs/dvdrental/`

**Why it was developed**:
- Creates clean, business-ready tables from raw CDC
- Eliminates duplicates from CDC event replay
- Provides consistent schema across all tables
- Configurable per-table (different PKs, columns, transforms)
- Schema evolution support (new columns auto-added)

**Configuration** (`config/silver/configs/dvdrental/<table>.json`):
```json
{
  "table_id": "film",
  "primary_key": "film_id",
  "source_topic": "cdc.public.film",
  "columns": [
    {"name": "film_id", "type": "int"},
    {"name": "title", "type": "string"},
    {"name": "rental_rate", "type": "decimal"}
  ]
}
```

---

### Legacy Silver Notebooks

**What it does**: Legacy notebooks for specific tables (kept for reference).

**Files** (all under `processing/silver/`):
- `outdated__NB_process_to_silver.ipynb` - Rental-specific
- `outdated__NB_process_products_silver.ipynb` - Film-specific
- `outdated__NB_process_payment_silver.ipynb` - Payment-specific
- `outdated__NB_process_pumpfun_silver.ipynb` - pump.fun, superseded by the Lakeflow pipeline

**Why they exist**: Preserved for reference and comparison with the generic approach.
The `outdated__` prefix marks them as not wired into any job.

---

## Enterprise Layer

> **Status:** the Vault layer now ships as **dbt models**
> (`transformation/dbt_project/models/vault/`). The notebooks described in this
> section still exist under `processing/vault/` and are what
> `scripts/deploy_jobs.py` deploys as the `dvdrental-vault` job; the Asset
> Bundle path does not use them. See *dbt Project* below for the model counts
> that are actually current.

### Vault Metadata Notebook

**What it does**: Loads and parses the Data Vault 2.0 configuration, provides utility functions.

**How it works** (`processing/vault/NB_dv_metadata.ipynb`):
- Reads `dv_model.json` from Unity Catalog Volume
- Parses hub, link, satellite, PIT, bridge configurations
- Provides hash key generation functions:
  - `generate_hash_key()` - SHA-256 for hub keys
  - `generate_diff_hash()` - For satellite change detection
- Provides DDL helpers for creating vault tables

**Why it was developed**:
- Single source of truth for vault configuration
- Centralizes hash key computation (ensures consistency)
- Reduces code duplication across vault notebooks

---

### Hub Ingestion

**What it does**: Loads business keys into Hub tables.

**How it works** (`processing/vault/NB_ingest_to_hubs.ipynb`):
- Reads from Silver tables
- Computes SHA-256 hash key: `sha2(concat_ws("||", UPPER(TRIM(key))), 256)`
- MERGE on hash key (insert-only, no updates)
- Watermarked on `last_updated_dt` for incremental loads
- Creates 13 hub tables: film, rental, payment, customer, inventory, actor, staff, store, address, city, country, language, category

**Why it was developed**:
- Foundation of Data Vault 2.0 architecture
- Provides single version of truth for business keys
- Hash keys enable reliable linkage across tables
- Insert-only design ensures audit trail

---

### Link Ingestion

**What it does**: Loads relationships between hubs into Link tables.

**How it works** (`processing/vault/NB_ingest_to_links.ipynb`):
- Reads from Silver tables
- Resolves hash keys for each foreign key
- Computes composite hash key for the relationship
- Creates 19 link tables: rental-customer, rental-inventory, payment-rental, film-actor, etc.

**Why it was developed**:
- Captures all business relationships in standardized format
- Composite hash keys ensure relationship uniqueness
- Enables flexible querying across relationships
- Foundation for bridge tables and complex joins

---

### Satellite Ingestion

**What it does**: Loads attribute changes into Satellite tables.

**How it works** (`processing/vault/NB_ingest_to_satellites.ipynb`):
- Reads from Silver tables
- Computes DIFF_HASH: `sha2(concat_ws("||", coalesce(col, "NULL") for each column), 256)`
- Left joins against current satellite hash per hub key
- Appends only new/changed rows (append-only, no updates or deletes)
- Creates 15 satellite tables split by change rate (the dbt vault layer that superseded these notebooks has 20)

**Why it was developed**:
- Provides complete history of all attribute changes
- Append-only design ensures audit compliance
- DIFF_HASH enables efficient change detection
- Separate satellites for high/low change rate data
- No end-dating needed (PIT tables handle time-travel)

---

### Business Vault (PIT + Bridge)

**What it does**: Creates query-acceleration structures.

**How it works** (`processing/vault/NB_dv_business_vault.ipynb`):
- **PIT Tables**: Daily snapshots linking hub keys to satellite load dates
- **Bridge Tables**: Pre-joined multi-hop paths (rental→film, film→actor)

**Why it was developed**:
- PIT tables enable time-travel queries without full satellite scans
- Bridge tables simplify downstream queries
- Pre-computed joins improve query performance
- Separates historical storage from current-state access

---

## Analytics Layer

### NB_run_dbt / dbt Vault+Gold

**What it does**: Runs the full dbt build (vault + gold models) as a Databricks notebook task.

**How it works** (`transformation/NB_run_dbt.ipynb`):
- Uses the dbtRunner Python API — no subprocess or shell exec
- Selects `vault gold` targets in one `dbt build` call
- Runs as a single-task Databricks job (`dvdrental-vault-gold`) on the
  `scripts/deploy_jobs.py` path. The Asset Bundle path skips this notebook
  entirely and runs `dbt build` as two native `dbt_task`s instead
- dbt_packages are pre-committed to the repo; no `dbt deps` needed at runtime

**Why it was developed**:
- Integrates dbt into the Databricks job orchestration without a separate dbt Cloud account
- Serverless-compatible (pure Python, no CLI dependencies)
- Enables vault incremental models and gold marts to run in the same job

---

### dbt Project

**What it does**: Transforms Vault and Silver data into business-ready analytics models with built-in tests.

**Location**: `transformation/dbt_project/`

**Models**:

*Vault models* (`models/vault/`) — incremental, write to `workspace.vault.*`:
- **13 hub models** (hubs/): one per entity — actor, address, category, city,
  country, customer, film, inventory, language, payment, rental, staff, store
- **19 link models** (links/): all relationships
- **20 satellite models** (satellites/): attribute history, split by change rate
  (`*_core` / `*_details` / `*_pricing`)
- **4 PIT tables** (pit/): `pit_customer`, `pit_film`, `pit_payment`, `pit_rental`
  (materialized as tables)
- **2 bridge tables** (bridge/): `brg_film_cast`, `brg_rental_film`

*Gold models* (`models/gold/`) — write to `workspace.gold.*`. `gold_film` and
`gold_rental` are incremental; the other five are full-refresh tables:
- `gold_film`: Film with `rental_rate_tier` derived column
- `gold_rental`: Rental with `rental_status` and `total_paid`
- `gold_customer_summary`: Customer lifetime value and rental history
- `gold_inventory_status`: Stock levels and utilisation per store
- `gold_revenue_by_store`: Revenue aggregation by store
- `gold_film_popularity`: Rental frequency and revenue per film
- `gold_staff_performance`: Rental and payment counts per staff member

**Sources**: Silver tables in `workspace.silver.*`

**Why it was developed**:
- Provides business-facing analytics layer with version-controlled transformations
- Built-in data quality tests ensure reliability
- dbt incremental models keep vault layer in sync with Silver

**Local usage**:
```bash
cd transformation/dbt_project
dbt debug                          # Verify connection
dbt build --select vault gold      # Run models + tests
dbt test                           # Tests only
```

---

## Automation & Tools

### Job Deployment Script

**What it does**: Deploys/redeploys all Databricks jobs.

**How it works** (`scripts/deploy_jobs.py`):
- Creates/updates 5 dvdrental jobs: Bronze, Silver, Vault, Vault-Gold, Orchestrator
- Configures Git source (`git_source` pinned to `main`) for notebooks
- Sets up task dependencies (orchestrator chains all 4 sub-jobs)
- Supports `--run-orchestrator` / `--run-silver` for immediate execution after deploy
- Deploys **nothing** for pump.fun — the Pipelines API doesn't support `git_source`

**Why it was developed**:
- Infrastructure-as-code for Databricks jobs before Asset Bundles were adopted
- Reproducible deployments from a single command

---

### Databricks Asset Bundle

**What it does**: Declares every Databricks resource — dvdrental jobs plus the
pump.fun pipeline and its trigger job — in one file.

**How it works** (`orchestration/bundle/databricks.yml`):
- `dev` and `prod` targets; `sync.paths` covers the whole repo because task
  paths reach outside `orchestration/bundle/`
- Builds the Vault layer with native `dbt_task`s instead of the vault notebooks
- Declares the `pumpapi-lakehouse` Lakeflow pipeline (serverless, triggered) and
  the `pumpfun-bronze` job that fires it every 2 minutes

```bash
cd orchestration/bundle && databricks bundle deploy -t dev
```

**Why it was developed**:
- Environment-aware deployments (dev/prod targets)
- The only way to deploy the Lakeflow pipeline
- Supersedes `scripts/deploy_jobs.py`, which has not been retired yet

---

### Vault dbt Model Generator

**What it does**: Generates the dbt vault models from the Data Vault config.

**How it works** (`scripts/generate_vault_dbt_models.py`):
- Reads `config/datavault/dv_model.json`
- Emits hub / link / satellite / PIT / bridge models into
  `transformation/dbt_project/models/vault/`

**Why it was developed**:
- Keeps 58 vault models consistent with one config file
- Re-running it is how you add an entity to the vault layer

---

### Missing scripts referenced by Docker Compose

`infra/docker/docker-compose.yml` still declares `manual`-profile services that
invoke scripts which **are not in the repo**. These profiles fail as written:

| Compose service | Script it calls | Present? |
|-----------------|-----------------|----------|
| `deploy-databricks-jobs` | `scripts/push_secrets_to_databricks.py` | No |
| `deploy-databricks-jobs` | `scripts/deploy_job.py` (note: the real file is `deploy_jobs.py`) | No |
| `upload-vault-config` | `scripts/upload_vault_config.py` | No |
| `kafka-to-volume` | `scripts/kafka_to_volume.py` | No |

Secrets and vault-config upload therefore have no working automation in-repo
right now; the vault dbt models read their config from
`config/datavault/dv_model.json` at build time rather than from a Volume, so
only the notebook vault path needs the upload.

---

### DV 2.0 Generator

**What it does**: Auto-generates Data Vault design from Silver schema.

**Status**: **archived.** The generator has been moved to `archive/` (git-ignored)
now that its output — `config/datavault/dv_model.json` — is committed and treated
as the static source of truth. It is no longer importable from the repo.

**What it did**:
- 7-step CLI tool: analyze → classify → generate → review → validate → apply
- Analyzed the Silver schema and classified entities (hub/link/sat)
- Generated vault notebooks and `dv_model.json`
- Included an AI classifier for entity classification

**What remains**:
- `config/datavault/dv_model.json` — the approved output, still the vault config
- `design/dv2/` — the design documents and implementation log
- `generated/dv_sessions/` — the recorded generator sessions
- `scripts/generate_vault_dbt_models.py` — turns `dv_model.json` into dbt models

---

### Agent System

**What it does**: AI-assisted automation for Databricks operations.

**How it works**:
- `Agents/` - 26 specialized agent roles (data engineer, architect, job operator, etc.)
- `skills/` - 25 reusable skill definitions
- `processing/common/autonomous_agent.py` - Agent loop that generates code, uploads, runs, retries
- `processing/common/databricks_client.py` / `databricks_tools.py` - Workspace API helpers used by that loop

**Agents include**:
- `engineering-data-engineer.md` - CDC, dbt, pipeline work
- `databricks_architect.md` - Lakehouse topology decisions
- `databricks-job-operator.md` - Run jobs, capture run_id, track state
- `databricks-notebook-remediator.md` - Diagnose and fix failures
- `databricks-data-quality-analyst.md` - Validate output quality

**Why it was developed**:
- Automates repetitive Databricks tasks
- Provides consistent best practices
- Reduces manual intervention in operations
- Enables self-healing pipelines

---

### Data Quality & GDPR Tools

**What it does**: Schema drift detection, PII tagging, crypto-shredding erasure.

**How it works**:
- `NB_schema_drift_helpers.ipynb` - Detects schema changes with configurable policies
- `NB_pii_catalog_helpers.ipynb` - Tags PII columns in Unity Catalog
- `NB_process_erasure.ipynb` - 6-step GDPR erasure pipeline with AES-256-GCM encryption
- `NB_key_management_helpers.ipynb` - DEK management for crypto-shredding

**Why it was developed**:
- Ensures data quality across pipeline
- Supports GDPR compliance (right to erasure)
- Provides audit trail for schema changes
- Enables crypto-shredding for sensitive data

---

### Helper Notebooks

**What it does**: Utility functions for common operations.

All live under `processing/common/`:

| Notebook | Purpose |
|----------|---------|
| `NB_catalog_helpers.ipynb` | Table/schema creation, MERGE helpers, monitoring-table DDL |
| `NB_schema_contracts.ipynb` | Expected schema definitions for Bronze envelopes and Silver tables |
| `NB_silver_metadata.ipynb` | Per-table Silver config loader |
| `NB_reset_tables.ipynb` | Drop tables, clear checkpoints |
| `NB_confluence_generator.ipynb` | Generate Confluence docs (with `confluence_doc_generator.py`) |

---

## pump.fun Pipeline Components

A second, independent pipeline. It shares the lakehouse but no schemas, configs
or jobs with dvdrental.

### Websocket Producer

**What it does**: Streams live pump.fun trading events into a Unity Catalog Volume.

**How it works** (`ingestion/pumpfun/`):
- `app/pumpapi.py` — websocket client, reconnects with exponential backoff
- `app/writer.py` — buffers events; each flush (default 5000) is serialized as
  JSONL, zstd-compressed, base64-encoded into one JSON envelope file
- `app/uploader.py` — uploads completed envelopes via the Databricks Files API
- `app/main.py` — wires the three into one asyncio process

**Why it was developed**:
- pump.fun has no CDC-capable database behind it, so this plays the role
  Debezium+Kafka play for dvdrental
- Compression cuts upload size ~4-5x versus one line per event
- Runs outside Databricks (systemd unit or Docker) because it must be always-on,
  not scheduled

---

### Lakeflow Declarative Pipeline

**What it does**: Turns the compressed envelopes into Bronze + 5 Silver tables.

**How it works** (`pumpapi-lakehouse/transformations/`, `pyspark.pipelines`):

| File | Table | Notes |
|------|-------|-------|
| `bronze_pump_events.py` | `bronze.pump_events_raw` | Auto Loader + a `mapInPandas` worker that base64-decodes and zstd-decompresses each envelope, yielding one row per event line |
| `silver_pump_events.py` | `silver.pump_events` | Every event, typed, with `dp.expect` DQ checks |
| `silver_pump_tokens.py` | `silver.pump_tokens` | `action = 'create'` — one row per token launch |
| `silver_pump_transfers.py` | `silver.pump_transfers` | `action = 'transfer'`, `transfers[]` exploded |
| `silver_pump_pools.py` | `silver.pump_pools` | `createPool`/`migrate`/`add`/`remove` pool lifecycle events |
| `silver_pump_trades.py` | `silver.pump_trades` | `action IN (buy, sell)`, `breakdown[]` exploded |

**Why `mapInPandas`**: the original scalar UDF returned one fully decompressed
text column per envelope for Spark to `split()`+`explode()`, which exhausted the
reused Python worker's memory on a real backlog. Bounding batch sizes three
different ways did not help; emitting rows incrementally did.

---

### pump.fun Gold — Token Risk Scoring

**What it does**: Scores each token for rug-pull risk.

**How it works** (`processing/gold/NB_process_pump_token_risk.ipynb` →
`workspace.gold.gold_pump_token_risk`):
- Hard-blocker flags + weighted score + migration funnel status
- Selective incremental recompute: only mints with activity since the last run
- Runs as the second task of the `pumpfun-bronze` job, after the pipeline

**Why it is not in the Lakeflow pipeline**: selective per-mint recompute needs
`foreachBatch` + `MERGE`, which doesn't compose with the window functions the
scoring logic uses inside a declarative `@dp.table`. See
`design/pumpfun/RISK_SCORING_DESIGN.md`.

---

## Summary

| Category | Components | Purpose |
|----------|------------|---------|
| Infrastructure | Docker CDC Stack, PostgreSQL | Local development environment |
| Data Generation | Bulk loader + 3 continuous generators | Seed + continuous CDC test traffic |
| Ingestion (dvdrental) | Bronze + generic Silver notebooks | Raw → clean data |
| Enterprise | Vault: 13 hubs, 19 links, 20 sats, 4 PITs, 2 bridges | Historized, audit-ready layer |
| Analytics | dbt vault models + 7 gold marts | Business-ready models |
| Ingestion (pump.fun) | Websocket producer + Lakeflow pipeline | Live Solana events → Bronze + 5 Silver |
| Analytics (pump.fun) | `gold_pump_token_risk` notebook | Rug-pull risk scoring |
| Deployment | Asset Bundle (all) / `deploy_jobs.py` (dvdrental) | Two coexisting job graphs |
| Automation | scripts/deploy_jobs.py, Agent System, DQ/GDPR | Operations and compliance |