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

---

## Infrastructure

### Docker CDC Stack

**What it does**: Provides a complete local development environment with PostgreSQL, Kafka, Debezium, and supporting services.

**How it works**:
- `docker-compose.yml` defines 6 services: Zookeeper, Kafka, Schema Registry, PostgreSQL, Debezium Connect, Kafka UI
- PostgreSQL is configured with `wal_level=logical` for logical replication
- Debezium Connect reads PostgreSQL WAL and publishes to Kafka topics
- Schema Registry manages Avro schemas for Kafka messages

**Why it was developed**:
- Enables local development without cloud dependencies
- Provides a complete, reproducible CDC pipeline for testing
- Allows developers to iterate quickly without infrastructure costs
- Serves as a reference architecture for production deployments

**Files**:
- `docker-compose.yml` - Main stack definition
- `docker/init-dvdrental.sh` - Database initialization script
- `postgres-connector.json` - Debezium connector configuration

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

**How it works** (`ingestion/generators/load_generator.py`):
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
python3 ingestion/generators/load_generator.py
# With custom settings:
ITERATIONS=100 SLEEP_MIN=0.5 SLEEP_MAX=2.0 python3 ingestion/generators/load_generator.py
```

---

### Bulk Data Loader

**What it does**: Seeds a realistic initial data volume into PostgreSQL before running the pipeline for the first time.

**How it works** (`ingestion/generators/load_bulk_data.py`):
- Inserts 1000 customers and 1000 films
- Generates 10000+ DML events (rentals, returns, payments, film updates)
- Designed for one-time use before pipeline launch to ensure non-trivial data volumes

**Usage**:
```bash
python3 ingestion/generators/load_bulk_data.py
```

---

### Film Update Generator

**What it does**: Simulates catalog changes by updating film attributes (rental rate, duration, replacement cost).

**How it works** (`ingestion/generators/load_products_generator.py`):
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
python3 ingestion/generators/load_products_generator.py
```

---

### Reference Data Generator

**What it does**: Updates slowly-changing reference data (addresses, categories, etc.).

**How it works** (`generators/load_reference_generator.py`):
- Modifies reference tables at lower frequency
- Helps test handling of low-change vs high-change attributes

**Why it was developed**:
- Provides variety in change rates across satellites
- Tests satellite strategies (high-change vs low-change separation)
- Enables testing of PIT table functionality with real time-travel data

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

**Files**:
- `NB_process_to_silver.ipynb` - Rental-specific
- `NB_process_products_silver.ipynb` - Film-specific
- `NB_process_payment_silver.ipynb` - Payment-specific

**Why they exist**: Preserved for reference and comparison with generic approach.

---

## Enterprise Layer

### Vault Metadata Notebook

**What it does**: Loads and parses the Data Vault 2.0 configuration, provides utility functions.

**How it works** (`notebooks/vault/NB_dv_metadata.ipynb`):
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

**How it works** (`notebooks/vault/NB_ingest_to_hubs.ipynb`):
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

**How it works** (`notebooks/vault/NB_ingest_to_links.ipynb`):
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

**How it works** (`notebooks/vault/NB_ingest_to_satellites.ipynb`):
- Reads from Silver tables
- Computes DIFF_HASH: `sha2(concat_ws("||", coalesce(col, "NULL") for each column), 256)`
- Left joins against current satellite hash per hub key
- Appends only new/changed rows (append-only, no updates or deletes)
- Creates 15 satellite tables split by change rate

**Why it was developed**:
- Provides complete history of all attribute changes
- Append-only design ensures audit compliance
- DIFF_HASH enables efficient change detection
- Separate satellites for high/low change rate data
- No end-dating needed (PIT tables handle time-travel)

---

### Business Vault (PIT + Bridge)

**What it does**: Creates query-acceleration structures.

**How it works** (`notebooks/vault/NB_dv_business_vault.ipynb`):
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
- Runs as a single-task Databricks job (`dvdrental-vault-gold`)
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
- **15 hub models** (hubs/): one per entity
- **19 link models** (links/): all relationships
- **15 satellite models** (satellites/): attribute history
- **4 PIT tables** (pit/): point-in-time snapshots (materialized as tables)
- **2 bridge tables** (bridge/): pre-joined many-to-many paths

*Gold models* (`models/gold/`) — write to `workspace.gold.*`:
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
- Creates/updates 5 jobs: Bronze, Silver, Vault, Vault-Gold, Orchestrator
- Configures Git source for notebooks
- Sets up task dependencies (orchestrator chains all 4 sub-jobs)
- Supports `--run-orchestrator` flag for immediate execution after deploy

**Why it was developed**:
- Infrastructure-as-code for Databricks jobs
- Reproducible deployments
- Single command to deploy entire pipeline
- Supports Slack webhooks for alerting

---

### Secret Management

**What it does**: Pushes Kafka bootstrap servers and credentials to Databricks secret scope.

**How it works** (`scripts/push_secrets_to_databricks.py`):
- Reads from `.env` file
- Creates/updates secrets in Databricks scope `dvdrental`
- Required for Databricks to connect to Kafka

**Why it was developed**:
- Secure credential management
- Avoids hardcoded secrets in notebooks
- Enables separation of Dev/Prod environments

---

### Vault Config Upload

**What it does**: Uploads Data Vault configuration to Unity Catalog Volume.

**How it works** (`scripts/upload_vault_config.py`):
- Copies `dv_model.json` to Volume at `mnt/pipeline_configs/datavault/`
- Vault notebooks read config from this location

**Why it was developed**:
- Centralized configuration for vault notebooks
- Enables configuration changes without code changes
- Supports Dev/Test/Prod environments

---

### DV 2.0 Generator

**What it does**: Auto-generates Data Vault design from Silver schema.

**How it works** (`generators/dv_generator/`):
- 7-step CLI tool: analyze → classify → generate → review → validate → apply
- Analyzes Silver schema and classifies entities (hub/link/sat)
- Generates vault notebooks and `dv_model.json`
- Includes AI classifier for intelligent entity classification

**Why it was developed**:
- Eliminates manual vault design effort
- Ensures consistent naming and structure
- Speeds up Data Vault implementation
- Validates design before applying

**Usage**:
```bash
python -m generators.dv_generator.main --analyze \
  --config-dir pipeline_configs/silver/dvdrental --no-ai

python -m generators.dv_generator.main --resume <session_id> --from-step step6_validator
```

---

### Agent System

**What it does**: AI-assisted automation for Databricks operations.

**How it works**:
- `Agents/` - 24 specialized agent roles (data engineer, architect, job operator, etc.)
- `skills/` - 24 reusable skill definitions
- `runtime/autonomous_agent.py` - Agent loop that generates code, uploads, runs, retries

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

| Notebook | Purpose |
|----------|---------|
| `NB_catalog_helpers.ipynb` | Table creation, MERGE helpers |
| `NB_schema_contracts.ipynb` | Expected schema definitions |
| `NB_silver_metadata.ipynb` | Per-table config loader |
| `NB_reset_tables.ipynb` | Drop tables, clear checkpoints |
| `NB_confluence_generator.ipynb` | Generate Confluence docs |

---

## Summary

| Category | Components | Purpose |
|----------|------------|---------|
| Infrastructure | Docker CDC Stack, PostgreSQL | Local development environment |
| Data Generation | Bulk loader + 2 continuous generators | Seed + continuous CDC test traffic |
| Ingestion | Bronze + Silver notebooks | Raw → clean data |
| Enterprise | Vault (Hubs, Links, Sats, PIT, Bridge) | Historized, audit-ready layer |
| Analytics | dbt vault models + 7 gold marts, NB_run_dbt | Business-ready models via Databricks job |
| Automation | scripts/deploy_jobs.py, Agent System, DQ/GDPR | Operations and compliance |