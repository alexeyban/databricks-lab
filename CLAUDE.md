# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks CDC Lakehouse Lab — an end-to-end reference implementation of a Change Data Capture pipeline from the **dvdrental** PostgreSQL sample database into a Databricks medallion lakehouse (Bronze → Silver → Vault → Gold). It doubles as a working demo and a starting template for production CDC pipelines.

**All 15 source tables captured via Debezium:**

Reference / Dimension: `country`, `city`, `address`, `language`, `category`, `actor`, `store`, `staff`, `customer`

Transaction / Fact: `film`, `film_actor`, `film_category`, `inventory`, `rental`, `payment`

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
```

The PostgreSQL service runs the `docker/init-dvdrental.sh` script on first start, which downloads and restores the full dvdrental dataset (~1000 films, ~16k rentals, ~14k payments) and creates the logical replication publication.

## Python Setup

```bash
pip install -r requirements.txt

# Copy and fill in Databricks credentials
cp .envexample .env
```

## Data Generators

```bash
# Film generator (updates rental_rate, rental_duration, replacement_cost on existing films)
python3 generators/load_products_generator.py

# Rental + payment generator (new rentals, film returns, payments)
python3 generators/load_generator.py

# Optional env vars: ITERATIONS, SLEEP_MIN, SLEEP_MAX
# DB env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
```

## dbt (Gold Layer)

```bash
cd cdc_gold
dbt debug          # verify connection
dbt build          # run models + tests
dbt test           # data quality tests only
dbt run            # run models only
```

## Architecture

### Data Flow

```
PostgreSQL dvdrental (WAL)
  → Debezium Connect (Kafka topics: cdc.public.* — all 15 tables)
    → Databricks Bronze (raw Debezium envelopes in Delta tables)
      → Databricks Silver (current-state via MERGE, with schema evolution)
        → Databricks Vault (Data Vault 2.0: Hubs / Links / Satellites / PIT / Bridge)
          → dbt Gold (business-ready models with data quality tests)
```

### Notebooks

- **`notebooks/bronze/NB_ingest_to_bronze.ipynb`**: Structured streaming from Kafka → Bronze Delta tables (topic pattern `cdc.public.*`, dynamic table naming)
- **`notebooks/silver/NB_process_to_silver.ipynb`**: Debezium MERGE into `silver.silver_rental` (merge key: `rental_id`)
- **`notebooks/silver/NB_process_products_silver.ipynb`**: Debezium MERGE into `silver.silver_film` (merge key: `film_id`)
- **`notebooks/silver/NB_process_payment_silver.ipynb`**: Debezium MERGE into `silver.silver_payment` (merge key: `payment_id`)
- **`notebooks/vault/NB_dv_metadata.ipynb`**: DV 2.0 config loader + SHA-256 hash key / DIFF_HASH / DDL helpers
- **`notebooks/vault/NB_ingest_to_hubs.ipynb`**: Silver → 13 Hubs (insert-only MERGE, watermarked)
- **`notebooks/vault/NB_ingest_to_links.ipynb`**: Silver → 19 Links (insert-only MERGE, depends on Hubs)
- **`notebooks/vault/NB_ingest_to_satellites.ipynb`**: Silver → 15 Satellites (append-only via DIFF_HK change detection)
- **`notebooks/vault/NB_dv_business_vault.ipynb`**: 4 PIT tables (daily snapshot spine) + 2 Bridge tables
- **`notebooks/helpers/NB_schema_drift_helpers.ipynb`**: Schema drift detection with configurable policies (`strict`, `additive_only`, `permissive`) and alerting (Slack, Teams, email)
- **`notebooks/helpers/NB_catalog_helpers.ipynb`**: Table/schema creation utilities (`create_silver_table_rental/film/payment`, `build_merge_clauses`, `execute_merge`)
- **`notebooks/helpers/NB_schema_contracts.ipynb`**: Expected schema definitions for all Bronze/Silver/Gold layers

### Databricks Tables

| Layer | Table | Key |
|-------|-------|-----|
| Bronze | workspace.bronze.* (15 tables) | — |
| Silver | workspace.silver.silver_film | film_id |
| Silver | workspace.silver.silver_rental | rental_id |
| Silver | workspace.silver.silver_payment | payment_id |
| Silver | workspace.silver.silver_* (12 ref tables) | entity PK |
| Vault | workspace.vault.hub_* (13 hubs) | SHA-256 HK |
| Vault | workspace.vault.lnk_* (17 links) | composite HK |
| Vault | workspace.vault.sat_* (14 satellites) | HK + LOAD_DATE |
| Vault | workspace.vault.pit_* (4 PITs) | HK + snapshot_date |
| Vault | workspace.vault.brg_* (2 bridges) | — |
| Gold | workspace.gold.gold_film | film_id |
| Gold | workspace.gold.gold_rental | rental_id |
| Monitoring | workspace.monitoring.schema_drift_log | — |

### Schema Evolution (Silver Layer)

Silver notebooks dynamically detect new columns in Debezium events, add them to Delta tables via `.option("mergeSchema", "true")`, and rebuild MERGE statements from the current table schema. All schema changes are logged to `monitoring.schema_drift_log`.

### Numeric Decoding (Debezium)

PostgreSQL `NUMERIC` columns (`rental_rate`, `replacement_cost`, `amount`) are encoded by Debezium as `{scale: INT, value: BASE64_STRING}`. Silver notebooks decode them with:
```python
expr("cast(conv(hex(unbase64(raw_value)), 16, 10) as double) / pow(10, scale)")
```

### Orchestration

`Orders-ingest-job.yaml` defines the Databricks workflow: Bronze ingest runs first, then all 15 Silver tasks run in parallel after it succeeds, then Vault notebooks run in sequence (Hubs → Links + Satellites in parallel → Business Vault).

### DV 2.0 Design

Full vault layer model and auto-generator design: `design/dv2/`
- `DV2_VAULT_LAYER_PLAN.md` — 13 hubs, 19 links, 15 satellites, 4 PITs, 2 bridges; all design decisions locked
- `DV2_GENERATOR_DESIGN.md` — 7-step generator tool (schema analysis → classification → artifact generation → human review → validation → apply)
- `IMPLEMENTATION_LOG.md` — all 14 modules complete; generator is fully operational

## DV 2.0 Generator

The `generators/dv_generator/` meta-tool generates a complete DV 2.0 vault layer from Silver schema configs.

```bash
# Fresh run (steps 1-5, then pauses for human review)
python -m generators.dv_generator.main --analyze \
  --config-dir pipeline_configs/silver/dvdrental --no-ai

# Resume after Jupyter review
python -m generators.dv_generator.main --resume <session_id> --from-step step6_validator

# Re-run a specific step
python -m generators.dv_generator.main --resume <session_id> --from-step step3_artifact_gen
```

Key outputs already committed: `pipeline_configs/datavault/dv_model.json` (final config) and `notebooks/vault/` (5 vault notebooks).

For full generator docs see `README.md § DV 2.0 Generator`.

## Agent System

The `/Agents/` directory contains 24 markdown files defining specialized agent personalities. The `/skills/` directory contains 24 reusable skill definitions. The `/runtime/` directory contains the Python agent loop (`autonomous_agent.py`) that generates code via LLM, uploads it to Databricks, runs it, and retries on failures. See `AGENT_PROMPT_EXAMPLES.md` for prompt templates.
