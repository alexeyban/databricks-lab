# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks CDC Lakehouse Lab — an end-to-end reference implementation of a Change Data Capture pipeline from the **dvdrental** PostgreSQL sample database into a Databricks medallion lakehouse (Bronze → Silver → Gold). It doubles as a working demo and a starting template for production CDC pipelines.

**Source tables captured via Debezium:**
- `film` — catalogue dimension (updates: pricing, duration)
- `rental` — primary transaction (inserts: new rentals; updates: return_date set)
- `payment` — financial transactions for rentals (inserts only)

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
  → Debezium Connect (Kafka topics: cdc.public.film, cdc.public.rental, cdc.public.payment)
    → Databricks Bronze (raw Debezium envelopes in Delta tables)
      → Databricks Silver (current-state via MERGE, with schema evolution)
        → dbt Gold (business-ready models with data quality tests)
```

### Notebooks

- **`notebooks/bronze/NB_ingest_to_bronze.ipynb`**: Structured streaming from Kafka → Bronze Delta tables (topic pattern `cdc.public.*`, dynamic table naming)
- **`notebooks/silver/NB_process_to_silver.ipynb`**: Debezium MERGE into `silver.silver_rental` (merge key: `rental_id`)
- **`notebooks/silver/NB_process_products_silver.ipynb`**: Debezium MERGE into `silver.silver_film` (merge key: `film_id`)
- **`notebooks/silver/NB_process_payment_silver.ipynb`**: Debezium MERGE into `silver.silver_payment` (merge key: `payment_id`)
- **`notebooks/helpers/NB_schema_drift_helpers.ipynb`**: Schema drift detection with configurable policies (`strict`, `additive_only`, `permissive`) and alerting (Slack, Teams, email)
- **`notebooks/helpers/NB_catalog_helpers.ipynb`**: Table/schema creation utilities (`create_silver_table_rental/film/payment`, `build_merge_clauses`, `execute_merge`)
- **`notebooks/helpers/NB_schema_contracts.ipynb`**: Expected schema definitions for all Bronze/Silver/Gold layers

### Databricks Tables

| Layer | Table | Key |
|-------|-------|-----|
| Bronze | workspace.bronze.film | — |
| Bronze | workspace.bronze.rental | — |
| Bronze | workspace.bronze.payment | — |
| Silver | workspace.silver.silver_film | film_id |
| Silver | workspace.silver.silver_rental | rental_id |
| Silver | workspace.silver.silver_payment | payment_id |
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

`Orders-ingest-job.yaml` defines the Databricks workflow: Bronze ingest runs first, then all three Silver tasks run in parallel after it succeeds.

## Agent System

The `/Agents/` directory contains 24 markdown files defining specialized agent personalities. The `/skills/` directory contains 24 reusable skill definitions. The `/runtime/` directory contains the Python agent loop (`autonomous_agent.py`) that generates code via LLM, uploads it to Databricks, runs it, and retries on failures. See `AGENT_PROMPT_EXAMPLES.md` for prompt templates.
