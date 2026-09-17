# Architecture

This document describes the high-level architecture of the Databricks CDC Lakehouse Lab, including data flow, technology stack, and layer descriptions.

---

## Data Flow

```
┌─────────────────┐      ┌─────────────┐      ┌─────────┐
│  PostgreSQL    │      │  Debezium   │      │  Kafka  │
│  dvdrental      │─────►│  Connect    │─────►│         │
│  (WAL enabled)  │      │             │      │         │
└─────────────────┘      └─────────────┘      └────┬────┘
                                                   │
                                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATABRICKS                                  │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐     │
│  │ Bronze  │───►│ Silver  │───►│  Vault  │───►│  Gold   │     │
│  │ (raw)   │    │ (clean) │    │ (DV 2.0)│    │ (dbt)   │     │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘     │
│       │              │              │              │           │
│  Debezium      Current-state   Historized    Business       │
│  envelopes     tables via      audit-ready   models         │
│                MERGE            time-travel                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Source** | PostgreSQL 15 | Source database with logical replication (WAL) |
| **CDC Capture** | Debezium 2.5 | Capture PostgreSQL WAL changes |
| **Message Bus** | Kafka 7.6 | Stream CDC events |
| **Orchestration** | Databricks Jobs | Schedule and run notebooks |
| **Storage** | Delta Lake | ACID transactions on object storage |
| **Catalog** | Unity Catalog | Schema discovery, governance, access control |
| **Analytics** | dbt | Transform and test Gold layer |
| **Data Quality** | Custom + dbt-expectations | DQ checks and schema drift detection |

---

## Source Database (dvdrental)

The project captures all **15 tables** from the PostgreSQL `dvdrental` sample database:

### Reference / Dimension Tables
| Table | Key | Description |
|-------|-----|-------------|
| `country` | country_id | Country lookup |
| `city` | city_id | City → country |
| `address` | address_id | Address → city |
| `language` | language_id | Film language |
| `category` | category_id | Film category |
| `actor` | actor_id | Actor dimension |
| `store` | store_id | Store dimension |
| `staff` | staff_id | Staff → address, store |
| `customer` | customer_id | Customer → address, store |

### Transaction / Fact Tables
| Table | Key | Changes Captured |
|-------|-----|-----------------|
| `film` | film_id | rental_rate, rental_duration, replacement_cost |
| `film_actor` | film_id, actor_id | Junction: film ↔ actor |
| `film_category` | film_id, category_id | Junction: film ↔ category |
| `inventory` | inventory_id | Film copies per store |
| `rental` | rental_id | New rentals, return_date updates |
| `payment` | payment_id | Payment inserts |

---

## Lakehouse Layers

### Bronze Layer
- **Purpose**: Raw, immutable storage of CDC events
- **Storage**: Delta tables in `workspace.bronze.*`
- **Data Format**: Full Debezium envelopes (before/after values, metadata)
- **Schema**: Dynamic per topic, evolves with source schema changes
- **Example Tables**: `bronze.public_actor`, `bronze.public_film`, etc.

### Silver Layer
- **Purpose**: Clean, current-state tables ready for business consumption
- **Storage**: Delta tables in `workspace.silver.silver_*`
- **Processing**: MERGE on entity PK for deduplication
- **Features**: Schema evolution, Debezium transforms, numeric decoding
- **Example Tables**: `silver_film`, `silver_rental`, `silver_payment`, etc.

### Vault Layer (Data Vault 2.0)
- **Purpose**: Enterprise-grade, fully historized, audit-ready data
- **Storage**: Delta tables in `workspace.vault.*`
- **Built by**: dbt models in `transformation/dbt_project/models/vault/` (incremental).
  The older notebook implementation still exists under `processing/vault/` and is
  what `scripts/deploy_jobs.py` deploys — see *Databricks Jobs* below.
- **Components** (dbt model counts):
  - **Hubs** (13): Business key storage with SHA-256 hash keys
  - **Links** (19): Relationships between hubs
  - **Satellites** (20): Append-only attribute tracking via DIFF_HASH
  - **PIT Tables** (4): `pit_customer`, `pit_film`, `pit_payment`, `pit_rental` — point-in-time snapshots
  - **Bridge Tables** (2): `brg_film_cast`, `brg_rental_film` — pre-joined many-to-many structures

### Gold Layer
- **Purpose**: Business-ready analytics models
- **Storage**: Delta tables in `workspace.gold.*`
- **Tool**: dbt (`transformation/dbt_project/`) for transformations and tests
- **Run on Databricks**: via `transformation/NB_run_dbt.ipynb` (dbtRunner API, no subprocess)
- **Models**: `gold_film`, `gold_rental`, `gold_customer_summary`, `gold_inventory_status`, `gold_revenue_by_store`, `gold_film_popularity`, `gold_staff_performance`

---

## Monitoring Layer

The project includes a dedicated monitoring schema for observability:

| Table | Purpose |
|-------|---------|
| `monitoring.schema_drift_log` | Schema change tracking |
| `bronze.quarantine` | Bronze envelopes that fail validation |
| `monitoring.dq_results` | Data quality test results |
| `monitoring.pii_column_registry` | PII column inventory |
| `monitoring.subject_key_store` | GDPR subject keys |
| `monitoring.erasure_requests` | GDPR erasure queue |
| `monitoring.erasure_registry` | Erasure status tracking |
| `monitoring.vault_load_log` | Vault load metrics — DDL helper exists in `NB_catalog_helpers`, not yet written to (see ROADMAP) |

---

## Databricks Jobs

Two deployment mechanisms coexist in the repo and define **different job graphs**.

### Databricks Asset Bundles — `orchestration/bundle/databricks.yml`

The complete picture: all dvdrental resources plus everything pump.fun.
Deploy with `databricks bundle deploy -t dev` from `orchestration/bundle/`.

| Resource | Type | Tasks | Description |
|----------|------|-------|-------------|
| `dvdrental-bronze` | job | 1 | Kafka → Bronze Delta |
| `dvdrental-silver` | job | 15 | Bronze → Silver MERGE, one task per table |
| `dvdrental-vault-gold` | job | 2 | `dbt build --select vault` then `--select gold`, as `dbt_task` against SQL warehouse `53165753164ae80e` |
| `dvdrental-orchestrator` | job | 2 | Silver → Vault+Gold; scheduled daily 02:00 UTC (PAUSED). Bronze is excluded — it is a streaming job |
| `pumpapi-lakehouse` | pipeline | — | Lakeflow Declarative Pipeline: Bronze + 5 Silver tables, serverless, triggered |
| `pumpfun-bronze` | job | 2 | Triggers `pumpapi-lakehouse`, then `gold_pump_token_risk`; cron every 2 min |

### Raw Jobs API — `scripts/deploy_jobs.py`

The older path, dvdrental only. It builds the Vault layer from the
`processing/vault/` **notebooks** rather than the dbt vault models, and the
orchestrator chains four jobs instead of two.

| Job | ID | Tasks | Description |
|-----|----|-------|-------------|
| `dvdrental-bronze` | 325293262130713 | 1 | Kafka → Bronze Delta |
| `dvdrental-silver` | 1099814608698427 | 15 | Bronze → Silver MERGE |
| `dvdrental-vault` | 950203691556666 | 4 | Hubs → Links+Sats → Business Vault (notebooks) |
| `dvdrental-vault-gold` | looked up by name | 1 | `transformation/NB_run_dbt.ipynb` (dbtRunner API) |
| `dvdrental-orchestrator` | 684287727358557 | 4 | Bronze → Silver → Vault → Vault-Gold |

The Pipelines API does not support the `git_source` mechanism `deploy_jobs.py`
relies on, so that script manages no pump.fun resources at all. The bundle is
the direction of travel; `deploy_jobs.py` has not been retired yet.

---

## pump.fun Near-Real-Time Pipeline

A second, independent ingestion pipeline streams live trading activity from
the [pump.fun](https://pump.fun) Solana launchpad, alongside the dvdrental
CDC pipeline above. It does not share the Bronze/Silver/Vault/Gold catalog
schemas or jobs with dvdrental — same lakehouse, separate domain.

```
pump.fun (Solana on-chain trades, pool/token lifecycle events)
  → PumpAPI websocket feed
    → ingestion/pumpfun (standalone Python service, e.g. systemd/Docker — outside Databricks)
      → serialize batch as JSONL → compress (zstd) → base64-encode → one JSON envelope per file
        → Unity Catalog Volume (/Volumes/workspace/default/mnt/pumpapi)
          → pumpapi-lakehouse (Lakeflow Declarative Pipeline — one DAG, triggered every 2 min):
              bronze_pump_events.py:   decode → decompress → split → parse → workspace.bronze.pump_events_raw
              silver_pump_events.py:   all events, typed columns          → workspace.silver.pump_events
              silver_pump_tokens.py:   action = 'create'                  → workspace.silver.pump_tokens
              silver_pump_transfers.py: action = 'transfer', exploded     → workspace.silver.pump_transfers
              silver_pump_pools.py:    createPool/migrate/add/remove      → workspace.silver.pump_pools
              silver_pump_trades.py:   action IN (buy, sell), exploded    → workspace.silver.pump_trades
          → processing/gold/NB_process_pump_token_risk.ipynb (separate notebook task, after the pipeline)
              → workspace.gold.gold_pump_token_risk
```

- **Producer** (`ingestion/pumpfun/`): a long-running websocket client
  (reconnects with exponential backoff) and a buffering writer — the same
  role Debezium/Kafka play for dvdrental, but for a source with no
  CDC-capable database underneath it. Each flushed batch (default 5000
  events) is serialized as JSONL, compressed with **zstd**, and
  base64-encoded into a single JSON envelope
  (`{"_source", "_ingested_at", "event_count", "codec", "data_b64", ...}`)
  — one small file per batch instead of one line per event, cutting upload
  size ~4-5x. A separate uploader pushes completed envelope files to a
  Unity Catalog Volume via the Databricks Files API. Runs outside
  Databricks (systemd unit or Docker included); see
  `ingestion/pumpfun/README.md`.
- **Bronze + Silver** (`pumpapi-lakehouse/`, a **Lakeflow Declarative
  Pipeline** authored with `pyspark.pipelines` — the current Databricks
  API, not the classic `import dlt` module): one pipeline, one DAG, six
  transformation files:
  - `bronze_pump_events.py` — Auto Loader reads the envelope files with a
    fixed schema, then a **`mapInPandas`** worker base64-decodes and
    zstd-decompresses `data_b64` back into JSONL text (`zstandard` is
    installed via the pipeline's `environment.dependencies`, not a notebook
    `%pip install`) and yields one row per line. This replaced a scalar
    Python UDF that returned one big decompressed-text column per envelope
    for Spark to `split()`+`explode()` downstream — that shape OOM'd the
    reused Python worker on a real backlog. Each event lands as raw JSON text in
    `workspace.bronze.pump_events_raw` — the event shape varies per
    `action` (`buy`, `sell`, `create`, `migrate`, `createPool`, `transfer`,
    etc.), so Bronze still does not impose a struct schema on it.
  - `silver_pump_events.py` — parses **every** event against one fixed
    schema into typed columns, with `dp.expect` data-quality checks
    (non-null `signature`/`action`).
  - `silver_pump_tokens.py` — filters to `action = 'create'`; one row per
    token launch (initial price/market cap, mint/freeze authority as
    rug-pull risk signals).
  - `silver_pump_transfers.py` — filters to `action = 'transfer'`; explodes
    the event's `transfers[]` array so each wallet-to-wallet transfer gets
    its own row.
  - `silver_pump_pools.py` — filters to `action IN (createPool, migrate,
    add, remove)`; one row per pool lifecycle event, carrying the
    liquidity/pool-trust fields the risk-scoring Gold layer needs.
  - `silver_pump_trades.py` — filters to `action IN (buy, sell)`; explodes
    each event's `breakdown[]` array so each individual trade gets its own
    row, which is what lets Gold detect bundled/sniped launch buys.

  This replaced an earlier two-job design (separate Bronze Auto Loader
  notebook + separate Silver notebook job, with a `trades`/`tokens` table
  split) — kept for reference at
  `ingestion/consumers/outdated__NB_ingest_pumpfun_to_bronze.ipynb`,
  `ingestion/consumers/outdated__NB_dlt_pumpfun_bronze.ipynb`, and
  `processing/silver/outdated__NB_process_pumpfun_silver.ipynb`.
- **Gold** (`processing/gold/NB_process_pump_token_risk.ipynb` →
  `workspace.gold.gold_pump_token_risk`): hard-blocker flags + weighted risk
  score + migration funnel status, recomputed incrementally for only the mints
  touched since the last run. It is deliberately **not** part of the Lakeflow
  pipeline — selective per-mint recompute needs `foreachBatch` + `MERGE`, which
  doesn't compose with the window functions the scoring logic depends on inside
  a declarative `@dp.table`. It runs as the second task of the `pumpfun-bronze`
  job. See `design/pumpfun/RISK_SCORING_DESIGN.md`.
- **Vault**: no Data Vault layer for pump.fun; whether it needs one is still
  open — see `ROADMAP.md`.
- **Scheduling**: the pipeline is triggered (not continuous), fired every 2
  minutes by the `pumpfun-bronze` job's `pipeline_task` — the same
  `availableNow` pattern used by `dvdrental-bronze`.
- **Deployment**: `pumpapi-lakehouse` (the Lakeflow pipeline) and
  `pumpfun-bronze` (the job that triggers it) are declared in
  `orchestration/bundle/databricks.yml` and deployed with
  `databricks bundle deploy` — the Pipelines API doesn't support the
  `git_source` mechanism `scripts/deploy_jobs.py` relies on for plain
  notebook jobs, so that script manages no pump.fun resources at all.

---

## Local Development vs Production

### Local Development
- Uses `infra/docker/docker-compose.yml` for the full CDC stack (run `docker compose` from `infra/docker/`)
- PostgreSQL, Kafka, Debezium run locally
- ngrok tunnel for Databricks → Kafka connectivity

### Production Considerations
- Cloud Kafka (Confluent, MSK, etc.)
- Databricks Serverless or SQL warehouses
- Unity Catalog for governance
- Secret management via Azure Key Vault or AWS KMS