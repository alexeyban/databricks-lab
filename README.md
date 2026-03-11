# Databricks CDC Lakehouse Lab

This project demonstrates an end-to-end CDC pipeline from PostgreSQL into a Databricks medallion lakehouse:

Agent-system inspiration for this repository was borrowed from [agency-agents](https://github.com/msitarzewski/agency-agents/).

- PostgreSQL generates row-level changes for `orders` and `products` tables
- Debezium captures WAL changes and publishes them to Kafka topics with the `cdc` prefix
- A Databricks Bronze notebook ingests raw Kafka events into Delta tables per source table
- A Databricks Silver notebook normalizes Debezium envelopes into current-state Delta tables with **schema evolution support**
- dbt builds a Gold presentation model with foreign key relationships and data quality tests

## Schema Design

### Source Tables (PostgreSQL)

**products**
- `id` (SERIAL PRIMARY KEY)
- `product_name` (TEXT)
- `weight` (NUMERIC)
- `color` (TEXT)
- `created_at`, `updated_at` (TIMESTAMP)

**orders**
- `id` (SERIAL PRIMARY KEY)
- `product_id` (INTEGER FOREIGN KEY → products.id)
- `price` (NUMERIC)
- `created_at` (TIMESTAMP)

### Medallion Architecture

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `bronze.orders`, `bronze.products` | Raw Debezium CDC events |
| Silver | `silver.silver_orders`, `silver.silver_products` | Current-state with schema evolution |
| Gold | `gold.gold_orders`, `gold.gold_products` | Business-ready with denormalization |

## Repository Layout

- `docker-compose.yml`: local PostgreSQL, Kafka, Debezium Connect, Schema Registry, Kafka UI
- `init-db.sql`: source schema bootstrap with foreign key relationships
- `migrate_schema.sql`: migration script for existing deployments (TEXT → FK)
- `postgres-connector.json`: Debezium PostgreSQL source connector definition
- `generators/`: local data generators for `orders` and `products`
- `notebooks/bronze/`: Databricks Bronze ingestion notebook
- `notebooks/silver/`: Databricks Silver merge notebooks with schema evolution
- `notebooks/helpers/`: Reusable catalog helper functions
  - `NB_catalog_helpers.ipynb`: Schema/table creation utilities
  - `NB_schema_drift_helpers.ipynb`: Schema drift detection and alerting
  - `NB_schema_contracts.ipynb`: Expected schema contracts for all layers
- `Orders-ingest-job.yaml`: Databricks job definition for Bronze and Silver processing
- `cdc_gold/`: dbt project for Gold models with referential integrity tests

## End-to-End Flow

1. Start local CDC infrastructure with Docker Compose.
2. Register `postgres-connector.json` in Debezium Connect.
3. Run the products generator first (orders reference products):
   ```bash
   python3 generators/load_products_generator.py
   ```
4. Run the orders generator:
   ```bash
   python3 generators/load_generator.py
   ```
5. Run the Bronze notebook to ingest raw CDC events.
6. Run the Silver notebooks:
   - `NB_process_to_silver.ipynb` for orders
   - `NB_process_products_silver.ipynb` for products
7. Run dbt in `cdc_gold/` to build the Gold layer with referential integrity.

## Schema Evolution

The Silver layer supports **schema evolution** - new columns added to the source schema will be automatically propagated:

- **Backward compatible**: Legacy `product` (TEXT) field is preserved as `product_legacy` during transition
- **Forward compatible**: New fields in Debezium events are automatically added to Silver tables
- **Dynamic MERGE**: Upsert logic adapts to the current table schema at runtime

To enable schema evolution in your own pipelines:
1. Set `.option("mergeSchema", "true")` on streaming reads/writes
2. Use flexible JSON schemas that include both old and new fields
3. Build MERGE statements dynamically based on existing columns

## Schema Drift Detection

The pipeline includes **schema drift detection** with configurable policies and alerting:

### Policies

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `strict` | Block on any schema change | Gold layer, regulated data |
| `additive_only` | Allow new columns, block removals/type changes | Silver layer (default) |
| `permissive` | Log drift but never block | Bronze layer, exploratory |

### Alert Channels

- **log**: Log to notebook/stdout (default)
- **webhook**: Send to Slack or Microsoft Teams
- **email**: Send via SMTP (requires configuration)
- **all**: Use all configured channels

### Monitoring Table

All drift events are logged to `{catalog}.monitoring.schema_drift_log`:

```sql
SELECT * FROM workspace.monitoring.schema_drift_log
ORDER BY detected_at DESC
LIMIT 10;
```

### Notebook Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MONITORING_SCHEMA` | `monitoring` | Schema for drift log table |
| `SCHEMA_POLICY` | `additive_only` | Validation policy |
| `ALERT_CHANNEL` | `log` | Alert delivery method |
| `WEBHOOK_URL` | (empty) | Slack/Teams webhook URL |

### Example: Configure Slack Alerts

```bash
# Run Silver notebook with Slack alerts
dbutils.widgets.set("SCHEMA_POLICY", "additive_only")
dbutils.widgets.set("ALERT_CHANNEL", "webhook")
dbutils.widgets.set("WEBHOOK_URL", "https://hooks.slack.com/services/XXX/YYY/ZZZ")
```

### Severity Levels

| Severity | Trigger | Action |
|----------|---------|--------|
| `NONE` | No drift detected | None |
| `WARNING` | Additive changes only | Log + alert |
| `CRITICAL` | Removed columns or type changes | Block pipeline + alert |

## Migrating Existing Deployments

If you have an existing deployment with the old `product TEXT` column, run the migration script:

```bash
psql -h localhost -U postgres -d demo -f migrate_schema.sql
```

This script:
1. Adds a `product_id` column
2. Maps existing product names to product IDs
3. Drops the old `product` column
4. Adds a foreign key constraint
5. Creates an index for join performance

**Warning**: Run only after products exist and product names match order values.

## Local Infrastructure

Start the local stack:

```bash
docker compose up -d
```

Register the connector after Kafka Connect is ready:

```bash
curl -X POST http://localhost:8083/connectors   -H 'Content-Type: application/json'   --data @postgres-connector.json
```

Kafka is configured with two listeners:

- `kafka:9092` for other containers in Docker
- `${KAFKA_EXTERNAL_HOST:-localhost}:${KAFKA_EXTERNAL_PORT:-9093}` for host or remote clients

If Databricks needs to reach Kafka from outside your machine, set `KAFKA_EXTERNAL_HOST` and `KAFKA_EXTERNAL_PORT` before starting Compose.

## Secret Hygiene

Do not commit real credentials to this repository.

Use [\.envexample](/home/legion/PycharmProjects/gitlab/databricks-lab/.envexample) as the template for local configuration and keep your real values only in a local `.env` or in your secret manager.

Example:

```bash
cp .envexample .env
```

Then edit `.env` locally with your real Databricks values. Do not commit `.env`, `.databrickscfg`, or generated logs.

## Source Data Generators

Install the local Python dependency:

```bash
python3 -m pip install -r requirements.txt
```

Run the generators:

```bash
python3 generators/load_generator.py
python3 generators/load_products_generator.py
```

Optional environment variables:

- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
- `ITERATIONS` to stop after a fixed number of mutations
- `SLEEP_MIN`, `SLEEP_MAX` to control event cadence

## Databricks Runtime Expectations

Create or grant access to a Unity Catalog catalog named `workspace`, or pass another catalog name through notebook parameters.

Bronze notebook parameters:

- `KAFKA_BOOTSTRAP`: reachable Kafka bootstrap, for example `broker.example.com:9093`
- `TOPIC_PATTERN`: defaults to `cdc.public.*`
- `CATALOG`: defaults to `workspace`
- `BRONZE_SCHEMA`: defaults to `bronze`
- `CHECKPOINT_PATH`: Delta checkpoint path for Bronze ingestion

Silver notebook parameters:

- `CATALOG`: defaults to `workspace`
- `BRONZE_SCHEMA`: defaults to `bronze`
- `BRONZE_TABLE`: defaults to `orders`
- `SILVER_SCHEMA`: defaults to `silver`
- `SILVER_TABLE`: defaults to `silver_orders`
- `CHECKPOINT_PATH`: Delta checkpoint path for Silver processing

## dbt Gold Layer

The dbt project in `cdc_gold/` builds two Gold models:

**gold_products**
- Current-state product dimension with weight classification
- Source: `silver.silver_products`

**gold_orders**
- Current-state orders fact table with denormalized product name
- Foreign key relationship to `gold_products`
- Price band classification (low/medium/high)
- Source: `silver.silver_orders`

Typical commands:

```bash
cd cdc_gold
dbt debug
dbt build
```

Both models are incremental and include data quality tests for NOT NULL, UNIQUE, and referential integrity.
