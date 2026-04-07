# Using This Repo For Your Own Projects

This repository can be reused as a starter for a CDC-to-Databricks lakehouse project.

## What You Can Reuse

- Local CDC stack with PostgreSQL, Kafka, Debezium, Schema Registry, and Kafka UI
- Metadata-driven Bronze and Silver notebook patterns (all tables, one generic notebook)
- DV 2.0 generator meta-tool — generates vault notebooks from Silver configs automatically
- Schema drift detection helpers
- 4-job Databricks architecture (Bronze / Silver / Vault / Orchestrator) via `deploy_job.py`
- dbt Gold layer structure

## Recommended Adaptation Path

### 1. Replace the source-domain schema

Update:

- `docker/init-dvdrental.sh` or your own DB init script
- `postgres-connector.json` — topic prefix and table filter
- `generators/load_generator.py` — replace dvdrental mutation logic with your domain tables

### 2. Adapt Bronze ingestion

`notebooks/bronze/NB_ingest_to_bronze.ipynb` works for any Kafka topic pattern — usually
no changes needed beyond widget defaults (topic pattern, catalog, checkpoint path).

### 3. Adapt Silver processing

The generic Silver framework requires only one thing per table: a JSON config file.

For each source table, add a file at `pipeline_configs/silver/<source>/<table_id>.json`:

```json
{
  "table_id": "orders",
  "bronze_table": "orders",
  "silver_table": "silver_orders",
  "cdc_contract_key": "cdc.public.orders",
  "silver_contract_key": "silver.orders",
  "checkpoint_name": "orders_silver_generic",
  "primary_keys": ["order_id"],
  "dedupe_order_columns": ["updated_at", "event_ts_ms", "bronze_offset"],
  "merge_core_fields": ["order_id", "customer_id", "total", "status", "updated_at"],
  "field_mappings": [
    {"target": "order_id", "source_paths": ["after.order_id", "before.order_id"]},
    {"target": "total", "transform": "decimal_from_debezium_bytes",
     "source_paths": ["after.total", "before.total"], "precision": 12, "scale": 2}
  ]
}
```

Add corresponding schema contracts for your tables to `notebooks/helpers/NB_schema_contracts.ipynb`.

The generic notebook `notebooks/silver/NB_process_to_silver_generic.ipynb` handles all
tables without modification.

### 4. Generate the Vault layer

Run the DV 2.0 generator against your Silver configs:

```bash
python -m generators.dv_generator.main --analyze \
  --config-dir pipeline_configs/silver/<your-source> --no-ai
```

Review the output in `generated/dv_sessions/<id>/05_review_notebook.ipynb`, then apply:

```bash
python -m generators.dv_generator.main --resume <session_id> --from-step step6_validator
```

This writes `pipeline_configs/datavault/dv_model.json` and generates all vault notebooks.

Upload the model to your Unity Catalog Volume:

```bash
python3 scripts/upload_vault_config.py
```

### 5. Update schema contracts and drift rules

Modify `notebooks/helpers/NB_schema_contracts.ipynb` to add schema definitions for your
Bronze CDC envelopes and Silver tables before treating the pipeline as production-ready.

### 6. Replace Gold models

Update the dbt project in `cdc_gold/` so Gold reflects your business entities, metrics, and tests.

Focus on:

- `sources.yml` — point to your Silver/Vault tables
- models — replace `gold_film` / `gold_rental` with your domain models
- schema tests — add referential integrity + not-null tests

### 7. Configure Databricks jobs

Edit constants at the top of `scripts/deploy_job.py`:

```python
GIT_URL    = "https://github.com/<your-org>/<your-repo>"
GIT_BRANCH = "main"
CATALOG    = "your_catalog"
```

Update `SILVER_TABLES` to your table list, then deploy:

```bash
set -a && source .env && set +a
python3 scripts/deploy_job.py --kafka-bootstrap <host:port>
```

## Secret Handling

Do not commit real credentials. Use `.envexample` as the template:

```bash
cp .envexample .env
# fill in DATABRICKS_HOST, DATABRICKS_TOKEN, and any other vars
```

Keep real values only in local environment variables, Databricks Secret Scopes, or
CI/CD secret managers. See `design/plan_push_secrets_to_databricks.md` for a script
that syncs `.env` → Databricks Secret Scope.

## Suggested Bootstrapping Flow

1. Copy this repository.
2. Replace the source schema and generators with your domain tables.
3. Add per-table Silver configs to `pipeline_configs/silver/<source>/`.
4. Update schema contracts.
5. Run the DV 2.0 generator to produce vault notebooks.
6. Replace Gold dbt models.
7. Update `deploy_job.py` constants and deploy all 4 jobs.
8. Upload `dv_model.json` to the Volume with `upload_vault_config.py`.
9. Trigger a full pipeline run and validate row counts at each layer.
