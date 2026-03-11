# Using This Repo For Your Own Projects

This repository can be reused as a starter for a CDC-to-Databricks lakehouse project.

## What You Can Reuse

- Local CDC stack with PostgreSQL, Kafka, Debezium, Schema Registry, and Kafka UI
- Databricks Bronze and Silver notebook patterns
- dbt Gold layer structure
- Schema drift detection helpers
- Smoke-test automation with ngrok-aware Kafka bootstrap handling

## Recommended Adaptation Path

### 1. Replace the source-domain schema

Update:

- [init-db.sql](/home/legion/PycharmProjects/gitlab/databricks-lab/init-db.sql)
- [migrate_schema.sql](/home/legion/PycharmProjects/gitlab/databricks-lab/migrate_schema.sql)
- [postgres-connector.json](/home/legion/PycharmProjects/gitlab/databricks-lab/postgres-connector.json)
- [generators/load_generator.py](/home/legion/PycharmProjects/gitlab/databricks-lab/generators/load_generator.py)
- [generators/load_products_generator.py](/home/legion/PycharmProjects/gitlab/databricks-lab/generators/load_products_generator.py)

Replace the sample `orders` and `products` logic with your domain tables and change-event patterns.

### 2. Adapt Bronze ingestion

Use [NB_ingest_to_bronze.ipynb](/home/legion/PycharmProjects/gitlab/databricks-lab/notebooks/bronze/NB_ingest_to_bronze.ipynb) as the ingestion template.

Change:

- topic pattern
- expected source contracts
- monitoring schema and checkpoint paths
- table routing logic if your topic naming differs

### 3. Adapt Silver processing

Use:

- [NB_process_to_silver.ipynb](/home/legion/PycharmProjects/gitlab/databricks-lab/notebooks/silver/NB_process_to_silver.ipynb)
- [NB_process_products_silver.ipynb](/home/legion/PycharmProjects/gitlab/databricks-lab/notebooks/silver/NB_process_products_silver.ipynb)

as merge-pattern references.

Replace:

- Debezium payload schema
- business keys
- deduplication logic
- merge clauses
- table-specific transformations

### 4. Update schema contracts and drift rules

Modify:

- [NB_schema_contracts.ipynb](/home/legion/PycharmProjects/gitlab/databricks-lab/notebooks/helpers/NB_schema_contracts.ipynb)
- [NB_schema_drift_helpers.ipynb](/home/legion/PycharmProjects/gitlab/databricks-lab/notebooks/helpers/NB_schema_drift_helpers.ipynb)

Define contracts for your own Bronze, Silver, and Gold tables before treating the pipeline as production-ready.

### 5. Replace Gold models

Update the dbt project in [cdc_gold](/home/legion/PycharmProjects/gitlab/databricks-lab/cdc_gold) so Gold reflects your business entities, metrics, and tests.

Focus on:

- sources
- models
- schema tests
- referential integrity tests

### 6. Configure Databricks execution

Review:

- [Orders-ingest-job.yaml](/home/legion/PycharmProjects/gitlab/databricks-lab/Orders-ingest-job.yaml)
- [skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py](/home/legion/PycharmProjects/gitlab/databricks-lab/skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py)

Replace:

- notebook paths
- catalog/schema names
- checkpoint paths
- job ids
- repository URL and branch

## Secret Handling

Do not commit real credentials.

Use:

- [\.envexample](/home/legion/PycharmProjects/gitlab/databricks-lab/.envexample) as the template
- local `.env` for real values

Keep real values only in local environment variables, secret stores, or CI/CD secret managers.

## Suggested Bootstrapping Flow

1. Copy this repository.
2. Replace the source schema and generators with your domain tables.
3. Update Bronze and Silver notebook logic for your CDC payloads.
4. Update schema contracts and drift policy.
5. Replace Gold dbt models.
6. Configure Databricks jobs and smoke test inputs.
7. Run the smoke test end-to-end before using the repo as a project baseline.

## Fast Validation

For a quick end-to-end validation after adapting the project, run:

```bash
python3 skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py
```

That gives you one command to verify:

- Docker stack health
- ngrok-aware Kafka exposure
- source mutation generation
- Databricks notebook execution
- final terminal success or failure
