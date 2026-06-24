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

### Directory Structure

```
infra/                       # ALL environment setup
├── docker/
│   ├── docker-compose.yml
│   ├── docker-compose.override.yml
│   ├── Dockerfile
│   ├── entrypoint-dbt-gold.sh
│   ├── entrypoint-generate-cdc-traffic.sh
│   ├── init-dvdrental.sh
│   └── profiles-cdc-gold.yml
├── terraform/              # (optional later)
│   └── databricks/
└── scripts/                # bootstrap (init topics, db, etc)

ingestion/                  # ingestion layer (bronze)
├── kafka/                  # Kafka-related configs
├── cdc/                    # CDC connector configs
│   └── postgres-connector.json
├── consumers/              # Bronze consumers
│   └── NB_ingest_to_bronze.ipynb
└── generators/             # data mutation scripts
    ├── load_generator.py
    ├── load_products_generator.py
    └── load_reference_generator.py

processing/                 # ALL processing logic
├── bronze/                 # Bronze processing
│   └── NB_ingest_to_bronze.ipynb
├── silver/                 # Silver processing
│   ├── notebooks/
│   │   ├── NB_process_to_silver_generic.ipynb
│   │   ├── outdated__NB_process_payment_silver.ipynb
│   │   ├── outdated__NB_process_products_silver.ipynb
│   │   └── outdated__NB_process_to_silver.ipynb
│   ├── configs/            # Silver table configurations
│   │   ├── products.json
│   │   └── orders.json
│   ├── dvdrental/          # Table-specific silver configs
│   │   ├── film.json
│   │   ├── country.json
│   │   └── ... (15 table configs)
│   └── dq_queries/         # Silver data quality checks
│       ├── assert_gold_payment_totals_match_silver.sql
│       ├── assert_total_products_order_positive_amount.sql
│       ├── assert_total_products_order_unique_grain.sql
│       └── ...
├── vault/                  # Vault processing
│   ├── notebooks/
│   │   ├── NB_dv_metadata.ipynb
│   │   ├── NB_ingest_to_hubs.ipynb
│   │   ├── NB_ingest_to_links.ipynb
│   │   ├── NB_ingest_to_satellites.ipynb
│   │   └── NB_dv_business_vault.ipynb
│   ├── dv_model.json       # Data Vault model definition
│   └── pii/                # PII classification configurations
└── common/                 # Shared processing helpers
    ├── notebooks/
    │   ├── NB_silver_metadata.ipynb
    │   ├── NB_key_management_helpers.ipynb
    │   ├── NB_pii_catalog_helpers.ipynb
    │   ├── NB_reset_tables.ipynb
    │   ├── NB_schema_drift_helpers.ipynb
    │   ├── NB_schema_contracts.ipynb
    │   ├── NB_catalog_helpers.ipynb
    │   ├── NB_process_erasure.ipynb
    │   └── NB_confluence_generator.ipynb
    ├── databricks_client.py
    ├── databricks_tools.py
    ├── confluence_doc_generator.py
    ├── normalize_notebooks.py
    └── autonomous_agent.py

orchestration/              # jobs / workflows
├── databricks_jobs/        # Databricks job deployment and management
│   ├── deploy_job.py
│   ├── migrate_and_run.py
│   ├── smoke_test_notebooks.py
│   ├── reset_databricks_tables.py
│   ├── prepare_ngrok_kafka.py
│   └── scripts/
├── bundle/                 # Databricks bundles
└── schedules/              # job schedules

transformation/             # dbt (gold)
└── dbt_project/            # dbt gold models and tests
    ├── dbt_project.yml
    ├── models/
    │   ├── gold/
    │   └── example/
    ├── tests/
    ├── macros/
    ├── analyses
    ├── seeds
    ├── snapshots
    └── target/

config/                     # ALL configuration files
├── dev/                    # development environment
│   └── .env
├── prod/                   # production environment
│   └── (placeholder)
├── datavault/              # Data Vault configurations
│   └── dv_model.json
├── silver/                 # Silver layer configurations
│   └── configs/
│       ├── products.json
│       └── orders.json
└── .envexample             # example environment file

tests/                      # test scripts
├── test_databricks.py
└── ...

scripts/                    # CLI utilities (deploy, reset)
├── apply_vault_comments.py
├── deploy_job.py
├── dvdrental.sql
├── kafka_to_volume.py
├── patch_dv_model_types.py
├── push_secrets_to_databricks.py
├── reset_checkpoints.py
├── reset_vault.py
├── setup_pii_secrets.py
├── smoke_test_vault.py
└── upload_vault_config.py

README.md
```

### Alternative: kafka-to-volume (no ngrok required)

Databricks Serverless cannot reach a local ngrok Kafka endpoint. Use the built-in
`kafka-to-volume` Docker profile to upload CDC events to a Databricks Volume landing zone,
allowing Bronze to use Auto Loader instead of direct Kafka connectivity.

```bash
docker compose --profile kafka-to-volume up -d kafka-to-volume
```

### Source Tables (PostgreSQL dvdrental — all 15)

> **Note:** When using local ngrok for Kafka connectivity, 3 tables (`category`, `country`,
> `film_actor`) may not reach Bronze due to Databricks Serverless network limitations.
> Use the `kafka-to-volume` Docker profile as a workaround (see Architecture section).

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
|-------|------------------|