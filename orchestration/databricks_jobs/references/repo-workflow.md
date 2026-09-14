# Repo Workflow Reference

## Scope

This skill is specific to the `databricks-lab` repository.

## Source database

The local PostgreSQL instance is seeded with the **dvdrental** sample database (~1000 films, ~16k rentals, ~14k payments) on first start via `docker/init-dvdrental.sh`. No manual schema initialisation is needed.

CDC captures three tables: `public.film`, `public.rental`, `public.payment`.

## Local services

The local stack is defined in `docker-compose.yml` and includes:

- `zookeeper`
- `kafka`
- `schema-registry`
- `postgres`
- `connect`
- `kafka-ui`

Use `docker compose up -d` from the repository root to start the stack.

If Databricks must reach Kafka through a tunnel, do not hardcode the ngrok endpoint in committed files. Discover the current tunnel first, then start Compose with:

- `KAFKA_EXTERNAL_HOST=<ngrok-host>`
- `KAFKA_EXTERNAL_PORT=<ngrok-port>`

The tunnel can be discovered from `http://127.0.0.1:4040/api/tunnels`.

## Connector registration

Register CDC capture with:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @postgres-connector.json
```

If the connector already exists, the API may return a conflict. Report that as already configured if the connector is healthy.

Captured topics: `cdc.public.film`, `cdc.public.rental`, `cdc.public.payment`

## Load generators

```bash
python3 generators/load_products_generator.py   # film updates
python3 generators/load_generator.py             # rental inserts, returns, payment inserts
```

Useful environment variables:

- `ITERATIONS`
- `SLEEP_MIN`
- `SLEEP_MAX`
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

## Resetting Databricks tables

To drop all Bronze/Silver/Gold tables and clear streaming checkpoints before a fresh load:

```bash
python3 skills/docker-databricks-lab-ops/scripts/reset_databricks_tables.py \
  --cluster-id <cluster-id> --catalog workspace
```

Use `--dry-run` first to preview what will be dropped.

This triggers `notebooks/helpers/NB_reset_tables` on Databricks.

## Databricks operations

Existing project helpers live in:

- `runtime/databricks_client.py`
- `runtime/databricks_tools.py`

The repository expects:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

`Orders-ingest-job.yaml` defines the Databricks job:
- **Schedule**: every 5 minutes (enabled, `UNPAUSED`)
- **Bronze task**: `notebooks/bronze/NB_ingest_to_bronze` (Kafka → Delta, `availableNow=True`)
- **Silver tasks** (parallel after Bronze): `NB_process_to_silver` (rental), `NB_process_products_silver` (film), `NB_process_payment_silver` (payment)

Because ngrok changes after each restart, prefer passing `KAFKA_BOOTSTRAP` dynamically at run time instead of editing the job permanently.

## Verification expectations

For a minimal end-to-end verification:

1. Start Docker services
2. Discover current ngrok Kafka endpoint and use it for advertised listeners and Databricks bootstrap
3. Register the connector
4. Run bounded film and rental generators
5. Launch a Databricks job or notebook run
6. Poll until terminal state
7. Report success or return the exact failure state message
