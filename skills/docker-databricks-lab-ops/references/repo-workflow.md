# Repo Workflow Reference

## Scope

This skill is specific to the `databricks-lab` repository.

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

## Load generators

Run product mutations first:

```bash
python3 generators/load_products_generator.py
python3 generators/load_generator.py
```

Useful environment variables:

- `ITERATIONS`
- `SLEEP_MIN`
- `SLEEP_MAX`
- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

## Databricks operations

Existing project helpers live in:

- `runtime/databricks_client.py`
- `runtime/databricks_tools.py`

The repository already expects:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

`Orders-ingest-job.yaml` defines a sample Databricks job with Bronze and Silver notebook tasks. Use it as a reference when the user asks to validate notebook execution or job wiring.

Because ngrok changes after each restart, prefer passing `KAFKA_BOOTSTRAP` dynamically at run time instead of editing the job permanently.

## Verification expectations

For a minimal end-to-end verification:

1. Start Docker services
2. Discover current ngrok Kafka endpoint and use it for advertised listeners and Databricks bootstrap
3. Register the connector
4. Run bounded product and order generators
5. Launch a Databricks job or notebook run
6. Poll until terminal state
7. Report success or return the exact failure state message
