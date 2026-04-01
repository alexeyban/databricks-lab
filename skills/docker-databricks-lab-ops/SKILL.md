---
name: docker-databricks-lab-ops
description: Start and verify the local Docker CDC lab (dvdrental), run the PostgreSQL load generators, reset Databricks tables, trigger Databricks notebook jobs through databricks.sdk, and check whether Bronze/Silver notebooks completed successfully. Use when Codex needs to bring up the repo's local infrastructure, generate CDC traffic, reset or clean Databricks tables, execute Databricks jobs, poll run status, inspect failures, or validate notebook outputs for this lab.
---

# Docker Databricks Lab Ops

## Overview

Use this skill for the operational loop of this repository: bring up Docker services, generate source-table mutations, reset Databricks tables, execute a Databricks notebook or job, and verify whether the notebook run finished successfully.

Prefer the bundled scripts over rewriting shell commands. They encode the repository-specific paths and the expected sequence.

## Workflow

### 1. Inspect the repo inputs first

- Confirm `docker-compose.yml`, `postgres-connector.json`, `Orders-ingest-job.yaml`, and the target notebook paths exist.
- Read [references/repo-workflow.md](./references/repo-workflow.md) if you need the repo-specific sequence or parameters.

### 2. Bring up the local CDC stack

- Use `scripts/start_stack.sh`.
- This runs `docker compose up -d` from the repository root.
- If the user asked for verification, follow with `docker compose ps` or service-specific health checks.
- If Kafka must be reachable from Databricks through an internal network boundary, first use `scripts/prepare_ngrok_kafka.py` and then start Compose with the discovered `KAFKA_EXTERNAL_HOST` and `KAFKA_EXTERNAL_PORT`.

### 3. Register Debezium connector if CDC ingestion needs to be exercised

- Use `scripts/register_connector.sh`.
- Only do this after Kafka Connect is accepting requests.
- If the connector already exists, report that clearly instead of treating it as a fatal failure.

### 4. Run load generators

- Use `scripts/run_generators.sh`.
- The first argument is film update iterations, the second is rental/payment iterations.
- Prefer bounded runs for verification by passing `ITERATIONS`; avoid indefinite generators unless the user asked for sustained load.

Example:

```bash
skills/docker-databricks-lab-ops/scripts/run_generators.sh 20 40
```

This runs 20 film mutations and 40 rental/payment mutations.

### 5. Reset Databricks tables (when starting fresh)

- Use `scripts/reset_databricks_tables.py` to drop all Bronze/Silver/Gold tables and clear streaming checkpoints before a fresh load.
- Requires `--cluster-id` or will submit via git source.
- Use `--dry-run` to preview what would be dropped without dropping.

```bash
python3 skills/docker-databricks-lab-ops/scripts/reset_databricks_tables.py \
  --cluster-id <cluster-id> \
  --catalog workspace

# preview only:
python3 skills/docker-databricks-lab-ops/scripts/reset_databricks_tables.py \
  --cluster-id <cluster-id> \
  --dry-run
```

### 6. Trigger a Databricks notebook job

- Use `scripts/run_databricks_notebook.py`.
- Provide either:
  - `--job-id` to run an existing Databricks job, or
  - `--notebook-path` and `--cluster-id` to submit a one-off notebook run.
- For dynamic Kafka exposure, pass `--notebook-param KAFKA_BOOTSTRAP=<ngrok-host:port>` so the current tunnel endpoint is used at run time instead of a stale static value.
- This script uses `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from the environment.

Examples:

```bash
python3 skills/docker-databricks-lab-ops/scripts/run_databricks_notebook.py \
  --job-id 123 \
  --notebook-param KAFKA_BOOTSTRAP=0.tcp.eu.ngrok.io:12345
```

```bash
python3 skills/docker-databricks-lab-ops/scripts/run_databricks_notebook.py \
  --notebook-path /Workspace/agent/notebook \
  --cluster-id 0123-456abc-cluster
```

### 7. Verify notebook behavior

- Treat the job as successful only when lifecycle is terminal and result is `SUCCESS`.
- On failure, capture:
  - `run_id`
  - lifecycle state
  - result state
  - state message
  - notebook path or job id
- If the run succeeded, summarize which notebook or job was exercised and what evidence was collected.

### 8. Report the outcome

- State what was started locally.
- State whether load generation ran and with what iteration counts.
- State which Databricks job or notebook was executed.
- State whether notebook verification passed or failed.
- If it failed, include the exact failure message and the next corrective step.

### 9. Use the smoke test when the user wants one-command verification

- Use `scripts/smoke_test_notebooks.py`.
- It discovers or starts an ngrok tunnel, restarts Docker with the correct advertised Kafka listener, registers the connector, runs bounded load generators, triggers the Databricks job, and waits for terminal results.
- Pass `--reset --cluster-id <id>` to drop all tables and checkpoints before the smoke run.

```bash
# Full smoke test with table reset:
python3 skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py \
  --job-id 123 \
  --reset \
  --cluster-id <cluster-id>

# Smoke test without reset (append to existing data):
python3 skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py \
  --job-id 123
```

## Scripts

- `scripts/start_stack.sh`: start Docker Compose services for the lab
- `scripts/prepare_ngrok_kafka.py`: discover or start an ngrok TCP tunnel for Kafka and print the current public bootstrap
- `scripts/register_connector.sh`: register the Debezium connector from `postgres-connector.json`
- `scripts/run_generators.sh`: run film and rental/payment generators
- `scripts/reset_databricks_tables.py`: drop all Bronze/Silver/Gold Delta tables and clear streaming checkpoints
- `scripts/run_databricks_notebook.py`: launch or submit a Databricks run and poll to completion
- `scripts/smoke_test_notebooks.py`: run the end-to-end smoke test with dynamic ngrok bootstrap handling

## References

- `references/repo-workflow.md`: repo-specific execution order, assumptions, and parameters
