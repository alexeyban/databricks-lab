---
name: docker-databricks-lab-ops
description: Start and verify the local Docker CDC lab, run the PostgreSQL load generators, trigger Databricks notebook jobs through databricks.sdk, and check whether Bronze/Silver notebooks completed successfully. Use when Codex needs to bring up the repo's local infrastructure, generate CDC traffic, execute Databricks jobs, poll run status, inspect failures, or validate notebook outputs for this lab.
---

# Docker Databricks Lab Ops

## Overview

Use this skill for the operational loop of this repository: bring up Docker services, generate source-table mutations, execute a Databricks notebook or job, and verify whether the notebook run finished successfully.

Prefer the bundled scripts over rewriting shell commands. They encode the repository-specific paths and the expected sequence.

## Workflow

### 1. Inspect the repo inputs first

- Confirm `docker-compose.yml`, `postgres-connector.json`, `Orders-ingest-job.yaml`, and the target notebook paths exist.
- Read [references/repo-workflow.md](./references/repo-workflow.md) if you need the repo-specific sequence or parameters.

### 2. Bring up the local CDC stack

- Use `scripts/start_stack.sh`.
- This runs `docker compose up -d` from the repository root.
- If the user asked for verification, follow with `docker compose ps` or service-specific health checks.

### 3. Register Debezium connector if CDC ingestion needs to be exercised

- Use `scripts/register_connector.sh`.
- Only do this after Kafka Connect is accepting requests.
- If the connector already exists, report that clearly instead of treating it as a fatal failure.

### 4. Run load generators

- Use `scripts/run_generators.sh`.
- Start `load_products_generator.py` before `load_generator.py`, because orders depend on products.
- Prefer bounded runs for verification by passing `ITERATIONS`; avoid indefinite generators unless the user asked for sustained load.

Example:

```bash
skills/docker-databricks-lab-ops/scripts/run_generators.sh 20 40
```

This runs 20 product mutations and 40 order mutations.

### 5. Trigger a Databricks notebook job

- Use `scripts/run_databricks_notebook.py`.
- Provide either:
  - `--job-id` to run an existing Databricks job, or
  - `--notebook-path` and `--cluster-id` to submit a one-off notebook run.
- This script uses `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from the environment.

Examples:

```bash
python3 skills/docker-databricks-lab-ops/scripts/run_databricks_notebook.py \
  --job-id 123
```

```bash
python3 skills/docker-databricks-lab-ops/scripts/run_databricks_notebook.py \
  --notebook-path /Workspace/agent/notebook \
  --cluster-id 0123-456abc-cluster
```

### 6. Verify notebook behavior

- Treat the job as successful only when lifecycle is terminal and result is `SUCCESS`.
- On failure, capture:
  - `run_id`
  - lifecycle state
  - result state
  - state message
  - notebook path or job id
- If the run succeeded, summarize which notebook or job was exercised and what evidence was collected.

### 7. Report the outcome

- State what was started locally.
- State whether load generation ran and with what iteration counts.
- State which Databricks job or notebook was executed.
- State whether notebook verification passed or failed.
- If it failed, include the exact failure message and the next corrective step.

## Scripts

- `scripts/start_stack.sh`: start Docker Compose services for the lab
- `scripts/register_connector.sh`: register the Debezium connector from `postgres-connector.json`
- `scripts/run_generators.sh`: run product and order generators in the correct order
- `scripts/run_databricks_notebook.py`: launch or submit a Databricks run and poll to completion

## References

- `references/repo-workflow.md`: repo-specific execution order, assumptions, and parameters
