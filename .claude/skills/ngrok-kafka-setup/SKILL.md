---
name: ngrok-kafka-setup
description: Sets up a fresh ngrok TCP tunnel for local Kafka, updates .env with the new KAFKA_EXTERNAL_HOST/PORT, and re-deploys the dvdrental-bronze Databricks job. Run this before any end-to-end Databricks pipeline test.
disable-model-invocation: true
---

# ngrok-kafka-setup

Sets up a fresh ngrok TCP tunnel exposing local Kafka (port 9093) to Databricks, then re-deploys the Bronze job with the new bootstrap address.

## When to use

- Before running end-to-end Databricks pipeline tests
- After restarting the local Docker stack (ngrok URL changes)
- When the Bronze job fails with Kafka timeout errors
- When the ngrok free-tier session expires

## Steps performed

1. Kill any existing ngrok process
2. Start a new `ngrok tcp 9093` tunnel
3. Poll the ngrok local API (`http://localhost:4040`) for the public URL
4. Update `.env` → `KAFKA_EXTERNAL_HOST` and `KAFKA_EXTERNAL_PORT`
5. Re-deploy `dvdrental-bronze` via `scripts/deploy_job.py --kafka-bootstrap <host:port>`

## Prerequisites

- `ngrok` installed and authenticated (`ngrok config check-update`)
- Local Docker stack running (`docker compose up -d`) — Kafka must be on port 9093
- `.env` with `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
- Python env with `databricks-sdk` (`pip install -r requirements.txt`)

## Execution

Run the following command:

```bash
bash .claude/skills/ngrok-kafka-setup/scripts/setup_ngrok_kafka.sh
```

After the script completes, trigger an orchestrator run to test end-to-end:

```bash
set -a && source .env && set +a
python3 scripts/deploy_job.py \
  --kafka-bootstrap "${KAFKA_EXTERNAL_HOST}:${KAFKA_EXTERNAL_PORT}" \
  --run
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `ERROR: no tunnels found` | Increase `WAIT_SECONDS` in script or check ngrok auth |
| Bronze job: `TimeoutException: Timed out waiting for a node assignment` | ngrok session expired — re-run this skill |
| ngrok: `Your account is limited to 1 simultaneous ngrok agent session` | Sign into ngrok, or upgrade plan |
| `.env not found` | Run from repo root: `cd /path/to/databricks-lab` |

## Notes

- Databricks Serverless cannot reach `localhost:9093` directly — ngrok is the only viable path for local dev
- The ngrok free tier assigns a **random port** on each restart; this skill handles that automatically
- The script logs ngrok output to `/tmp/ngrok-kafka.log`
- Only `dvdrental-bronze` is re-deployed (Silver/Vault/Orchestrator don't need the bootstrap address)
