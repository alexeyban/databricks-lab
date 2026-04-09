#!/usr/bin/env bash
# setup_ngrok_kafka.sh — Start ngrok TCP tunnel for Kafka, push to Databricks secrets,
#                        and redeploy all pipeline jobs.
#
# Usage: bash .claude/skills/ngrok-kafka-setup/scripts/setup_ngrok_kafka.sh
#
# Requirements:
#   - ngrok installed and authenticated (ngrok config check-update)
#   - .env file with DATABRICKS_HOST, DATABRICKS_TOKEN
#   - Python 3 + databricks-sdk (pip install -r requirements.txt)
#   - Docker Kafka running on localhost:9093 (external listener)

set -euo pipefail

KAFKA_LOCAL_PORT=9093
NGROK_API=http://localhost:4040/api/tunnels
ENV_FILE=".env"
WAIT_SECONDS=8

echo "=== ngrok Kafka tunnel setup ==="

# ── 1. Kill any existing ngrok process ──────────────────────────────────────
if pgrep -x ngrok > /dev/null 2>&1; then
  echo "[1/5] Stopping existing ngrok process..."
  pkill -x ngrok || true
  sleep 2
else
  echo "[1/5] No existing ngrok process found."
fi

# ── 2. Start new ngrok TCP tunnel ────────────────────────────────────────────
echo "[2/5] Starting ngrok TCP tunnel → localhost:${KAFKA_LOCAL_PORT}..."
nohup ngrok tcp "${KAFKA_LOCAL_PORT}" > /tmp/ngrok-kafka.log 2>&1 &
NGROK_PID=$!
echo "      ngrok PID=${NGROK_PID}"

# ── 3. Wait for tunnel to come up ────────────────────────────────────────────
echo "[3/5] Waiting ${WAIT_SECONDS}s for tunnel to establish..."
sleep "${WAIT_SECONDS}"

TUNNEL_URL=$(curl -s "${NGROK_API}" | python3 -c "
import json, sys
data = json.load(sys.stdin)
tunnels = data.get('tunnels', [])
if not tunnels:
    print('ERROR: no tunnels found', file=sys.stderr)
    sys.exit(1)
url = tunnels[0]['public_url'].replace('tcp://', '')
print(url)
")

if [[ -z "${TUNNEL_URL}" ]]; then
  echo "ERROR: Could not get tunnel URL from ngrok API. Check /tmp/ngrok-kafka.log"
  exit 1
fi

KAFKA_HOST="${TUNNEL_URL%%:*}"
KAFKA_PORT="${TUNNEL_URL##*:}"
echo "      Tunnel: ${TUNNEL_URL}  (host=${KAFKA_HOST}, port=${KAFKA_PORT})"

# ── 4. Update .env ────────────────────────────────────────────────────────────
echo "[4/5] Updating ${ENV_FILE} and pushing to Databricks secrets..."

if grep -q "^KAFKA_EXTERNAL_HOST=" "${ENV_FILE}"; then
  sed -i "s|^KAFKA_EXTERNAL_HOST=.*|KAFKA_EXTERNAL_HOST=${KAFKA_HOST}|" "${ENV_FILE}"
else
  echo "KAFKA_EXTERNAL_HOST=${KAFKA_HOST}" >> "${ENV_FILE}"
fi

if grep -q "^KAFKA_EXTERNAL_PORT=" "${ENV_FILE}"; then
  sed -i "s|^KAFKA_EXTERNAL_PORT=.*|KAFKA_EXTERNAL_PORT=${KAFKA_PORT}|" "${ENV_FILE}"
else
  echo "KAFKA_EXTERNAL_PORT=${KAFKA_PORT}" >> "${ENV_FILE}"
fi

# Load updated .env into environment so SDK can authenticate
set -a && source "${ENV_FILE}" && set +a

# Push KAFKA_EXTERNAL_HOST + KAFKA_EXTERNAL_PORT (and any other keys) to
# Databricks secret scope 'dvdrental'. The Bronze notebook reads these directly.
python3 scripts/push_secrets_to_databricks.py

# ── 5. Redeploy all jobs (Bronze no longer needs --kafka-bootstrap flag) ─────
echo "[5/5] Redeploying Databricks pipeline jobs..."
python3 scripts/deploy_job.py

echo ""
echo "=== Done ==="
echo "  Kafka bootstrap : ${KAFKA_HOST}:${KAFKA_PORT}"
echo "  Secrets scope   : dvdrental  (kafka-external-host / kafka-external-port)"
echo "  ngrok log       : /tmp/ngrok-kafka.log"
echo ""
echo "Run the orchestrator for an end-to-end test:"
echo "  set -a && source .env && set +a && python3 scripts/deploy_job.py --run"
