# TODO — Next Session

## Blocked: ngrok free-tier limit reached
ngrok tunnel `7.tcp.eu.ngrok.io:13721` hit the connection limit.
On next session: restart ngrok (`python3 skills/docker-databricks-lab-ops/scripts/prepare_ngrok_kafka.py`),
get the new `host:port`, restart Docker Compose with `KAFKA_EXTERNAL_HOST` / `KAFKA_EXTERNAL_PORT` set,
then re-trigger the Bronze job with the new `KAFKA_BOOTSTRAP`.

---

## 1. Fix failing Bronze job (PRIORITY)

**Last run:** `1041487975075330` — `INTERNAL_ERROR/FAILED`  
**Cause:** Ingest_to_Bronze workload failed. Likely Kafka timeout because ngrok hit its limit
mid-run (same `STREAM_FAILED` / `TimeoutException: listTopics` pattern seen earlier).  
**Fix:**
```bash
# 1. Start fresh ngrok tunnel
python3 skills/docker-databricks-lab-ops/scripts/prepare_ngrok_kafka.py
# note new host:port, e.g. X.tcp.eu.ngrok.io:YYYYY

# 2. Restart Docker with the new advertised listener
export KAFKA_EXTERNAL_HOST=<host>
export KAFKA_EXTERNAL_PORT=<port>
docker compose up -d

# 3. Verify connector is running
curl http://localhost:8083/connectors/postgres-connector/status

# 4. Trigger Bronze job
python3 -c "
from dotenv import load_dotenv; load_dotenv('.env')
import os; from databricks.sdk import WorkspaceClient
client = WorkspaceClient(host=os.getenv('DATABRICKS_HOST').rstrip('/'), token=os.getenv('DATABRICKS_TOKEN'))
run = client.jobs.run_now(job_id=574281734474239, notebook_params={'KAFKA_BOOTSTRAP': '<host>:<port>'})
print('run_id=', run.run_id)
"
```

---

## 2. Verify all 15 Bronze tables have data

12 of 15 tables were created before the job failed. After re-running Bronze, verify:

```python
# Run via SQL API (warehouse 53165753164ae80e)
SHOW TABLES IN workspace.bronze;
SELECT table_name, COUNT(*) FROM workspace.bronze.<table> GROUP BY 1;
```

Expected tables:
`actor`, `address`, `category`, `city`, `country`, `customer`,
`film`, `film_actor`, `film_category`, `inventory`, `language`,
`payment`, `rental`, `staff`, `store`

Missing as of last check: `category`, `country`, `film_actor`
(were in Kafka topics but not yet written before job failed)

---

## 3. Investigate Bronze failure root cause

Check run output for `Ingest_to_Bronze` task:
```python
out = client.jobs.get_run_output(run_id=<bronze_task_run_id>)
print(out.error)
```
Confirm it is the ngrok `TimeoutException` and not a schema/data issue.

---

## 4. Update Databricks job via API after ngrok restart

The job definition in Databricks has `KAFKA_BOOTSTRAP: localhost:9093` as a placeholder.
Always override via `notebook_params` at trigger time — do NOT commit ngrok URLs.

Job ID: `574281734474239`  
Script: `skills/docker-databricks-lab-ops/scripts/migrate_and_run.py --skip-legacy-drop --skip-reset`

---

## 5. (Optional) Update README Bronze table list

`README.md` still lists only film/rental/payment in the medallion table section.
Update to list all 15 Bronze tables once they are all confirmed in Databricks.

---

## 6. (Optional) Drop legacy PostgreSQL tables

`orders` and `products` still exist in the `demo` database and create empty Kafka topics:
```sql
DROP TABLE IF EXISTS orders, products CASCADE;
```
This cleans up `cdc.public.orders` and `cdc.public.products` (0-record topics).

---

## Current State Summary

| Component | State |
|---|---|
| PostgreSQL dvdrental | Running, all 15 tables loaded (1k films, 16k rentals, 14.6k payments) |
| Debezium connector | RUNNING, capturing all 15 tables, signal table enabled |
| Kafka topics | 15 CDC topics populated (full snapshot done) |
| Bronze tables in Databricks | 12/15 exist (category, country, film_actor missing) |
| Silver tables | silver_film, silver_rental, silver_payment (from earlier successful run) |
| Gold models | gold_film, gold_rental (dbt — not re-run this session) |
| Job schedule | **PAUSED** (was queuing up every 5 min — do not unpause until ngrok stable) |
| Last git commit | `db11b2a` on `main` |

## Key IDs
- Databricks job: `574281734474239`
- SQL warehouse: `53165753164ae80e`  
- Databricks host: `https://dbc-25ad0935-ed54.cloud.databricks.com`
- Last failed run: `1041487975075330`
