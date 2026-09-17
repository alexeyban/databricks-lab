# Troubleshooting Guide

This document covers common issues you may encounter when running the Databricks CDC Lakehouse Lab and their solutions.

---

## Table of Contents

1. [Infrastructure Issues](#infrastructure-issues)
2. [Kafka / Debezium Issues](#kafka--debezium-issues)
3. [Databricks Connection Issues](#databricks-connection-issues)
4. [Pipeline Execution Issues](#pipeline-execution-issues)
5. [dbt Gold Issues](#dbt-gold-issues)
6. [Data Quality Issues](#data-quality-issues)
7. [General Debugging Tips](#general-debugging-tips)

---

## Infrastructure Issues

### Docker Container Won't Start

**Symptoms**: `docker compose up -d` fails or containers exit immediately.

**Solutions**:
```bash
# Check container status
docker compose ps

# View logs
docker compose logs postgres
docker compose logs kafka
docker compose logs connect

# Common fix: Prune Docker
docker system prune -a
```

---

### PostgreSQL Fails to Initialize

**Symptoms**: PostgreSQL container exits, `dvdrental` tables not created.

**Solutions**:
1. Check if SQL dump file exists:
   ```bash
   ls -la scripts/dvdrental.sql
   ```

2. Verify PostgreSQL is ready:
   ```bash
   docker exec -it postgres psql -U postgres -d demo -c "\dt"
   ```

3. Re-run initialization:
   ```bash
   docker compose down
   docker volume rm databricks-lab_postgres_data  # If using named volume
   docker compose up -d
   ```

---

### Port Conflicts

**Symptoms**: Error messages about ports already in use (5432, 9092, 8083, 8085, etc.).

**Solutions**:
```bash
# Find what's using the port
lsof -i :5432  # or other port

# Kill the process or change port in docker-compose.yml
```

---

## Kafka / Debezium Issues

### Debezium Connector Won't Start

**Symptoms**: Connector stays in `UNASSIGNED` state or fails to start.

**Solutions**:
1. Check connector status:
   ```bash
   curl http://localhost:8083/connectors/postgres-connector/status
   ```

2. Verify PostgreSQL replication slot:
   ```bash
   docker exec -it postgres psql -U postgres -d demo -c \
     "SELECT * FROM pg_replication_slots;"
   ```

3. Verify publication:
   ```bash
   docker exec -it postgres psql -U postgres -d demo -c \
     "SELECT * FROM pg_publication;"
   ```

4. Recreate connector:
   ```bash
   curl -X DELETE http://localhost:8083/connectors/postgres-connector
   curl -X POST http://localhost:8083/connectors \
     -H 'Content-Type: application/json' \
     --data @postgres-connector.json
   ```

---

### No Messages in Kafka Topics

**Symptoms**: Topics exist but no messages, or topics not created.

**Solutions**:
1. Check topics:
   ```bash
   # Via Kafka UI at http://localhost:8085
   # Or via command line
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

2. Verify Debezium is capturing:
   ```bash
   docker logs debezium-connect 2>&1 | grep -i "snapshot\|error"
   ```

3. Force a new snapshot:
   ```json
   {
     "post-processor.resnapshot": {
       "type": "execute",
       "execute": "ALTER TABLE public.film ALTER COLUMN film_id RESTART;"
     }
   }
   ```
   Or use the Debezium signaling table to trigger a snapshot.

---

### Schema Registry Issues

**Symptoms**: Errors about missing schema, schema compatibility failures.

**Solutions**:
1. Check Schema Registry logs:
   ```bash
   docker compose logs schema-registry
   ```

2. Verify schema registration:
   ```bash
   curl http://localhost:8081/subjects
   ```

3. Delete and recreate connector (will create new schema versions):

---

### Kafka to Volume Consumer Issues

**Symptoms**: Bronze layer not receiving data, landing zone empty.

**Solutions**:
1. Check consumer logs:
   ```bash
   docker compose logs kafka-to-volume
   ```

2. Verify Databricks credentials:
   ```bash
   echo $DATABRICKS_HOST
   echo $DATABRICKS_TOKEN
   ```

3. Check Unity Catalog Volume exists:
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   # List volumes in catalog/Schema
   ```

---

## Databricks Connection Issues

### Cannot Connect to Databricks

**Symptoms**: Python scripts fail with connection errors.

**Solutions**:
1. Verify `.env` file has correct credentials:
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-token
   ```

2. Test connection:
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   # Try a simple API call
   w.clusters.list()
   ```

3. Check token has correct permissions (workspace, catalog access)

4. Verify workspace URL is correct (no trailing slash)

---

### Secrets Not Available

**Symptoms**: Job fails with "secret not found" error.

**Solutions**:
1. Check secrets in Databricks:
   ```bash
   databricks secrets list --scope dvdrental
   ```

2. Re-run secret push:
   ```bash
   python3 scripts/push_secrets_to_databricks.py
   ```

3. Verify scope exists:
   ```bash
   databricks secrets list-scopes
   ```

---

### Notebook Upload Fails

**Symptoms**: Error uploading notebooks to Databricks.

**Solutions**:
1. Check Git credentials in `.env`
2. Verify Git URL is correct in `deploy_job.py`
3. Check branch exists in repository

---

### Job Run Fails

**Symptoms**: Job runs but tasks fail.

**Solutions**:
1. Check notebook cell outputs in Databricks UI
2. Review job run logs:
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   for run in w.jobs.list_runs(job_name='dvdrental-bronze', limit=1):
       print(run.result_state)
   ```

3. Use `databricks-notebook-remediator` skill for automated diagnosis

---

## Pipeline Execution Issues

### Bronze Layer Not Receiving Data

**Symptoms**: Bronze tables empty, no new rows.

**Solutions**:
1. Check Kafka topic has messages
2. Verify Bronze notebook is running/has checkpoint
3. Check Auto Loader path exists in Volume
4. Verify ngrok tunnel if using direct Kafka

---

### Silver Layer Merge Failures

**Symptoms**: MERGE errors, schema mismatch.

**Solutions**:
1. Check Bronze table schema:
   ```sql
   DESCRIBE TABLE workspace.bronze.public_film;
   ```

2. Check Silver config matches Bronze:
   ```bash
   cat pipeline_configs/silver/dvdrental/film.json
   ```

3. Verify primary key in config matches source
4. Check for schema drift in Bronze

---

### Vault Layer Missing Data

**Symptoms**: Hubs/Links/Satellites empty or incomplete.

**Solutions**:
1. Verify Silver layer has data
2. Check `dv_model.json` config is uploaded:
   ```python
   # In vault notebook
   dbutils.fs.head("/Volumes/workspace/default/mnt/pipeline_configs/datavault/dv_model.json")
   ```

3. Verify hash key computation matches
4. Check load order (Hubs → Links → Satellites)

---

### Duplicate Data in Vault

**Symptoms**: Duplicate hub keys, link relationships.

**Solutions**:
1. Verify hash key formula is consistent
2. Check for duplicate business keys in Silver
3. Verify MERGE condition uses hash key

---

### PIT Table Issues

**Symptoms**: PIT table empty or incorrect.

**Solutions**:
1. Verify satellites have data
2. Check snapshot_date range configuration
3. Verify satellite load dates are populated

---

## dbt Gold Issues

### dbt Debug Fails

**Symptoms**: `dbt debug` shows connection errors.

**Solutions**:
```bash
cd cdc_gold
# Check profiles.yml exists and is correct
cat ~/.dbt/profiles.yml

# Or set profile via env var
export DBT_PROFILES_DIR=.
```

---

### dbt Build Fails

**Symptoms**: Model compilation or execution errors.

**Solutions**:
```bash
# Run with debug output
dbt build --debug

# Check target/compiled for generated SQL
ls target/compiled/cdc_gold/models/

# Verify source tables exist
dbt run --select source:*
```

---

### dbt Tests Fail

**Symptoms**: Test assertions fail.

**Solutions**:
1. Check test results:
   ```bash
   dbt test --verbose
   ```

2. Review `dq_results` table:
   ```sql
   SELECT * FROM workspace.monitoring.dq_results;
   ```

3. Fix source data or adjust test expectations

---

## Data Quality Issues

### Schema Drift Detected

**Symptoms**: New columns in Bronze, alerts triggered.

**Solutions**:
1. Check schema_drift_log:
   ```sql
   SELECT * FROM workspace.monitoring.schema_drift_log;
   ```

2. Update Silver config if change is expected
3. Review drift detection policy (strict/additive_only/permissive)

---

### PII Not Tagged

**Symptoms**: PII columns not in registry.

**Solutions**:
1. Run PII tagging notebook:
   - `notebooks/helpers/NB_pii_catalog_helpers.ipynb`

2. Verify pii_config.json:
   ```bash
   cat pipeline_configs/pii/pii_config.json
   ```

3. Check Unity Catalog tags applied

---

### GDPR Erasure Failed

**Symptoms**: Erasure request stuck, encryption errors.

**Solutions**:
1. Check erasure_requests table:
   ```sql
   SELECT * FROM workspace.monitoring.erasure_requests;
   ```

2. Verify key management:
   - Check DEK exists in subject_key_store
   - Verify encryption/decryption functions work

3. Run erasure notebook manually:
   - `notebooks/helpers/NB_process_erasure.ipynb`

---

## General Debugging Tips

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Use Databricks Utilities

```python
# In Databricks notebook
dbutils.fs.ls("/Volumes/workspace/default/mnt/")
dbutils.widgets.get("table_name")
display(spark.sql("SELECT * FROM ..."))
```

### Check Query Plans

```python
df.explain(True)
```

### Use the Agent System

For automated diagnosis and remediation:
- Use `databricks-notebook-remediator` skill
- Use `databricks-data-quality-analyst` skill

### Review Logs in Order

1. Databricks notebook cell outputs
2. Databricks job run logs
3. Docker container logs
4. Kafka consumer logs

---

## Getting Help

### Self-Help Resources
- `README.md` - Project overview and setup
- `ROADMAP.md` - Current status and known issues
- `design/` - Detailed design documents
- `docs/` - All documentation files

### Debugging Commands

```bash
# Infrastructure
docker compose logs -f
docker compose ps
docker exec -it postgres psql -U postgres -d demo

# Kafka
curl http://localhost:8083/connectors/postgres-connector/status
curl http://localhost:8081/subjects

# Databricks
python3 -c "from databricks.sdk import WorkspaceClient; w = WorkspaceClient(); print([c.cluster_name for c in w.clusters.list()])"

# dbt
cd cdc_gold && dbt debug && dbt build
```

---

## Known Limitations

1. **Three tables missing from Bronze**: category, country, film_actor may not be ingested due to ngrok connectivity issues from Databricks Serverless
2. **AI classifier requires API key**: DV2 generator AI features need LLM_API_KEY
3. **ngrok tunnel expires**: Must restart tunnel periodically in development