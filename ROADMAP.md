# Databricks CDC Lakehouse Lab — Roadmap

Current state of the project and prioritised next steps.

## What is complete

| Area | Status |
|------|--------|
| Local CDC stack (Docker: Zookeeper, Kafka, PostgreSQL 15, Debezium, Schema Registry, Kafka UI) | Done |
| Debezium connector — all 15 dvdrental tables captured | Done |
| Bronze layer — structured streaming from Kafka → 15 Delta tables | Done |
| Silver layer — metadata-driven generic notebook for all 15 tables | Done |
| Silver layer — per-table configs (`pipeline_configs/silver/dvdrental/*.json`) | Done |
| Silver layer — schema contracts for all 15 Bronze CDC envelopes + Silver tables | Done |
| Schema drift detection + alerting (strict / additive_only / permissive) | Done |
| DV 2.0 vault layer design (13 hubs, 19 links, 15 sats, 4 PITs, 2 bridges) | Done |
| DV 2.0 generator (14-module CLI tool — analyze → review → validate → apply) | Done |
| Vault notebooks generated and committed (`notebooks/vault/`) | Done |
| `pipeline_configs/datavault/dv_model.json` — approved DV 2.0 config | Done |
| Vault notebooks wired into `dvdrental-vault` Databricks job | Done |
| 4-job architecture (Bronze / Silver / Vault / Orchestrator) via `deploy_job.py` | Done |
| dbt Gold layer (gold_film, gold_rental — models + data quality tests) | Done |
| Data generators (rental/payment inserts, film attribute updates) | Done |

---

## Next steps

### 1. Ingest missing Bronze tables

Three tables (`category`, `country`, `film_actor`) are not yet in Bronze Delta because Databricks
Serverless cannot reach the local ngrok Kafka endpoint.

**Options:**
- Use a cloud Kafka (Confluent Cloud free tier, MSK, or similar) reachable from Databricks
- Run a Databricks cluster (not Serverless) that can access ngrok
- Manually produce Kafka records for those tables using the Debezium REST API snapshot

**Acceptance:** All 15 `workspace.bronze.*` tables exist with non-zero row counts.

---

### 2. dbt Gold layer expansion

Currently Gold has only `gold_film` and `gold_rental`. The vault layer enables richer business models.

**Tasks:**
- Add `gold_customer` — customer lifetime value, rental history, payment totals
- Add `gold_inventory` — stock levels, utilisation rate per store
- Add `gold_staff_performance` — rental/payment counts per staff member
- Add `gold_film_popularity` — rental frequency, revenue per film
- Extend data quality tests (`dbt test`) to cover new models
- Document lineage: Silver → Vault → Gold for each new model

---

### 3. Vault monitoring

No observability exists for the vault layer yet.

**Tasks:**
- Add `workspace.monitoring.vault_load_log` Delta table: one row per vault notebook run
  (hub/link/sat name, rows_inserted, load_date, duration_s, status)
- Extend `NB_schema_drift_helpers` to cover vault layer schema changes
- Add a Databricks SQL dashboard showing hub/link/sat row counts over time

---

### 4. Run AI classifier with live environment

The `step2b_ai_classifier` is wired in but requires `LLM_API_KEY` + `DATABRICKS_WAREHOUSE_ID`.

**Tasks:**
- Set env vars and run a fresh `--analyze` without `--no-ai`
- Inspect `02b_merged_classification.json` — verify AI and heuristics agree on dvdrental
- Check `decisions.log` for any `MERGE:heuristic_only` or `MERGE:ai_only` discrepancies
- Tune the AI system prompt if disagreements are semantically incorrect

**Expected outcome:** Zero LOW-confidence entities (AI and heuristics fully agree on a well-known schema).

---

### 5. Generator — re-usability for other source schemas

The DV 2.0 generator is currently validated against dvdrental. Making it schema-agnostic unlocks
its use for any new CDC source.

**Tasks:**
- Add `generators/dv_generator/export_silver_config.py` — introspects a live Databricks Silver
  schema and writes `pipeline_configs/silver/<source>/*.json` automatically
- Document the end-to-end flow for a net-new source in `design/dv2/DV2_GENERATOR_DESIGN.md`
- Test against a second source schema (e.g. Chinook or Northwind)

---

### 6. Data Quality + GDPR (future)

Detailed multi-phase plan in `dq_gdpr_todo.md`:
- Phase 1: PII inventory + monitoring table
- Phase 2: SQL-based DQ checks per Silver table
- Phase 3: GDPR crypto-shredding (column-level encryption, subject deletion)
- Phases 4–7: audit logs, dashboards, automated testing
