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
| DV 2.0 type propagation — `column_types` in `SatDef` + `dv_model.json`; satellite DDL uses source-accurate Spark types (`DECIMAL`, `BOOLEAN`, `TIMESTAMP`); Silver→satellite casting | Done |
| Vault notebooks generated and committed (`notebooks/vault/`) | Done |
| `pipeline_configs/datavault/dv_model.json` — approved DV 2.0 config | Done |
| Vault notebooks wired into `dvdrental-vault` Databricks job | Done |
| Layer-based architecture refactor (ingestion / processing / transformation / config / orchestration / scripts) | Done |
| 5-job architecture (Bronze / Silver / Vault / Vault-Gold / Orchestrator) via `scripts/deploy_jobs.py` | Done |
| dbt vault models in `transformation/dbt_project/` — 15 hubs, 19 links, 15 sats, 4 PITs, 2 bridges | Done |
| dbt Gold layer — gold_film, gold_rental + 5 new gold marts (customer_summary, inventory_status, revenue_by_store, film_popularity, staff_performance) | Done |
| `dvdrental-vault-gold` Databricks job — NB_run_dbt wrapper, dbtRunner API, pre-committed dbt_packages | Done |
| Orchestrator updated to 4-task chain (bronze → silver → vault → vault-gold) | Done |
| Bulk data loader (`ingestion/generators/load_bulk_data.py`) — 1000 customers, 1000 films, 10000+ DML events | Done |
| End-to-end orchestrator validated and passing | Done |
| Data generators (rental/payment inserts, film attribute updates) | Done |
| Docker operational profiles (dbt-gold, generate-cdc-traffic, deploy-databricks-jobs, upload-vault-config, kafka-to-volume) | Done |
| PII inventory config (`pipeline_configs/pii/pii_config.json`) + Unity Catalog column tagging | Done |
| DQ monitoring table (`monitoring.dq_results`) + `write_dq_result()` helper | Done |
| AES-256-GCM key management (`NB_key_management_helpers`) + `monitoring.subject_key_store` | Done |
| Silver PII encryption (per-subject DEKs in `NB_process_to_silver_generic`) | Done |
| Silver DQ assertions (PK uniqueness, FK null rate, payment amount range) | Done |
| Bronze quarantine table + envelope validation | Done |
| GDPR erasure tables (`erasure_requests`, `erasure_registry`, `erasure_audit_log`) | Done |
| Vault DQ integrity checks (Hub HK uniqueness, Link FK presence, Satellite orphan check) | Done |
| Gold dbt test expansion (dbt_expectations, payment reconciliation, `suppress_erased_subjects` macro) | Done |
| GDPR erasure pipeline — 6-step `NB_process_erasure` (suppress → shred → delete Bronze → validate → complete) | Done |
| `dvdrental-dq-gdpr` job (daily VACUUM, erasure processing, SLA checks) | Done |
| DQ + GDPR runbooks (`design/runbooks/DQ_INCIDENT_RUNBOOK.md`, `ERASURE_SOP.md`) | Done |
| Confluence documentation generator (`runtime/confluence_doc_generator.py`) | Done |
| Agent system: 24 specialized agents + 24 skills | Done |

---

## Next steps

### 1. Productionize Kafka connectivity

The `kafka-to-volume` Docker profile resolved the Databricks Serverless ngrok limitation, and all 15 Bronze tables are available. The next step is to move to cloud Kafka so ngrok is no longer needed in any environment.

**Tasks:**
- Migrate to cloud Kafka (Confluent Cloud free tier, MSK, or similar)
- Update `push_secrets_to_databricks.py` with cloud broker details
- Remove ngrok references from quickstart and architecture docs

---

### 2. DQ/GDPR job deployment

The `dvdrental-dq-gdpr` job is fully designed (see `design/dq_gdpr/IMPLEMENTATION_PLAN.md`) but not yet deployed via `scripts/deploy_jobs.py`.

**Tasks:**
- Add `dvdrental-dq-gdpr` job definition to `scripts/deploy_jobs.py`
- Wire VACUUM, erasure processing, SLA check notebooks as tasks
- Validate end-to-end erasure pipeline

---

### 3. CI/CD

**Tasks:**
- GitHub Actions workflow: run `dbt build` on PR (using Databricks SQL warehouse)
- Lint notebooks on push
- Run unit tests for generator scripts

---

### 4. Databricks Asset Bundles

Replace `scripts/deploy_jobs.py` with a DAB bundle for environment-aware deployments.

**Tasks:**
- Create `orchestration/bundle/databricks.yml` with all 5 jobs
- Support dev/staging/prod targets
- Integrate with CI/CD pipeline

---

### 5. Vault monitoring

No observability exists for the vault layer yet.

**Tasks:**
- Add `workspace.monitoring.vault_load_log` Delta table: one row per vault notebook run
  (hub/link/sat name, rows_inserted, load_date, duration_s, status)
- Extend `NB_schema_drift_helpers` to cover vault layer schema changes
- Add a Databricks SQL dashboard showing hub/link/sat row counts over time

---

### 6. Run AI classifier with live environment

The `step2b_ai_classifier` is wired in but requires `LLM_API_KEY` + `DATABRICKS_WAREHOUSE_ID`.

**Tasks:**
- Set env vars and run a fresh `--analyze` without `--no-ai`
- Inspect `02b_merged_classification.json` — verify AI and heuristics agree on dvdrental
- Check `decisions.log` for any `MERGE:heuristic_only` or `MERGE:ai_only` discrepancies
- Tune the AI system prompt if disagreements are semantically incorrect

**Expected outcome:** Zero LOW-confidence entities (AI and heuristics fully agree on a well-known schema).

---

### 7. Generator — re-usability for other source schemas

The DV 2.0 generator is currently validated against dvdrental. Making it schema-agnostic unlocks
its use for any new CDC source.

**Tasks:**
- Add `generators/dv_generator/export_silver_config.py` — introspects a live Databricks Silver
  schema and writes `pipeline_configs/silver/<source>/*.json` automatically
- Document the end-to-end flow for a net-new source in `design/dv2/DV2_GENERATOR_DESIGN.md`
- Test against a second source schema (e.g. Chinook or Northwind)

---

### 8. DQ dashboards

Phase 4 of `design/dq_gdpr/IMPLEMENTATION_PLAN.md`.

**Tasks:**
- Databricks SQL DQ dashboard (pass rate by layer 30d, recent failures, Bronze volume trend)
- Databricks SQL GDPR SLA dashboard (open requests by age, completed erasures trend)
- Export dashboard JSON to `design/dashboards/`
