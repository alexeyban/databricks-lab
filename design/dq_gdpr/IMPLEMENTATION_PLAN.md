# DQ + GDPR Implementation Plan
**Repo:** databricks-lab / alexeyban

> **Status (2026-04-10): Phases 1–3 fully implemented. Phase 4 (dashboards) pending.**
>
> All notebooks, scripts, job tasks, and runbooks from Phases 1–3 are committed.
> See `ROADMAP.md` for the remaining Phase 4 dashboard work.

---

## Architecture summary

Two parallel workstreams — Data Quality (DQ) and GDPR / crypto-shredding — sharing a common
monitoring foundation. Implemented in phase order: GDPR encryption at Silver must be in place
before DQ checks run against Silver columns.

**Erasure strategy:** crypto-shredding — encrypt PII at Silver with per-subject AES-256-GCM DEKs
stored encrypted in a key vault; on erasure, delete the key. Immediate suppression via
`erasure_registry` anti-join at Gold.

**Key decisions:**
- Key vault backend: Databricks secret scopes (non-production/dev); document clearly; replace with Azure Key Vault / AWS KMS before prod
- Job wiring: new `dvdrental-dq-gdpr` job added to `scripts/deploy_job.py`
- Vault satellites: store ciphertext as-is — crypto-shredding works uniformly across Silver + Vault
- Silver encryption: `NB_process_to_silver_generic.ipynb` only (metadata-driven via pii_config.json)

**Monitoring tables:** all DQ results → `monitoring.dq_results`; all GDPR state →
`monitoring.{pii_column_registry, subject_key_store, erasure_requests, erasure_registry, erasure_audit_log}`.

---

## Phase 1 — Foundation ✅ Complete
*No pipeline changes. Pure infrastructure setup.*

### Task 1.1 — PII inventory config + column tagging notebook
**New files:**
- `pipeline_configs/pii/pii_config.json` — JSON array, one object per column across all 15 tables:
  ```json
  {"table":"silver.silver_customer","column":"email","sensitivity":"pii_direct",
   "subject_id_col":"customer_id","encrypt":true,"notes":"login identifier"}
  ```
  Sensitivity levels: `pii_direct` (email, name, phone), `pii_indirect` (FK to PII via address_id),
  `non_pii` (amounts, timestamps, codes, surrogate keys).
  PII tables: `silver_customer` (email, first_name, last_name, address_id indirect),
  `silver_staff` (email, first_name, last_name, address_id indirect, picture),
  `silver_address` (address, address2, phone, postal_code). All others: non_pii / encrypt=false.

- `notebooks/helpers/NB_pii_catalog_helpers.ipynb` — loads pii_config.json, applies Unity Catalog
  column tags via `ALTER TABLE ... ALTER COLUMN ... SET TAGS ('pii' = 'direct')`, prints summary,
  writes to `monitoring.pii_column_registry`.

**Modified files:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add `pii_column_registry` DDL:
  ```sql
  CREATE TABLE IF NOT EXISTS monitoring.pii_column_registry (
    table_name STRING, column_name STRING, sensitivity STRING,
    subject_id_col STRING, encrypt BOOLEAN, tagged_at TIMESTAMP
  ) USING DELTA;
  ```

**Acceptance criteria:**
- pii_config.json committed with all 15 tables covered
- NB_pii_catalog_helpers runs without error
- monitoring.pii_column_registry populated
- Unity Catalog tags visible on silver_customer.email

---

### Task 1.2 — Unified monitoring table + write_dq_result helper
**Modified files:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add:
  ```sql
  CREATE TABLE IF NOT EXISTS monitoring.dq_results (
    run_id STRING, layer STRING, table_name STRING, check_name STRING,
    status STRING, observed_value DOUBLE, threshold DOUBLE,
    message STRING, checked_at TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
  ```
  And helper function:
  ```python
  def write_dq_result(spark, run_id, layer, table_name, check_name,
                      status, observed_value=None, threshold=None, message=None):
  ```

**Acceptance criteria:**
- monitoring.dq_results created and queryable
- write_dq_result() callable from any notebook via %run

---

### Task 1.3 — Key management notebook + subject_key_store
**New files:**
- `notebooks/helpers/NB_key_management_helpers.ipynb` — AES-256-GCM using Databricks secret scopes:
  - `get_or_create_dek(subject_id, subject_type) -> bytes`
  - `get_dek_map(subject_ids, subject_type) -> dict` — batch fetch (avoids N+1 calls)
  - `encrypt_value(plaintext, dek) -> bytes` — AES-256-GCM (authenticated)
  - `decrypt_value(ciphertext, dek) -> str` — returns `'[ERASED]'` if DEK is None
  - `shred_subject(subject_id, subject_type)` — deletes secret + stamps shredded_at
  - **Must include warning:** "DEKs in Databricks secret scope — NOT suitable for production PII."

**Modified files:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add `monitoring.subject_key_store` DDL:
  ```sql
  CREATE TABLE IF NOT EXISTS monitoring.subject_key_store (
    subject_id STRING, subject_type STRING, encrypted_dek BINARY,
    kek_version STRING, created_at TIMESTAMP, shredded_at TIMESTAMP
  ) USING DELTA;
  ```

**Acceptance criteria:**
- subject_key_store table created
- encrypt → decrypt round-trip verified
- After shred_subject(), decrypt_value() returns '[ERASED]'

---

## Phase 2 — Bronze + Silver hardening ✅ Complete
*Bronze quarantine live. Silver PII encrypted. Silver DQ writing results.*

### Task 2.1 — Bronze quarantine table + routing
**Modified files:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add `bronze.quarantine` DDL
- `notebooks/bronze/NB_ingest_to_bronze.ipynb` — in foreachBatch:
  - `is_valid_envelope()` UDF: JSON parseable, `op` ∈ {r,c,u,d}, `source.table` present,
    `kafka_timestamp` within 24h
  - Bad rows → `bronze.quarantine` with `quarantine_reason` + `quarantined_at`
  - End of batch: call `check_volume_anomaly()` (see Task 4.4)

**Acceptance criteria:**
- Deliberately malformed message lands in bronze.quarantine
- Volume WARN fires when generator is stopped
- Good messages unaffected

---

### Task 2.2 — Bronze TTL (GDPR)
**Modified files:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — after each bronze DDL, add:
  ```sql
  ALTER TABLE bronze.{table} SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration'         = 'interval 30 days'
  );
  ```
- VACUUM task added in Phase 4 Task 4.5 (via dvdrental-dq-gdpr job)

---

### Task 2.3 — Silver PII encryption (GDPR)
**Modified files:**
- `notebooks/silver/NB_process_to_silver_generic.ipynb` — after Debezium decode, before MERGE:
  1. Load pii_config for current table_id (filter encrypt=true)
  2. If PII columns exist: `dek_map = get_dek_map(distinct_subject_ids, subject_type)`
  3. `dek_broadcast = spark.sparkContext.broadcast(dek_map)`
  4. UDF `encrypt_udf(value, subject_id) -> BinaryType`
  5. Apply to each PII column; MERGE proceeds writing ciphertext
  6. `dek_broadcast.unpersist()` immediately after write
  Non-PII tables: zero overhead (no encrypt=true entries)

**Legacy notebooks NOT modified** (reference only): NB_process_to_silver.ipynb,
NB_process_products_silver.ipynb, NB_process_payment_silver.ipynb

**Acceptance criteria:**
- silver_customer.email is BINARY after MERGE
- decrypt_value(email, get_dek(customer_id)) returns original value
- silver_film/silver_payment unchanged (no PII)
- subject_key_store has one row per unique customer processed

---

### Task 2.4 — Silver DQ assertions
Add post-MERGE DQ block to `NB_process_to_silver_generic.ipynb`:
- PK uniqueness → FAIL + raise exception if > 0
- Null rate on critical FK columns
- Payment amount range check (0.99–11.99)
- All results → write_dq_result()

**New files:**
- `dq_queries/silver/check_rental_pk.sql`
- `dq_queries/silver/check_payment_amount.sql`
- `dq_queries/silver/check_rental_fk_customer.sql`
- `dq_queries/silver/check_customer_pk.sql`
- `dq_queries/silver/check_film_pk.sql`

---

### Task 2.5 — Erasure management tables
**Modified files:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add DDL for:
  - `monitoring.erasure_requests` (request_id, subject_id, subject_type, requested_at, status, requester, notes)
  - `monitoring.erasure_registry` (subject_id, subject_type, suppressed_at)
  - `monitoring.erasure_audit_log` (request_id, step, completed_at, evidence, operator)

---

## Phase 3 — Vault + Gold + Erasure pipeline ✅ Complete
*Full erasure cycle end-to-end. Vault DQ live. Gold certified.*

### Task 3.1 — Vault integrity checks
**Modified files:**
- `notebooks/vault/NB_ingest_to_hubs.ipynb` — post-load: hub HK uniqueness per hub
- `notebooks/vault/NB_ingest_to_links.ipynb` — post-load: both hub FKs present (raises on FAIL)
- `notebooks/vault/NB_ingest_to_satellites.ipynb` — post-load: no orphaned satellite rows
- `notebooks/vault/NB_dv_business_vault.ipynb` — PIT/Bridge row count sanity
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add `monitoring.vault_load_log` DDL

---

### Task 3.2 — Gold dbt test expansion
**New files:**
- `cdc_gold/packages.yml` — dbt_utils + dbt_expectations
- `cdc_gold/tests/assert_gold_payment_totals_match_silver.sql` — Gold vs Silver sum reconciliation
- `cdc_gold/macros/write_dq_results.sql` — on-run-end macro emitting dbt results to monitoring.dq_results

**Modified files:**
- `cdc_gold/dbt_project.yml` — add on-run-end hook
- `cdc_gold/models/gold_rental.yml` — dbt_expectations column + table tests
- `cdc_gold/models/gold_film.yml` — equivalent test coverage
- `cdc_gold/models/sources.yml` — add monitoring.erasure_registry source

---

### Task 3.3 — NB_process_erasure notebook (GDPR)
**New files:**
- `notebooks/helpers/NB_process_erasure.ipynb` — parameters: REQUEST_ID, DRY_RUN (default true)

  6 steps:
  1. Load + validate request (status='pending')
  2. Immediate Gold suppression → INSERT into erasure_registry, UPDATE status='suppressed'
  3. DEK deletion → shred_subject() + stamp shredded_at
  4. Physical DELETE from Bronze (JSON field scan: $.after.customer_id / $.before.customer_id)
  5. Post-shred validation → attempt decrypt, verify returns '[ERASED]', write dq_result PASS/FAIL
  6. Complete → UPDATE status='complete', log_step('complete')

  **Security:** coerce subject_id to int() at step 1 (dvdrental IDs are always integers);
  raise ValueError on non-integer to prevent SQL injection.
  `log_step()` uses spark.createDataFrame() for parameterized writes.
  DRY_RUN=true prints full plan, writes nothing.

**Acceptance criteria:**
- DRY_RUN=true prints plan without side effects
- DRY_RUN=false completes all 6 steps, erasure_audit_log has 6 rows
- Post-shred validation PASS in dq_results
- Gold view excludes erased subject

---

### Task 3.4 — Gold suppression views (GDPR)
**New files:**
- `cdc_gold/macros/suppress_erased_subjects.sql`:
  ```sql
  {% macro suppress_erased_subjects(subject_id_col, subject_type) %}
    {{ subject_id_col }} NOT IN (
      SELECT subject_id FROM {{ source('monitoring', 'erasure_registry') }}
      WHERE subject_type = '{{ subject_type }}'
    )
  {% endmacro %}
  ```

**Modified files:**
- `cdc_gold/models/gold_rental.sql` — add WHERE clause via macro
- `cdc_gold/models/sources.yml` — add monitoring.erasure_registry

---

## Phase 4 — Visibility + operationalisation 🔲 Pending
*Dashboards live. Alerts configured. Runbooks documented.*

### Task 4.1 — DQ monitoring dashboard
- Databricks SQL dashboard, 3 queries: pass rate by layer (30d), recent failures, Bronze volume trend
- Export: `design/dashboards/dq_dashboard.json`

### Task 4.2 — GDPR SLA dashboard
- Databricks SQL dashboard: open requests by age (AT RISK if > 25d), completed erasures trend
- Export: `design/dashboards/gdpr_dashboard.json`

### Task 4.3 — Slack alerting
**Modified files:**
- `notebooks/helpers/NB_schema_drift_helpers.ipynb` — add `alert_dq_failure()` and
  `alert_erasure_sla_risk()` reusing existing `_send_webhook_alert()` infrastructure
- `notebooks/silver/NB_process_to_silver_generic.ipynb` — call alert_dq_failure() on FAIL
- All 4 vault notebooks — call alert_dq_failure() on FAIL

### Task 4.4 — Row count anomaly detection
**Modified files:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add `check_volume_anomaly(spark, run_id, table_name, current_count, lookback_days=7, threshold_pct=0.5)`: rolling 7-day average, WARN < 50%, FAIL at 0
- `notebooks/bronze/NB_ingest_to_bronze.ipynb` — call per topic table end of micro-batch (refactoring inline impl from Task 2.1)

### Task 4.5 — New dvdrental-dq-gdpr job + runbooks
**Modified files:**
- `scripts/deploy_job.py` — add `dvdrental-dq-gdpr` job with tasks:
  - `vacuum_bronze_tables` — daily; VACUUM RETAIN 720h all 15 tables; write dq_result per table
  - `process_erasure_requests` — daily; process all pending erasure_requests
  - `check_erasure_sla` — daily; alert on requests age > 25 days

**New files:**
- `design/runbooks/DQ_INCIDENT_RUNBOOK.md`
- `design/runbooks/ERASURE_SOP.md`

---

## File inventory

### New files to create
```
pipeline_configs/pii/pii_config.json
notebooks/helpers/NB_pii_catalog_helpers.ipynb
notebooks/helpers/NB_key_management_helpers.ipynb
notebooks/helpers/NB_process_erasure.ipynb
dq_queries/silver/check_rental_pk.sql
dq_queries/silver/check_payment_amount.sql
dq_queries/silver/check_rental_fk_customer.sql
dq_queries/silver/check_customer_pk.sql
dq_queries/silver/check_film_pk.sql
cdc_gold/packages.yml
cdc_gold/tests/assert_gold_payment_totals_match_silver.sql
cdc_gold/macros/write_dq_results.sql
cdc_gold/macros/suppress_erased_subjects.sql
design/dashboards/dq_dashboard.json
design/dashboards/gdpr_dashboard.json
design/runbooks/DQ_INCIDENT_RUNBOOK.md
design/runbooks/ERASURE_SOP.md
```

### Existing files to modify
```
notebooks/helpers/NB_catalog_helpers.ipynb
notebooks/helpers/NB_schema_drift_helpers.ipynb
notebooks/silver/NB_process_to_silver_generic.ipynb
notebooks/bronze/NB_ingest_to_bronze.ipynb
notebooks/vault/NB_ingest_to_hubs.ipynb
notebooks/vault/NB_ingest_to_links.ipynb
notebooks/vault/NB_ingest_to_satellites.ipynb
notebooks/vault/NB_dv_business_vault.ipynb
cdc_gold/dbt_project.yml
cdc_gold/models/gold_rental.sql
cdc_gold/models/gold_rental.yml
cdc_gold/models/gold_film.yml
cdc_gold/models/sources.yml
scripts/deploy_job.py
```

### Legacy Silver notebooks — NOT modified (reference only)
```
notebooks/silver/NB_process_to_silver.ipynb
notebooks/silver/NB_process_products_silver.ipynb
notebooks/silver/NB_process_payment_silver.ipynb
```

---

## Security notes

1. **SQL injection in NB_process_erasure** — coerce subject_id to `int()` at step 1 (dvdrental IDs are always integers); raise ValueError on failure.
2. **Databricks secrets as KEK** — non-production only; no DEK recovery path after shredding. Document replacement path for production.
3. **Broadcast DEK maps** — call `dek_broadcast.unpersist()` immediately after MERGE write to avoid DEKs lingering in executor memory.

---

## Verification (end-to-end)

| Phase | Smoke test |
|-------|-----------|
| 1 | Run NB_pii_catalog_helpers → pii_column_registry populated, UC tag visible on silver_customer.email |
| 2 | Insert customer, run Silver generic → email is BINARY; inject dup rental_id → FAIL + exception in dq_results |
| 3 | Insert row in erasure_registry → dbt build excludes subject; run NB_process_erasure DRY_RUN=false → 6 audit_log rows, post_shred_unreadable=PASS |
| 4 | After full pipeline run → dashboards load with data; backdate erasure_request to 26d → Slack alert fires |
