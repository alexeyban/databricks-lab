# DQ + GDPR implementation plan
# databricks-lab / alexeyban
#
# Use this file with Claude Code. Work phase by phase.
# Each task has: context, files to create/modify, acceptance criteria.
# Start a Claude Code session with:
#   "Read dq_gdpr_todo.md and implement Phase 1."

---

## Architecture summary

Two parallel workstreams — Data Quality (DQ) and GDPR / crypto-shredding —
sharing a common monitoring foundation. They must be implemented in phase order
because GDPR encryption at Silver must be in place before DQ checks are written
against Silver columns (checking null rates on ciphertext is meaningless).

Erasure strategy: crypto-shredding (encrypt PII at Silver with per-subject AES
keys stored encrypted in an external key vault; on erasure, delete the key).
Immediate suppression via erasure_registry anti-join at Gold.

All DQ check results write to a single unified table: monitoring.dq_results.
All GDPR state lives in monitoring.{pii_column_registry, subject_key_store,
erasure_requests, erasure_registry, erasure_audit_log}.

---

## Phase 1 — Foundation (weeks 1–2)
### Goal: shared infrastructure, no pipeline changes, nothing encrypted yet.

---

### TASK 1.1 — PII inventory
**Type:** new file + new notebook
**Effort:** 1 day

Create `pipeline_configs/pii/pii_config.json` — machine-readable map of every
PII column across all 15 dvdrental Silver tables.

Schema per entry:
```json
{
  "table": "silver.silver_customer",
  "column": "email",
  "sensitivity": "pii_direct",
  "subject_id_col": "customer_id",
  "encrypt": true,
  "notes": "login identifier"
}
```

Sensitivity levels:
- `pii_direct`   — directly identifies a person (email, name, phone)
- `pii_indirect` — links to PII via FK (address_id in rental)
- `non_pii`      — amounts, timestamps, codes, surrogate keys

Known PII columns to capture (extend as needed):
- silver_customer:  email, first_name, last_name, address_id (indirect)
- silver_staff:     email, first_name, last_name, address_id (indirect), picture
- silver_address:   address, address2, phone, postal_code
- All other tables: non_pii (film attributes, rental times, payment amounts)

After creating pii_config.json, create notebook:
`notebooks/helpers/NB_pii_catalog_helpers.ipynb`

Notebook responsibilities:
- Load pii_config.json
- Apply Unity Catalog column tags to each PII column:
  `ALTER TABLE ... ALTER COLUMN ... SET TAGS ('pii' = 'direct')`
- Print a summary table: table | column | sensitivity | tagged_at
- Write one row per column to `monitoring.pii_column_registry`:
  (table_name, column_name, sensitivity, subject_id_col, encrypt, tagged_at)

**Files to create:**
- `pipeline_configs/pii/pii_config.json`
- `notebooks/helpers/NB_pii_catalog_helpers.ipynb`

**Acceptance criteria:**
- pii_config.json committed with all 15 tables covered
- NB_pii_catalog_helpers runs without error in Databricks
- monitoring.pii_column_registry populated
- Unity Catalog tags visible on silver_customer.email

---

### TASK 1.2 — Unified monitoring table
**Type:** DDL + helper notebook extension
**Effort:** 0.5 day

Add `monitoring.dq_results` Delta table. This is the single target for every
DQ check result from every layer — Bronze assertions, Silver SQL checks,
Vault integrity queries, dbt test outcomes, and GDPR post-shred validation.

DDL (add to `notebooks/helpers/NB_catalog_helpers.ipynb`):
```sql
CREATE TABLE IF NOT EXISTS monitoring.dq_results (
  run_id        STRING,
  layer         STRING,       -- bronze | silver | vault | gold
  table_name    STRING,
  check_name    STRING,
  status        STRING,       -- PASS | FAIL | WARN
  observed_value DOUBLE,
  threshold     DOUBLE,
  message       STRING,
  checked_at    TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

Also add a shared Python helper function `write_dq_result(...)` to
`notebooks/helpers/NB_catalog_helpers.ipynb` that any notebook can %run
and call to write a result row without duplicating the INSERT logic.

```python
def write_dq_result(spark, run_id, layer, table_name, check_name,
                    status, observed_value=None, threshold=None, message=None):
    from pyspark.sql import Row
    from datetime import datetime
    row = Row(
        run_id=run_id, layer=layer, table_name=table_name,
        check_name=check_name, status=status,
        observed_value=float(observed_value) if observed_value is not None else None,
        threshold=float(threshold) if threshold is not None else None,
        message=message, checked_at=datetime.utcnow()
    )
    spark.createDataFrame([row]).write.format("delta") \
        .mode("append").saveAsTable("monitoring.dq_results")
```

**Files to modify:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` — add DDL + write_dq_result()

**Acceptance criteria:**
- monitoring.dq_results table created and queryable
- write_dq_result() callable from any notebook via %run

---

### TASK 1.3 — Key management design + subject_key_store
**Type:** new notebook + new table + key vault integration
**Effort:** 2–3 days (key vault setup is the long pole)

Create `monitoring.subject_key_store`:
```sql
CREATE TABLE IF NOT EXISTS monitoring.subject_key_store (
  subject_id      STRING,       -- e.g. customer_id as string
  subject_type    STRING,       -- customer | staff
  encrypted_dek   BINARY,       -- AES-256 DEK encrypted by KEK
  kek_version     STRING,       -- key vault key version used
  created_at      TIMESTAMP,
  shredded_at     TIMESTAMP     -- null until erasure
)
USING DELTA;
```

Create `notebooks/helpers/NB_key_management_helpers.ipynb` with:

```python
# Key vault connection — choose one:
# Azure:  from azure.keyvault.keys import KeyClient
# AWS:    import boto3; kms = boto3.client('kms')
# Local dev fallback: Databricks secret scope (NOT for production PII)

def get_or_create_dek(subject_id: str, subject_type: str) -> bytes:
    """Return plaintext DEK for subject. Create and store if not exists."""
    ...

def encrypt_value(plaintext: str, dek: bytes) -> bytes:
    """AES-256-GCM encrypt a single string value."""
    ...

def decrypt_value(ciphertext: bytes, dek: bytes) -> str:
    """AES-256-GCM decrypt. Returns '[ERASED]' if DEK not found."""
    ...

def shred_subject(subject_id: str, subject_type: str) -> None:
    """Delete DEK from key store. Ciphertext becomes permanently unreadable."""
    ...
```

Important implementation notes:
- Use AES-256-GCM (authenticated encryption — detects tampering)
- One DEK per subject_id, not per column
- Batch DEK fetches in Silver MERGE: one lookup per unique subject_id in the
  micro-batch, not one per row (avoids N+1 key vault calls)
- For local dev without a real key vault, use Databricks secret scope as a
  stand-in, but document this clearly as non-production only

**Files to create:**
- `notebooks/helpers/NB_key_management_helpers.ipynb`
- `notebooks/helpers/NB_catalog_helpers.ipynb` (add subject_key_store DDL)

**Acceptance criteria:**
- subject_key_store table created
- get_or_create_dek() and encrypt_value() / decrypt_value() tested in isolation
- shred_subject() tested: after shred, decrypt_value() returns '[ERASED]'
- Key vault connection verified (even if using secret scope for dev)

---

## Phase 2 — Bronze + Silver hardening (weeks 3–5)
### Goal: Bronze quarantine live, Silver PII encrypted, Silver DQ writing results.

---

### TASK 2.1 — Bronze quarantine table + routing
**Type:** modify existing notebook
**Effort:** 1 day

Add `bronze.quarantine` table and route bad Debezium messages into it from
`notebooks/bronze/NB_ingest_to_bronze.ipynb`.

A message is bad if any of:
- JSON parse fails (envelope not parseable)
- `op` field missing or not in {r, c, u, d}
- `source.table` field missing (cannot determine target table)
- `kafka_timestamp` more than 24 hours behind current time

Routing logic — in the streaming foreachBatch function:
```python
good_df = batch_df.filter(is_valid_envelope(col("value")))
bad_df  = batch_df.filter(~is_valid_envelope(col("value")))

good_df.write...  # existing Bronze write
bad_df.withColumn("quarantine_reason", get_rejection_reason(col("value"))) \
      .withColumn("quarantined_at", current_timestamp()) \
      .write.format("delta").mode("append").saveAsTable("bronze.quarantine")
```

Also add volume anomaly check at end of each micro-batch:
- Compute rows written this batch
- Compare to rolling 7-day average (read from monitoring.dq_results
  where check_name = 'bronze_volume' for the last 7 days)
- If current < 50% of average: write WARN to monitoring.dq_results
- If current = 0 and average > 0: write FAIL to monitoring.dq_results

**Files to modify:**
- `notebooks/bronze/NB_ingest_to_bronze.ipynb`

**New tables created:**
- `bronze.quarantine` — add DDL to NB_catalog_helpers

**Acceptance criteria:**
- Deliberately malformed test message lands in bronze.quarantine, not bronze table
- Volume WARN fires when generator is stopped mid-run
- Good messages unaffected — existing Bronze tests still pass

---

### TASK 2.2 — Bronze TTL policy (GDPR)
**Type:** table property + scheduled job task
**Effort:** 0.5 day

Set Delta retention on all Bronze tables to 30 days. Add a scheduled VACUUM
task to `Orders-ingest-job.yaml`.

In `notebooks/helpers/NB_catalog_helpers.ipynb`, after creating each bronze table:
```sql
ALTER TABLE bronze.{table_name}
SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.logRetentionDuration'         = 'interval 30 days'
);
```

Add to `Orders-ingest-job.yaml` a new task `vacuum_bronze_tables` that runs
after Bronze ingest, once per day (not every 5-minute run):
```python
for table in bronze_tables:
    spark.sql(f"VACUUM bronze.{table} RETAIN 720 HOURS")
    # 720h = 30 days
```

Write a dq_result row after each VACUUM confirming completion:
```python
write_dq_result(spark, run_id, 'bronze', table,
                'vacuum_completed', 'PASS', message=f"retained 720h")
```

**Files to modify:**
- `notebooks/helpers/NB_catalog_helpers.ipynb`
- `Orders-ingest-job.yaml`

**Acceptance criteria:**
- All 15 bronze tables have 30-day retention property set
- VACUUM task runs in job without error
- dq_results contains vacuum_completed rows after run

---

### TASK 2.3 — Silver PII encryption (GDPR)
**Type:** modify 3 existing Silver notebooks + 12 reference notebooks
**Effort:** 3 days

Modify Silver MERGE notebooks to encrypt PII columns at write time.
Start with the 3 high-value tables: silver_customer, silver_staff, silver_rental.
Extend to all 15 once the pattern is validated.

Pattern for each Silver MERGE notebook:
```python
# 1. %run NB_key_management_helpers to get encrypt_value, get_or_create_dek

# 2. After parsing Debezium envelope, before writing to Silver:
pii_cols = load_pii_config_for_table("silver.silver_customer")
# pii_cols = [("email", "customer_id"), ("first_name", "customer_id"), ...]

# 3. Batch DEK fetch — one per unique subject_id
subject_ids = decoded_df.select("customer_id").distinct().collect()
dek_map = {row.customer_id: get_or_create_dek(str(row.customer_id), "customer")
           for row in subject_ids}
dek_broadcast = spark.sparkContext.broadcast(dek_map)

# 4. UDF to encrypt
@udf(BinaryType())
def encrypt_udf(value, subject_id):
    dek = dek_broadcast.value.get(subject_id)
    if dek is None or value is None:
        return None
    return encrypt_value(str(value), dek)

# 5. Apply to each PII column
for col_name, id_col in pii_cols:
    decoded_df = decoded_df.withColumn(
        col_name, encrypt_udf(col(col_name), col(id_col))
    )

# 6. Existing MERGE continues unchanged — now writing ciphertext
```

Non-PII columns (film_id, rental_date, amount, etc.) pass through unchanged.

For silver_rental: rental has no direct PII columns. Only address_id (indirect).
Mark as pii_indirect in pii_config — no encryption needed on the rental table
itself, but FK to customer_id is the link to PII in silver_customer.

**Files to modify:**
- `notebooks/silver/NB_process_to_silver.ipynb`         (silver_rental)
- `notebooks/silver/NB_process_products_silver.ipynb`   (silver_film — no PII)
- `notebooks/silver/NB_process_payment_silver.ipynb`    (silver_payment — no PII)
- All 12 reference table notebooks — add encryption where pii_config says encrypt=true

**Acceptance criteria:**
- silver_customer.email column contains binary ciphertext after MERGE
- decrypt_value(silver_customer.email, get_dek(customer_id)) returns original email
- silver_film and silver_payment unchanged (no PII columns)
- subject_key_store has one row per unique customer_id processed
- Existing Silver smoke tests still pass (add decryption step to assertions)

---

### TASK 2.4 — Silver DQ assertions
**Type:** modify 3 Silver notebooks + new SQL files
**Effort:** 2 days

Add post-MERGE DQ assertion block to each Silver notebook. All results write
to monitoring.dq_results via write_dq_result().

Standard assertion set (adapt per table):
```python
run_id = dbutils.widgets.get("RUN_ID")  # passed from job

# 1. PK uniqueness
dup_count = spark.sql(f"""
    SELECT COUNT(*) - COUNT(DISTINCT rental_id) FROM silver.silver_rental
""").collect()[0][0]
write_dq_result(spark, run_id, 'silver', 'silver_rental',
                'pk_uniqueness', 'PASS' if dup_count == 0 else 'FAIL',
                observed_value=dup_count, threshold=0)

# 2. Null rate on critical columns
null_rate = spark.sql(f"""
    SELECT COUNT(*) FILTER (WHERE customer_id IS NULL) * 1.0 / COUNT(*)
    FROM silver.silver_rental
""").collect()[0][0]
write_dq_result(spark, run_id, 'silver', 'silver_rental',
                'customer_id_null_rate', 'PASS' if null_rate == 0 else 'FAIL',
                observed_value=null_rate, threshold=0)

# 3. Payment amount range (NUMERIC decode correctness)
# dvdrental payments are between $0.99 and $11.99
out_of_range = spark.sql(f"""
    SELECT COUNT(*) FROM silver.silver_payment
    WHERE amount < 0.99 OR amount > 11.99
""").collect()[0][0]
write_dq_result(spark, run_id, 'silver', 'silver_payment',
                'amount_range', 'PASS' if out_of_range == 0 else 'FAIL',
                observed_value=out_of_range, threshold=0,
                message='Expected 0.99–11.99 (dvdrental range)')

# 4. Block job on FAIL for critical checks (PK, null PK)
if dup_count > 0:
    raise Exception(f"DQ CRITICAL: PK violation in silver_rental ({dup_count} duplicates)")
```

Also create stored SQL checks in `dq_queries/silver/`:

`dq_queries/silver/check_rental_pk.sql`:
```sql
SELECT COUNT(*) - COUNT(DISTINCT rental_id) AS duplicate_count
FROM silver.silver_rental
-- PASS if result = 0
```

`dq_queries/silver/check_payment_amount.sql`:
```sql
SELECT COUNT(*) AS out_of_range_count
FROM silver.silver_payment
WHERE amount < 0.99 OR amount > 11.99
-- PASS if result = 0
```

`dq_queries/silver/check_rental_fk_customer.sql`:
```sql
SELECT COUNT(*) AS orphaned_rentals
FROM silver.silver_rental r
LEFT JOIN silver.silver_customer c ON r.customer_id = c.customer_id
WHERE c.customer_id IS NULL
-- PASS if result = 0 (WARN acceptable — customer may arrive next batch)
```

**Files to modify:**
- `notebooks/silver/NB_process_to_silver.ipynb`
- `notebooks/silver/NB_process_payment_silver.ipynb`
- `notebooks/silver/NB_process_products_silver.ipynb`

**Files to create:**
- `dq_queries/silver/check_rental_pk.sql`
- `dq_queries/silver/check_payment_amount.sql`
- `dq_queries/silver/check_rental_fk_customer.sql`
- `dq_queries/silver/check_customer_pk.sql`
- `dq_queries/silver/check_film_pk.sql`

**Acceptance criteria:**
- monitoring.dq_results has rows after every Silver run
- PK violation test: manually insert duplicate, verify FAIL row + job exception
- Payment amount test: inject out-of-range value, verify FAIL row
- PASS rows visible for clean runs

---

### TASK 2.5 — Erasure request + registry tables (GDPR)
**Type:** new tables + DDL
**Effort:** 0.5 day

Add erasure management tables. DDL in `notebooks/helpers/NB_catalog_helpers.ipynb`:

```sql
CREATE TABLE IF NOT EXISTS monitoring.erasure_requests (
  request_id    STRING,
  subject_id    STRING,
  subject_type  STRING,
  requested_at  TIMESTAMP,
  status        STRING,   -- pending | suppressed | shredded | complete
  requester     STRING,
  notes         STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS monitoring.erasure_registry (
  subject_id    STRING,
  subject_type  STRING,
  suppressed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS monitoring.erasure_audit_log (
  request_id    STRING,
  step          STRING,   -- suppressed | dek_deleted | bronze_deleted | vacuum_scheduled | complete
  completed_at  TIMESTAMP,
  evidence      STRING,
  operator      STRING
) USING DELTA;
```

**Files to modify:**
- `notebooks/helpers/NB_catalog_helpers.ipynb`

**Acceptance criteria:**
- All three tables created and queryable
- Can manually INSERT a test erasure request and read it back

---

## Phase 3 — Vault + Gold + Erasure pipeline (weeks 6–9)
### Goal: full erasure cycle working end-to-end, vault DQ live, Gold certified.

---

### TASK 3.1 — Vault integrity checks
**Type:** modify existing vault notebooks
**Effort:** 2 days

Add DQ assertion block at the end of each vault notebook. Write results to
monitoring.dq_results. Also populate monitoring.vault_load_log (Roadmap item #6).

Create `monitoring.vault_load_log`:
```sql
CREATE TABLE IF NOT EXISTS monitoring.vault_load_log (
  run_id          STRING,
  notebook        STRING,
  vault_object    STRING,
  rows_inserted   LONG,
  pk_unique       BOOLEAN,
  orphan_count    LONG,
  status          STRING,
  load_date       TIMESTAMP
) USING DELTA;
```

Standard vault DQ checks (implement in each vault notebook):

For hub notebooks (`NB_ingest_to_hubs.ipynb`):
```python
# Hub HK uniqueness — should always be 0 (insert-only MERGE)
for hub in hub_configs:
    dup_hk = spark.sql(f"""
        SELECT COUNT(*) - COUNT(DISTINCT {hub.hk_col}) FROM vault.{hub.table}
    """).collect()[0][0]
    write_dq_result(spark, run_id, 'vault', hub.table,
                    'hub_hk_unique', 'PASS' if dup_hk == 0 else 'FAIL',
                    observed_value=dup_hk, threshold=0)
```

For link notebooks (`NB_ingest_to_links.ipynb`):
```python
# Both hub FKs must exist in their respective hub tables
for link in link_configs:
    missing_hk1 = spark.sql(f"""
        SELECT COUNT(*) FROM vault.{link.table} l
        LEFT JOIN vault.{link.hub1_table} h ON l.{link.hk1_col} = h.{link.hk1_col}
        WHERE h.{link.hk1_col} IS NULL
    """).collect()[0][0]
    write_dq_result(spark, run_id, 'vault', link.table,
                    f'fk_{link.hub1_table}_integrity',
                    'PASS' if missing_hk1 == 0 else 'FAIL',
                    observed_value=missing_hk1, threshold=0)
    if missing_hk1 > 0:
        raise Exception(f"DQ CRITICAL: orphaned link rows in {link.table}")
```

For satellite notebooks (`NB_ingest_to_satellites.ipynb`):
```python
# Every sat row must have a parent hub row
for sat in sat_configs:
    orphan_count = spark.sql(f"""
        SELECT COUNT(*) FROM vault.{sat.table} s
        LEFT JOIN vault.{sat.hub_table} h ON s.{sat.hk_col} = h.{sat.hk_col}
        WHERE h.{sat.hk_col} IS NULL
    """).collect()[0][0]
    write_dq_result(spark, run_id, 'vault', sat.table,
                    'sat_no_orphans',
                    'PASS' if orphan_count == 0 else 'FAIL',
                    observed_value=orphan_count, threshold=0)
```

**Files to modify:**
- `notebooks/vault/NB_ingest_to_hubs.ipynb`
- `notebooks/vault/NB_ingest_to_links.ipynb`
- `notebooks/vault/NB_ingest_to_satellites.ipynb`
- `notebooks/vault/NB_dv_business_vault.ipynb`
- `notebooks/helpers/NB_catalog_helpers.ipynb` (add vault_load_log DDL)

**Acceptance criteria:**
- monitoring.vault_load_log has rows after every vault run
- monitoring.dq_results has vault-layer rows after every vault run
- Orphan detection test: manually insert orphaned sat row, verify FAIL + exception

---

### TASK 3.2 — Gold dbt test expansion
**Type:** modify dbt project
**Effort:** 2 days

Add `dbt-expectations` package and expand test coverage to full certification
standard. Update `cdc_gold/packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
```

Add to `cdc_gold/models/gold_rental.yml`:
```yaml
models:
  - name: gold_rental
    columns:
      - name: rental_id
        tests: [not_null, unique]
      - name: customer_id
        tests: [not_null, relationships: {to: ref('gold_customer'), field: customer_id}]
      - name: rental_date
        tests: [not_null, dbt_expectations.expect_column_values_to_be_between:
                  {min_value: "cast('2005-01-01' as date)", max_value: "current_date()"}]
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
```

Add `cdc_gold/models/gold_film.yml` with equivalent coverage.

Add business rule test — total payment amount must match Silver sum:
`cdc_gold/tests/assert_gold_payment_totals_match_silver.sql`:
```sql
-- Test fails if Gold and Silver payment totals diverge by more than 0.01
SELECT ABS(gold_total - silver_total) AS discrepancy
FROM (
  SELECT SUM(total_payments) AS gold_total FROM {{ ref('gold_rental') }}
) g,
(
  SELECT SUM(amount) AS silver_total FROM {{ source('silver', 'silver_payment') }}
) s
WHERE ABS(gold_total - silver_total) > 0.01
```

Add `on-run-end` hook to emit dbt test results into monitoring.dq_results.
In `cdc_gold/dbt_project.yml`:
```yaml
on-run-end:
  - "{{ write_dq_results_from_run(results) }}"
```

Create macro `cdc_gold/macros/write_dq_results.sql` that reads
`{{ results }}` and inserts PASS/FAIL rows into monitoring.dq_results.

**Files to modify:**
- `cdc_gold/packages.yml`
- `cdc_gold/dbt_project.yml`
- `cdc_gold/models/gold_rental.yml`
- `cdc_gold/models/gold_film.yml`

**Files to create:**
- `cdc_gold/tests/assert_gold_payment_totals_match_silver.sql`
- `cdc_gold/macros/write_dq_results.sql`

**Acceptance criteria:**
- `dbt test` passes clean on dvdrental data
- monitoring.dq_results has gold-layer rows after `dbt build`
- Business rule test catches a manually introduced payment total discrepancy

---

### TASK 3.3 — NB_process_erasure notebook (GDPR)
**Type:** new notebook
**Effort:** 3 days (most complex task in programme)

Create `notebooks/helpers/NB_process_erasure.ipynb`.

This notebook processes one erasure request end-to-end. Parameters:
- `REQUEST_ID` — from monitoring.erasure_requests
- `DRY_RUN`    — default true; set false to execute destructively

Step 1 — Load and validate request:
```python
request = spark.sql(f"""
    SELECT * FROM monitoring.erasure_requests
    WHERE request_id = '{request_id}' AND status = 'pending'
""").first()
if request is None:
    raise Exception(f"No pending request found: {request_id}")
subject_id   = request.subject_id
subject_type = request.subject_type
```

Step 2 — Immediate Gold suppression (synchronous, fast):
```python
if not dry_run:
    spark.sql(f"""
        INSERT INTO monitoring.erasure_registry
        VALUES ('{subject_id}', '{subject_type}', current_timestamp())
    """)
    spark.sql(f"""
        UPDATE monitoring.erasure_requests
        SET status = 'suppressed'
        WHERE request_id = '{request_id}'
    """)
    log_step(request_id, 'suppressed', f"Gold views now exclude {subject_id}")
```

Step 3 — Delete DEK from subject_key_store (crypto-shredding):
```python
if not dry_run:
    shred_subject(subject_id, subject_type)  # from NB_key_management_helpers
    spark.sql(f"""
        UPDATE monitoring.subject_key_store
        SET shredded_at = current_timestamp()
        WHERE subject_id = '{subject_id}'
    """)
    log_step(request_id, 'dek_deleted',
             f"DEK for {subject_id} deleted from key store and key vault")
```

Step 4 — Physical DELETE from Bronze (belt + braces):
```python
if not dry_run:
    bronze_tables_with_subject = ['bronze.customer', 'bronze.rental', ...]
    for table in bronze_tables_with_subject:
        spark.sql(f"""
            DELETE FROM {table}
            WHERE get_json_object(value, '$.after.customer_id') = '{subject_id}'
               OR get_json_object(value, '$.before.customer_id') = '{subject_id}'
        """)
    log_step(request_id, 'bronze_deleted',
             f"Bronze rows deleted for {subject_id}")
```

Step 5 — Post-shred validation (write to dq_results):
```python
# Attempt to read + decrypt a satellite row for this subject
# Should return '[ERASED]' for all PII columns
test_row = spark.sql(f"""
    SELECT hk_customer FROM vault.hub_customer
    WHERE hk_customer = sha2(upper(trim('{subject_id}')), 256)
""").first()

if test_row:
    sat_row = spark.sql(f"""
        SELECT email FROM vault.sat_customer
        WHERE hk_customer = '{test_row.hk_customer}'
        LIMIT 1
    """).first()
    # Try to decrypt — should fail (return '[ERASED]')
    dek = get_or_create_dek(subject_id, subject_type)  # should return None after shred
    shred_verified = (dek is None)
    write_dq_result(spark, request_id, 'gdpr', 'vault.sat_customer',
                    'post_shred_unreadable',
                    'PASS' if shred_verified else 'FAIL',
                    message=f"subject {subject_id} shred verified: {shred_verified}")
```

Step 6 — Complete request + audit log:
```python
log_step(request_id, 'complete',
         f"Erasure complete for {subject_id} / {subject_type}")
spark.sql(f"""
    UPDATE monitoring.erasure_requests
    SET status = 'complete'
    WHERE request_id = '{request_id}'
""")
```

Helper `log_step()`:
```python
def log_step(request_id, step, evidence):
    spark.sql(f"""
        INSERT INTO monitoring.erasure_audit_log
        VALUES ('{request_id}', '{step}', current_timestamp(), '{evidence}',
                current_user())
    """)
```

**Files to create:**
- `notebooks/helpers/NB_process_erasure.ipynb`

**Files to modify:**
- `Orders-ingest-job.yaml` — add `process_erasure_requests` task (runs daily,
  processes all pending requests)

**Acceptance criteria:**
- DRY_RUN=true prints full plan without writing anything
- DRY_RUN=false completes all 6 steps for a test subject
- erasure_audit_log has one row per step
- Post-shred validation writes PASS to dq_results
- Gold view no longer returns the erased subject after suppression step
- decrypt_value() returns '[ERASED]' for all PII columns after DEK deletion

---

### TASK 3.4 — Gold suppression views (GDPR)
**Type:** modify dbt Gold models
**Effort:** 1 day

Wrap every Gold model that surfaces PII with an `erasure_registry` anti-join.
This ensures erased subjects never appear in Gold outputs, even before the
async shredding is complete.

Add dbt source for erasure_registry in `cdc_gold/models/sources.yml`:
```yaml
sources:
  - name: monitoring
    tables:
      - name: erasure_registry
```

Add macro `cdc_gold/macros/suppress_erased_subjects.sql`:
```sql
{% macro suppress_erased_subjects(subject_id_col, subject_type) %}
  {{ subject_id_col }} NOT IN (
    SELECT subject_id FROM {{ source('monitoring', 'erasure_registry') }}
    WHERE subject_type = '{{ subject_type }}'
  )
{% endmacro %}
```

Apply in `gold_rental.sql` WHERE clause:
```sql
WHERE {{ suppress_erased_subjects('r.customer_id', 'customer') }}
```

Apply in `gold_film.sql` — no PII, no suppression needed.
Apply in future `gold_customer.sql` — will need suppression.

**Files to modify:**
- `cdc_gold/models/gold_rental.sql`
- `cdc_gold/models/sources.yml`

**Files to create:**
- `cdc_gold/macros/suppress_erased_subjects.sql`

**Acceptance criteria:**
- After inserting a test row into erasure_registry, `dbt build` Gold model
  excludes that subject from output
- dbt test still passes (suppression does not cause uniqueness failures)

---

## Phase 4 — Visibility + operationalisation (weeks 10–12)
### Goal: dashboards live, alerts configured, runbooks documented.

---

### TASK 4.1 — DQ monitoring dashboard
**Type:** Databricks SQL dashboard
**Effort:** 1 day

Create a Databricks SQL dashboard from monitoring.dq_results.

Required queries:

Query 1 — Pass rate by layer (last 30 runs):
```sql
SELECT layer,
       COUNT(*) FILTER (WHERE status = 'PASS') * 100.0 / COUNT(*) AS pass_rate_pct,
       MAX(checked_at) AS last_checked
FROM monitoring.dq_results
WHERE checked_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY layer
ORDER BY layer
```

Query 2 — All failures in last run:
```sql
SELECT layer, table_name, check_name, status,
       observed_value, threshold, message, checked_at
FROM monitoring.dq_results
WHERE status IN ('FAIL', 'WARN')
  AND checked_at >= (
    SELECT MAX(checked_at) - INTERVAL 1 HOUR FROM monitoring.dq_results
  )
ORDER BY layer, table_name
```

Query 3 — Volume anomaly trend (Bronze):
```sql
SELECT DATE(checked_at) AS run_date, observed_value AS row_count
FROM monitoring.dq_results
WHERE check_name = 'bronze_volume'
ORDER BY run_date DESC
LIMIT 30
```

Export dashboard definition to `design/dashboards/dq_dashboard.json`.

**Files to create:**
- `design/dashboards/dq_dashboard.json`

**Acceptance criteria:**
- Dashboard loads without error after a full pipeline run
- All three queries return data
- Pass rate visible per layer

---

### TASK 4.2 — GDPR SLA dashboard
**Type:** Databricks SQL dashboard
**Effort:** 0.5 day

Create a second dashboard from monitoring.erasure_requests + erasure_audit_log.

Required queries:

Query 1 — Open requests by age:
```sql
SELECT request_id, subject_type, requested_at,
       DATEDIFF(current_date(), DATE(requested_at)) AS age_days,
       status,
       CASE WHEN DATEDIFF(current_date(), DATE(requested_at)) > 25
            THEN 'AT RISK' ELSE 'OK' END AS sla_status
FROM monitoring.erasure_requests
WHERE status != 'complete'
ORDER BY age_days DESC
```

Query 2 — Completed erasures last 30 days:
```sql
SELECT DATE(completed_at) AS completion_date, COUNT(*) AS completed_count
FROM monitoring.erasure_audit_log
WHERE step = 'complete'
  AND completed_at >= current_date() - INTERVAL 30 DAYS
GROUP BY DATE(completed_at)
ORDER BY completion_date DESC
```

Export to `design/dashboards/gdpr_dashboard.json`.

**Files to create:**
- `design/dashboards/gdpr_dashboard.json`

**Acceptance criteria:**
- Dashboard shows AT RISK flag for any request older than 25 days
- Completed erasures chart populated after running NB_process_erasure

---

### TASK 4.3 — Slack alerting
**Type:** extend existing schema_drift_helpers pattern
**Effort:** 1 day

Extend `notebooks/helpers/NB_schema_drift_helpers.ipynb` to add two new
alert types reusing the existing webhook channel infrastructure:

Alert type 1 — DQ critical failure:
```python
def alert_dq_failure(check_name, table_name, layer, observed_value,
                     threshold, message, webhook_url):
    payload = {
        "text": f":x: *DQ FAILURE* — `{layer}.{table_name}`\n"
                f"Check: `{check_name}`\n"
                f"Observed: `{observed_value}` (threshold: `{threshold}`)\n"
                f"{message}"
    }
    send_webhook(payload, webhook_url)
```

Alert type 2 — GDPR SLA at risk:
```python
def alert_erasure_sla_risk(request_id, subject_type, age_days, webhook_url):
    payload = {
        "text": f":warning: *GDPR SLA AT RISK* — request `{request_id}`\n"
                f"Subject type: `{subject_type}`\n"
                f"Age: *{age_days} days* (deadline: 30 days)\n"
                f"Check monitoring.erasure_requests immediately."
    }
    send_webhook(payload, webhook_url)
```

Add SLA check task to `Orders-ingest-job.yaml` (daily):
```python
# Read all open requests older than 25 days and alert
at_risk = spark.sql("""
    SELECT * FROM monitoring.erasure_requests
    WHERE status != 'complete'
      AND DATEDIFF(current_date(), DATE(requested_at)) > 25
""").collect()
for req in at_risk:
    alert_erasure_sla_risk(req.request_id, req.subject_type,
                           req.age_days, webhook_url)
```

Call `alert_dq_failure()` from Silver and Vault notebooks when a FAIL is
written to dq_results (pass WEBHOOK_URL as notebook parameter).

**Files to modify:**
- `notebooks/helpers/NB_schema_drift_helpers.ipynb`
- `Orders-ingest-job.yaml`
- Silver notebooks (call alert on FAIL)
- Vault notebooks (call alert on FAIL)

**Acceptance criteria:**
- Slack message received when Silver PK violation is injected
- Slack message received when erasure request is manually backdated to 26 days ago
- No alert fires on clean run

---

### TASK 4.4 — Row count anomaly detection
**Type:** new utility function + Bronze notebook modification
**Effort:** 1 day

Add rolling average anomaly detection for Bronze message volume.
This is the highest-leverage check for the "no visibility until consumer
complains" problem — a Debezium disconnect shows up here within one run.

```python
def check_volume_anomaly(spark, run_id, table_name, current_count,
                         lookback_days=7, threshold_pct=0.5):
    """
    Compare current_count to rolling average over last lookback_days.
    Write WARN if < threshold_pct of average.
    Write FAIL if current_count = 0 and average > 0.
    """
    avg_result = spark.sql(f"""
        SELECT AVG(observed_value) AS rolling_avg
        FROM monitoring.dq_results
        WHERE check_name = 'bronze_volume'
          AND table_name = '{table_name}'
          AND checked_at >= current_timestamp() - INTERVAL {lookback_days} DAYS
          AND status != 'FAIL'
    """).first()

    rolling_avg = avg_result.rolling_avg or 0

    if rolling_avg == 0:
        status = 'PASS'  # no baseline yet
    elif current_count == 0:
        status = 'FAIL'
    elif current_count < rolling_avg * threshold_pct:
        status = 'WARN'
    else:
        status = 'PASS'

    write_dq_result(spark, run_id, 'bronze', table_name, 'bronze_volume',
                    status, observed_value=current_count,
                    threshold=rolling_avg * threshold_pct,
                    message=f"rolling_avg={rolling_avg:.0f}")
    return status
```

Call from `NB_ingest_to_bronze.ipynb` at end of each micro-batch, once per
topic table.

**Files to modify:**
- `notebooks/helpers/NB_catalog_helpers.ipynb` (add check_volume_anomaly)
- `notebooks/bronze/NB_ingest_to_bronze.ipynb` (call per micro-batch)

**Acceptance criteria:**
- After 7 days of baseline, stopping the generator triggers WARN within one run
- Restarting generator returns to PASS within one run
- dq_results bronze_volume rows show observed and threshold values

---

### TASK 4.5 — Job YAML integration + runbooks
**Type:** YAML update + markdown documentation
**Effort:** 1 day

Update `Orders-ingest-job.yaml` to include all new tasks in the correct order:

New tasks to add (after existing Bronze → Silver → Vault → Gold DAG):
- `run_silver_dq`        — after all Silver tasks, runs dq_queries/silver/ SQL
- `run_vault_dq`         — after all Vault tasks, reads vault_load_log
- `process_erasure_requests` — daily, processes pending requests
- `check_erasure_sla`    — daily, alerts on at-risk requests
- `vacuum_bronze_tables` — daily, enforces 30-day TTL

Create `design/runbooks/DQ_INCIDENT_RUNBOOK.md`:
```markdown
# DQ incident response runbook

## Symptoms
- Slack alert: "DQ FAILURE — silver.silver_rental / pk_uniqueness"
- Dashboard shows FAIL row in monitoring.dq_results

## Investigation steps
1. Query monitoring.dq_results for the failing check and run_id
2. Check bronze.quarantine for rejected messages in the same run window
3. Run notebooks/helpers/NB_schema_drift_helpers manually to compare schemas
4. Check Kafka UI for topic lag or message format changes
5. If duplicate keys: check source PostgreSQL for replication slot issues

## Resolution
- Duplicate keys: rerun NB_reset_tables for affected table, then reprocess
- Schema change: update NB_schema_contracts, re-run with new policy
- Generator crash: restart generator, verify volume returns to baseline
```

Create `design/runbooks/ERASURE_SOP.md`:
```markdown
# Erasure standard operating procedure

## On receiving a right-to-erasure request
1. INSERT into monitoring.erasure_requests with status='pending'
2. Run NB_process_erasure with DRY_RUN=true, review output
3. Run NB_process_erasure with DRY_RUN=false
4. Verify erasure_audit_log has all 6 steps completed
5. Verify post_shred_unreadable = PASS in monitoring.dq_results
6. Confirm to requester within 30 days of request date
7. Retain erasure_audit_log row permanently (legal evidence)

## SLA
- Suppression (Gold hidden): within 1 job run (~5 minutes)
- Crypto-shred complete: within 24 hours
- Legal confirmation to subject: within 30 days

## Escalation
- SLA at risk (>25 days): Slack alert fires automatically
- Key vault unavailable: escalate to platform team, do not proceed
```

**Files to modify:**
- `Orders-ingest-job.yaml`

**Files to create:**
- `design/runbooks/DQ_INCIDENT_RUNBOOK.md`
- `design/runbooks/ERASURE_SOP.md`

**Acceptance criteria:**
- Full job DAG runs end-to-end with all new tasks
- Runbooks reviewed and approved by one other team member
- Both dashboards accessible after a full job run

---

## New files summary

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
cdc_gold/tests/assert_gold_payment_totals_match_silver.sql
cdc_gold/macros/write_dq_results.sql
cdc_gold/macros/suppress_erased_subjects.sql
design/dashboards/dq_dashboard.json
design/dashboards/gdpr_dashboard.json
design/runbooks/DQ_INCIDENT_RUNBOOK.md
design/runbooks/ERASURE_SOP.md
```

## Modified files summary

```
notebooks/helpers/NB_catalog_helpers.ipynb
notebooks/helpers/NB_schema_drift_helpers.ipynb
notebooks/bronze/NB_ingest_to_bronze.ipynb
notebooks/silver/NB_process_to_silver.ipynb
notebooks/silver/NB_process_products_silver.ipynb
notebooks/silver/NB_process_payment_silver.ipynb
notebooks/vault/NB_ingest_to_hubs.ipynb
notebooks/vault/NB_ingest_to_links.ipynb
notebooks/vault/NB_ingest_to_satellites.ipynb
notebooks/vault/NB_dv_business_vault.ipynb
cdc_gold/packages.yml
cdc_gold/dbt_project.yml
cdc_gold/models/gold_rental.sql
cdc_gold/models/gold_rental.yml
cdc_gold/models/gold_film.yml
cdc_gold/models/sources.yml
Orders-ingest-job.yaml
```

## New monitoring tables summary

```
monitoring.pii_column_registry
monitoring.dq_results
monitoring.subject_key_store
monitoring.erasure_requests
monitoring.erasure_registry
monitoring.erasure_audit_log
monitoring.vault_load_log
bronze.quarantine
```

---
_Generated from design sessions in claude.ai project databricks-lab._
_Implement phase by phase. Do not start Phase 2 until Phase 1 acceptance criteria are met._
