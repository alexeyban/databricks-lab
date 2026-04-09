# GDPR Erasure Standard Operating Procedure

**Repo:** databricks-lab  
**Legal basis:** GDPR Article 17 — Right to Erasure  
**SLA:** 30 calendar days from request submission  
**Early-warning threshold:** 25 days (SLA alert fires)

---

## Overview

Erasure is implemented via **crypto-shredding**:
1. The subject's AES-256-GCM Data Encryption Key (DEK) is deleted from the secret scope.
2. All Silver PII columns for this subject become permanently unreadable (`[ERASED]`).
3. Gold views immediately exclude the subject via `erasure_registry` anti-join.
4. Bronze raw events are physically deleted.

**DEV WARNING:** DEKs are stored in a Databricks secret scope — non-production only.  
In production, replace with Azure Key Vault / AWS KMS before handling real PII.

---

## Step 0 — Receive erasure request

Log the request in `monitoring.erasure_requests`:

```python
from pyspark.sql import Row
from datetime import datetime, timezone
import uuid

spark.createDataFrame([Row(
    request_id   = str(uuid.uuid4()),
    subject_id   = "42",               # customer_id (must be integer string)
    subject_type = "silver_customer",
    requested_at = datetime.now(timezone.utc),
    status       = "pending",
    requester    = "legal@company.com",
    notes        = "Customer erasure request — ticket #1234",
)]).write.format("delta").mode("append").saveAsTable("workspace.monitoring.erasure_requests")
```

---

## Step 1 — Dry run (mandatory before production erasure)

```
Databricks → Workflows → NB_process_erasure
  Parameters:
    REQUEST_ID = <uuid from above>
    DRY_RUN    = true
    OPERATOR   = <your name>
```

Review the output — confirm:
- Subject ID is correct
- Bronze tables to be scanned are correct
- No unexpected side effects

---

## Step 2 — Execute erasure

```
Databricks → Workflows → NB_process_erasure
  Parameters:
    REQUEST_ID = <uuid>
    DRY_RUN    = false
    OPERATOR   = <your name>
```

The notebook will execute all 6 steps and log each to `erasure_audit_log`.

---

## Step 3 — Verify

```sql
-- Confirm request is complete
SELECT request_id, subject_id, status, requester
FROM workspace.monitoring.erasure_requests
WHERE request_id = '<uuid>';

-- Confirm 6 audit steps logged
SELECT step, completed_at, evidence
FROM workspace.monitoring.erasure_audit_log
WHERE request_id = '<uuid>'
ORDER BY completed_at;

-- Confirm subject is in erasure_registry (Gold suppressed)
SELECT * FROM workspace.monitoring.erasure_registry
WHERE subject_id = '42' AND subject_type = 'silver_customer';

-- Confirm DQ validation passed
SELECT check_name, status, message
FROM workspace.monitoring.dq_results
WHERE table_name = 'silver_customer'
  AND check_name = 'post_shred_unreadable'
ORDER BY checked_at DESC
LIMIT 5;
```

---

## Step 4 — Gold verification

```bash
cd cdc_gold
dbt run --select gold_rental   # erasure_registry anti-join will exclude subject
dbt test --select gold_rental
```

Confirm the erased customer_id does not appear in `gold_rental`.

---

## SLA monitoring

The `dvdrental-dq-gdpr` job runs daily at 02:00 UTC and calls `check_erasure_sla`.  
Any request older than 25 days fires a Slack/Teams alert.

```sql
-- Requests approaching SLA deadline
SELECT
    request_id,
    subject_id,
    subject_type,
    requested_at,
    status,
    datediff(current_date(), cast(requested_at as date)) AS age_days
FROM workspace.monitoring.erasure_requests
WHERE status NOT IN ('complete')
  AND datediff(current_date(), cast(requested_at as date)) > 20
ORDER BY age_days DESC;
```

---

## Key store audit

```sql
-- View all subjects with active DEKs (shredded_at IS NULL)
SELECT subject_id, subject_type, kek_version, created_at, shredded_at
FROM workspace.monitoring.subject_key_store
WHERE shredded_at IS NULL
ORDER BY created_at DESC;
```

---

## Escalation

| Condition | Action |
|-----------|--------|
| Step 5 (post-shred validation) FAIL | Do NOT mark complete. Escalate to Data Engineering immediately. Investigate secret scope. |
| Request age > 28 days | Legal team must be notified. |
| Request age = 30 days (SLA breach) | DPO notification required. File incident report. |

---

## Checklist

- [ ] Erasure request logged in `monitoring.erasure_requests` (status=pending)
- [ ] Dry run completed and reviewed
- [ ] Live erasure executed (DRY_RUN=false)
- [ ] 6 audit log rows present in `erasure_audit_log`
- [ ] `post_shred_unreadable` = PASS in `dq_results`
- [ ] Subject absent from `gold_rental` after `dbt run`
- [ ] Request status = complete
- [ ] Confirmation sent to requester
