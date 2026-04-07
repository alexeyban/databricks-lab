# DQ Incident Runbook

**Repo:** databricks-lab  
**Layer coverage:** Bronze → Silver → Vault → Gold

---

## 1. Triage — identify the failing check

```sql
-- Recent DQ failures (last 24 hours)
SELECT run_id, layer, table_name, check_name, status, observed_value, message, checked_at
FROM workspace.monitoring.dq_results
WHERE status IN ('FAIL', 'WARN')
  AND checked_at >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY checked_at DESC;
```

```sql
-- Failure trend by check (last 7 days)
SELECT check_name, table_name, layer, COUNT(*) AS failures
FROM workspace.monitoring.dq_results
WHERE status = 'FAIL'
  AND checked_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1, 2, 3
ORDER BY failures DESC;
```

---

## 2. Bronze — quarantine review

```sql
-- Recent quarantined events
SELECT quarantine_reason, table_name, COUNT(*) AS n, MAX(quarantined_at) AS last_seen
FROM workspace.bronze.quarantine
WHERE quarantined_at >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1, 2
ORDER BY n DESC;

-- Sample bad messages
SELECT * FROM workspace.bronze.quarantine
WHERE quarantined_at >= current_timestamp() - INTERVAL 1 HOUR
LIMIT 10;
```

**json_parse_error**: connector misconfiguration or schema change upstream.  
**invalid_op**: Debezium op field outside {r,c,u,d} — check connector version.  
**stale_event**: events older than 24h — check Kafka lag or connector restart.  
**null_value**: empty Kafka message — investigate producer.

**Volume anomaly (WARN/FAIL on `batch_row_count`):**
- WARN: batch count < 50% of 7-day average — check if generator is running.
- FAIL: zero rows — Kafka topic empty or connector stopped.

```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-connector/status
```

---

## 3. Silver — PK uniqueness failure

**`pk_uniqueness` FAIL** means MERGE produced duplicate PKs — this should not happen.

1. Check if the Bronze source has duplicate offsets:
   ```sql
   SELECT table_name, COUNT(*) AS n FROM workspace.bronze.<table>
   WHERE kafka_timestamp >= current_timestamp() - INTERVAL 1 HOUR
   GROUP BY table_name HAVING COUNT(*) > 1;
   ```
2. Check deduplication config in `pipeline_configs/silver/dvdrental/<TABLE_ID>.json`  
   — `dedupe_order_columns` and `primary_keys` must be correct.
3. If genuine duplicates exist in Silver, run a manual dedup:
   ```sql
   -- Preview
   WITH ranked AS (
     SELECT *, ROW_NUMBER() OVER (PARTITION BY rental_id ORDER BY last_updated_dt DESC) AS rn
     FROM workspace.silver.silver_rental
   )
   SELECT * FROM ranked WHERE rn > 1;
   ```

**`payment_amount_range` FAIL**: payments outside [0.99, 11.99].
- May indicate a schema/decimal decoding issue — check `decimal_from_debezium_bytes` transform.
- Check raw Bronze value: `get_json_object(value, '$.after.amount')`.

---

## 4. Vault — integrity failures

**`hk_uniqueness` FAIL** (Hubs): hash key collision — check business key columns in `dv_model.json`.

**`fk_present` FAIL** (Links): null hub FK — hub was not loaded before link.  
Re-run `dvdrental-vault` job; hubs task must complete before links.

**`no_orphan_rows` FAIL** (Satellites): satellite HK not in parent hub.  
Run hub ingestion again, then re-check. If persistent, investigate `hub_key_source_column` config.

---

## 5. Gold — dbt test failures

```bash
cd cdc_gold
dbt test --select gold_rental gold_film
```

Check `assert_gold_payment_totals_match_silver.sql` output:
```sql
-- discrepancies > $0.01
SELECT * FROM workspace.gold.gold_rental g
JOIN workspace.silver.silver_payment p USING (rental_id)
-- ...
```

---

## 6. Escalation

| Severity | Who | SLA |
|----------|-----|-----|
| Bronze quarantine > 1% of events | Data Engineering | 4 hours |
| Silver PK uniqueness FAIL | Data Engineering | 1 hour |
| Vault HK uniqueness FAIL | Data Engineering + Data Architecture | 2 hours |
| Gold totals mismatch > $1 | Data Engineering + Finance | 2 hours |

---

## 7. Resolution checklist

- [ ] Root cause identified and documented
- [ ] Fix deployed and verified (re-run job task)
- [ ] `dq_results` shows PASS for the previously-failing check
- [ ] Downstream consumers notified if data was incorrect
- [ ] Post-mortem created if severity was CRITICAL
