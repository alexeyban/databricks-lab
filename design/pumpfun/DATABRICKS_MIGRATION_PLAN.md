# pump.fun Risk-Scoring System — Migration off Databricks

**Status (2026-09-15): Draft plan, not started.** Triggered by the project
exceeding Databricks' free tier — paid tier runs ~$50/day (~$1,500/month).
Scope: the memecoin scoring subsystem only (`ingestion/pumpfun/`,
`pumpapi-lakehouse/`, `processing/gold/NB_process_pump_token_risk.ipynb`,
`workspace.bronze/silver/gold.pump_*` tables) — the dvdrental CDC lab is
out of scope and assumed to stay on Databricks (or be migrated separately
later) unless decided otherwise. If dvdrental stays on Databricks in the
same workspace, note that the $50/day figure is a *combined* bill, not
necessarily 100% attributable to pump.fun alone — see "Cost estimate"
caveats.

---

## 1. Current architecture inventory

| Layer | Component | Databricks-specific? |
|---|---|---|
| Ingestion | `ingestion/pumpfun/app/` — Python websocket client, systemd/Docker | No — already self-hosted. Only its **upload target** (Databricks Files API → a Unity Catalog Volume) is Databricks-specific. |
| Landing | `/Volumes/workspace/default/mnt/pumpapi` (Unity Catalog Volume) | Yes |
| Bronze+Silver pipeline | `pumpapi-lakehouse/transformations/*.py` — Lakeflow Declarative Pipeline (`pyspark.pipelines`, formerly `dlt`) | Yes — the declarative `@dp.table`/`@dp.expect` framework is Databricks-only. The underlying engine (Spark Structured Streaming) is not. |
| Gold scoring | `processing/gold/NB_process_pump_token_risk.ipynb` — plain PySpark Structured Streaming, `foreachBatch` + Delta `MERGE` (`.withSchemaEvolution()`) | **Mostly not.** This was already written as a plain notebook specifically because Lakeflow's declarative framework couldn't express incremental per-mint MERGE — it's the component closest to portable as-is. |
| Table format | Delta Lake (`USING DELTA`) | **No — Delta Lake itself is Linux Foundation open source** (`delta-spark` package). Databricks is one engine that reads/writes it, not a requirement. |
| Catalog/governance | Unity Catalog (3-level `catalog.schema.table`, managed Volumes, permissions) | Yes |
| Compute | Databricks Serverless (Jobs compute + SQL Warehouse), billed per DBU | Yes |
| Orchestration | Databricks Jobs (`orchestration/bundle/databricks.yml`, Databricks Asset Bundles, cron schedule, `depends_on` task chain) | Yes |
| Checkpoints | Unity Catalog Volume path (`/Volumes/workspace/default/mnt/checkpoints/gold_pump_token_risk`) | Yes (just a storage path — the checkpoint *format* is plain Spark Structured Streaming, portable) |
| Ad-hoc SQL (used throughout this session's investigations) | Databricks SQL Warehouse (`/api/2.0/sql/statements`) | Yes |
| Secrets | `.env` (Databricks host/token) | No — already outside Databricks |

**Measured current data volumes** (via `DESCRIBE DETAIL`, 2026-09-15):

| Table | Size | Files |
|---|---|---|
| `bronze.pump_events_raw` | 55.2 GB | 1,091 |
| `silver.pump_events` | 86.7 GB | 601 |
| `silver.pump_trades` | 30.7 GB | 203 |
| `silver.pump_transfers` | 13.7 GB | 181 |
| `silver.pump_tokens` | 0.02 GB | 18 |
| `silver.pump_pools` | 0.09 GB | 19 |
| `gold.gold_pump_token_risk` | 0.01 GB | 2 |
| **Total** | **~186 GB** | |

This is a modest scale — well within what a single well-specced VM (or a
small 2-3 node cluster) handles comfortably, *not* big-data-cluster
territory. Most of the current volume reflects a one-time historical
backlog catch-up (the producer had gaps; per `PUMPFUN_PIPELINE.md`'s
landing-volume listing, real data spans roughly 2026-09-03 to present with
missing days in between) rather than steady-state daily growth — plan
storage projections conservatively (see §4) rather than extrapolating
today's total linearly.

## 2. Proposed open-source stack

| Component | Recommendation | Why |
|---|---|---|
| Object storage | Cloud object storage (S3 / GCS / Cloudflare R2), **not** self-hosted MinIO | At ~186 GB, storage cost is trivial (~$4-5/month on S3 Standard) regardless of provider. Self-hosting MinIO adds real operational burden (replication, disk failure handling) for no meaningful cost benefit at this scale. R2 specifically has zero egress fees, worth a look if querying from outside the storage region. |
| Table format | **Delta Lake, unchanged** (`delta-spark`) | Already the format in use; fully open source; no reason to pay a migration cost switching to Iceberg/Hudi unless multi-engine interop becomes a hard requirement later. |
| Catalog | **Unity Catalog OSS** (Databricks open-sourced it under Apache 2.0 in June 2024) | Closest match to what's already in use — same 3-level namespace, Volumes concept, permission model — minimizes rework in table/path references throughout `pumpapi-lakehouse/` and the gold notebook. Backed by Postgres for metadata. Hive Metastore is the fallback if UC OSS proves immature for self-hosting. |
| Compute engine | Self-hosted **Apache Spark** (Standalone mode to start; Spark-on-Kubernetes if/when multi-tenant scaling is needed) | Bronze/Silver/Gold logic is already PySpark — this is the smallest-diff path. Structured Streaming (the engine under Lakeflow's declarative layer) is fully open source. |
| Bronze+Silver pipeline framework | Plain PySpark Structured Streaming jobs (`spark.readStream...writeStream.trigger(availableNow=True)`), replacing `@dp.table`/`@dp.expect` | The declarative decorators go away; DQ checks (`valid_mint`, `valid_signature`, etc.) get reimplemented as explicit post-write counts, reusing the **existing** `monitoring.dq_results` + `write_dq_result()` pattern already built for dvdrental (`processing/common/NB_catalog_helpers.ipynb`) — no new DQ framework needed. |
| Gold scoring | **Unchanged in substance** — `processing/gold/NB_process_pump_token_risk.ipynb` already plain Structured Streaming + Delta MERGE, no Databricks-only API beyond the Spark Connect `batch_df.sparkSession` pattern (simplifies back to a plain `spark` session reference in classic Spark) and the landing/checkpoint paths. |
| Orchestration | **Dagster** | The medallion bronze→silver→gold dependency chain maps naturally onto Dagster's asset model (vs. Airflow's more task-centric DAGs); built-in scheduling replaces the `quartz_cron_expression` + `depends_on` chain in `databricks.yml` with comparable retry/observability tooling out of the box. Airflow is the safer/more battle-tested fallback if the team already knows it better than Dagster. |
| Ad-hoc SQL / dashboards | **DuckDB** (reads Delta tables directly via the `delta` extension) for investigative queries (the kind run throughout this session); **Trino** only if concurrent multi-user BI-tool access becomes a real requirement | At ~186 GB total and single-analyst usage patterns so far, DuckDB is dramatically simpler to operate than standing up a Trino cluster, and costs nothing extra to run. |
| Secrets | `.env` / SOPS-encrypted files, or HashiCorp Vault if the broader team already runs one | No change forced by this migration; Databricks secret scopes were never used by this subsystem. |

## 3. Migration plan by phase

**Phase 0 — Infra bring-up (no pump.fun changes yet)**
- Provision compute (see §4 for sizing options), object storage bucket, Unity Catalog OSS (+ its Postgres backing store), Dagster.
- Stand up Spark Standalone (or point at the target Kubernetes cluster).

**Phase 1 — Bronze+Silver: de-Lakeflow the pipeline**
- Rewrite `pumpapi-lakehouse/transformations/*.py`: drop `@dp.table`/`@dp.expect`, add explicit `spark.readStream...writeStream.format("delta").trigger(availableNow=True).start(); query.awaitTermination()` per table (bronze → 5 silver, in dependency order — same order the current DAG already encodes).
- Reimplement the `dp.expect` DQ checks (`valid_mint`, `valid_signature`, `valid_trader`, etc.) as post-write assertions writing to `monitoring.dq_results`.
- Point `SOURCE_PATH`/table names at the new catalog; repoint the producer's upload target at the new object store.

**Phase 2 — Gold: minimal-diff port**
- `NB_process_pump_token_risk.ipynb` → convert to a plain `.py` script; swap `batch_df.sparkSession` for a module-level `spark`; repoint `GOLD_TABLE`/`CHECKPOINT_PATH`. Scoring logic itself (all `_signal` functions) is untouched.

**Phase 3 — Orchestration**
- Define bronze/silver/gold as Dagster assets with the same dependency edges as today's `pumpfun-bronze` job (`run_pumpapi_lakehouse` → `run_gold_pump_token_risk`); replace the 2-minute cron with a Dagster schedule.

**Phase 4 — Cutover**
- Run both stacks in parallel for a short overlap window, diff row counts / sample `gold_pump_token_risk` scores between old and new, then point the producer and any consumers (Telegram bot, dashboards) at the new stack and decommission the Databricks jobs/pipeline for this subsystem.

**Not migrating (assumed out of scope):** dvdrental CDC lab, its Vault/Gold dbt layer, and the shared Databricks workspace those depend on.

## 4. Cost estimate

**Current baseline:** ~$50/day ≈ **$1,500/month** (Databricks paid tier).
Caveat: if dvdrental keeps running in the same workspace, some portion of
this is not attributable to pump.fun alone — get a DBU-level cost
breakdown by job/pipeline before treating the full figure as "savings."

**Self-hosted OSS, two sizing options** (workload is bursty — one
micro-batch every 2 minutes, not continuous 100% CPU, so a single
moderately-sized host is plausible for current volumes):

| | Budget (VPS provider, e.g. Hetzner/OVH) | Cloud-native (AWS, same provider as current storage) |
|---|---|---|
| Compute (8-16 vCPU / 32-64 GB, Spark + Dagster + Unity Catalog OSS + DuckDB co-located) | ~$80-150/mo (dedicated/cloud server) | ~$220-370/mo on-demand (`r5.2xlarge`-class); ~$150-220/mo with a 1-yr reserved/savings commitment |
| Object storage (~186 GB now, budget headroom to ~1-2 TB) | ~$5-20/mo (S3-compatible: Backblaze B2, Wasabi) | ~$5-45/mo (S3 Standard) |
| Metadata Postgres (Unity Catalog OSS backing store) | Co-located on the same box, ~$0 extra | ~$15-30/mo (small RDS instance) or co-located, ~$0 extra |
| **Total** | **~$100-200/month** | **~$200-450/month** |

**Rough savings**: on the order of **$1,000-1,400/month (70-90%)** vs. the
current $50/day figure, *before* accounting for:
- **Migration engineering time** (one-time cost, not in the monthly
  figures above) — Phase 1's DQ-check reimplementation and Phase 3's
  Dagster setup are the largest chunks of new work; Phase 2 is close to a
  find-and-replace.
- **Ongoing operational burden** — Databricks currently absorbs cluster
  patching, autoscaling, and much of the "keep Spark running" toil that a
  self-hosted stack shifts onto whoever operates it. This is a real,
  recurring cost even though it doesn't show up as a dollar line item.
- **Growth**: storage cost stays trivial even at 5-10x current volume;
  compute is the component to re-size if event throughput grows
  materially, but the bursty (2-minute-cadence) access pattern gives
  headroom before a bigger host or a second node is needed.

## 5. Open questions

- Is the $50/day Databricks figure pump.fun-only, or the combined
  workspace bill including dvdrental? Determines the real avoided-cost
  baseline.
- On-prem/VPS vs. staying in the current cloud provider (AWS, per the
  `s3://dbstorage-prod-bazl8/...` paths seen in error output this
  session) — affects both cost and how much of the existing
  networking/IAM setup can be reused.
- Does Unity Catalog OSS's self-hosting maturity meet the bar, or is
  Hive Metastore the safer near-term choice with a later re-evaluation?
- Team's existing familiarity with Dagster vs. Airflow — the "right"
  orchestrator is partly a function of who's on call for it.
