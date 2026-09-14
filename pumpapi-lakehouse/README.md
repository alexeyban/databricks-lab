# pumpapi-lakehouse

A [Lakeflow Declarative Pipeline](https://docs.databricks.com/aws/en/dlt/)
(the modern `pyspark.pipelines` API — successor to `import dlt`) that turns
the compressed batch envelopes landed by [`../ingestion/pumpfun`](../ingestion/pumpfun/)
into Bronze + Silver Delta tables. One pipeline, one DAG: Bronze and all
five Silver tables run together as a single triggered update.

Deployed via Databricks Asset Bundles — see the `pumpapi-lakehouse` resource
in [`../orchestration/bundle/databricks.yml`](../orchestration/bundle/databricks.yml)
and the `pumpfun-bronze` job that triggers it every 2 minutes.

```bash
cd orchestration/bundle
databricks bundle deploy -t dev
```

## Pipeline graph

```
/Volumes/workspace/default/mnt/pumpapi  (zstd+base64 batch envelopes)
  → bronze_pump_events.py     → workspace.bronze.pump_events_raw   (decode → decompress → one row per event)
      → silver_pump_events.py   → silver.pump_events    (all events, typed columns, DQ expectations)
      → silver_pump_tokens.py   → silver.pump_tokens     (action = 'create', one row per token)
      → silver_pump_transfers.py→ silver.pump_transfers  (action = 'transfer', one row per transfer, exploded)
      → silver_pump_pools.py    → silver.pump_pools      (action IN createPool/migrate/add/remove, one row per pool event)
      → silver_pump_trades.py   → silver.pump_trades     (action IN buy/sell, one row per trade, breakdown exploded)
```

## transformations/

| File | Table | Notes |
|------|-------|-------|
| `bronze_pump_events.py` | `workspace.bronze.pump_events_raw` | Auto Loader over the landing Volume; base64-decodes + zstd-decompresses each envelope's `data_b64` (via a small Python UDF — `zstandard` is installed through the pipeline's `environment.dependencies`, not a cluster library), splits the batch back into individual event lines, and keeps each event's raw JSON as the `event` column. Partitioned by `_ingested_date`. |
| `silver_pump_events.py` | `silver.pump_events` | Parses every event (regardless of `action`) against a fixed schema into typed columns; `dp.expect` DQ checks require non-null `signature`/`action`. |
| `silver_pump_tokens.py` | `silver.pump_tokens` | Filters to `action = 'create'`; one row per token launch, with initial price/market cap and mint/freeze authority (rug-pull risk signals). |
| `silver_pump_transfers.py` | `silver.pump_transfers` | Filters to `action = 'transfer'`; explodes the event's `transfers[]` array so each wallet-to-wallet transfer inside a transaction gets its own row. |
| `silver_pump_pools.py` | `silver.pump_pools` | Filters to `action IN (createPool, migrate, add, remove)`; one row per pool lifecycle event, carrying liquidity/pool-trust fields (`burnedLiquidity`, `lockedLiquidityAfterMigration`, `poolCreatedBy`, `mayhemMode`, etc.) for the risk-scoring Gold layer (`design/pumpfun/RISK_SCORING_DESIGN.md`). |
| `silver_pump_trades.py` | `silver.pump_trades` | Filters to `action IN (buy, sell)`; explodes each event's `breakdown[]` array so each individual (non-aggregated) trade gets its own row — this is what lets Gold detect bundled/sniped buys at launch. |

## Why one pipeline instead of separate Bronze/Silver jobs

This replaces an earlier two-job design (a plain Auto Loader notebook for
Bronze + a separate notebook job for Silver — both now kept for reference
under `ingestion/consumers/outdated__*` and `processing/silver/outdated__*`).
A single Lakeflow pipeline is a better fit here: Bronze and Silver share one
continuous streaming DAG, dependencies between them are expressed in code
(`spark.readStream.table(BRONZE_TABLE)`) rather than job scheduling, and
Databricks manages one cluster/update lifecycle instead of two.

## Compression compatibility

The producer (`ingestion/pumpfun/app/writer.py`) compresses each batch with
zstd and base64-encodes it before upload — see
[`../design/pumpfun/PUMPFUN_PIPELINE.md`](../design/pumpfun/PUMPFUN_PIPELINE.md)
for why. `bronze_pump_events.py` is the only file that needs to know this;
everything downstream just sees a plain `event` JSON-string column, same as
if Bronze had landed raw uncompressed JSON.
