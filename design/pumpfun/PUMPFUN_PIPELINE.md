# pump.fun Near-Real-Time Pipeline
**Repo:** databricks-lab / alexeyban

> **Status (2026-09-11): Bronze + Silver implemented as `pumpapi-lakehouse`,
> one Lakeflow Declarative Pipeline (`pyspark.pipelines`) decoding
> zstd+base64 compressed batches. Vault/Gold not started — see
> `ROADMAP.md` §9.**

---

## Why this exists

The dvdrental pipeline demonstrates CDC-based ingestion (Debezium + Kafka)
into a medallion lakehouse. pump.fun is a deliberately different source
shape: a public websocket feed with no underlying database to run CDC
against, high event volume, and a payload whose structure varies per
`action` type rather than a fixed table schema. It's a second reference
implementation in the same repo for "how do you land a near-real-time
external API feed into the same lakehouse conventions."

## Source: PumpAPI / pump.fun

[pump.fun](https://pump.fun) is a Solana token launchpad. PumpAPI
(`wss://stream.pumpapi.io/`) streams every on-chain trade, pool, and token
lifecycle event across pump.fun, pump-amm, Raydium, and Meteora pools in
real time, as one JSON event per message. Full field glossary:
`ingestion/pumpfun/docs/glossary_of_event_properties.txt`.

Key `action` values: `buy`, `sell`, `create`, `migrate`, `createPool`,
`add`, `remove`, `transfer`, `claimCashback`, `claimCreatorFees`. One
Solana transaction can emit multiple events (see `breakdown` in the
glossary), so `signature` alone is not a unique row key.

## Design decisions (locked)

1. **Producer stays a standalone service, not a Databricks job.** It must be
   always-on (websocket, reconnect/backoff) — a poor fit for Databricks
   Jobs' run-then-terminate model. Vendored into `ingestion/pumpfun/` from
   the original `pumpapi-ingestor` project so the whole pipeline lives in
   one repo; deployed via the included systemd unit.
2. **Landing format is a compressed batch envelope in a Unity Catalog
   Volume** (`/Volumes/workspace/default/mnt/pumpapi`), uploaded via the
   Databricks Files API — the same "producer writes files, Databricks Auto
   Loader reads them" pattern as the `kafka-to-volume` fallback already used
   for dvdrental Bronze, so no new connectivity story (no ngrok, no direct
   socket from Databricks) was needed. Each flushed batch (default 5000
   events) is serialized as JSONL, compressed with **zstd** (~4-5x smaller
   in practice), and base64-encoded into one JSON envelope per file:
   `{"_source", "_ingested_at", "event_count", "codec", "uncompressed_bytes",
   "data_b64"}`. This trades a bit of CPU (compress on the producer,
   decompress in Bronze) for meaningfully less network/storage volume at
   the file-upload cadence this producer runs at — a deliberate choice over
   landing raw JSONL text files directly.
3. **Bronze + Silver are one Lakeflow Declarative Pipeline**
   (`pumpapi-lakehouse/`), not separate Bronze/Silver jobs, and it's
   authored with `pyspark.pipelines` (`from pyspark import pipelines as
   dp`) — the current Databricks API/style, not the classic `import dlt`
   module an earlier version of this pipeline used (kept for reference at
   `ingestion/consumers/outdated__NB_dlt_pumpfun_bronze.ipynb`). Bronze
   (`bronze_pump_events.py`) reads the envelope files via Auto Loader with
   a fixed schema (no inference needed — the envelope shape is fixed), a
   Python UDF base64-decodes and zstd-decompresses `data_b64` back into
   JSONL text (`zstandard` is installed via the pipeline's
   `environment.dependencies`, since these are plain `.py` files, not
   notebooks, so `%pip install` isn't available), and the result is split
   back into lines. `workspace.bronze.pump_events_raw` keeps each
   **event** payload as raw JSON text, not a parsed struct — schema
   inference/evolution on the decoded content would still be a poor fit
   for a payload whose shape depends on `action`. The pre-compression
   design (plain Auto Loader notebook, `cloudFiles.format = "text"` over
   raw JSONL) is kept for reference at
   `ingestion/consumers/outdated__NB_ingest_pumpfun_to_bronze.ipynb`.
4. **Silver is three per-action-family tables, not per fixed table
   config.** The existing metadata-driven Silver notebook
   (`processing/silver/NB_process_to_silver_generic.ipynb`) is built around
   Debezium's before/after envelope and doesn't fit heterogeneous JSON — so
   pump.fun gets dedicated transformations instead of a new per-table JSON
   config, all reading `spark.readStream.table(BRONZE_TABLE)`:
   `silver_pump_events.py` (every event, one fixed schema, typed columns),
   `silver_pump_tokens.py` (`action = 'create'`, one row per token launch),
   and `silver_pump_transfers.py` (`action = 'transfer'`, `posexplode`d so
   each wallet-to-wallet transfer inside a transaction gets its own row).
   This replaced an earlier `buy`/`sell` trades + `create`/`migrate` tokens
   split (kept for reference at
   `processing/silver/outdated__NB_process_pumpfun_silver.ipynb`); the
   remaining action types (`buy`/`sell`, `migrate`, `createPool`/`add`/
   `remove`, `claimCashback`, `claimCreatorFees`) are left in Bronze only
   for now (§ROADMAP.md 9).
5. **Independent resource graph, not chained into `dvdrental-orchestrator`.**
   Different domain, different cadence, different failure blast radius.
   `pumpapi-lakehouse` (the pipeline) and `pumpfun-bronze` (the job that
   triggers it) are the only two pump.fun resources, on a 2-minute cron
   schedule. Because the Pipelines API doesn't support the `git_source`
   mechanism the Jobs API does, both are declared in
   `orchestration/bundle/databricks.yml` and deployed with
   `databricks bundle deploy`; `scripts/deploy_jobs.py` (raw REST +
   `git_source`) manages no pump.fun resources at all.
6. **No Vault layer yet.** Data Vault 2.0 exists in this repo to historize
   mutable CDC state (updates/deletes) with full audit trail. pump.fun
   events are naturally append-only (every Silver table here is an
   append/insert stream, not a MERGE-upserted current-state table), so the
   case for Hubs/Links/Satellites is weaker — revisit once Gold
   requirements are clearer.

## Data flow

```
pump.fun on-chain activity
  → PumpAPI websocket
    → ingestion/pumpfun/app (buffer → JSONL → zstd-compress → base64 → Databricks Files API)
      → /Volumes/workspace/default/mnt/pumpapi  (one JSON envelope per batch, git-ignored runtime data)
        → pumpapi-lakehouse (Lakeflow pipeline, triggered every 2 min by the
          pumpfun-bronze job's pipeline_task):
            bronze_pump_events.py    → workspace.bronze.pump_events_raw   (decode → decompress → parse)
            silver_pump_events.py    → workspace.silver.pump_events       (all events, typed)
            silver_pump_tokens.py    → workspace.silver.pump_tokens       (action = 'create')
            silver_pump_transfers.py → workspace.silver.pump_transfers    (action = 'transfer', exploded)
```

## Tables

| Table | Layer | Key | Notes |
|-------|-------|-----|-------|
| `bronze.pump_events_raw` | Bronze | none (append) | `event` is the raw per-event payload as JSON text |
| `silver.pump_events` | Silver | signature + action (not enforced) | every event, typed against one fixed schema, `_raw_event` kept for fallback |
| `silver.pump_tokens` | Silver | mint (not enforced) | one row per `create` event: initial price/market cap, mint/freeze authority |
| `silver.pump_transfers` | Silver | signature + transfer_index | one row per transfer inside a `transfer` event's `transfers[]` array |

## Open items

See `ROADMAP.md` §9 — remaining action types, Vault/Gold design, Bronze DQ
parity (quarantine + schema drift), and production deployment of the
producer service.
