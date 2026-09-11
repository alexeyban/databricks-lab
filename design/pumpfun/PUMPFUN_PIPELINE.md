# pump.fun Near-Real-Time Pipeline
**Repo:** databricks-lab / alexeyban

> **Status (2026-09-11): Bronze + Silver implemented, transport is zstd+base64
> compressed batches decoded by a Delta Live Tables pipeline. Vault/Gold not
> started — see `ROADMAP.md` §9.**

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
3. **Bronze decodes the transport via a Delta Live Tables pipeline**
   (`ingestion/consumers/NB_dlt_pumpfun_bronze.ipynb`), not a plain
   notebook job. Auto Loader reads the envelope files with a fixed schema
   (no inference needed — the envelope shape is fixed), a Python UDF
   base64-decodes and zstd-decompresses `data_b64` back into JSONL text
   (`zstandard` installed via `%pip install` in the pipeline source, which
   DLT supports), and the result is split back into lines and parsed. The
   final `bronze.pumpfun_events` table keeps each **event** payload as raw
   JSON text, not a parsed struct — Auto Loader schema inference/evolution
   on the decoded content would still be a poor fit for a payload whose
   shape depends on `action` (inferring one merged schema across all
   action types would produce an increasingly wide, mostly-null table).
   Only a handful of top-level fields (`action`, `mint`, `signature`,
   `poolId`) are extracted for pruning/debugging. The pre-compression
   design (plain Auto Loader notebook, `cloudFiles.format = "text"` over
   raw JSONL) is kept for reference at
   `ingestion/consumers/outdated__NB_ingest_pumpfun_to_bronze.ipynb`.
4. **Silver parses per action-family, not per fixed table config.** The
   existing metadata-driven Silver notebook
   (`processing/silver/NB_process_to_silver_generic.ipynb`) is built around
   Debezium's before/after envelope and doesn't fit heterogeneous JSON — so
   pump.fun gets a dedicated notebook
   (`processing/silver/NB_process_pumpfun_silver.ipynb`) instead of a new
   per-table JSON config. Scope for the first cut: `buy`/`sell` → append-only
   `silver_pumpfun_trades`; `create`/`migrate` → current-state
   `silver_pumpfun_tokens` (MERGE by `mint`). Remaining action types are
   left in Bronze only (§ROADMAP.md 9).
5. **Independent job graph, not chained into `dvdrental-orchestrator`.**
   Different domain, different cadence, different failure blast radius.
   `pumpfun-bronze-dlt` (the DLT pipeline), `pumpfun-bronze` (the job that
   triggers it), and `pumpfun-silver` are separate resources on their own
   2-minute cron schedule. Because the Pipelines API doesn't support the
   `git_source` mechanism the Jobs API does, the DLT pipeline and its
   trigger job are declared in `orchestration/bundle/databricks.yml` and
   deployed with `databricks bundle deploy`; `scripts/deploy_jobs.py`
   (raw REST + `git_source`) only manages `pumpfun-silver`.
6. **No Vault layer yet.** Data Vault 2.0 exists in this repo to historize
   mutable CDC state (updates/deletes) with full audit trail. pump.fun
   events are naturally append-only (trades) or already current-state
   (`silver_pumpfun_tokens`), so the case for Hubs/Links/Satellites is
   weaker — revisit once Gold requirements are clearer.

## Data flow

```
pump.fun on-chain activity
  → PumpAPI websocket
    → ingestion/pumpfun/app (buffer → JSONL → zstd-compress → base64 → Databricks Files API)
      → /Volumes/workspace/default/mnt/pumpapi  (one JSON envelope per batch, git-ignored runtime data)
        → pumpfun-bronze-dlt (DLT pipeline: decode → decompress → parse; triggered
          every 2 min by the pumpfun-bronze job's pipeline_task)
          → workspace.bronze.pumpfun_events (raw JSON text per event)
            → pumpfun-silver job (cron every 2 min)
              → workspace.silver.silver_pumpfun_trades   (buy/sell, append-only)
              → workspace.silver.silver_pumpfun_tokens   (create/migrate, current-state by mint)
```

## Tables

| Table | Layer | Key | Notes |
|-------|-------|-----|-------|
| `bronze.pumpfun_events` | Bronze (DLT) | none (append) | `event_json` is the raw event as JSON text |
| `silver.silver_pumpfun_trades` | Silver | signature + action + mint + pool_id | one row per trade side per pool |
| `silver.silver_pumpfun_tokens` | Silver | mint | mint/freeze authority, pool type, migration status |

## Open items

See `ROADMAP.md` §9 — remaining action types, Vault/Gold design, Bronze DQ
parity (quarantine + schema drift), and production deployment of the
producer service.
