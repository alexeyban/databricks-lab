# pump.fun Near-Real-Time Pipeline
**Repo:** databricks-lab / alexeyban

> **Status (2026-09-11): Bronze + Silver implemented. Vault/Gold not started — see `ROADMAP.md` §9.**

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
2. **Landing format is JSONL in a Unity Catalog Volume**
   (`/Volumes/workspace/default/mnt/pumpapi`), uploaded via the Databricks
   Files API — the same "producer writes files, Databricks Auto Loader
   reads them" pattern as the `kafka-to-volume` fallback already used for
   dvdrental Bronze, so no new connectivity story (no ngrok, no direct
   socket from Databricks) was needed.
3. **Bronze keeps the event payload as raw JSON text**, not a parsed
   struct. Auto Loader schema inference/evolution is a poor fit for a
   payload whose shape depends on `action` — inferring one merged schema
   across all action types would produce an increasingly wide, mostly-null
   table. `cloudFiles.format = "text"` is used specifically to avoid this;
   only a handful of top-level fields (`action`, `mint`, `signature`,
   `poolId`) are extracted for pruning/debugging.
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
   `pumpfun-bronze` and `pumpfun-silver` are separate Databricks jobs on
   their own 2-minute cron schedule (`orchestration/bundle/databricks.yml`,
   `scripts/deploy_jobs.py`).
6. **No Vault layer yet.** Data Vault 2.0 exists in this repo to historize
   mutable CDC state (updates/deletes) with full audit trail. pump.fun
   events are naturally append-only (trades) or already current-state
   (`silver_pumpfun_tokens`), so the case for Hubs/Links/Satellites is
   weaker — revisit once Gold requirements are clearer.

## Data flow

```
pump.fun on-chain activity
  → PumpAPI websocket
    → ingestion/pumpfun/app (buffer → JSONL → Databricks Files API)
      → /Volumes/workspace/default/mnt/pumpapi  (JSONL, git-ignored runtime data)
        → pumpfun-bronze job (Auto Loader, cron every 2 min)
          → workspace.bronze.pumpfun_events (raw JSON text)
            → pumpfun-silver job (cron every 2 min)
              → workspace.silver.silver_pumpfun_trades   (buy/sell, append-only)
              → workspace.silver.silver_pumpfun_tokens   (create/migrate, current-state by mint)
```

## Tables

| Table | Layer | Key | Notes |
|-------|-------|-----|-------|
| `bronze.pumpfun_events` | Bronze | none (append) | `event_json` is the raw event as JSON text |
| `silver.silver_pumpfun_trades` | Silver | signature + action + mint + pool_id | one row per trade side per pool |
| `silver.silver_pumpfun_tokens` | Silver | mint | mint/freeze authority, pool type, migration status |

## Open items

See `ROADMAP.md` §9 — remaining action types, Vault/Gold design, Bronze DQ
parity (quarantine + schema drift), and production deployment of the
producer service.
