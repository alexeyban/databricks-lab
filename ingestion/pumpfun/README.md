# pump.fun ingestor

Standalone, always-on service that streams live trading events from the
[pump.fun](https://pump.fun) Solana launchpad via the PumpAPI websocket feed,
buffers them locally as JSONL, and uploads completed files to a Databricks
Unity Catalog Volume. This is the entry point for the `pumpfun-bronze` /
`pumpfun-silver` Databricks pipeline described in
[`../../docs/architecture.md`](../../docs/architecture.md).

It runs outside Databricks (a long-lived Python process, not a notebook job)
for the same reason Kafka/Debezium run outside Databricks for the dvdrental
CDC pipeline: it's a continuous external producer, not a scheduled batch task.

## Data flow

```
pump.fun (Solana on-chain trades, pools, token launches)
  → PumpAPI websocket (wss://stream.pumpapi.io/)
    → this service (buffer → JSONL → Databricks Files API)
      → Unity Catalog Volume (/Volumes/workspace/default/mnt/pumpapi)
        → pumpfun-bronze job (Auto Loader, near-real-time)
          → pumpfun-silver job (typed trade + token tables)
```

## Running locally

```bash
cd ingestion/pumpfun
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in DATABRICKS_HOST / DATABRICKS_TOKEN
python -m app.main
```

## Running as a systemd service

```bash
sudo cp -r . /opt/databricks-lab/ingestion/pumpfun
sudo cp systemd/pumpfun-ingestor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pumpfun-ingestor
journalctl -u pumpfun-ingestor -f
```

## Modules

| File | Responsibility |
|------|-----------------|
| `app/config.py` | Env-driven configuration (websocket URL, batching, Databricks target) |
| `app/pumpapi.py` | Websocket client — reconnects with exponential backoff, parses one JSON event per message |
| `app/writer.py` | Buffers events and flushes them to timestamped JSONL files under `data/pumpapi/<date>/` |
| `app/uploader.py` | Uploads completed JSONL files to the Databricks Volume via the Files API, then moves them to `uploaded/` |
| `app/main.py` | Wires the three above into one asyncio process |

## Event schema

See [`docs/glossary_of_event_properties.txt`](docs/glossary_of_event_properties.txt)
for the full field glossary. Each landed JSONL line has the shape:

```json
{"_source": "pumpapi", "_ingested_at": "2026-09-11T12:00:00+00:00", "event": { ... raw PumpAPI event ... }}
```

`event.action` is one of `transfer`, `create`, `buy`, `sell`, `migrate`,
`createPool`, `add`, `remove`, `claimCashback`, `claimCreatorFees`, etc. — see
the glossary for the meaning of each field. The Bronze layer keeps this
payload as raw JSON text; Silver parses specific action types into typed
tables (currently `buy`/`sell` → `silver_pumpfun_trades`, and
`create`/`migrate` → `silver_pumpfun_tokens`).

## Data / secrets

`data/` (local JSONL landing buffer) and `.env` are git-ignored — never
commit them. `data/` also accumulates on disk; the `uploaded/` subfolders
under it are safe to prune periodically once you've confirmed the files
landed in the Volume.
