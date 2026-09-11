# pump.fun ingestor

Standalone, always-on service that streams live trading events from the
[pump.fun](https://pump.fun) Solana launchpad via the PumpAPI websocket feed,
buffers them locally, compresses+encodes each batch, and uploads it to a
Databricks Unity Catalog Volume. This is the entry point for the
`pumpfun-bronze-dlt` / `pumpfun-silver` Databricks pipeline described in
[`../../docs/architecture.md`](../../docs/architecture.md).

It runs outside Databricks (a long-lived Python process, not a notebook job)
for the same reason Kafka/Debezium run outside Databricks for the dvdrental
CDC pipeline: it's a continuous external producer, not a scheduled batch task.

## Data flow

```
pump.fun (Solana on-chain trades, pools, token launches)
  → PumpAPI websocket (wss://stream.pumpapi.io/)
    → this service: buffer → serialize batch as JSONL → zstd-compress
      → base64-encode → one JSON envelope file per batch → Databricks Files API
        → Unity Catalog Volume (/Volumes/workspace/default/mnt/pumpapi)
          → pumpfun-bronze-dlt (Delta Live Tables): decode → decompress → parse
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

## Running with Docker

```bash
cd ingestion/pumpfun
cp .env.example .env   # fill in DATABRICKS_HOST / DATABRICKS_TOKEN
docker compose up -d --build
docker compose logs -f
```

The local JSONL buffer (`./data`) is mounted as a volume so completed/uploaded
files survive container restarts and stay inspectable from the host. Stop
with `docker compose down`.

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
| `app/config.py` | Env-driven configuration (websocket URL, batching, compression, Databricks target) |
| `app/pumpapi.py` | Websocket client — reconnects with exponential backoff, parses one JSON event per message |
| `app/writer.py` | Buffers events; each flush serializes the batch as JSONL, zstd-compresses it, base64-encodes it, and writes one JSON envelope file under `data/pumpapi/<date>/` |
| `app/uploader.py` | Uploads completed envelope files to the Databricks Volume via the Files API, then moves them to `uploaded/` |
| `app/main.py` | Wires the three above into one asyncio process |

## Event schema

See [`docs/glossary_of_event_properties.txt`](docs/glossary_of_event_properties.txt)
for the full field glossary. Each landed file is one JSON envelope holding a
whole compressed batch:

```json
{"_source": "pumpapi", "_ingested_at": "2026-09-11T12:00:00+00:00", "event_count": 5000, "codec": "zstd", "uncompressed_bytes": 6673900, "data_b64": "<base64 of zstd-compressed JSONL>"}
```

`data_b64` decodes (base64 → zstd-decompress) back into JSONL text, one line
per event, each shaped like:

```json
{"_source": "pumpapi", "_ingested_at": "2026-09-11T12:00:00+00:00", "event": { ... raw PumpAPI event ... }}
```

`event.action` is one of `transfer`, `create`, `buy`, `sell`, `migrate`,
`createPool`, `add`, `remove`, `claimCashback`, `claimCreatorFees`, etc. — see
the glossary for the meaning of each field. The Bronze DLT pipeline
(`ingestion/consumers/NB_dlt_pumpfun_bronze.ipynb`) reverses the
compression and lands each event as raw JSON text; Silver parses specific
action types into typed tables (currently `buy`/`sell` →
`silver_pumpfun_trades`, and `create`/`migrate` → `silver_pumpfun_tokens`).

## Data / secrets

`data/` (local JSONL landing buffer) and `.env` are git-ignored — never
commit them. `data/` also accumulates on disk; the `uploaded/` subfolders
under it are safe to prune periodically once you've confirmed the files
landed in the Volume.
