# Databricks CDC Lakehouse Lab — Documentation

Welcome to the documentation for this Databricks CDC Lakehouse Lab project.

## Overview

This is an **end-to-end reference implementation** of a Change Data Capture (CDC) pipeline from PostgreSQL (`dvdrental`) into a Databricks medallion lakehouse, plus a second, independent near-real-time pipeline ingesting **pump.fun** Solana trading events.

## Documentation Index

| File | Description |
|------|-------------|
| [architecture.md](architecture.md) | High-level architecture, data flow, technology stack, both pipelines |
| [components.md](components.md) | Detailed breakdown of all components, how they work, and why they exist |
| [target_audience.md](target_audience.md) | Who should use this project and their use cases |
| [quickstart.md](quickstart.md) | Getting started guide — how to run the pipeline |
| [troubleshooting.md](troubleshooting.md) | Common issues and their solutions |

## Quick Reference

### dvdrental data flow
```
PostgreSQL → Debezium → Kafka → Databricks
                                  ↓
                            Bronze (raw CDC)
                                  ↓
                            Silver (current-state)
                                  ↓
                            Vault (Data Vault 2.0, dbt models)
                                  ↓
                            Gold (dbt marts)
```

### pump.fun data flow
```
PumpAPI websocket → ingestion/pumpfun (outside Databricks)
                                  ↓
                      Unity Catalog Volume (zstd+base64 envelopes)
                                  ↓
                      pumpapi-lakehouse (Lakeflow Declarative Pipeline)
                                  ↓
                      Bronze (pump_events_raw) + 5 Silver tables
                                  ↓
                      gold_pump_token_risk (separate notebook task)
```

### Source Database
- **dvdrental** — 15 PostgreSQL tables (actors, films, rentals, payments, etc.)

### Key Commands
```bash
# Start local infrastructure (compose lives in infra/docker/)
cd infra/docker && docker compose up -d

# Seed and generate CDC traffic
python3 ingestion/load_bulk_data.py
python3 ingestion/load_generator.py
python3 ingestion/load_products_generator.py

# Deploy Databricks resources (Asset Bundles — covers dvdrental + pump.fun)
cd orchestration/bundle && databricks bundle deploy -t dev

# Or the older raw Jobs API path (dvdrental only)
python3 scripts/deploy_jobs.py

# Run dbt vault + gold locally
cd transformation/dbt_project && dbt build --select vault gold
```

## Additional Resources

- [README.md](../README.md) — Project README with setup instructions
- [ROADMAP.md](../ROADMAP.md) — Current status and future work
- [CLAUDE.md](../CLAUDE.md) — Developer guidance for AI assistants
- [AGENTS.md](../AGENTS.md) — Agent system documentation
- [design/](../design/) — Design documents and implementation logs
- [dv2_design/](dv2_design/) — Data Vault 2.0 design notes (historical snapshot; the
  authoritative copies live in [`../design/dv2/`](../design/dv2/))
