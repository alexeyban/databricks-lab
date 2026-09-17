# Databricks CDC Lakehouse Lab — Documentation

Welcome to the comprehensive documentation for this Databricks CDC Lakehouse Lab project.

## Overview

This is an **end-to-end reference implementation** of a Change Data Capture (CDC) pipeline from PostgreSQL (`dvdrental` database) into a Databricks medallion lakehouse architecture.

## Documentation Index

| File | Description |
|------|-------------|
| [architecture.md](architecture.md) | High-level architecture, data flow, technology stack |
| [components.md](components.md) | Detailed breakdown of all components, how they work, and why they exist |
| [target_audience.md](target_audience.md) | Who should use this project and their use cases |
| [quickstart.md](quickstart.md) | Getting started guide - how to run the pipeline |
| [troubleshooting.md](troubleshooting.md) | Common issues and their solutions |

## Quick Reference

### Data Flow
```
PostgreSQL → Debezium → Kafka → Databricks
                                  ↓
                            Bronze (raw CDC)
                                  ↓
                            Silver (current-state)
                                  ↓
                            Vault (Data Vault 2.0)
                                  ↓
                            Gold (dbt models)
```

### Source Database
- **dvdrental** - 15 PostgreSQL tables (actors, films, rentals, payments, etc.)

### Key Commands
```bash
# Start local infrastructure
docker compose up -d

# Run data generators
python3 generators/load_generator.py
python3 generators/load_products_generator.py

# Deploy Databricks jobs
python3 scripts/deploy_job.py

# Run dbt Gold layer
cd cdc_gold && dbt build
```

## Additional Resources

- [README.md](../README.md) - Project README with setup instructions
- [ROADMAP.md](../ROADMAP.md) - Current status and future work
- [CLAUDE.md](../CLAUDE.md) - Developer guidance for AI assistants
- [AGENTS.md](../AGENTS.md) - Agent system documentation
- [design/](./) - Design documents and implementation logs