# dbt Project — Vault + Gold Layer

dbt project for the Vault incremental models and Gold presentation layer of the Databricks CDC dvdrental lab.

**Project name** (dbt_project.yml): `cdc_gold`
**Location**: `transformation/dbt_project/`
**Catalogs**: `workspace.vault.*` (vault models), `workspace.gold.*` (gold models)

---

## Running on Databricks

The canonical way to run this project is via the `transformation/NB_run_dbt.ipynb` notebook, which uses the dbtRunner Python API (no subprocess). It is wired into the `dvdrental-vault-gold` Databricks job and runs after the vault notebook job completes.

dbt_packages are committed to the repo — no `dbt deps` needed at runtime.

---

## Local Usage

```bash
cd transformation/dbt_project
dbt debug                          # Verify connection
dbt build --select vault gold      # Run all vault + gold models and tests
dbt test                           # Data quality tests only
dbt run --select vault gold        # Models only (no tests)
```

---

## Sources

All models source from Silver tables in `workspace.silver.*`:

| Silver Table | Key |
|-------------|-----|
| `workspace.silver.silver_film` | film_id |
| `workspace.silver.silver_rental` | rental_id |
| `workspace.silver.silver_payment` | payment_id |
| `workspace.silver.silver_customer` | customer_id |
| `workspace.silver.silver_inventory` | inventory_id |
| `workspace.silver.silver_actor` | actor_id |
| `workspace.silver.silver_staff` | staff_id |
| `workspace.silver.silver_store` | store_id |
| (+ 7 more silver tables) | |

---

## Vault Models (`models/vault/`)

All vault models are **incremental** and write to `workspace.vault.*`. They replicate and extend the Python notebook vault layer using dbt's incremental materialization.

### Hubs (`hubs/`) — 15 models

One hub per entity: `hub_film`, `hub_rental`, `hub_payment`, `hub_customer`, `hub_inventory`, `hub_actor`, `hub_staff`, `hub_store`, `hub_address`, `hub_city`, `hub_country`, `hub_language`, `hub_category`, and 2 additional hubs.

Each hub: SHA-256 hash key, business key, load date, record source.

### Links (`links/`) — 19 models

All relationships between hubs: `lnk_rental_customer`, `lnk_rental_inventory`, `lnk_payment_rental`, `lnk_film_actor`, `lnk_film_category`, and 14 more. Each link carries a composite SHA-256 hash key.

### Satellites (`satellites/`) — 15 models

Attribute history per hub, append-only via DIFF_HASH change detection. One satellite per Silver table (or per change-rate group).

### PIT Tables (`pit/`) — 4 models

Materialized as **tables** (not incremental). Daily snapshot spine joining hub keys to satellite load dates:
`pit_film`, `pit_rental`, `pit_customer`, `pit_inventory`.

### Bridge Tables (`bridge/`) — 2 models

Pre-joined many-to-many paths for query acceleration:
`brg_rental_film`, `brg_film_actor`.

---

## Gold Models (`models/gold/`)

All gold models are incremental and write to `workspace.gold.*`.

| Model | Source | Description |
|-------|--------|-------------|
| `gold_film` | silver_film | Film with `rental_rate_tier` (budget / standard / premium) |
| `gold_rental` | silver_rental + silver_payment | Rental with `rental_status` and `total_paid` |
| `gold_customer_summary` | silver_customer + silver_rental + silver_payment | Customer lifetime value, rental history, payment totals |
| `gold_inventory_status` | silver_inventory + silver_rental | Stock levels and utilisation rate per store |
| `gold_revenue_by_store` | silver_payment + silver_rental + silver_staff | Revenue aggregation by store |
| `gold_film_popularity` | silver_rental + silver_film | Rental frequency and revenue per film |
| `gold_staff_performance` | silver_rental + silver_payment + silver_staff | Rental and payment counts per staff member |

---

## Data Quality Tests

- `silver_film`: `film_id` unique + not null, `title` not null
- `silver_rental`: `rental_id` unique + not null, `last_updated_dt` not null
- `silver_payment`: `payment_id` unique + not null, `rental_id` not null, `amount` not null
- Gold models: row count > 0, key uniqueness, referential integrity
- Source freshness driven by `last_updated_dt`
