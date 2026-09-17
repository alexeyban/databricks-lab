# dbt Project — Vault + Gold Layer

dbt project for the Vault incremental models and Gold presentation layer of the Databricks CDC dvdrental lab.

**Project name** (dbt_project.yml): `cdc_gold`
**Location**: `transformation/dbt_project/`
**Catalogs**: `workspace.vault.*` (vault models), `workspace.gold.*` (gold models)

---

## Running on Databricks

Two paths exist, depending on how the jobs were deployed:

- **Asset Bundle** (`orchestration/bundle/databricks.yml`) — the `dvdrental-vault-gold`
  job runs two native `dbt_task`s against SQL warehouse `53165753164ae80e`:
  `dbt deps && dbt build --select vault`, then `dbt build --select gold`.
- **`scripts/deploy_jobs.py`** — the same job instead runs
  `transformation/NB_run_dbt.ipynb`, which calls the dbtRunner Python API
  (no subprocess), after the `dvdrental-vault` notebook job completes.

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

### Hubs (`hubs/`) — 13 models

One hub per entity: `hub_actor`, `hub_address`, `hub_category`, `hub_city`, `hub_country`, `hub_customer`, `hub_film`, `hub_inventory`, `hub_language`, `hub_payment`, `hub_rental`, `hub_staff`, `hub_store`.

Each hub: SHA-256 hash key, business key, load date, record source.

### Links (`links/`) — 19 models

All relationships between hubs: `lnk_rental_customer`, `lnk_rental_inventory`, `lnk_payment_rental`, `lnk_film_actor`, `lnk_film_category`, and 14 more. Each link carries a composite SHA-256 hash key.

### Satellites (`satellites/`) — 20 models

Attribute history per hub, append-only via DIFF_HASH change detection. Entities with
mixed change rates are split — e.g. `sat_film_core`, `sat_film_details` and
`sat_film_pricing`; `sat_customer_core` and `sat_customer_details`.

### PIT Tables (`pit/`) — 4 models

Materialized as **tables** (not incremental). Daily snapshot spine joining hub keys to satellite load dates:
`pit_customer`, `pit_film`, `pit_payment`, `pit_rental`.

### Bridge Tables (`bridge/`) — 2 models

Pre-joined many-to-many paths for query acceleration:
`brg_rental_film`, `brg_film_cast`.

---

## Gold Models (`models/gold/`)

All gold models write to `workspace.gold.*`. `gold_film` and `gold_rental` are
incremental; the other five are full-refresh tables.

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

**Source tests** (`models/sources.yml`) — PK `not_null` + `unique` on every Silver
table, plus `not_null` on junction-table foreign keys and key attributes such as
`silver_film.title`.

**Gold model tests** (`models/gold/gold_rental.yml`, which covers both
`gold_rental` and `gold_film`):
- `dbt_expectations.expect_table_row_count_to_be_between(min_value: 1)` on both models
- `gold_rental`: `rental_id` unique + not null; `inventory_id`, `customer_id`,
  `rental_date`, `last_updated_dt` not null; `rental_status` in `open`/`returned`;
  `total_paid` between 0 and 200
- `gold_film`: `film_id` unique + not null; `title` not null; `rental_rate` between
  0.99 and 4.99; `rental_rate_tier` in `budget`/`standard`/`premium`

**Singular tests** (`tests/`):
- `assert_gold_payment_totals_match_silver.sql` — Gold/Silver payment reconciliation
- `assert_total_products_order_positive_amount.sql`
- `assert_total_products_order_unique_grain.sql`

**Macros** (`macros/`):
- `write_dq_results()` — `on-run-end` hook writing every test result to `monitoring.dq_results`
- `suppress_erased_subjects()` — GDPR suppression applied in Gold models
