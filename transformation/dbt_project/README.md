# cdc_gold

dbt project for the Gold presentation layer of the Databricks CDC dvdrental lab.

## Expected upstream tables

| Layer | Table | Key |
|-------|-------|-----|
| Silver | `workspace.silver.silver_film` | film_id |
| Silver | `workspace.silver.silver_rental` | rental_id |
| Silver | `workspace.silver.silver_payment` | payment_id |

## Models

### gold_film

Incremental table built from `silver.silver_film`.

Adds a derived column:
- `rental_rate_tier`: `budget` (≤1.99) · `standard` (≤3.99) · `premium` (>3.99)

### gold_rental

Incremental table joining `silver.silver_rental` with total payments from `silver.silver_payment`.

Adds derived columns:
- `rental_status`: `open` (no return date) or `returned`
- `total_paid`: sum of payments for the rental

## Usage

```bash
cd cdc_gold
dbt debug          # verify connection
dbt build          # run models + tests
dbt test           # data quality tests only
dbt run            # run models only
```

## Data quality tests

- `silver_film`: `film_id` unique + not null, `title` not null
- `silver_rental`: `rental_id` unique + not null, `last_updated_dt` not null
- `silver_payment`: `payment_id` unique + not null, `rental_id` not null, `amount` not null
- Source freshness driven by `last_updated_dt`
