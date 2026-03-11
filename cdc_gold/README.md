# cdc_gold

This dbt project builds the Gold presentation layer for the Databricks CDC lab.

## Expected upstream objects

- Bronze raw events are written into `workspace.bronze.orders`
- Silver current-state data is merged into `workspace.silver.silver_orders`
- dbt reads from the `silver.silver_orders` source and writes Gold models into the `gold` schema

## Usage

```bash
cd cdc_gold
dbt debug
dbt build
```

## Models

- `gold_orders`: presentation-friendly current-state orders mart with semantic columns and incremental merge behavior

## Data quality

The project includes:

- source tests for uniqueness and nullability on `silver_orders`
- model tests on `gold_orders`
- a source freshness policy driven by `last_updated_dt`
