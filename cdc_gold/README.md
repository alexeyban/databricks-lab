# cdc_gold

This dbt project builds the Gold presentation layer for the Databricks CDC lab.

## Expected upstream objects

- Bronze raw events are written into `workspace.bronze.orders`
- Silver current-state data is merged into `workspace.silver.silver_orders`
- dbt reads from `silver.silver_orders` and `silver.silver_products` and writes Gold models into the `gold` schema

## Usage

```bash
cd cdc_gold
dbt debug
dbt build
```

## Models

- `total_products_order`: aggregated total order amount by product name and color

## Data quality

The project includes:

- source tests for uniqueness and nullability on `silver_orders` and `silver_products`
- model and singular tests on `total_products_order`
- a source freshness policy driven by `last_updated_dt`
