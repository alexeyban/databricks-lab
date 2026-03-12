# Silver Metadata Refactor

## Goal

Refactor the Silver layer from table-specific notebooks into one metadata-driven
Bronze-to-Silver framework that can run one task per table in parallel in a
Databricks Workflow job.

## Current State

- `notebooks/silver/NB_process_to_silver.ipynb` handles `orders`
- `notebooks/silver/NB_process_products_silver.ipynb` handles `products`
- both notebooks duplicate the same control flow:
  - load helper notebooks
  - read widgets and build config
  - validate schema drift
  - read one Bronze table as a stream
  - parse Debezium payload
  - deduplicate latest event per key
  - merge into Silver with `foreachBatch`

## Refactoring Direction

Introduce a universal Silver runtime made of:

1. a metadata registry describing each source table
2. a generic Bronze-to-Silver notebook driven by `TABLE_ID`
3. a fan-out Databricks Workflow that runs one generic Silver task per table

## Target Architecture

### Shared Runtime

- `notebooks/helpers/NB_silver_metadata.ipynb`
  - registry of Silver table configs
  - metadata access helpers
  - target-table creation from schema contracts
  - optional one-time bootstrap hooks for legacy migration cases
- `notebooks/silver/NB_process_to_silver_generic.ipynb`
  - reads `TABLE_ID`
  - loads metadata for the table
  - validates schema contracts and drift policy
  - parses Debezium CDC payload generically
  - applies declarative field transforms
  - deduplicates by metadata-defined keys and ordering
  - merges into Silver using shared merge helpers

### Metadata Responsibilities

Each table config should define:

- Bronze source table name
- Silver target table name
- CDC contract key
- Silver contract key
- checkpoint suffix
- primary keys
- dedupe ordering columns
- merge field list
- field mappings and transform types
- optional bootstrap hook name

### Workflow Responsibilities

- Bronze stays a single upstream ingestion task
- Silver becomes a parallel fan-out stage
- each Silver task calls the same notebook with a different `TABLE_ID`
- each task gets its own checkpoint path

## Metadata Model

Example shape:

```python
{
    "table_id": "orders",
    "bronze_table": "orders",
    "silver_table": "silver_orders",
    "cdc_contract_key": "cdc.orders",
    "silver_contract_key": "silver.orders",
    "checkpoint_name": "orders_silver_generic",
    "primary_keys": ["id"],
    "dedupe_order_columns": ["event_time", "event_ts_ms", "bronze_offset"],
    "merge_core_fields": ["id", "product_id", "product_legacy", "price", "created_at"],
    "field_mappings": [
        {"target": "id", "source_paths": ["after.id", "before.id"]},
        {"target": "price", "transform": "decimal_from_debezium_struct", "source_paths": ["after.price", "before.price"], "precision": 12, "scale": 2},
    ],
}
```

## Migration Plan

### Phase 1 - Add generic framework beside current notebooks

- keep existing Silver notebooks unchanged for safety
- add the metadata notebook and generic Silver notebook
- add a parallel job definition that targets the new generic runtime

### Phase 2 - Shadow validation

- run generic Silver notebook into shadow targets if needed
- compare:
  - row counts
  - PK uniqueness
  - delete behavior
  - decimal decoding
  - sampled record equality

### Phase 3 - Workflow cutover

- switch the production workflow from table-specific notebooks to the generic one
- retain one task per table so Silver can scale horizontally

### Phase 4 - Cleanup

- retire old table-specific notebooks after confidence is established
- remove one-time legacy bootstrap logic once not needed

## Risks

- legacy orders reconciliation currently depends on `silver_products`
- schema evolution is still controlled by metadata; unknown fields will not be
  auto-published unless added to contracts and mappings
- dedupe ordering based only on event time is weak without extra tie-breakers
- metadata mistakes become a runtime failure mode and need validation

## Recommended Guardrails

- validate metadata at notebook start
- keep one checkpoint path per Silver table
- include `event_ts_ms` and Bronze offset in dedupe ordering
- keep drift detection enabled for every generic run
- support optional hooks only as exceptions, not as the default path

## Deliverables Added In This Refactor

- `notebooks/helpers/NB_silver_metadata.ipynb`
- `notebooks/silver/NB_process_to_silver_generic.ipynb`
- `Orders-ingest-job.yaml`

## Cutover Recommendation

Use the new generic notebook for new tables immediately. For `orders` and
`products`, validate side by side first, then switch the Databricks job to the
parallel generic workflow.
