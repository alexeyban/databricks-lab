# TASK_04: step1_analyzer.py — Schema Analyzer

## File
`generators/dv_generator/steps/step1_analyzer.py`

## Purpose
Reads all Silver table schema definitions from offline config files (`pipeline_configs/silver/*.json`) and optionally from schema contract exports, then builds a unified column/type/nullability/FK-hint map for every table. Writes `01_schema_analysis.json` to the session folder. This is the only step that reads from the project config — all downstream steps work from the analysis output.

## Depends on
- `TASK_01: models.py` — `TableDef`, `ColumnDef`
- `TASK_02: decision_logger.py` — `DecisionLogger`
- `TASK_03: session.py` — `Session`

## Inputs
- `pipeline_configs/silver/*.json` — Silver metadata files (one per table, already exist in repo)
- `pipeline_configs/silver/schema_contracts/` — optional per-table expected schema exports (if present)
- `Session` instance (for session_dir and step-skip logic)

## Outputs
- `{session_dir}/01_schema_analysis.json` — list of serialised `TableDef` objects

### `01_schema_analysis.json` structure
```json
[
  {
    "name": "silver_film",
    "source_table": "silver.silver_film",
    "columns": [
      {"name": "film_id",        "data_type": "integer",   "nullable": false, "is_pk": true,  "is_fk": false, "fk_table": null, "fk_column": null},
      {"name": "language_id",    "data_type": "integer",   "nullable": false, "is_pk": false, "is_fk": true,  "fk_table": "silver_language", "fk_column": "language_id"},
      {"name": "rental_rate",    "data_type": "numeric",   "nullable": false, "is_pk": false, "is_fk": false, "fk_table": null, "fk_column": null},
      {"name": "last_updated_dt","data_type": "timestamp", "nullable": true,  "is_pk": false, "is_fk": false, "fk_table": null, "fk_column": null}
    ],
    "pk_columns": ["film_id"],
    "fk_hints": {"language_id": "silver_language"}
  }
]
```

## Key classes / functions

```python
import json
from pathlib import Path
from ..models import TableDef, ColumnDef
from ..decision_logger import DecisionLogger
from ..session import Session

class SchemaAnalyzer:
    """Reads Silver configs and produces a unified TableDef list.

    Usage:
        analyzer = SchemaAnalyzer(
            config_dir="pipeline_configs/silver",
            session=session,
            logger=logger
        )
        tables = analyzer.run()
    """

    KNOWN_PK_SUFFIXES = ["_id"]          # heuristic: col ending in _id on a table named X → X_id is PK
    FK_SUFFIX = "_id"                     # any col ending _id that is NOT the table's own PK → FK hint

    def __init__(self, config_dir: str, session: Session, logger: DecisionLogger):
        self.config_dir = Path(config_dir)
        self.session = session
        self.logger = logger
        self.output_path = session.session_dir / "01_schema_analysis.json"

    def run(self) -> list[TableDef]:
        """Execute analysis (or load cached result if step already done)."""
        if self.session.is_step_done("step1_analyzer"):
            return self._load_cached()

        tables = []
        for config_file in sorted(self.config_dir.glob("*.json")):
            table = self._parse_config(config_file)
            if table:
                tables.append(table)
                self.logger.log(
                    step="step1_analyzer",
                    entity=table.name,
                    rule="A1: config file parsed",
                    confidence=...,
                    reason=f"Loaded {len(table.columns)} columns from {config_file.name}"
                )

        self._save(tables)
        self.session.mark_step_done("step1_analyzer", metadata={"table_count": len(tables)})
        return tables

    def _parse_config(self, config_file: Path) -> TableDef | None:
        """Parse one Silver JSON config into a TableDef."""

    def _infer_fk_hints(self, columns: list[ColumnDef], table_name: str) -> dict[str, str]:
        """
        Heuristic FK detection (S2 rule fallback):
        Any col ending in _id that is NOT the table's own PK is flagged as a
        potential FK. The target table name is inferred by stripping _id and
        prepending 'silver_': e.g. language_id → silver_language.
        """

    def _save(self, tables: list[TableDef]) -> None:
        """Write 01_schema_analysis.json."""

    def _load_cached(self) -> list[TableDef]:
        """Deserialise 01_schema_analysis.json."""
```

## Logic walkthrough
1. Check `session.is_step_done("step1_analyzer")` — if True, load and return cached JSON.
2. Glob all `*.json` files in `pipeline_configs/silver/`.
3. For each file, parse the JSON into a `TableDef` + list of `ColumnDef`.
4. Identify PK columns from the config's `merge_key` or `primary_key` field (depends on existing Silver config format — check actual files before implementing).
5. Run `_infer_fk_hints`: any column ending in `_id` that is not the table's own PK is treated as a FK hint. Target table = `silver_{col_name[:-3]}` (strip `_id`).
6. Log one `DecisionEntry` per table.
7. Serialise all `TableDef` objects to `01_schema_analysis.json`.
8. Call `session.mark_step_done`.

### FK heuristic examples
| Table | Column | Inferred FK target |
|-------|--------|--------------------|
| silver_film | language_id | silver_language |
| silver_rental | inventory_id | silver_inventory |
| silver_rental | customer_id | silver_customer |
| silver_rental | staff_id | silver_staff |
| silver_payment | rental_id | silver_rental |

## Type inference (`_infer_type_from_mapping`)

Types are resolved in priority order:

1. Explicit `data_type` field in the field mapping → normalised via `_norm_type()`
2. `transform` field:
   - `decimal_from_debezium_bytes` / `decimal_from_json_paths` → `"numeric"`
   - `epoch_micros_to_timestamp` / `epoch_millis_to_timestamp` → `"timestamp"`
3. Column-name heuristics (applied if no transform matched):
   - `col.endswith("_id")` or `col == "id"` → `"integer"`
   - `col in {"active", "activebool", "enabled", "flag"}` → `"boolean"`
   - `"date" in col` or `"time" in col` or `col.endswith("_at")` → `"timestamp"`
   - default → `"varchar"`

> **Note:** The timestamp heuristic uses `col.endswith("_at")` — not `"at" in col`. The broader substring match was too aggressive and incorrectly classified `rating`, `rental_duration`, and `special_features` as timestamps.

## Acceptance criteria
- Running on the dvdrental Silver configs produces exactly 15 `TableDef` entries
- Each `TableDef` has a non-empty `pk_columns` list
- `silver_rental` has `fk_hints` containing `inventory_id`, `customer_id`, `staff_id`
- `silver_film` has `fk_hints` containing `language_id`
- `silver_film.rental_rate` infers type `"numeric"` (via `decimal_from_debezium_bytes` transform)
- `silver_customer.activebool` infers type `"boolean"` (column-name heuristic)
- `silver_film.rating` infers type `"varchar"` (not timestamp — `"at" in col` is NOT the rule)
- Re-running after `session.mark_step_done("step1_analyzer")` loads from cache without re-parsing files
- `01_schema_analysis.json` is valid JSON and round-trips through `_load_cached()` without data loss
