"""
DV 2.0 Generator — Schema Analyzer (TASK_04)

Reads Silver table schema definitions from pipeline_configs/silver/**/*.json
and builds a unified TableDef list. Writes 01_schema_analysis.json.
"""
from __future__ import annotations

import json
from pathlib import Path

from ..decision_logger import DecisionLogger
from ..models import ColumnDef, ConfidenceLevel, TableDef
from ..session import Session

# Columns that are considered audit/infrastructure, not DV payload
AUDIT_COLUMNS = frozenset(
    {"last_update", "last_updated_dt", "created_at", "updated_at", "create_date",
     "event_time", "event_ts_ms", "bronze_offset", "ingested_at"}
)

# Data type normalisation — Debezium / Spark type strings → canonical names
_TYPE_ALIASES: dict[str, str] = {
    "int": "integer", "int4": "integer", "int8": "bigint",
    "int2": "smallint", "serial": "integer",
    "varchar": "varchar", "char": "varchar", "text": "varchar", "bpchar": "varchar",
    "float4": "float", "float8": "double", "float": "double",
    "decimal": "numeric", "number": "numeric",
    "bool": "boolean",
    "timestamptz": "timestamp", "timestamp with time zone": "timestamp",
    "date": "date",
}


def _norm_type(raw: str) -> str:
    return _TYPE_ALIASES.get(raw.lower().strip(), raw.lower().strip())


class SchemaAnalyzer:
    """Reads Silver configs and produces a unified TableDef list.

    Config files are discovered from ``config_dir`` recursively (``**/*.json``).
    Files must follow the dvdrental Silver config format with at least:
    - ``table_id``, ``silver_table``, ``primary_keys``, ``field_mappings``

    An optional ``fk_references`` array ``[{"column": "...", "target_table": "..."}]``
    explicitly declares foreign keys. If absent, FK hints are inferred heuristically
    from column names ending in ``_id`` that are not the table's own PK.
    """

    def __init__(
        self,
        config_dir: str,
        session: Session,
        logger: DecisionLogger,
    ) -> None:
        self.config_dir = Path(config_dir)
        self.session = session
        self.logger = logger
        self.output_path = session.session_dir / "01_schema_analysis.json"

    def run(self) -> list[TableDef]:
        """Execute analysis (or load cached result if step already done)."""
        if self.session.is_step_done("step1_analyzer"):
            return self._load_cached()

        tables: list[TableDef] = []
        config_files = sorted(self.config_dir.rglob("*.json"))
        if not config_files:
            raise FileNotFoundError(
                f"No Silver config files found in {self.config_dir}. "
                "Expected *.json files with 'table_id' and 'field_mappings'."
            )

        for config_file in config_files:
            table = self._parse_config(config_file)
            if table is None:
                continue
            tables.append(table)
            self.logger.log(
                step="step1_analyzer",
                entity=table.name,
                rule="A1: config file parsed",
                confidence=ConfidenceLevel.HIGH,
                reason=(
                    f"Loaded {len(table.columns)} columns, "
                    f"PKs={table.pk_columns}, "
                    f"FKs={list(table.fk_hints.keys())} "
                    f"from {config_file.name}"
                ),
            )

        self._save(tables)
        self.session.mark_step_done(
            "step1_analyzer",
            metadata={"table_count": len(tables)},
        )
        return tables

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_config(self, config_file: Path) -> TableDef | None:
        """Parse one Silver JSON config into a TableDef."""
        try:
            cfg = json.loads(config_file.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            self.logger.log(
                step="step1_analyzer",
                entity=config_file.stem,
                rule="A0: parse error",
                confidence=ConfidenceLevel.LOW,
                reason=f"Could not parse {config_file}: {exc}",
            )
            return None

        if not cfg.get("enabled", True):
            return None  # skip disabled tables silently

        table_id: str = cfg.get("table_id", config_file.stem)
        silver_table: str = cfg.get("silver_table", f"silver_{table_id}")
        source_table = f"silver.{silver_table}"
        primary_keys: list[str] = cfg.get("primary_keys", [])

        # Build columns from field_mappings
        columns: list[ColumnDef] = []
        seen: set[str] = set()
        for fm in cfg.get("field_mappings", []):
            col_name: str = fm["target"]
            if col_name in seen:
                continue
            seen.add(col_name)
            data_type = _infer_type_from_mapping(fm)
            col = ColumnDef(
                name=col_name,
                data_type=data_type,
                nullable=(col_name not in primary_keys),
                is_pk=(col_name in primary_keys),
            )
            columns.append(col)

        # Explicit FK references from config (preferred)
        explicit_fks: dict[str, str] = {}
        for ref in cfg.get("fk_references", []):
            explicit_fks[ref["column"]] = ref["target_table"]

        # Heuristic FK detection for columns not covered explicitly
        fk_hints = dict(explicit_fks)
        for col in columns:
            if col.name in fk_hints:
                continue
            if col.name not in primary_keys and col.name.endswith("_id"):
                inferred_target = "silver_" + col.name[:-3]  # strip _id
                fk_hints[col.name] = inferred_target
                self.logger.log(
                    step="step1_analyzer",
                    entity=table_id,
                    rule="A2: FK heuristic",
                    confidence=ConfidenceLevel.MEDIUM,
                    reason=(
                        f"Column '{col.name}' ends with _id and is not a PK → "
                        f"inferred FK to {inferred_target!r}"
                    ),
                )

        # Annotate ColumnDef.is_fk
        for col in columns:
            if col.name in fk_hints:
                col.is_fk = True
                col.fk_table = fk_hints[col.name]
                col.fk_column = col.name  # same column name in target (convention)

        return TableDef(
            name=silver_table,
            source_table=source_table,
            columns=columns,
            pk_columns=primary_keys,
            fk_hints=fk_hints,
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self, tables: list[TableDef]) -> None:
        data = [_table_to_dict(t) for t in tables]
        self.output_path.write_text(json.dumps(data, indent=2))

    def _load_cached(self) -> list[TableDef]:
        data = json.loads(self.output_path.read_text())
        return [_table_from_dict(d) for d in data]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _infer_type_from_mapping(fm: dict) -> str:
    """Infer a canonical data type from a field mapping entry."""
    transform = fm.get("transform", "")
    explicit_type = fm.get("data_type", "")
    if explicit_type:
        return _norm_type(explicit_type)
    if transform in ("decimal_from_debezium_bytes", "decimal_from_json_paths"):
        return "numeric"
    if transform in ("epoch_micros_to_timestamp", "epoch_millis_to_timestamp"):
        return "timestamp"
    col_name: str = fm.get("target", "")
    # Heuristic type guessing from column name
    if col_name.endswith("_id") or col_name in {"id"}:
        return "integer"
    if col_name in {"active", "activebool", "enabled", "flag"}:
        return "boolean"
    if "date" in col_name or "time" in col_name or col_name.endswith("_at"):
        return "timestamp"
    return "varchar"


def _table_to_dict(t: TableDef) -> dict:
    return {
        "name": t.name,
        "source_table": t.source_table,
        "pk_columns": t.pk_columns,
        "fk_hints": t.fk_hints,
        "columns": [
            {
                "name": c.name,
                "data_type": c.data_type,
                "nullable": c.nullable,
                "is_pk": c.is_pk,
                "is_fk": c.is_fk,
                "fk_table": c.fk_table,
                "fk_column": c.fk_column,
            }
            for c in t.columns
        ],
    }


def _table_from_dict(d: dict) -> TableDef:
    columns = [
        ColumnDef(
            name=c["name"],
            data_type=c["data_type"],
            nullable=c["nullable"],
            is_pk=c["is_pk"],
            is_fk=c["is_fk"],
            fk_table=c.get("fk_table"),
            fk_column=c.get("fk_column"),
        )
        for c in d["columns"]
    ]
    return TableDef(
        name=d["name"],
        source_table=d["source_table"],
        columns=columns,
        pk_columns=d["pk_columns"],
        fk_hints=d["fk_hints"],
    )
