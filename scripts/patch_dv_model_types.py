"""
patch_dv_model_types.py — one-off migration script

Reads Silver field mapping configs from pipeline_configs/silver/dvdrental/,
infers source data types using the same heuristic as step1_analyzer, then
injects a `column_types` dict into every satellite entry of dv_model.json.

Run from repo root:
    python3 scripts/patch_dv_model_types.py
"""
from __future__ import annotations

import json
from pathlib import Path

SILVER_DIR = Path("pipeline_configs/silver/dvdrental")
DV_MODEL   = Path("pipeline_configs/datavault/dv_model.json")

# Canonical type → kept as-is in JSON (matches step1_analyzer normalization)
_TYPE_NORM: dict[str, str] = {
    "int":          "integer",
    "int4":         "integer",
    "int8":         "bigint",
    "int2":         "smallint",
    "integer":      "integer",
    "bigint":       "bigint",
    "smallint":     "smallint",
    "bool":         "boolean",
    "boolean":      "boolean",
    "varchar":      "varchar",
    "text":         "varchar",
    "char":         "varchar",
    "bpchar":       "varchar",
    "numeric":      "numeric",
    "decimal":      "numeric",
    "float":        "numeric",
    "double":       "numeric",
    "timestamp":    "timestamp",
    "timestamptz":  "timestamp",
    "date":         "date",
}


def _norm_type(raw: str) -> str:
    return _TYPE_NORM.get(raw.lower().split("(")[0].strip(), "varchar")


def _infer_type(fm: dict) -> str:
    """Mirror of step1_analyzer._infer_type_from_mapping()."""
    transform    = fm.get("transform", "")
    explicit     = fm.get("data_type", "")
    if explicit:
        return _norm_type(explicit)
    if transform in ("decimal_from_debezium_bytes", "decimal_from_json_paths"):
        return "numeric"
    if transform in ("epoch_micros_to_timestamp", "epoch_millis_to_timestamp"):
        return "timestamp"
    col: str = fm.get("target", "")
    if col.endswith("_id") or col == "id":
        return "integer"
    if col in {"active", "activebool", "enabled", "flag"}:
        return "boolean"
    if "date" in col or "time" in col or col.endswith("_at"):
        return "timestamp"
    return "varchar"


def build_type_maps() -> dict[str, dict[str, str]]:
    """Return {silver_table_name: {col_name: canonical_type}} for all Silver configs."""
    result: dict[str, dict[str, str]] = {}
    for cfg_path in sorted(SILVER_DIR.glob("*.json")):
        cfg = json.loads(cfg_path.read_text())
        table_name = "silver_" + cfg_path.stem          # e.g. "silver_actor"
        col_types: dict[str, str] = {}
        for fm in cfg.get("field_mappings", []):
            target = fm.get("target")
            if target:
                col_types[target] = _infer_type(fm)
        result[table_name] = col_types
    return result


def patch(dv_model: dict, type_maps: dict[str, dict[str, str]]) -> int:
    """Inject column_types into each satellite. Returns count of patched satellites."""
    patched = 0
    for sat in dv_model.get("satellites", []):
        # source_table is like "silver.silver_actor" → key is "silver_actor"
        table_key = sat["source_table"].split(".")[-1]
        col_map   = type_maps.get(table_key, {})
        sat["column_types"] = {
            col: col_map.get(col, "varchar")
            for col in sat.get("tracked_columns", [])
        }
        patched += 1
    return patched


def main() -> None:
    type_maps = build_type_maps()
    print(f"Loaded type maps for {len(type_maps)} Silver tables")

    model = json.loads(DV_MODEL.read_text())
    n = patch(model, type_maps)
    DV_MODEL.write_text(json.dumps(model, indent=2))
    print(f"Patched {n} satellites in {DV_MODEL}")

    # Spot-check output
    for sat in model["satellites"]:
        print(f"  {sat['name']}: {sat['column_types']}")


if __name__ == "__main__":
    main()
