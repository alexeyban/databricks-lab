"""
DV 2.0 Generator — Vault Notebook Generator (TASK_07)

Generates the five Databricks vault notebooks as .ipynb files, fully populated
with executable PySpark code derived from dv_model_draft.json. Each notebook
iterates over its entity type from the config and performs the correct DV 2.0
operation. Output lands in {session_dir}/notebooks/ and is copied to
notebooks/vault/ by the applier (step7).
"""
from __future__ import annotations

import json
from pathlib import Path

from ..models import DVModel
from ..session import Session


class NotebookGenerator:
    """Generates the 5 vault .ipynb notebooks from a DVModel."""

    NOTEBOOKS = [
        "NB_dv_metadata",
        "NB_ingest_to_hubs",
        "NB_ingest_to_links",
        "NB_ingest_to_satellites",
        "NB_dv_business_vault",
    ]

    def __init__(self, model: DVModel, session: Session) -> None:
        self.model = model
        self.session = session
        self.output_dir = session.session_dir / "notebooks"

    def run(self) -> None:
        """Generate all vault notebooks (or skip if already done)."""
        if self.session.is_step_done("step3b_notebook_gen"):
            return
        self.output_dir.mkdir(exist_ok=True)
        self._gen_metadata_nb()
        self._gen_hubs_nb()
        self._gen_links_nb()
        self._gen_satellites_nb()
        self._gen_business_vault_nb()
        self.session.mark_step_done("step3b_notebook_gen")

    # ------------------------------------------------------------------
    # Cell / notebook helpers
    # ------------------------------------------------------------------

    def _make_cell(self, source: str, cell_type: str = "code") -> dict:
        """Build a single notebook cell dict."""
        cell: dict = {
            "cell_type": cell_type,
            "source": source,
            "metadata": {},
        }
        if cell_type == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        return cell

    def _make_notebook(self, cells: list[dict]) -> dict:
        """Assemble a minimal valid .ipynb dict."""
        return {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                }
            },
            "cells": cells,
        }

    def _write_nb(self, name: str, notebook: dict) -> None:
        """Serialise notebook dict to {output_dir}/{name}.ipynb."""
        path = self.output_dir / f"{name}.ipynb"
        path.write_text(json.dumps(notebook, indent=2))

    # ------------------------------------------------------------------
    # NB_dv_metadata
    # ------------------------------------------------------------------

    def _gen_metadata_nb(self) -> None:
        cells = [
            self._make_cell(
                "# NB_dv_metadata — DV 2.0 Shared Helpers\n\n"
                "Provides shared utilities used by all vault notebooks:\n"
                "- `load_model()`: reads the dv_model.json config\n"
                "- `generate_hash_key()` / `generate_diff_hash()`: PySpark hash expressions\n"
                "- `create_hub_table()`, `create_link_table()`, `create_sat_table()`: DDL helpers\n"
                "- `create_pit_table()`, `create_bridge_table()`: business vault DDL\n"
                "- `get_latest_diff_hash()`: window function for satellite CDC",
                "markdown",
            ),
            self._make_cell(
                'dbutils.widgets.text("MODEL_PATH", "pipeline_configs/datavault/dv_model.json", "DV Model JSON Path")\n'
                'dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog name")\n'
                'dbutils.widgets.text("VAULT_SCHEMA", "vault", "Vault schema name")\n'
                "\n"
                'MODEL_PATH   = dbutils.widgets.get("MODEL_PATH")\n'
                'CATALOG      = dbutils.widgets.get("CATALOG")\n'
                'VAULT_SCHEMA = dbutils.widgets.get("VAULT_SCHEMA")'
            ),
            self._make_cell(
                "import json\n"
                "from pathlib import Path\n"
                "\n"
                "def load_model(model_path: str) -> dict:\n"
                '    """Load dv_model.json and return as dict."""\n'
                "    p = Path(model_path)\n"
                "    if not p.is_absolute():\n"
                "        # Try relative to the repo root (Databricks workspace)\n"
                "        p = Path('/Workspace/Repos') / p\n"
                "    if p.exists():\n"
                "        return json.loads(p.read_text())\n"
                "    # Fallback: dbutils.fs.head for DBFS paths\n"
                "    content = dbutils.fs.head(f'dbfs:/{model_path}', 1_000_000)\n"
                "    return json.loads(content)\n"
                "\n"
                "model = load_model(MODEL_PATH)\n"
                'print(f"Loaded model: {len(model[\'hubs\'])} hubs, {len(model[\'links\'])} links, {len(model[\'satellites\'])} satellites")'
            ),
            self._make_cell(
                "from pyspark.sql import functions as F\n"
                "\n"
                "def generate_hash_key(bk_cols: list) -> F.Column:\n"
                '    """Generate SHA-256 hash key from business key columns."""\n'
                "    return F.sha2(\n"
                '        F.concat_ws("||", *[F.upper(F.trim(F.col(c).cast("string"))) for c in bk_cols]),\n'
                "        256,\n"
                "    )\n"
                "\n"
                "def generate_diff_hash(tracked_cols: list) -> F.Column:\n"
                '    """Generate SHA-256 diff hash from tracked satellite columns."""\n'
                "    return F.sha2(\n"
                '        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in tracked_cols]),\n'
                "        256,\n"
                "    )"
            ),
            self._make_cell(
                "def create_hub_table(hub_cfg: dict) -> None:\n"
                '    """Create a hub Delta table if it does not exist."""\n'
                "    table = f\"{CATALOG}.{VAULT_SCHEMA}.{hub_cfg['name'].lower()}\"\n"
                "    bk_cols = ', '.join(f\"{c} STRING\" for c in hub_cfg['business_key_columns'])\n"
                "    spark.sql(f\"\"\"\n"
                "        CREATE TABLE IF NOT EXISTS {table} (\n"
                "            HK_{hub_cfg['name'][4:]} STRING NOT NULL,\n"
                "            {bk_cols},\n"
                "            LOAD_DATE TIMESTAMP,\n"
                "            RECORD_SOURCE STRING\n"
                "        ) USING DELTA\n"
                "    \"\"\")\n"
                "\n"
                "def create_link_table(lnk_cfg: dict) -> None:\n"
                '    """Create a link Delta table if it does not exist."""\n'
                "    table = f\"{CATALOG}.{VAULT_SCHEMA}.{lnk_cfg['name'].lower()}\"\n"
                "    fk_cols = ', '.join(f\"HK_{r['hub'][4:]} STRING\" for r in lnk_cfg['hub_references'])\n"
                "    spark.sql(f\"\"\"\n"
                "        CREATE TABLE IF NOT EXISTS {table} (\n"
                "            HK_{lnk_cfg['name'][4:]} STRING NOT NULL,\n"
                "            {fk_cols},\n"
                "            LOAD_DATE TIMESTAMP,\n"
                "            RECORD_SOURCE STRING\n"
                "        ) USING DELTA\n"
                "    \"\"\")\n"
                "\n"
                "def create_sat_table(sat_cfg: dict) -> None:\n"
                '    """Create a satellite Delta table if it does not exist."""\n'
                "    table = f\"{CATALOG}.{VAULT_SCHEMA}.{sat_cfg['name'].lower()}\"\n"
                "    tracked_cols = ', '.join(f\"{c} STRING\" for c in sat_cfg['tracked_columns'])\n"
                "    parent_hub = sat_cfg['parent_hub']\n"
                "    spark.sql(f\"\"\"\n"
                "        CREATE TABLE IF NOT EXISTS {table} (\n"
                "            HK_{parent_hub[4:]} STRING NOT NULL,\n"
                "            LOAD_DATE TIMESTAMP NOT NULL,\n"
                "            DIFF_HK STRING,\n"
                "            {tracked_cols},\n"
                "            RECORD_SOURCE STRING\n"
                "        ) USING DELTA\n"
                "    \"\"\")"
            ),
            self._make_cell(
                "def create_pit_table(pit_cfg: dict) -> None:\n"
                '    """Create a PIT Delta table if it does not exist."""\n'
                "    table = f\"{CATALOG}.{VAULT_SCHEMA}.{pit_cfg['name'].lower()}\"\n"
                "    hub_name = pit_cfg['hub']\n"
                "    sat_cols = '\\n'.join(f\"    {s}_LDTS TIMESTAMP,\\n    {s}_HK STRING,\" for s in pit_cfg['satellites'])\n"
                "    spark.sql(f\"\"\"\n"
                "        CREATE TABLE IF NOT EXISTS {table} (\n"
                "            HK_{hub_name[4:]} STRING NOT NULL,\n"
                "            SNAPSHOT_DATE DATE NOT NULL,\n"
                "            {sat_cols}\n"
                "            LOAD_DATE TIMESTAMP\n"
                "        ) USING DELTA\n"
                "    \"\"\")\n"
                "\n"
                "def create_bridge_table(brg_cfg: dict) -> None:\n"
                '    """Create a bridge Delta table if it does not exist."""\n'
                "    table = f\"{CATALOG}.{VAULT_SCHEMA}.{brg_cfg['name'].lower()}\"\n"
                "    hk_cols = '\\n'.join(f\"    HK_{p[4:]} STRING,\" for p in brg_cfg['path'] if p.startswith('HUB_'))\n"
                "    spark.sql(f\"\"\"\n"
                "        CREATE TABLE IF NOT EXISTS {table} (\n"
                "            {hk_cols}\n"
                "            LOAD_DATE TIMESTAMP\n"
                "        ) USING DELTA\n"
                "    \"\"\")"
            ),
            self._make_cell(
                "from pyspark.sql import Window\n"
                "\n"
                "def get_latest_diff_hash(sat_table: str, hk_col: str) -> 'DataFrame':\n"
                '    """Return the latest DIFF_HK per hub key from an existing satellite table.\n'
                "\n"
                "    Uses a window function to find the most recent record per hub key,\n"
                "    which is then LEFT JOINed against incoming data to detect changes.\n"
                '    """\n'
                "    w = Window.partitionBy(hk_col).orderBy(F.col('LOAD_DATE').desc())\n"
                "    return (\n"
                "        spark.table(sat_table)\n"
                "        .withColumn('_rn', F.row_number().over(w))\n"
                "        .filter(F.col('_rn') == 1)\n"
                "        .select(hk_col, 'DIFF_HK')\n"
                "    )"
            ),
        ]
        self._write_nb("NB_dv_metadata", self._make_notebook(cells))

    # ------------------------------------------------------------------
    # NB_ingest_to_hubs
    # ------------------------------------------------------------------

    def _gen_hubs_nb(self) -> None:
        cells = [
            self._make_cell(
                "# NB_ingest_to_hubs — Silver → Hubs\n\n"
                "Reads each Silver source table defined in the DV model config,\n"
                "computes the SHA-256 hash key from business key columns, and performs\n"
                "an INSERT-only MERGE into the corresponding vault hub table.\n"
                "No updates — hubs are insert-only by DV 2.0 design.",
                "markdown",
            ),
            self._make_cell(
                "# Load shared helpers\n"
                "%run ../helpers/NB_catalog_helpers\n"
                "%run ./NB_dv_metadata"
            ),
            self._make_cell(
                'dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog name")\n'
                'dbutils.widgets.text("VAULT_SCHEMA", "vault", "Vault schema")\n'
                'dbutils.widgets.text("SILVER_SCHEMA", "silver", "Silver schema")\n'
                'dbutils.widgets.text("MODEL_PATH", "pipeline_configs/datavault/dv_model.json", "DV Model JSON Path")\n'
                'dbutils.widgets.text("WATERMARK_TS", "", "Optional: only process records >= this timestamp")\n'
                "\n"
                'CATALOG       = dbutils.widgets.get("CATALOG")\n'
                'VAULT_SCHEMA  = dbutils.widgets.get("VAULT_SCHEMA")\n'
                'SILVER_SCHEMA = dbutils.widgets.get("SILVER_SCHEMA")\n'
                'MODEL_PATH    = dbutils.widgets.get("MODEL_PATH")\n'
                'WATERMARK_TS  = dbutils.widgets.get("WATERMARK_TS")'
            ),
            self._make_cell(
                "model = load_model(MODEL_PATH)\n"
                'print(f"Loaded {len(model[\'hubs\'])} hubs from model")'
            ),
            self._make_cell(
                "from pyspark.sql import functions as F\n"
                "from delta.tables import DeltaTable\n"
                "\n"
                "hub_counts = {}\n"
                "\n"
                "for hub_cfg in model['hubs']:\n"
                "    if not hub_cfg.get('enabled', True):\n"
                "        print(f\"  Skipping disabled hub: {hub_cfg['name']}\")\n"
                "        continue\n"
                "\n"
                "    hub_name    = hub_cfg['name']\n"
                "    source_tbl  = hub_cfg['source_table']\n"
                "    bk_cols     = hub_cfg['business_key_columns']\n"
                "    load_dt_col = hub_cfg['load_date_column']\n"
                "    rec_src     = hub_cfg['record_source']\n"
                "    hk_alias    = f\"HK_{hub_name[4:]}\"\n"
                "    target_tbl  = f\"{CATALOG}.{VAULT_SCHEMA}.{hub_name.lower()}\"\n"
                "\n"
                "    # Ensure target table exists\n"
                "    create_hub_table(hub_cfg)\n"
                "\n"
                "    # Read Silver source\n"
                "    src_df = spark.table(source_tbl)\n"
                "    if WATERMARK_TS:\n"
                "        src_df = src_df.filter(F.col(load_dt_col) >= WATERMARK_TS)\n"
                "\n"
                "    # Compute hash key\n"
                "    src_df = (\n"
                "        src_df\n"
                "        .withColumn(hk_alias, generate_hash_key(bk_cols))\n"
                "        .withColumn('LOAD_DATE', F.col(load_dt_col).cast('timestamp'))\n"
                "        .withColumn('RECORD_SOURCE', F.lit(rec_src))\n"
                "        .select(hk_alias, *bk_cols, 'LOAD_DATE', 'RECORD_SOURCE')\n"
                "        .dropDuplicates([hk_alias])\n"
                "    )\n"
                "\n"
                "    # INSERT-only MERGE (DV 2.0: hubs never update)\n"
                "    hub_tbl = DeltaTable.forName(spark, target_tbl)\n"
                "    (\n"
                "        hub_tbl.alias('tgt')\n"
                "        .merge(src_df.alias('src'), f\"tgt.{hk_alias} = src.{hk_alias}\")\n"
                "        .whenNotMatchedInsertAll()\n"
                "        .execute()\n"
                "    )\n"
                "\n"
                "    count = spark.table(target_tbl).count()\n"
                "    hub_counts[hub_name] = count\n"
                "    print(f\"  {hub_name}: {count:,} total rows in {target_tbl}\")"
            ),
            self._make_cell(
                "print('\\nHub ingestion complete.')\n"
                "for hub_name, cnt in hub_counts.items():\n"
                "    print(f'  {hub_name}: {cnt:,} rows')"
            ),
        ]
        self._write_nb("NB_ingest_to_hubs", self._make_notebook(cells))

    # ------------------------------------------------------------------
    # NB_ingest_to_links
    # ------------------------------------------------------------------

    def _gen_links_nb(self) -> None:
        cells = [
            self._make_cell(
                "# NB_ingest_to_links — Silver → Links\n\n"
                "Reads each Silver source table defined in the DV model config,\n"
                "resolves FK hash keys for each hub reference, computes the composite\n"
                "link hash key, and performs an INSERT-only MERGE into the vault link table.\n"
                "Links are insert-only by DV 2.0 design.",
                "markdown",
            ),
            self._make_cell(
                "%run ../helpers/NB_catalog_helpers\n"
                "%run ./NB_dv_metadata"
            ),
            self._make_cell(
                'dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog name")\n'
                'dbutils.widgets.text("VAULT_SCHEMA", "vault", "Vault schema")\n'
                'dbutils.widgets.text("SILVER_SCHEMA", "silver", "Silver schema")\n'
                'dbutils.widgets.text("MODEL_PATH", "pipeline_configs/datavault/dv_model.json", "DV Model JSON Path")\n'
                'dbutils.widgets.text("WATERMARK_TS", "", "Optional: only process records >= this timestamp")\n'
                "\n"
                'CATALOG       = dbutils.widgets.get("CATALOG")\n'
                'VAULT_SCHEMA  = dbutils.widgets.get("VAULT_SCHEMA")\n'
                'SILVER_SCHEMA = dbutils.widgets.get("SILVER_SCHEMA")\n'
                'MODEL_PATH    = dbutils.widgets.get("MODEL_PATH")\n'
                'WATERMARK_TS  = dbutils.widgets.get("WATERMARK_TS")\n'
                "\n"
                "model = load_model(MODEL_PATH)\n"
                'print(f"Loaded {len(model[\'links\'])} links from model")'
            ),
            self._make_cell(
                "from pyspark.sql import functions as F\n"
                "from delta.tables import DeltaTable\n"
                "\n"
                "link_counts = {}\n"
                "\n"
                "for lnk_cfg in model['links']:\n"
                "    if not lnk_cfg.get('enabled', True):\n"
                "        print(f\"  Skipping disabled link: {lnk_cfg['name']}\")\n"
                "        continue\n"
                "\n"
                "    lnk_name    = lnk_cfg['name']\n"
                "    source_tbl  = lnk_cfg['source_table']\n"
                "    hub_refs    = lnk_cfg['hub_references']\n"
                "    load_dt_col = lnk_cfg['load_date_column']\n"
                "    rec_src     = lnk_cfg['record_source']\n"
                "    lk_alias    = f\"HK_{lnk_name[4:]}\"\n"
                "    target_tbl  = f\"{CATALOG}.{VAULT_SCHEMA}.{lnk_name.lower()}\"\n"
                "\n"
                "    # Ensure target table exists\n"
                "    create_link_table(lnk_cfg)\n"
                "\n"
                "    # Read Silver source\n"
                "    src_df = spark.table(source_tbl)\n"
                "    if WATERMARK_TS:\n"
                "        src_df = src_df.filter(F.col(load_dt_col) >= WATERMARK_TS)\n"
                "\n"
                "    # Compute FK hash keys for each hub reference\n"
                "    for ref in hub_refs:\n"
                "        hub_n  = ref['hub']\n"
                "        src_col = ref['source_column']\n"
                "        hk_col  = f\"HK_{hub_n[4:]}\"\n"
                "        src_df = src_df.withColumn(\n"
                "            hk_col,\n"
                "            F.sha2(F.concat_ws('||', F.upper(F.trim(F.col(src_col).cast('string')))), 256)\n"
                "        )\n"
                "\n"
                "    # Composite link hash key from all FK source columns\n"
                "    fk_src_cols = [ref['source_column'] for ref in hub_refs]\n"
                "    src_df = (\n"
                "        src_df\n"
                "        .withColumn(lk_alias, generate_hash_key(fk_src_cols))\n"
                "        .withColumn('LOAD_DATE', F.col(load_dt_col).cast('timestamp'))\n"
                "        .withColumn('RECORD_SOURCE', F.lit(rec_src))\n"
                "    )\n"
                "\n"
                "    # Select only the columns that exist in the target schema\n"
                "    hk_cols = [f\"HK_{ref['hub'][4:]}\" for ref in hub_refs]\n"
                "    src_df = src_df.select(lk_alias, *hk_cols, 'LOAD_DATE', 'RECORD_SOURCE').dropDuplicates([lk_alias])\n"
                "\n"
                "    # INSERT-only MERGE\n"
                "    lnk_tbl = DeltaTable.forName(spark, target_tbl)\n"
                "    (\n"
                "        lnk_tbl.alias('tgt')\n"
                "        .merge(src_df.alias('src'), f\"tgt.{lk_alias} = src.{lk_alias}\")\n"
                "        .whenNotMatchedInsertAll()\n"
                "        .execute()\n"
                "    )\n"
                "\n"
                "    count = spark.table(target_tbl).count()\n"
                "    link_counts[lnk_name] = count\n"
                "    print(f\"  {lnk_name}: {count:,} total rows in {target_tbl}\")\n"
                "\n"
                "print('\\nLink ingestion complete.')"
            ),
        ]
        self._write_nb("NB_ingest_to_links", self._make_notebook(cells))

    # ------------------------------------------------------------------
    # NB_ingest_to_satellites
    # ------------------------------------------------------------------

    def _gen_satellites_nb(self) -> None:
        cells = [
            self._make_cell(
                "# NB_ingest_to_satellites — Silver → Satellites (append-only)\n\n"
                "For each satellite defined in the model config:\n"
                "1. Compute the hub hash key (HK) and diff hash (DIFF_HK) from tracked columns\n"
                "2. LEFT JOIN against the latest DIFF_HK already stored in the satellite\n"
                "3. INSERT only rows where DIFF_HK has changed (new row) or no previous row exists\n\n"
                "Satellites are **append-only** — no updates or deletes, change detection via DIFF_HK.",
                "markdown",
            ),
            self._make_cell(
                "%run ../helpers/NB_catalog_helpers\n"
                "%run ./NB_dv_metadata"
            ),
            self._make_cell(
                'dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog name")\n'
                'dbutils.widgets.text("VAULT_SCHEMA", "vault", "Vault schema")\n'
                'dbutils.widgets.text("SILVER_SCHEMA", "silver", "Silver schema")\n'
                'dbutils.widgets.text("MODEL_PATH", "pipeline_configs/datavault/dv_model.json", "DV Model JSON Path")\n'
                'dbutils.widgets.text("WATERMARK_TS", "", "Optional: only process records >= this timestamp")\n'
                "\n"
                'CATALOG       = dbutils.widgets.get("CATALOG")\n'
                'VAULT_SCHEMA  = dbutils.widgets.get("VAULT_SCHEMA")\n'
                'SILVER_SCHEMA = dbutils.widgets.get("SILVER_SCHEMA")\n'
                'MODEL_PATH    = dbutils.widgets.get("MODEL_PATH")\n'
                'WATERMARK_TS  = dbutils.widgets.get("WATERMARK_TS")\n'
                "\n"
                "model = load_model(MODEL_PATH)\n"
                'print(f"Loaded {len(model[\'satellites\'])} satellites from model")'
            ),
            self._make_cell(
                "from pyspark.sql import functions as F\n"
                "from delta.tables import DeltaTable\n"
                "\n"
                "sat_new_rows = {}\n"
                "\n"
                "for sat_cfg in model['satellites']:\n"
                "    if not sat_cfg.get('enabled', True):\n"
                "        print(f\"  Skipping disabled satellite: {sat_cfg['name']}\")\n"
                "        continue\n"
                "\n"
                "    sat_name      = sat_cfg['name']\n"
                "    source_tbl    = sat_cfg['source_table']\n"
                "    parent_hub    = sat_cfg['parent_hub']\n"
                "    hk_src_col    = sat_cfg['hub_key_source_column']\n"
                "    tracked_cols  = sat_cfg['tracked_columns']\n"
                "    load_dt_col   = sat_cfg['load_date_column']\n"
                "    rec_src       = sat_cfg['record_source']\n"
                "    hk_col        = f\"HK_{parent_hub[4:]}\"\n"
                "    target_tbl    = f\"{CATALOG}.{VAULT_SCHEMA}.{sat_name.lower()}\"\n"
                "\n"
                "    # Ensure target table exists\n"
                "    create_sat_table(sat_cfg)\n"
                "\n"
                "    # Read Silver source\n"
                "    src_df = spark.table(source_tbl)\n"
                "    if WATERMARK_TS:\n"
                "        src_df = src_df.filter(F.col(load_dt_col) >= WATERMARK_TS)\n"
                "\n"
                "    # Compute hub hash key and diff hash\n"
                "    src_df = (\n"
                "        src_df\n"
                "        .withColumn(hk_col, F.sha2(\n"
                "            F.concat_ws('||', F.upper(F.trim(F.col(hk_src_col).cast('string')))), 256))\n"
                "        .withColumn('DIFF_HK', generate_diff_hash(tracked_cols))\n"
                "        .withColumn('LOAD_DATE', F.col(load_dt_col).cast('timestamp'))\n"
                "        .withColumn('RECORD_SOURCE', F.lit(rec_src))\n"
                "        .select(hk_col, 'LOAD_DATE', 'DIFF_HK', *tracked_cols, 'RECORD_SOURCE')\n"
                "    )\n"
                "\n"
                "    # Get latest DIFF_HK already stored per hub key (for change detection)\n"
                "    try:\n"
                "        latest_hk_df = get_latest_diff_hash(target_tbl, hk_col)\n"
                "        # LEFT JOIN: keep only rows where DIFF_HK changed or no prior row\n"
                "        new_rows_df = (\n"
                "            src_df.alias('src')\n"
                "            .join(latest_hk_df.alias('lat'), on=hk_col, how='left')\n"
                "            .filter(\n"
                "                F.col('lat.DIFF_HK').isNull() |\n"
                "                (F.col('src.DIFF_HK') != F.col('lat.DIFF_HK'))\n"
                "            )\n"
                "            .select('src.*')\n"
                "        )\n"
                "    except Exception:\n"
                "        # Table is empty — insert all rows\n"
                "        new_rows_df = src_df\n"
                "\n"
                "    # Append new/changed rows only\n"
                "    new_rows_df.write.format('delta').mode('append').saveAsTable(target_tbl)\n"
                "\n"
                "    new_count = new_rows_df.count()\n"
                "    sat_new_rows[sat_name] = new_count\n"
                "    print(f\"  {sat_name}: {new_count:,} new rows appended → {target_tbl}\")"
            ),
            self._make_cell(
                "print('\\nSatellite ingestion complete.')\n"
                "for sat_name, cnt in sat_new_rows.items():\n"
                "    print(f'  {sat_name}: {cnt:,} new rows')"
            ),
        ]
        self._write_nb("NB_ingest_to_satellites", self._make_notebook(cells))

    # ------------------------------------------------------------------
    # NB_dv_business_vault
    # ------------------------------------------------------------------

    def _gen_business_vault_nb(self) -> None:
        cells = [
            self._make_cell(
                "# NB_dv_business_vault — PIT & Bridge Tables\n\n"
                "Generates the business vault layer on top of the raw vault:\n"
                "- **PIT tables**: Point-in-Time snapshots aligning satellite versions to a date spine\n"
                "- **Bridge tables**: Multi-hop join shortcuts across hub/link chains\n\n"
                "Run this notebook daily (or on demand) after hub/link/satellite loads complete.",
                "markdown",
            ),
            self._make_cell(
                "%run ../helpers/NB_catalog_helpers\n"
                "%run ./NB_dv_metadata"
            ),
            self._make_cell(
                "from datetime import date\n"
                "\n"
                'dbutils.widgets.text("CATALOG", "workspace", "Unity Catalog name")\n'
                'dbutils.widgets.text("VAULT_SCHEMA", "vault", "Vault schema")\n'
                'dbutils.widgets.text("MODEL_PATH", "pipeline_configs/datavault/dv_model.json", "DV Model JSON Path")\n'
                'dbutils.widgets.text("SNAPSHOT_DATE", str(date.today()), "Snapshot date (YYYY-MM-DD)")\n'
                "\n"
                'CATALOG       = dbutils.widgets.get("CATALOG")\n'
                'VAULT_SCHEMA  = dbutils.widgets.get("VAULT_SCHEMA")\n'
                'MODEL_PATH    = dbutils.widgets.get("MODEL_PATH")\n'
                'SNAPSHOT_DATE = dbutils.widgets.get("SNAPSHOT_DATE")\n'
                "\n"
                "model = load_model(MODEL_PATH)\n"
                'print(f"Snapshot date: {SNAPSHOT_DATE}")\n'
                'print(f"PIT tables: {len(model.get(\'pit_tables\', []))}")\n'
                'print(f"Bridge tables: {len(model.get(\'bridge_tables\', []))}")'
            ),
            self._make_cell(
                "# ── PIT Tables ──────────────────────────────────────────────────────────────\n"
                "# For each PIT table: find the latest satellite record version as of SNAPSHOT_DATE\n"
                "# and write a snapshot row for each hub key.\n"
                "\n"
                "from pyspark.sql import functions as F, Window\n"
                "\n"
                "for pit_cfg in model.get('pit_tables', []):\n"
                "    if not pit_cfg.get('enabled', True):\n"
                "        print(f\"  Skipping disabled PIT: {pit_cfg['name']}\")\n"
                "        continue\n"
                "\n"
                "    pit_name   = pit_cfg['name']\n"
                "    hub_name   = pit_cfg['hub']\n"
                "    sat_names  = pit_cfg['satellites']\n"
                "    hk_col     = f\"HK_{hub_name[4:]}\"\n"
                "    target_tbl = f\"{CATALOG}.{VAULT_SCHEMA}.{pit_name.lower()}\"\n"
                "\n"
                "    # Ensure PIT table exists\n"
                "    create_pit_table(pit_cfg)\n"
                "\n"
                "    # Start with hub keys as the spine\n"
                "    hub_tbl  = f\"{CATALOG}.{VAULT_SCHEMA}.{hub_name.lower()}\"\n"
                "    spine_df = spark.table(hub_tbl).select(hk_col).distinct()\n"
                "    spine_df = spine_df.withColumn('SNAPSHOT_DATE', F.lit(SNAPSHOT_DATE).cast('date'))\n"
                "    spine_df = spine_df.withColumn('LOAD_DATE', F.current_timestamp())\n"
                "\n"
                "    # For each satellite: find max LOAD_DATE <= SNAPSHOT_DATE\n"
                "    for sat_name in sat_names:\n"
                "        sat_tbl = f\"{CATALOG}.{VAULT_SCHEMA}.{sat_name.lower()}\"\n"
                "        try:\n"
                "            sat_df = (\n"
                "                spark.table(sat_tbl)\n"
                "                .filter(F.col('LOAD_DATE').cast('date') <= F.lit(SNAPSHOT_DATE))\n"
                "            )\n"
                "            w = Window.partitionBy(hk_col).orderBy(F.col('LOAD_DATE').desc())\n"
                "            sat_latest = (\n"
                "                sat_df\n"
                "                .withColumn('_rn', F.row_number().over(w))\n"
                "                .filter(F.col('_rn') == 1)\n"
                "                .select(hk_col,\n"
                "                        F.col('LOAD_DATE').alias(f'{sat_name}_LDTS'),\n"
                "                        F.col('DIFF_HK').alias(f'{sat_name}_HK'))\n"
                "            )\n"
                "            spine_df = spine_df.join(sat_latest, on=hk_col, how='left')\n"
                "        except Exception as e:\n"
                "            print(f\"    Warning: could not join {sat_name}: {e}\")\n"
                "\n"
                "    # Delete existing snapshot for this date, then insert\n"
                "    try:\n"
                "        spark.sql(f\"DELETE FROM {target_tbl} WHERE SNAPSHOT_DATE = '{SNAPSHOT_DATE}'\")\n"
                "    except Exception:\n"
                "        pass\n"
                "    spine_df.write.format('delta').mode('append').saveAsTable(target_tbl)\n"
                "    print(f\"  {pit_name}: {spine_df.count():,} snapshot rows written for {SNAPSHOT_DATE}\")"
            ),
            self._make_cell(
                "# ── Bridge Tables ────────────────────────────────────────────────────────────\n"
                "# Build multi-hop join chains from the path definition.\n"
                "# Each path alternates: HUB → LINK → HUB → LINK → HUB ...\n"
                "\n"
                "for brg_cfg in model.get('bridge_tables', []):\n"
                "    if not brg_cfg.get('enabled', True):\n"
                "        print(f\"  Skipping disabled bridge: {brg_cfg['name']}\")\n"
                "        continue\n"
                "\n"
                "    brg_name   = brg_cfg['name']\n"
                "    path       = brg_cfg['path']\n"
                "    target_tbl = f\"{CATALOG}.{VAULT_SCHEMA}.{brg_name.lower()}\"\n"
                "\n"
                "    # Ensure bridge table exists\n"
                "    create_bridge_table(brg_cfg)\n"
                "\n"
                "    # Start with first hub\n"
                "    first_hub = path[0]\n"
                "    first_hk  = f\"HK_{first_hub[4:]}\"\n"
                "    result_df = spark.table(f\"{CATALOG}.{VAULT_SCHEMA}.{first_hub.lower()}\").select(first_hk)\n"
                "\n"
                "    # Walk the path: join each link then the next hub\n"
                "    i = 1\n"
                "    while i < len(path) - 1:\n"
                "        link_name = path[i]\n"
                "        next_hub  = path[i + 1]\n"
                "        link_hk   = f\"HK_{link_name[4:]}\"\n"
                "        src_hk    = f\"HK_{path[i-1][4:]}\"\n"
                "        tgt_hk    = f\"HK_{next_hub[4:]}\"\n"
                "\n"
                "        link_df = spark.table(f\"{CATALOG}.{VAULT_SCHEMA}.{link_name.lower()}\")\n"
                "        result_df = result_df.join(link_df, on=src_hk, how='inner').drop(link_hk)\n"
                "        i += 2\n"
                "\n"
                "    # Add load date and write\n"
                "    result_df = result_df.withColumn('LOAD_DATE', F.current_timestamp())\n"
                "    result_df.write.format('delta').mode('overwrite').saveAsTable(target_tbl)\n"
                "    print(f\"  {brg_name}: {result_df.count():,} rows written to {target_tbl}\")\n"
                "\n"
                "print('\\nBusiness vault refresh complete.')"
            ),
        ]
        self._write_nb("NB_dv_business_vault", self._make_notebook(cells))
