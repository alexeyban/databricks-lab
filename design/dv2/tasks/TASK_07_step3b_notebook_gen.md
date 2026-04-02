# TASK_07: step3b_notebook_gen.py — Vault Notebook Generator

## File
`generators/dv_generator/steps/step3b_notebook_gen.py`

## Purpose
Generates the five Databricks vault notebooks as `.ipynb` files, fully populated with executable PySpark code derived from `dv_model_draft.json`. Each notebook iterates over its entity type from the config and performs the correct DV 2.0 operation. Output lands in `{session_dir}/notebooks/` and is later copied to `notebooks/vault/` by the applier (step7).

## Depends on
- `TASK_01: models.py` — `DVModel`, `HubDef`, `LinkDef`, `SatDef`, `PitDef`, `BridgeDef`
- `TASK_03: session.py` — `Session`
- `TASK_06: step3_artifact_gen.py` — consumes model from `03_dv_model_draft.json`

## Inputs
- `DVModel` (fully populated, from `03_dv_model_draft.json`)
- `Session` instance

## Outputs
Five `.ipynb` files in `{session_dir}/notebooks/`:
```
NB_dv_metadata.ipynb
NB_ingest_to_hubs.ipynb
NB_ingest_to_links.ipynb
NB_ingest_to_satellites.ipynb
NB_dv_business_vault.ipynb
```

## Notebook cell structure

### Jupyter notebook JSON format
```python
# Build a minimal valid .ipynb dict
notebook = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
    "cells": [cell, ...]
}
# Each cell:
{"cell_type": "code", "source": "...", "metadata": {}, "outputs": [], "execution_count": None}
{"cell_type": "markdown", "source": "## Section heading", "metadata": {}}
```

## NB_dv_metadata — cell plan
| # | Type | Content |
|---|------|---------|
| 1 | Markdown | `# NB_dv_metadata — DV 2.0 Shared Helpers` + description |
| 2 | Code | Widget params: `MODEL_PATH`, `CATALOG`, `VAULT_SCHEMA` |
| 3 | Code | `load_model()` — reads `dv_model.json`, returns dict |
| 4 | Code | `generate_hash_key(bk_cols)` — returns PySpark Column expr |
| 5 | Code | `generate_diff_hash(tracked_cols)` — returns PySpark Column expr |
| 6 | Code | `create_hub_table(hub_cfg)`, `create_link_table(lnk_cfg)`, `create_sat_table(sat_cfg)` |
| 7 | Code | `create_pit_table(pit_cfg)`, `create_bridge_table(brg_cfg)` |
| 8 | Code | `get_latest_diff_hash(sat_table, hk_col)` — window function over satellite |

## NB_ingest_to_hubs — cell plan
| # | Type | Content |
|---|------|---------|
| 1 | Markdown | `# NB_ingest_to_hubs — Silver → Hubs` |
| 2 | Code | `%run ../helpers/NB_catalog_helpers` + `%run ./NB_dv_metadata` |
| 3 | Code | Widget params: `CATALOG`, `VAULT_SCHEMA`, `SILVER_SCHEMA`, `MODEL_PATH`, `WATERMARK_TS` |
| 4 | Code | `model = load_model(MODEL_PATH)` |
| 5 | Code | Loop over `model["hubs"]` — for each hub: read Silver, compute hash key, MERGE insert-only |
| 6 | Code | Log counts per hub |

*One loop iteration per hub; loop body is templated generically from hub config.*

## NB_ingest_to_links — cell plan
| # | Type | Content |
|---|------|---------|
| 1 | Markdown | `# NB_ingest_to_links — Silver → Links` |
| 2 | Code | `%run` dependencies |
| 3 | Code | Widget params |
| 4 | Code | Loop over `model["links"]` — resolve FK hash keys, compute composite link hash, MERGE insert-only |

## NB_ingest_to_satellites — cell plan
| # | Type | Content |
|---|------|---------|
| 1 | Markdown | `# NB_ingest_to_satellites — Silver → Satellites (append-only)` |
| 2 | Code | `%run` dependencies |
| 3 | Code | Widget params |
| 4 | Code | Loop over `model["satellites"]`: compute DIFF_HK, LEFT JOIN latest DIFF_HK per hub key, INSERT new rows only |
| 5 | Code | Log new rows appended per satellite |

## NB_dv_business_vault — cell plan
| # | Type | Content |
|---|------|---------|
| 1 | Markdown | `# NB_dv_business_vault — PIT & Bridge Tables` |
| 2 | Code | `%run` dependencies |
| 3 | Code | Widget params: add `SNAPSHOT_DATE` (default today) |
| 4 | Code | PIT loop: generate date spine, window function to find max LOAD_DATE ≤ snapshot_date, write PIT |
| 5 | Code | Bridge loop: build multi-hop join chain from `path`, write bridge |

## Key classes / functions

```python
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

    def __init__(self, model: DVModel, session: Session):
        self.model = model
        self.session = session
        self.output_dir = session.session_dir / "notebooks"

    def run(self) -> None:
        if self.session.is_step_done("step3b_notebook_gen"):
            return
        self.output_dir.mkdir(exist_ok=True)
        self._gen_metadata_nb()
        self._gen_hubs_nb()
        self._gen_links_nb()
        self._gen_satellites_nb()
        self._gen_business_vault_nb()
        self.session.mark_step_done("step3b_notebook_gen")

    def _make_cell(self, source: str, cell_type: str = "code") -> dict: ...
    def _make_notebook(self, cells: list[dict]) -> dict: ...
    def _write_nb(self, name: str, notebook: dict) -> None: ...
    def _gen_metadata_nb(self) -> None: ...
    def _gen_hubs_nb(self) -> None: ...
    def _gen_links_nb(self) -> None: ...
    def _gen_satellites_nb(self) -> None: ...
    def _gen_business_vault_nb(self) -> None: ...
```

## Logic walkthrough
Each `_gen_*_nb()` method builds a list of cell dicts using `_make_cell()`, assembles a notebook dict, and writes it via `_write_nb()`. Code cells contain f-string rendered Python code that references the model config at runtime (not hardcoded entity names — the notebooks loop over the JSON config).

## Acceptance criteria
- 5 `.ipynb` files produced in `{session_dir}/notebooks/`
- Each file is valid Jupyter notebook JSON (parseable by `nbformat.read`)
- `NB_ingest_to_hubs.ipynb` contains a cell with a loop over `model["hubs"]`
- `NB_ingest_to_satellites.ipynb` contains DIFF_HK computation and LEFT JOIN pattern
- `NB_dv_business_vault.ipynb` contains PIT snapshot spine logic
- No hardcoded entity names (HUB_FILM etc.) in notebook cells — all driven by model config loop
