# TASK_09: step5_review.py — Human Review Notebook Generator

## File
`generators/dv_generator/steps/step5_review.py`

## Purpose
Generates `05_review_notebook.ipynb` — a Jupyter notebook the human opens to review, correct, and approve the auto-generated DV 2.0 model. The notebook contains editable Python dicts (one per hub/link/sat), a summary table of the model, flagged LOW-confidence items, and a "save and validate" cell that writes `05_human_review.json`. The human edits the dicts in-place and runs the save cell; the generator resumes from step6 using the edited JSON.

## Depends on
- `TASK_01: models.py` — `DVModel`
- `TASK_02: decision_logger.py` — `DecisionLogger` (for flagged items)
- `TASK_03: session.py` — `Session`

## Inputs
- `DVModel` (from `03_dv_model_draft.json`)
- `DecisionLogger.read_flagged()` — LOW confidence items
- `Session` instance

## Outputs
- `{session_dir}/05_review_notebook.ipynb` — editable Jupyter notebook
- `{session_dir}/05_human_review_template.json` — snapshot of the model before edits (for diff tracking)

## Review notebook cell plan

| # | Type | Content |
|---|------|---------|
| 1 | Markdown | `# DV 2.0 Model Review` intro, instructions for the human |
| 2 | Markdown | `## Model Summary` stats table |
| 3 | Code | `import json, pandas as pd` + display summary DataFrame |
| 4 | Markdown | `## ⚠️ Flagged Items (LOW confidence)` |
| 5 | Code | Display `flagged_df` — DataFrame of LOW-confidence decisions |
| 6 | Markdown | `## Hubs — Edit below to correct hub definitions` |
| 7 | Code | `hubs = [<rendered list of hub dicts>]` — **this cell is human-editable** |
| 8 | Markdown | `## Links` |
| 9 | Code | `links = [<rendered list of link dicts>]` — **human-editable** |
| 10 | Markdown | `## Satellites` |
| 11 | Code | `satellites = [<rendered list of sat dicts>]` — **human-editable** |
| 12 | Markdown | `## PIT Tables` |
| 13 | Code | `pit_tables = [<rendered list>]` — **human-editable** |
| 14 | Markdown | `## Bridge Tables` |
| 15 | Code | `bridge_tables = [<rendered list>]` — **human-editable** |
| 16 | Markdown | `## Save & Validate — run this cell when done` |
| 17 | Code | Save cell (see below) |

### Save cell (cell 17)
```python
import json, pathlib
review = {
    "hubs": hubs,
    "links": links,
    "satellites": satellites,
    "pit_tables": pit_tables,
    "bridge_tables": bridge_tables,
}
out = pathlib.Path("{session_dir}/05_human_review.json")
out.write_text(json.dumps(review, indent=2))
print(f"Saved to {out}. Run: python -m generators.dv_generator.main --resume {session_id} --from-step step6_validator")
```

## Key classes / functions

```python
import json
from pathlib import Path
from dataclasses import asdict
from ..models import DVModel, HubDef, LinkDef, SatDef
from ..decision_logger import DecisionLogger
from ..session import Session

class ReviewGenerator:
    """Generates the human review notebook from a DVModel."""

    def __init__(self, model: DVModel, logger: DecisionLogger, session: Session):
        self.model = model
        self.logger = logger
        self.session = session
        self.output_dir = session.session_dir

    def run(self) -> None:
        if self.session.is_step_done("step5_review"):
            return
        self._write_template_json()
        self._write_review_notebook()
        self.session.mark_step_done("step5_review")
        self.session.set_status("awaiting_review")
        print(f"\n>>> Open {self.output_dir}/05_review_notebook.ipynb in Jupyter to review and approve the model.")
        print(f">>> After saving, resume with: python -m generators.dv_generator.main --resume {self.session.state.session_id} --from-step step6_validator")

    def _write_template_json(self) -> None:
        """Write 05_human_review_template.json as a baseline snapshot."""

    def _write_review_notebook(self) -> None:
        """Build and write 05_review_notebook.ipynb."""

    def _render_editable_list(self, items: list) -> str:
        """Render a Python list of dicts as pretty-printed source code."""
        return "[\n" + ",\n".join(f"    {json.dumps(item, indent=8)}" for item in items) + "\n]"

    def _make_cell(self, source: str, cell_type: str = "code") -> dict: ...
    def _write_nb(self, name: str, cells: list[dict]) -> None: ...
```

## Logic walkthrough
1. Write `05_human_review_template.json` — a snapshot of the model at this point (used by validator to detect what changed).
2. Build notebook cells: intro markdown, summary DataFrame code, flagged items DataFrame code, then one markdown + one editable code cell per entity type.
3. Editable cells contain `json.dumps`-rendered Python dicts — human can edit values directly in Jupyter.
4. Save cell hardcodes the session_dir path and session_id for the resume command.
5. After writing notebook, set session status to `"awaiting_review"` and print instructions.
6. Mark step done — generator pauses here until human runs `--resume`.

## Acceptance criteria
- `05_review_notebook.ipynb` is valid Jupyter JSON
- Cell 7 contains `hubs = [...]` with one dict per hub
- Cell 11 contains `satellites = [...]` with one dict per satellite
- Cell 17 contains `out.write_text(json.dumps(review, indent=2))` and the correct resume command
- `05_human_review_template.json` is valid JSON matching the model
- Session status is `"awaiting_review"` after `run()`
- If the human adds a key that doesn't exist in `HubDef`, the validator (step6) must catch it as an error
