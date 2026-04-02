# TASK_11: step7_applier.py — Applier (or Code Review Generator)

## File
`generators/dv_generator/steps/step7_applier.py`

## Purpose
The final step. Two paths:
- **Validation passed**: copy approved model and generated notebooks to their final destinations in the repo (`pipeline_configs/datavault/dv_model.json`, `notebooks/vault/*.ipynb`). Write `{session_dir}/07_final_model.json`.
- **Validation failed**: do NOT write anything to the repo. Instead generate `{session_dir}/07_code_review.md` — a human-readable report explaining each validation error with the exact fix required, plus the unchanged entity definition for context.

## Depends on
- `TASK_01: models.py`
- `TASK_03: session.py` — `Session`
- `TASK_10: step6_validator.py` — reads `06_validation_report.json`

## Inputs
- `{session_dir}/06_validation_report.json`
- `{session_dir}/03_dv_model_draft.json` or `{session_dir}/05_human_review.json` (the approved version)
- `{session_dir}/notebooks/*.ipynb` (generated in step3b)
- `Session` instance

## Outputs

### On validation pass
- `pipeline_configs/datavault/dv_model.json` — final approved config (overwrites if exists)
- `notebooks/vault/NB_dv_metadata.ipynb` (and 4 other notebooks)
- `{session_dir}/07_final_model.json` — same as `dv_model.json` (session archive)

### On validation fail
- `{session_dir}/07_code_review.md` — per-error explanation and fix guide

## `07_code_review.md` structure (on failure)

```markdown
# DV 2.0 Model Code Review — Validation Failed

**Session:** {session_id}
**Generated:** {timestamp}
**Errors:** {n}  **Warnings:** {m}

---

## Error V3 — LNK_RENTAL_CUSTOMER

**Message:** hub_reference 'HUB_RENTA' does not exist in hubs list

**Current value in 05_human_review.json:**
```json
{"hub": "HUB_RENTA", "source_column": "customer_id"}
```

**Fix:** Change `hub` to one of the valid hub names:
`HUB_FILM`, `HUB_RENTAL`, `HUB_PAYMENT`, `HUB_CUSTOMER`, ...

**To fix:** Edit `05_review_notebook.ipynb` cell 9, correct the hub name, re-run the save cell,
then resume: `python -m generators.dv_generator.main --resume {session_id} --from-step step6_validator`

---
(repeat for each error)

## Warnings (non-blocking)
...
```

## Key classes / functions

```python
import json, shutil
from pathlib import Path
from ..session import Session

class Applier:
    """Applies a validated DV model to the repo, or generates a code review on failure."""

    FINAL_CONFIG_PATH  = "pipeline_configs/datavault/dv_model.json"
    NOTEBOOKS_DEST_DIR = "notebooks/vault"

    def __init__(self, session: Session, repo_root: str = "."):
        self.session = session
        self.repo_root = Path(repo_root)
        self.session_dir = session.session_dir

    def run(self) -> bool:
        """Returns True if apply succeeded, False if code review was generated instead."""
        if self.session.is_step_done("step7_applier"):
            return True

        report = self._load_validation_report()

        if report["passed"]:
            self._apply(report)
            return True
        else:
            self._generate_code_review(report)
            return False

    def _apply(self, report: dict) -> None:
        """Copy artifacts to final repo locations."""
        # 1. Ensure target dirs exist
        config_path = self.repo_root / self.FINAL_CONFIG_PATH
        config_path.parent.mkdir(parents=True, exist_ok=True)

        # 2. Copy dv_model.json
        reviewed = self.session_dir / "05_human_review.json"
        shutil.copy(reviewed, config_path)
        shutil.copy(reviewed, self.session_dir / "07_final_model.json")

        # 3. Copy notebooks
        nb_src = self.session_dir / "notebooks"
        nb_dest = self.repo_root / self.NOTEBOOKS_DEST_DIR
        nb_dest.mkdir(parents=True, exist_ok=True)
        for nb_file in nb_src.glob("*.ipynb"):
            shutil.copy(nb_file, nb_dest / nb_file.name)

        self.session.mark_step_done("step7_applier")
        self.session.set_status("completed")
        print(f"✓ Applied: {config_path}")
        print(f"✓ Notebooks: {nb_dest}")

    def _generate_code_review(self, report: dict) -> None:
        """Generate 07_code_review.md for each validation error."""
        reviewed = json.loads((self.session_dir / "05_human_review.json").read_text())
        lines = [
            f"# DV 2.0 Model Code Review — Validation Failed",
            f"\n**Session:** {self.session.state.session_id}",
            f"**Errors:** {report['error_count']}  **Warnings:** {report['warning_count']}",
            "\n---\n",
        ]
        for error in report["errors"]:
            lines += self._format_error(error, reviewed)
        if report["warnings"]:
            lines.append("## Warnings (non-blocking)\n")
            for w in report["warnings"]:
                lines.append(f"- **{w['code']}** `{w['entity']}`: {w['message']}")

        out = self.session_dir / "07_code_review.md"
        out.write_text("\n".join(lines))
        self.session.set_status("failed")
        print(f"\n>>> Validation failed. See {out}")
        print(f">>> Fix errors in 05_review_notebook.ipynb, then resume:")
        print(f">>> python -m generators.dv_generator.main --resume {self.session.state.session_id} --from-step step6_validator")

    def _format_error(self, error: dict, model: dict) -> list[str]: ...
    def _load_validation_report(self) -> dict: ...
```

## Logic walkthrough
1. Load `06_validation_report.json`.
2. **Pass path**: copy `05_human_review.json` → `pipeline_configs/datavault/dv_model.json`. Copy all notebooks from `{session_dir}/notebooks/` → `notebooks/vault/`. Archive final model. Mark done + set status `completed`.
3. **Fail path**: for each error in the report, look up the offending entity in the reviewed model, format a markdown block with: error code + message, current JSON value, suggested fix, exact resume command. Write `07_code_review.md`. Set status `failed`. Print actionable instructions.
4. `_generate_code_review` does NOT mark `step7_applier` as done — allows the human to fix and re-run `step6_validator` + `step7_applier`.

## Acceptance criteria
- On passing validation: `pipeline_configs/datavault/dv_model.json` exists and is valid JSON
- On passing validation: `notebooks/vault/` contains all 5 `.ipynb` files
- On failing validation: `07_code_review.md` contains one section per error
- Each error section includes the exact current JSON value and a fix suggestion
- Each error section includes the exact `--resume --from-step` command
- Session status is `"completed"` on success, `"failed"` on failure
- Apply is idempotent: running twice with the same valid model doesn't corrupt anything
