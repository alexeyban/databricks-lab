# TASK_10: step6_validator.py — Model Validator

## File
`generators/dv_generator/steps/step6_validator.py`

## Purpose
Validates the human-reviewed model (`05_human_review.json`) against structural DV 2.0 rules and internal consistency checks. Produces `06_validation_report.json` with a pass/fail verdict, a list of errors, and a list of warnings. Step7 only proceeds if validation passes (zero errors).

## Depends on
- `TASK_01: models.py` — `DVModel`, `HubDef`, `LinkDef`, `SatDef`
- `TASK_03: session.py` — `Session`

## Inputs
- `{session_dir}/05_human_review.json` — human-edited model
- `{session_dir}/05_human_review_template.json` — pre-edit snapshot (for diff/change detection)
- `Session` instance

## Outputs
- `{session_dir}/06_validation_report.json`

### `06_validation_report.json` structure
```json
{
  "passed": false,
  "error_count": 2,
  "warning_count": 1,
  "errors": [
    {"code": "V3", "entity": "LNK_RENTAL_CUSTOMER", "message": "hub_reference 'HUB_RENTA' does not exist in hubs list"}
  ],
  "warnings": [
    {"code": "W2", "entity": "SAT_INVENTORY", "message": "Satellite has only 1 tracked column — consider merging with another satellite"}
  ],
  "diff_summary": {
    "hubs_changed": 0, "links_changed": 0, "satellites_changed": 1
  }
}
```

## Validation rules

### Errors (block proceed)
| Code | Check |
|------|-------|
| V1 | Every hub has `name`, `target_table`, `source_table`, `business_key_columns` (non-empty list), `load_date_column`, `record_source` |
| V2 | Every link has `hub_references` with ≥ 2 entries; each `hub` value exists in `hubs[].name` |
| V3 | Every link `hub_references[].hub` exists in the hubs list |
| V4 | Every satellite `parent_hub` exists in the hubs list |
| V5 | Every satellite has `tracked_columns` (non-empty list) |
| V6 | No duplicate names within each entity type (two hubs can't share a name) |
| V7 | Every PIT `hub` value exists in hubs list; every PIT `satellites[]` value exists in satellites list |
| V8 | Every Bridge `path[]` alternates Hub/Link names and all referenced names exist |
| V9 | `target_table` follows pattern `vault.{lowercase_name}` for all entities |

### Warnings (report but allow proceed)
| Code | Check |
|------|-------|
| W1 | Hub business_key_columns has > 1 column (composite BK — unusual, may indicate modeling error) |
| W2 | Satellite has only 1 tracked column (consider merging) |
| W3 | Link has > 3 hub_references (complex link — validate intentional) |
| W4 | Any entity has `enabled: false` (will be skipped at runtime — confirm intentional) |

## Key classes / functions

```python
import json
from pathlib import Path
from dataclasses import asdict
from ..models import DVModel, HubDef, LinkDef, SatDef, PitDef, BridgeDef
from ..session import Session

@dataclass
class ValidationIssue:
    code: str
    entity: str
    message: str

@dataclass
class ValidationReport:
    passed: bool
    errors: list[ValidationIssue]
    warnings: list[ValidationIssue]
    diff_summary: dict

class Validator:
    """Validates the human-reviewed DV model against DV 2.0 structural rules."""

    def __init__(self, session: Session):
        self.session = session
        self.review_path = session.session_dir / "05_human_review.json"
        self.template_path = session.session_dir / "05_human_review_template.json"
        self.output_path = session.session_dir / "06_validation_report.json"

    def run(self) -> ValidationReport:
        if self.session.is_step_done("step6_validator"):
            return self._load_cached()

        model_dict = json.loads(self.review_path.read_text())
        template_dict = json.loads(self.template_path.read_text())

        errors, warnings = [], []
        self._check_hubs(model_dict, errors, warnings)
        self._check_links(model_dict, errors, warnings)
        self._check_satellites(model_dict, errors, warnings)
        self._check_pit_tables(model_dict, errors, warnings)
        self._check_bridge_tables(model_dict, errors, warnings)
        self._check_uniqueness(model_dict, errors)

        diff = self._compute_diff(model_dict, template_dict)
        passed = len(errors) == 0

        report = ValidationReport(passed=passed, errors=errors, warnings=warnings, diff_summary=diff)
        self._save(report)

        if passed:
            self.session.mark_step_done("step6_validator")
        return report

    def _check_hubs(self, model: dict, errors: list, warnings: list) -> None:
        """Apply V1, W1 to all hubs."""

    def _check_links(self, model: dict, errors: list, warnings: list) -> None:
        """Apply V2, V3, W3."""

    def _check_satellites(self, model: dict, errors: list, warnings: list) -> None:
        """Apply V4, V5, W2."""

    def _check_pit_tables(self, model: dict, errors: list, warnings: list) -> None:
        """Apply V7."""

    def _check_bridge_tables(self, model: dict, errors: list, warnings: list) -> None:
        """Apply V8."""

    def _check_uniqueness(self, model: dict, errors: list) -> None:
        """Apply V6 and V9."""

    def _compute_diff(self, review: dict, template: dict) -> dict:
        """Count changed entities between pre- and post-human-edit model."""

    def _save(self, report: ValidationReport) -> None: ...
    def _load_cached(self) -> ValidationReport: ...
```

## Logic walkthrough
1. Load `05_human_review.json` and `05_human_review_template.json`.
2. Build a set of known hub names for V2/V3/V4/V7/V8 cross-reference checks.
3. Run all V-checks accumulating errors; run all W-checks accumulating warnings.
4. Compute diff: count how many hubs/links/sats differ between review and template.
5. `passed = len(errors) == 0`. Write `06_validation_report.json`.
6. If passed: `session.mark_step_done("step6_validator")`. If failed: do NOT mark done — step7 will see validation failed and generate code review instead.

## Acceptance criteria
- Valid dvdrental model passes with 0 errors
- Introducing a typo in a hub name referenced by a link → V3 error
- Removing `tracked_columns` from a satellite → V5 error
- Satellite with 1 tracked column → W2 warning (not an error)
- `passed: true` only when `errors` list is empty
- `diff_summary` correctly counts changed entities between template and review
- Session not marked done when errors > 0 (step7 must handle failed validation)
