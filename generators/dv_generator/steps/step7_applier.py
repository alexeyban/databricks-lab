"""
DV 2.0 Generator — Applier (TASK_11)

Final step. Two paths:
  - Validation passed: copy approved model + notebooks to final repo destinations.
  - Validation failed: generate 07_code_review.md with per-error explanations and fix guide.
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from ..session import Session


class Applier:
    """Applies a validated DV model to the repo, or generates a code review on failure."""

    FINAL_CONFIG_PATH = "pipeline_configs/datavault/dv_model.json"
    NOTEBOOKS_DEST_DIR = "notebooks/vault"
    STEP_NAME = "step7_applier"

    def __init__(self, session: Session, repo_root: str = ".") -> None:
        self.session = session
        self.repo_root = Path(repo_root)
        self.session_dir = session.session_dir

    def run(self) -> bool:
        """Run the applier. Returns True if apply succeeded, False if code review generated."""
        if self.session.is_step_done(self.STEP_NAME):
            return True

        report = self._load_validation_report()

        if report["passed"]:
            self._apply(report)
            return True
        else:
            self._generate_code_review(report)
            return False

    # ------------------------------------------------------------------
    # Pass path
    # ------------------------------------------------------------------

    def _apply(self, report: dict) -> None:
        """Copy artifacts to final repo locations and mark session completed."""
        config_path = self.repo_root / self.FINAL_CONFIG_PATH
        config_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy the human-reviewed (approved) model
        reviewed = self.session_dir / "05_human_review.json"
        shutil.copy(reviewed, config_path)
        shutil.copy(reviewed, self.session_dir / "07_final_model.json")

        # Copy generated vault notebooks
        nb_src = self.session_dir / "notebooks"
        nb_dest = self.repo_root / self.NOTEBOOKS_DEST_DIR
        nb_dest.mkdir(parents=True, exist_ok=True)
        copied_notebooks = []
        if nb_src.exists():
            for nb_file in sorted(nb_src.glob("*.ipynb")):
                dest_file = nb_dest / nb_file.name
                shutil.copy(nb_file, dest_file)
                copied_notebooks.append(nb_file.name)

        self.session.mark_step_done(
            self.STEP_NAME,
            metadata={
                "config_written": str(config_path),
                "notebooks_copied": copied_notebooks,
                "warnings": report.get("warning_count", 0),
            },
        )
        self.session.set_status("completed")

        print(f"Applied: {config_path}")
        print(f"Notebooks: {nb_dest} ({len(copied_notebooks)} files)")
        if report.get("warning_count", 0):
            print(f"  {report['warning_count']} warning(s) — see 06_validation_report.json")

    # ------------------------------------------------------------------
    # Fail path
    # ------------------------------------------------------------------

    def _generate_code_review(self, report: dict) -> None:
        """Generate 07_code_review.md with per-error explanations."""
        reviewed = json.loads((self.session_dir / "05_human_review.json").read_text())
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        lines: list[str] = [
            "# DV 2.0 Model Code Review — Validation Failed",
            "",
            f"**Session:** {self.session.state.session_id}",
            f"**Generated:** {ts}",
            f"**Errors:** {report['error_count']}  **Warnings:** {report['warning_count']}",
            "",
            "---",
            "",
        ]

        for error in report["errors"]:
            lines += self._format_error(error, reviewed)

        if report.get("warnings"):
            lines += [
                "## Warnings (non-blocking)",
                "",
            ]
            for w in report["warnings"]:
                lines.append(f"- **{w['code']}** `{w['entity']}`: {w['message']}")
            lines.append("")

        out = self.session_dir / "07_code_review.md"
        out.write_text("\n".join(lines))

        self.session.set_status("failed")

        print(f"\n>>> Validation failed ({report['error_count']} error(s)). See {out}")
        print(
            f">>> Fix errors in 05_review_notebook.ipynb, then resume:\n"
            f">>> python -m generators.dv_generator.main "
            f"--resume {self.session.state.session_id} --from-step step6_validator"
        )

    def _format_error(self, error: dict, model: dict) -> list[str]:
        """Format one error block with code, message, current JSON value, and fix guidance."""
        code = error.get("code", "?")
        entity_name = error.get("entity", "?")
        message = error.get("message", "")

        # Locate the offending entity in the model for context
        entity_json = self._find_entity(entity_name, model)
        entity_json_str = (
            json.dumps(entity_json, indent=2) if entity_json else "_entity not found_"
        )

        fix_hint = _FIX_HINTS.get(code, "Review the entity definition and correct the issue.")
        hub_names = [h.get("name") for h in model.get("hubs", []) if "name" in h]

        # Expand hub name list into the fix hint if placeholder present
        fix_hint = fix_hint.replace("{hub_names}", ", ".join(f"`{n}`" for n in hub_names[:8]))

        resume_cmd = (
            f"python -m generators.dv_generator.main "
            f"--resume {self.session.state.session_id} --from-step step6_validator"
        )

        return [
            f"## Error {code} — {entity_name}",
            "",
            f"**Message:** {message}",
            "",
            f"**Current value in 05_human_review.json:**",
            "```json",
            entity_json_str,
            "```",
            "",
            f"**Fix:** {fix_hint}",
            "",
            f"**To fix:** Edit `05_review_notebook.ipynb`, correct the issue, re-run the save cell, then resume:",
            f"```",
            resume_cmd,
            "```",
            "",
            "---",
            "",
        ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_entity(self, name: str, model: dict) -> dict | None:
        """Locate an entity by name across all entity types in the model."""
        for entity_type in ("hubs", "links", "satellites", "pit_tables", "bridge_tables"):
            for entity in model.get(entity_type, []):
                if entity.get("name") == name:
                    return entity
        return None

    def _load_validation_report(self) -> dict:
        path = self.session_dir / "06_validation_report.json"
        if not path.exists():
            raise FileNotFoundError(
                f"Validation report not found: {path}. Run step6_validator first."
            )
        return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Fix hint lookup by validation code
# ---------------------------------------------------------------------------

_FIX_HINTS: dict[str, str] = {
    "V1": (
        "Ensure the entity has all required fields: `name`, `target_table`, `source_table`, "
        "`business_key_columns` (non-empty list), `load_date_column`, `record_source`."
    ),
    "V2": (
        "Links must have at least 2 `hub_references` entries. Add the missing hub reference."
    ),
    "V3": (
        "Change the `hub` value to one of the valid hub names: {hub_names}."
    ),
    "V4": (
        "Change `parent_hub` to one of the valid hub names: {hub_names}."
    ),
    "V5": (
        "Add at least one column name to `tracked_columns` for this satellite."
    ),
    "V6": (
        "Rename the duplicate entity — each hub/link/satellite/pit/bridge must have a unique name."
    ),
    "V7": (
        "Ensure the PIT's `hub` and all `satellites` values reference valid entity names."
    ),
    "V8": (
        "Correct the bridge `path` list so it alternates Hub/Link/Hub/Link names "
        "and all referenced names exist in the model."
    ),
    "V9": (
        "Set `target_table` to `vault.<lowercase_table_name>` (e.g. `vault.hub_film`)."
    ),
}
