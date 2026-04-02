"""
DV 2.0 Generator — Human Review Notebook Generator (TASK_09)

Generates 05_review_notebook.ipynb — a Jupyter notebook the human opens to
review, correct, and approve the auto-generated DV 2.0 model. The notebook
contains editable Python dicts, flagged LOW-confidence items, and a save cell
that writes 05_human_review.json for step6_validator to consume.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from ..decision_logger import DecisionLogger
from ..models import DVModel, HubDef, LinkDef, SatDef
from ..session import Session
from .step2_rule_engine import _model_to_dict


class ReviewGenerator:
    """Generates the human review notebook from a DVModel."""

    def __init__(self, model: DVModel, logger: DecisionLogger, session: Session) -> None:
        self.model = model
        self.logger = logger
        self.session = session
        self.output_dir = session.session_dir

    def run(self) -> None:
        """Write template JSON and review notebook; pause for human review."""
        if self.session.is_step_done("step5_review"):
            return
        self._write_template_json()
        self._write_review_notebook()
        self.session.mark_step_done("step5_review")
        self.session.set_status("awaiting_review")
        nb_path = self.output_dir / "05_review_notebook.ipynb"
        print(
            f"\n>>> Open {nb_path} in Jupyter to review and approve the model."
        )
        print(
            f">>> After saving, resume with: "
            f"python -m generators.dv_generator.main "
            f"--resume {self.session.state.session_id} --from-step step6_validator"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _write_template_json(self) -> None:
        """Write 05_human_review_template.json as a baseline snapshot."""
        out = self.output_dir / "05_human_review_template.json"
        out.write_text(json.dumps(_model_to_dict(self.model), indent=2))

    def _render_editable_list(self, items: list) -> str:
        """Render a Python list of dicts as pretty-printed source code."""
        return "[\n" + ",\n".join(f"    {json.dumps(item, indent=8)}" for item in items) + "\n]"

    def _make_cell(self, source: str, cell_type: str = "code") -> dict:
        cell: dict = {
            "cell_type": cell_type,
            "source": source,
            "metadata": {},
        }
        if cell_type == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        return cell

    def _write_nb(self, name: str, cells: list[dict]) -> None:
        notebook = {
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
        (self.output_dir / f"{name}.ipynb").write_text(json.dumps(notebook, indent=2))

    # ------------------------------------------------------------------
    # Review notebook construction
    # ------------------------------------------------------------------

    def _write_review_notebook(self) -> None:
        """Build and write 05_review_notebook.ipynb."""
        model_dict = _model_to_dict(self.model)
        flagged = self.logger.read_flagged()

        session_id = self.session.state.session_id
        session_dir = str(self.output_dir)

        # ── Cell 1: Intro markdown ───────────────────────────────────────
        cell1 = self._make_cell(
            "# DV 2.0 Model Review\n\n"
            "Review the auto-generated Data Vault 2.0 model below.\n\n"
            "**Instructions:**\n"
            "1. Review each section (Hubs, Links, Satellites, PIT, Bridge)\n"
            "2. Edit any incorrect values directly in the code cells\n"
            "3. Run all cells in order\n"
            "4. Run the final **Save & Validate** cell to write `05_human_review.json`\n"
            "5. Follow the printed resume instructions to continue the generator\n\n"
            "Items marked ⚠️ in the flagged section have LOW confidence and should be reviewed carefully.",
            "markdown",
        )

        # ── Cell 2: Model summary markdown ──────────────────────────────
        n_hubs = len(self.model.hubs)
        n_links = len(self.model.links)
        n_sats = len(self.model.satellites)
        n_pits = len(self.model.pit_tables)
        n_brgs = len(self.model.bridge_tables)

        cell2 = self._make_cell(
            f"## Model Summary\n\n"
            f"| Entity | Count |\n"
            f"|--------|-------|\n"
            f"| Hubs | {n_hubs} |\n"
            f"| Links | {n_links} |\n"
            f"| Satellites | {n_sats} |\n"
            f"| PIT Tables | {n_pits} |\n"
            f"| Bridge Tables | {n_brgs} |",
            "markdown",
        )

        # ── Cell 3: Summary DataFrame code ──────────────────────────────
        cell3 = self._make_cell(
            "import json\n"
            "import pandas as pd\n"
            "\n"
            "summary_data = [\n"
            f"    {{'Entity Type': 'Hubs',           'Count': {n_hubs}}},\n"
            f"    {{'Entity Type': 'Links',          'Count': {n_links}}},\n"
            f"    {{'Entity Type': 'Satellites',     'Count': {n_sats}}},\n"
            f"    {{'Entity Type': 'PIT Tables',     'Count': {n_pits}}},\n"
            f"    {{'Entity Type': 'Bridge Tables',  'Count': {n_brgs}}},\n"
            "]\n"
            "pd.DataFrame(summary_data)"
        )

        # ── Cell 4: Flagged items markdown ──────────────────────────────
        cell4 = self._make_cell(
            "## ⚠️ Flagged Items (LOW confidence)\n\n"
            "These items were classified with LOW confidence and need human validation.\n"
            "If correct as-is, no action needed. If wrong, fix in the cells below.",
            "markdown",
        )

        # ── Cell 5: Flagged items DataFrame ─────────────────────────────
        if flagged:
            flagged_rows = [
                {
                    "step": e.step,
                    "entity": e.entity,
                    "rule": e.rule,
                    "reason": e.reason,
                    "timestamp": e.timestamp,
                }
                for e in flagged
            ]
            flagged_json = json.dumps(flagged_rows, indent=4)
            cell5 = self._make_cell(
                f"flagged_data = {flagged_json}\n"
                "flagged_df = pd.DataFrame(flagged_data)\n"
                "print(f'Flagged items: {len(flagged_df)}')\n"
                "flagged_df"
            )
        else:
            cell5 = self._make_cell(
                "flagged_df = pd.DataFrame(columns=['step', 'entity', 'rule', 'reason'])\n"
                "print('No LOW-confidence items flagged — all classifications are HIGH confidence.')\n"
                "flagged_df"
            )

        # ── Cell 6: Hubs header markdown ────────────────────────────────
        cell6 = self._make_cell(
            "## Hubs — Edit below to correct hub definitions\n\n"
            "Each dict represents one Hub. Required fields: `name`, `target_table`, "
            "`source_table`, `business_key_columns`, `load_date_column`, `record_source`.",
            "markdown",
        )

        # ── Cell 7: Hubs editable list ──────────────────────────────────
        hubs_list = self._render_editable_list(model_dict.get("hubs", []))
        cell7 = self._make_cell(f"hubs = {hubs_list}\n\nprint(f'Hubs defined: {{len(hubs)}}')")

        # ── Cell 8: Links header markdown ───────────────────────────────
        cell8 = self._make_cell(
            "## Links\n\n"
            "Each dict represents one Link. `hub_references` must reference valid hub names.",
            "markdown",
        )

        # ── Cell 9: Links editable list ─────────────────────────────────
        links_list = self._render_editable_list(model_dict.get("links", []))
        cell9 = self._make_cell(f"links = {links_list}\n\nprint(f'Links defined: {{len(links)}}')")

        # ── Cell 10: Satellites header markdown ─────────────────────────
        cell10 = self._make_cell(
            "## Satellites\n\n"
            "Each dict represents one Satellite. `parent_hub` must reference a valid hub name. "
            "`tracked_columns` must be non-empty.",
            "markdown",
        )

        # ── Cell 11: Satellites editable list ───────────────────────────
        sats_list = self._render_editable_list(model_dict.get("satellites", []))
        cell11 = self._make_cell(
            f"satellites = {sats_list}\n\nprint(f'Satellites defined: {{len(satellites)}}')"
        )

        # ── Cell 12: PIT header markdown ────────────────────────────────
        cell12 = self._make_cell(
            "## PIT Tables\n\n"
            "Point-in-Time tables. `hub` must reference a valid hub name. "
            "`satellites` must be a list of valid satellite names.",
            "markdown",
        )

        # ── Cell 13: PIT editable list ──────────────────────────────────
        pits_list = self._render_editable_list(model_dict.get("pit_tables", []))
        cell13 = self._make_cell(
            f"pit_tables = {pits_list}\n\nprint(f'PIT tables defined: {{len(pit_tables)}}')"
        )

        # ── Cell 14: Bridge header markdown ─────────────────────────────
        cell14 = self._make_cell(
            "## Bridge Tables\n\n"
            "Bridge tables. `path` must alternate Hub/Link names that exist in the model.",
            "markdown",
        )

        # ── Cell 15: Bridge editable list ───────────────────────────────
        brgs_list = self._render_editable_list(model_dict.get("bridge_tables", []))
        cell15 = self._make_cell(
            f"bridge_tables = {brgs_list}\n\nprint(f'Bridge tables defined: {{len(bridge_tables)}}')"
        )

        # ── Cell 16: Save & Validate header markdown ────────────────────
        cell16 = self._make_cell(
            "## Save & Validate — run this cell when done\n\n"
            "This cell writes your edits to `05_human_review.json` and prints the resume command.",
            "markdown",
        )

        # ── Cell 17: Save cell ──────────────────────────────────────────
        cell17 = self._make_cell(
            "import json, pathlib\n"
            "review = {\n"
            '    "hubs": hubs,\n'
            '    "links": links,\n'
            '    "satellites": satellites,\n'
            '    "pit_tables": pit_tables,\n'
            '    "bridge_tables": bridge_tables,\n'
            "}\n"
            f'out = pathlib.Path({session_dir!r}) / "05_human_review.json"\n'
            "out.write_text(json.dumps(review, indent=2))\n"
            'print(f"Saved to {out}")\n'
            "print(\n"
            '    "Run: python -m generators.dv_generator.main "\n'
            f'    f"--resume {session_id} --from-step step6_validator"\n'
            ")"
        )

        cells = [
            cell1, cell2, cell3, cell4, cell5,
            cell6, cell7,
            cell8, cell9,
            cell10, cell11,
            cell12, cell13,
            cell14, cell15,
            cell16, cell17,
        ]
        self._write_nb("05_review_notebook", cells)
