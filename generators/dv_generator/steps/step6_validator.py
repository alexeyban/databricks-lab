"""
DV 2.0 Generator — Model Validator (TASK_10)

Validates the human-reviewed model (05_human_review.json) against structural
DV 2.0 rules (V1-V9) and emits warnings (W1-W4). Produces 06_validation_report.json.
Step7 only proceeds when passed=True (zero errors).
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from ..session import Session


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ValidationIssue:
    code: str
    entity: str
    message: str


@dataclass
class ValidationReport:
    passed: bool
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)
    diff_summary: dict = field(default_factory=dict)

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class Validator:
    """Validates the human-reviewed DV model against DV 2.0 structural rules."""

    STEP_NAME = "step6_validator"

    def __init__(self, session: Session) -> None:
        self.session = session
        self.review_path = session.session_dir / "05_human_review.json"
        self.template_path = session.session_dir / "05_human_review_template.json"
        self.output_path = session.session_dir / "06_validation_report.json"

    def run(self) -> ValidationReport:
        if self.session.is_step_done(self.STEP_NAME):
            return self._load_cached()

        model = json.loads(self.review_path.read_text())
        template = json.loads(self.template_path.read_text())

        errors: list[ValidationIssue] = []
        warnings: list[ValidationIssue] = []

        self._check_hubs(model, errors, warnings)
        self._check_links(model, errors, warnings)
        self._check_satellites(model, errors, warnings)
        self._check_pit_tables(model, errors, warnings)
        self._check_bridge_tables(model, errors, warnings)
        self._check_uniqueness(model, errors)
        self._check_disabled_entities(model, warnings)

        diff = self._compute_diff(model, template)
        passed = len(errors) == 0

        report = ValidationReport(
            passed=passed,
            errors=errors,
            warnings=warnings,
            diff_summary=diff,
        )
        self._save(report)

        if passed:
            self.session.mark_step_done(
                self.STEP_NAME,
                metadata={
                    "errors": 0,
                    "warnings": len(warnings),
                    "diff_summary": diff,
                },
            )
        return report

    # ------------------------------------------------------------------
    # V1 — Hub required fields
    # ------------------------------------------------------------------

    def _check_hubs(self, model: dict, errors: list, warnings: list) -> None:
        for hub in model.get("hubs", []):
            name = hub.get("name", "<unnamed hub>")

            # V1: required fields
            missing = [
                f for f in ("name", "target_table", "source_table", "load_date_column", "record_source")
                if not hub.get(f)
            ]
            if missing:
                errors.append(ValidationIssue(
                    code="V1", entity=name,
                    message=f"Hub missing required fields: {missing}",
                ))
            bk = hub.get("business_key_columns", [])
            if not bk:
                errors.append(ValidationIssue(
                    code="V1", entity=name,
                    message="Hub has empty business_key_columns",
                ))

            # V9: target_table pattern
            self._check_target_table(hub, "hub", errors)

            # W1: composite BK
            if len(bk) > 1:
                warnings.append(ValidationIssue(
                    code="W1", entity=name,
                    message=f"Hub has composite business key ({bk}) — unusual, verify intentional",
                ))

    # ------------------------------------------------------------------
    # V2, V3 — Link integrity
    # ------------------------------------------------------------------

    def _check_links(self, model: dict, errors: list, warnings: list) -> None:
        hub_names = {h["name"] for h in model.get("hubs", []) if "name" in h}

        for lnk in model.get("links", []):
            name = lnk.get("name", "<unnamed link>")

            refs = lnk.get("hub_references", [])
            # V2: at least 2 hub references
            if len(refs) < 2:
                errors.append(ValidationIssue(
                    code="V2", entity=name,
                    message=f"Link has {len(refs)} hub_references — minimum 2 required",
                ))

            # V3: each referenced hub must exist
            for ref in refs:
                hub = ref.get("hub", "")
                if hub and hub not in hub_names:
                    errors.append(ValidationIssue(
                        code="V3", entity=name,
                        message=f"hub_reference '{hub}' does not exist in hubs list",
                    ))

            # V9
            self._check_target_table(lnk, "link", errors)

            # W3: complex link
            if len(refs) > 3:
                warnings.append(ValidationIssue(
                    code="W3", entity=name,
                    message=f"Link has {len(refs)} hub_references — complex link, verify intentional",
                ))

    # ------------------------------------------------------------------
    # V4, V5 — Satellite integrity
    # ------------------------------------------------------------------

    def _check_satellites(self, model: dict, errors: list, warnings: list) -> None:
        hub_names = {h["name"] for h in model.get("hubs", []) if "name" in h}

        for sat in model.get("satellites", []):
            name = sat.get("name", "<unnamed satellite>")

            # V4: parent_hub must exist
            parent = sat.get("parent_hub", "")
            if parent and parent not in hub_names:
                errors.append(ValidationIssue(
                    code="V4", entity=name,
                    message=f"parent_hub '{parent}' does not exist in hubs list",
                ))
            elif not parent:
                errors.append(ValidationIssue(
                    code="V4", entity=name,
                    message="Satellite missing parent_hub",
                ))

            # V5: tracked_columns must be non-empty
            tracked = sat.get("tracked_columns", [])
            if not tracked:
                errors.append(ValidationIssue(
                    code="V5", entity=name,
                    message="Satellite has empty tracked_columns",
                ))

            # V9
            self._check_target_table(sat, "satellite", errors)

            # W2: single tracked column
            if len(tracked) == 1:
                warnings.append(ValidationIssue(
                    code="W2", entity=name,
                    message="Satellite has only 1 tracked column — consider merging with another satellite",
                ))

    # ------------------------------------------------------------------
    # V7 — PIT integrity
    # ------------------------------------------------------------------

    def _check_pit_tables(self, model: dict, errors: list, warnings: list) -> None:
        hub_names = {h["name"] for h in model.get("hubs", []) if "name" in h}
        sat_names = {s["name"] for s in model.get("satellites", []) if "name" in s}

        for pit in model.get("pit_tables", []):
            name = pit.get("name", "<unnamed PIT>")

            hub = pit.get("hub", "")
            if hub and hub not in hub_names:
                errors.append(ValidationIssue(
                    code="V7", entity=name,
                    message=f"PIT references hub '{hub}' which does not exist",
                ))
            elif not hub:
                errors.append(ValidationIssue(
                    code="V7", entity=name,
                    message="PIT missing hub reference",
                ))

            for sat_ref in pit.get("satellites", []):
                if sat_ref not in sat_names:
                    errors.append(ValidationIssue(
                        code="V7", entity=name,
                        message=f"PIT references satellite '{sat_ref}' which does not exist",
                    ))

    # ------------------------------------------------------------------
    # V8 — Bridge path integrity
    # ------------------------------------------------------------------

    def _check_bridge_tables(self, model: dict, errors: list, warnings: list) -> None:
        hub_names = {h["name"] for h in model.get("hubs", []) if "name" in h}
        lnk_names = {lk["name"] for lk in model.get("links", []) if "name" in lk}

        for brg in model.get("bridge_tables", []):
            name = brg.get("name", "<unnamed bridge>")
            path = brg.get("path", [])

            if len(path) < 3:
                errors.append(ValidationIssue(
                    code="V8", entity=name,
                    message=f"Bridge path has only {len(path)} entries — minimum 3 required (Hub-Link-Hub)",
                ))
                continue

            # Path must alternate Hub / Link / Hub / Link / ...
            for i, entry in enumerate(path):
                if i % 2 == 0:  # even position → expect Hub
                    if entry not in hub_names:
                        errors.append(ValidationIssue(
                            code="V8", entity=name,
                            message=f"Bridge path position {i} ('{entry}') expected a Hub but not found",
                        ))
                else:  # odd position → expect Link
                    if entry not in lnk_names:
                        errors.append(ValidationIssue(
                            code="V8", entity=name,
                            message=f"Bridge path position {i} ('{entry}') expected a Link but not found",
                        ))

    # ------------------------------------------------------------------
    # V6 — Uniqueness; V9 — target_table pattern
    # ------------------------------------------------------------------

    def _check_uniqueness(self, model: dict, errors: list) -> None:
        for entity_type, key in (
            ("hubs", "hub"),
            ("links", "link"),
            ("satellites", "satellite"),
            ("pit_tables", "pit"),
            ("bridge_tables", "bridge"),
        ):
            names: list[str] = [e["name"] for e in model.get(entity_type, []) if "name" in e]
            seen: set[str] = set()
            for n in names:
                if n in seen:
                    errors.append(ValidationIssue(
                        code="V6", entity=n,
                        message=f"Duplicate {key} name '{n}'",
                    ))
                seen.add(n)

    # ------------------------------------------------------------------
    # V9 helper
    # ------------------------------------------------------------------

    def _check_target_table(self, entity: dict, entity_type: str, errors: list) -> None:
        name = entity.get("name", f"<unnamed {entity_type}>")
        target = entity.get("target_table", "")
        if not target:
            return  # missing target_table caught by V1 for hubs
        if not target.startswith("vault."):
            errors.append(ValidationIssue(
                code="V9", entity=name,
                message=f"target_table '{target}' must follow pattern 'vault.<lowercase_name>'",
            ))
        elif target != target.lower():
            errors.append(ValidationIssue(
                code="V9", entity=name,
                message=f"target_table '{target}' must be all lowercase",
            ))

    # ------------------------------------------------------------------
    # W4 — disabled entities
    # ------------------------------------------------------------------

    def _check_disabled_entities(self, model: dict, warnings: list) -> None:
        for entity_type in ("hubs", "links", "satellites", "pit_tables", "bridge_tables"):
            for entity in model.get(entity_type, []):
                if not entity.get("enabled", True):
                    warnings.append(ValidationIssue(
                        code="W4", entity=entity.get("name", f"<unnamed in {entity_type}>"),
                        message="Entity is disabled (enabled=false) — will be skipped at runtime",
                    ))

    # ------------------------------------------------------------------
    # Diff
    # ------------------------------------------------------------------

    def _compute_diff(self, review: dict, template: dict) -> dict:
        """Count entities changed between pre- and post-human-edit model."""
        def _changed(entity_type: str) -> int:
            rev_map = {e["name"]: e for e in review.get(entity_type, []) if "name" in e}
            tpl_map = {e["name"]: e for e in template.get(entity_type, []) if "name" in e}
            added = len(set(rev_map) - set(tpl_map))
            removed = len(set(tpl_map) - set(rev_map))
            modified = sum(
                1 for n in (set(rev_map) & set(tpl_map))
                if rev_map[n] != tpl_map[n]
            )
            return added + removed + modified

        return {
            "hubs_changed": _changed("hubs"),
            "links_changed": _changed("links"),
            "satellites_changed": _changed("satellites"),
            "pit_tables_changed": _changed("pit_tables"),
            "bridge_tables_changed": _changed("bridge_tables"),
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self, report: ValidationReport) -> None:
        data = {
            "passed": report.passed,
            "error_count": report.error_count,
            "warning_count": report.warning_count,
            "errors": [asdict(e) for e in report.errors],
            "warnings": [asdict(w) for w in report.warnings],
            "diff_summary": report.diff_summary,
        }
        self.output_path.write_text(json.dumps(data, indent=2))

    def _load_cached(self) -> ValidationReport:
        data = json.loads(self.output_path.read_text())
        return ValidationReport(
            passed=data["passed"],
            errors=[ValidationIssue(**e) for e in data["errors"]],
            warnings=[ValidationIssue(**w) for w in data["warnings"]],
            diff_summary=data.get("diff_summary", {}),
        )
