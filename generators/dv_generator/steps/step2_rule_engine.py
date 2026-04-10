"""
DV 2.0 Generator — Heuristic Rule Engine (TASK_05)

Applies DV 2.0 classification rules to the schema analysis output to determine
which Silver tables become Hubs, which FK relationships become Links, and how
columns split across Satellites. Writes 02_classification.json.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from ..decision_logger import DecisionLogger
from ..models import (
    ConfidenceLevel,
    DVModel,
    HubDef,
    LinkDef,
    LinkRef,
    SatDef,
    TableDef,
)
from ..session import Session

# Tables that are many-to-many junctions — they become Links only (no Hub)
JUNCTION_TABLES: frozenset[str] = frozenset(
    {"silver_film_actor", "silver_film_category"}
)

# Reference tables that are clearly business entities regardless of column count
REFERENCE_TABLES: frozenset[str] = frozenset(
    {
        "silver_country", "silver_city", "silver_language", "silver_category",
        "silver_actor", "silver_store", "silver_staff", "silver_address",
    }
)

# Column name keyword sets for satellite splitting (S2/S3 heuristic)
PRICING_KEYWORDS: frozenset[str] = frozenset(
    {"rate", "cost", "price", "amount", "fee", "salary", "replacement", "rental_rate",
     "rental_duration", "replacement_cost"}
)
STATUS_KEYWORDS: frozenset[str] = frozenset(
    {"active", "activebool", "status", "flag", "enabled"}
)
AUDIT_COLUMNS: frozenset[str] = frozenset(
    {"last_update", "last_updated_dt", "create_date", "created_at", "updated_at"}
)


class RuleEngine:
    """Classifies Silver tables into DV 2.0 entities using heuristic rules.

    Rules applied:

    **Hub rules**: R1 (single-col int PK → Hub), R2 (junction → no Hub),
    R3 (no PK → flag LOW), R4 (known reference table → Hub).

    **Link rules**: L1 (FK column → Link), L2 (junction table → multi-hub Link),
    L3 (3+ FK cols → one Link per FK).

    **Satellite rules**: S1 (all payload → one sat), S2 (pricing keywords → split),
    S3 (status keywords → split), S4 (audit-only → marker sat).
    """

    def __init__(
        self,
        tables: list[TableDef],
        session: Session,
        logger: DecisionLogger,
    ) -> None:
        self._tables: dict[str, TableDef] = {t.name: t for t in tables}
        self.session = session
        self.logger = logger
        self.output_path = session.session_dir / "02_classification.json"

    def run(self) -> DVModel:
        """Run classification (or load cached result if step already done)."""
        if self.session.is_step_done("step2_rule_engine"):
            return self._load_cached()

        model = DVModel()
        for table in self._tables.values():
            self._classify_hub(table, model)
        for table in self._tables.values():
            self._classify_links(table, model)
        for table in self._tables.values():
            self._classify_satellites(table, model)

        self._save(model)
        self.session.mark_step_done(
            "step2_rule_engine",
            metadata={
                "hubs": len(model.hubs),
                "links": len(model.links),
                "satellites": len(model.satellites),
            },
        )
        return model

    # ------------------------------------------------------------------
    # Hub classification
    # ------------------------------------------------------------------

    def _classify_hub(self, table: TableDef, model: DVModel) -> None:
        # R2: junction tables never become Hubs
        if table.name in JUNCTION_TABLES:
            self.logger.log(
                "step2_rule_engine", table.name, "R2: junction → skip hub",
                ConfidenceLevel.HIGH,
                f"{table.name} is a many-to-many junction table — will become a Link only",
            )
            return

        # R3: no PK → flag as LOW confidence
        if not table.pk_columns:
            self.logger.log(
                "step2_rule_engine", table.name, "R3: no PK → LOW confidence hub",
                ConfidenceLevel.LOW,
                f"{table.name} has no primary key — cannot determine business key",
            )
            confidence = ConfidenceLevel.LOW
        # R4: known reference table
        elif table.name in REFERENCE_TABLES:
            confidence = ConfidenceLevel.HIGH
            rule = "R4"
        # R1: single-column integer PK
        elif len(table.pk_columns) == 1 and self._is_int_pk(table):
            confidence = ConfidenceLevel.HIGH
            rule = "R1"
        else:
            # Composite PK or unknown type — lower confidence
            confidence = ConfidenceLevel.MEDIUM
            rule = "R1-composite"

        if table.pk_columns:
            bk_col = table.pk_columns[0]
            hub_name = self._hub_name(table.name)
            rule_label = locals().get("rule", "R1")
            self.logger.log(
                "step2_rule_engine", hub_name,
                f"{rule_label}: {table.pk_columns} → Hub",
                confidence,
                f"Business key: {bk_col}; source: {table.source_table}",
            )
            model.hubs.append(
                HubDef(
                    name=hub_name,
                    target_table=f"vault.{hub_name.lower()}",
                    source_table=table.source_table,
                    business_key_columns=list(table.pk_columns),
                    load_date_column=self._load_date_col(table),
                    record_source=f"cdc.dvdrental.{table.name.replace('silver_', '')}",
                    confidence=confidence,
                    rules_fired=[rule_label],
                )
            )

    # ------------------------------------------------------------------
    # Link classification
    # ------------------------------------------------------------------

    def _classify_links(self, table: TableDef, model: DVModel) -> None:
        if table.name in JUNCTION_TABLES:
            # L2: junction → single multi-hub Link
            hub_refs = [
                LinkRef(hub=self._hub_name_from_col(col, table), source_column=col)
                for col in table.pk_columns  # both PKs are FKs
            ]
            if len(hub_refs) >= 2:
                link_name = self._link_name_from_refs(hub_refs)
                self.logger.log(
                    "step2_rule_engine", link_name, "L2: junction → multi-hub Link",
                    ConfidenceLevel.HIGH,
                    f"Junction table {table.name} → Link with {len(hub_refs)} hub references",
                )
                model.links.append(
                    LinkDef(
                        name=link_name,
                        target_table=f"vault.{link_name.lower()}",
                        source_table=table.source_table,
                        hub_references=hub_refs,
                        load_date_column=self._load_date_col(table),
                        record_source=f"cdc.dvdrental.{table.name.replace('silver_', '')}",
                        confidence=ConfidenceLevel.HIGH,
                        rules_fired=["L2"],
                    )
                )
            return

        # L1 / L3: one Link per FK column
        fk_cols = [col for col in table.fk_hints if col not in table.pk_columns]
        for fk_col in fk_cols:
            target_table = table.fk_hints[fk_col]
            source_hub = self._hub_name(table.name)
            target_hub = self._hub_name(target_table)
            link_name = f"LNK_{source_hub[4:]}_{target_hub[4:]}"  # strip HUB_
            self.logger.log(
                "step2_rule_engine", link_name, "L1: FK column → Link",
                ConfidenceLevel.HIGH,
                f"{table.name}.{fk_col} → {target_table}",
            )
            model.links.append(
                LinkDef(
                    name=link_name,
                    target_table=f"vault.{link_name.lower()}",
                    source_table=table.source_table,
                    hub_references=[
                        LinkRef(hub=source_hub, source_column=table.pk_columns[0] if table.pk_columns else fk_col),
                        LinkRef(hub=target_hub, source_column=fk_col),
                    ],
                    load_date_column=self._load_date_col(table),
                    record_source=f"cdc.dvdrental.{table.name.replace('silver_', '')}",
                    confidence=ConfidenceLevel.HIGH,
                    rules_fired=["L1"],
                )
            )

    # ------------------------------------------------------------------
    # Satellite classification
    # ------------------------------------------------------------------

    def _classify_satellites(self, table: TableDef, model: DVModel) -> None:
        if table.name in JUNCTION_TABLES:
            return  # junction tables don't get satellites

        hub_name = self._hub_name(table.name)
        bk_col = table.pk_columns[0] if table.pk_columns else ""

        # Build type map for all columns in this table
        col_type_map: dict[str, str] = {col.name: col.data_type for col in table.columns}

        # Collect payload columns (non-PK, non-FK, non-audit)
        pk_set = set(table.pk_columns)
        fk_set = set(table.fk_hints.keys())
        payload: list[str] = []
        audit: list[str] = []

        for col in table.columns:
            if col.name in pk_set or col.name in fk_set:
                continue
            if col.name in AUDIT_COLUMNS:
                audit.append(col.name)
            else:
                payload.append(col.name)

        # Split payload using S2/S3 heuristics
        buckets = self._split_columns(payload, table.name)

        if not buckets and not audit:
            return  # no satellite needed (e.g. table is only PK + FK)

        # If no payload split, create single satellite
        if not buckets:
            buckets = {"DETAIL": payload or []}

        for suffix, cols in buckets.items():
            if not cols:
                continue
            sat_name = f"SAT_{hub_name[4:]}_{suffix}"  # strip HUB_
            reason = f"S1: all payload columns" if len(buckets) == 1 else f"S2/S3: split by {suffix.lower()} pattern"
            self.logger.log(
                "step2_rule_engine", sat_name,
                f"{'S1' if len(buckets)==1 else 'S2'}: → Satellite",
                ConfidenceLevel.HIGH,
                f"Parent: {hub_name}, tracked: {cols}",
            )
            model.satellites.append(
                SatDef(
                    name=sat_name,
                    target_table=f"vault.{sat_name.lower()}",
                    parent_hub=hub_name,
                    source_table=table.source_table,
                    hub_key_source_column=bk_col,
                    tracked_columns=cols,
                    column_types={c: col_type_map.get(c, "varchar") for c in cols},
                    load_date_column=self._load_date_col(table),
                    record_source=f"cdc.dvdrental.{table.name.replace('silver_', '')}",
                    split_reason=reason,
                    confidence=ConfidenceLevel.HIGH,
                    rules_fired=["S1" if len(buckets) == 1 else "S2"],
                )
            )

        # S4: marker satellite if table has audit columns but no payload
        if not payload and audit:
            sat_name = f"SAT_{hub_name[4:]}"
            self.logger.log(
                "step2_rule_engine", sat_name, "S4: audit-only → marker satellite",
                ConfidenceLevel.HIGH,
                f"Only audit columns found: {audit}",
            )
            model.satellites.append(
                SatDef(
                    name=sat_name,
                    target_table=f"vault.{sat_name.lower()}",
                    parent_hub=hub_name,
                    source_table=table.source_table,
                    hub_key_source_column=bk_col,
                    tracked_columns=audit,
                    column_types={c: col_type_map.get(c, "varchar") for c in audit},
                    load_date_column=self._load_date_col(table),
                    record_source=f"cdc.dvdrental.{table.name.replace('silver_', '')}",
                    split_reason="S4: marker satellite (audit columns only)",
                    confidence=ConfidenceLevel.HIGH,
                    rules_fired=["S4"],
                )
            )

    # ------------------------------------------------------------------
    # Column splitting (S2 / S3 heuristic)
    # ------------------------------------------------------------------

    def _split_columns(self, cols: list[str], table_name: str) -> dict[str, list[str]]:
        """Partition payload columns into named buckets by keyword pattern."""
        pricing: list[str] = []
        status: list[str] = []
        core: list[str] = []

        for col in cols:
            col_lower = col.lower()
            if any(kw in col_lower for kw in PRICING_KEYWORDS):
                pricing.append(col)
            elif col_lower in STATUS_KEYWORDS or any(kw in col_lower for kw in STATUS_KEYWORDS):
                status.append(col)
            else:
                core.append(col)

        # Only split if there's actually something to separate
        result: dict[str, list[str]] = {}
        if pricing and (core or status):
            result["CORE"] = core + status
            result["PRICING"] = pricing
        elif pricing:
            result["PRICING"] = pricing
        elif core or status:
            result["CORE"] = core + status
        return result

    # ------------------------------------------------------------------
    # Naming helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hub_name(table_name: str) -> str:
        """'silver_film' → 'HUB_FILM'"""
        base = table_name.replace("silver_", "").upper()
        return f"HUB_{base}"

    def _hub_name_from_col(self, col_name: str, table: TableDef) -> str:
        """Infer hub name from a FK column (e.g. 'film_id' → 'HUB_FILM')."""
        if col_name in table.fk_hints:
            return self._hub_name(table.fk_hints[col_name])
        # Strip _id suffix and guess
        return "HUB_" + col_name.removesuffix("_id").upper()

    @staticmethod
    def _link_name_from_refs(refs: list[LinkRef]) -> str:
        parts = [r.hub[4:] for r in refs]  # strip HUB_
        return "LNK_" + "_".join(parts)

    @staticmethod
    def _load_date_col(table: TableDef) -> str:
        names = {c.name for c in table.columns}
        for candidate in ("last_update", "last_updated_dt", "payment_date", "rental_date"):
            if candidate in names:
                return candidate
        return "last_update"

    @staticmethod
    def _is_int_pk(table: TableDef) -> bool:
        pk_col = table.pk_columns[0] if table.pk_columns else ""
        for col in table.columns:
            if col.name == pk_col and col.data_type in ("integer", "bigint", "smallint"):
                return True
        return False

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self, model: DVModel) -> None:
        self.output_path.write_text(json.dumps(_model_to_dict(model), indent=2))

    def _load_cached(self) -> DVModel:
        data = json.loads(self.output_path.read_text())
        return _model_from_dict(data)


# ------------------------------------------------------------------
# Serialisation helpers (shared with other steps)
# ------------------------------------------------------------------

def _model_to_dict(model: DVModel) -> dict:
    from dataclasses import asdict as _asdict
    import copy

    def _fix(obj):
        if isinstance(obj, dict):
            return {k: _fix(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_fix(i) for i in obj]
        if hasattr(obj, "value"):  # Enum
            return obj.value
        return obj

    return _fix(_asdict(model))


def _model_from_dict(d: dict) -> DVModel:
    from ..models import (
        BridgeDef, HubDef, LinkDef, LinkRef, PitDef, SatDef, ConfidenceLevel, DVModel,
    )

    def _conf(v):
        return ConfidenceLevel(v) if v else ConfidenceLevel.HIGH

    hubs = [
        HubDef(
            name=h["name"], target_table=h["target_table"],
            source_table=h["source_table"],
            business_key_columns=h["business_key_columns"],
            load_date_column=h["load_date_column"],
            record_source=h["record_source"], enabled=h["enabled"],
            confidence=_conf(h.get("confidence")),
            rules_fired=h.get("rules_fired", []),
        )
        for h in d.get("hubs", [])
    ]
    links = [
        LinkDef(
            name=lk["name"], target_table=lk["target_table"],
            source_table=lk["source_table"],
            hub_references=[LinkRef(**r) for r in lk["hub_references"]],
            load_date_column=lk["load_date_column"],
            record_source=lk["record_source"], enabled=lk["enabled"],
            confidence=_conf(lk.get("confidence")),
            rules_fired=lk.get("rules_fired", []),
        )
        for lk in d.get("links", [])
    ]
    satellites = [
        SatDef(
            name=s["name"], target_table=s["target_table"],
            parent_hub=s["parent_hub"], source_table=s["source_table"],
            hub_key_source_column=s["hub_key_source_column"],
            tracked_columns=s["tracked_columns"],
            column_types=s.get("column_types", {}),
            load_date_column=s["load_date_column"],
            record_source=s["record_source"],
            split_reason=s.get("split_reason"),
            enabled=s["enabled"],
            confidence=_conf(s.get("confidence")),
            rules_fired=s.get("rules_fired", []),
        )
        for s in d.get("satellites", [])
    ]
    pit_tables = [
        PitDef(
            name=p["name"], target_table=p["target_table"],
            hub=p["hub"], satellites=p["satellites"],
            snapshot_grain=p.get("snapshot_grain", "daily"),
            enabled=p["enabled"],
        )
        for p in d.get("pit_tables", [])
    ]
    bridge_tables = [
        BridgeDef(
            name=b["name"], target_table=b["target_table"],
            path=b["path"], enabled=b["enabled"],
        )
        for b in d.get("bridge_tables", [])
    ]
    return DVModel(
        hubs=hubs, links=links, satellites=satellites,
        pit_tables=pit_tables, bridge_tables=bridge_tables,
    )
