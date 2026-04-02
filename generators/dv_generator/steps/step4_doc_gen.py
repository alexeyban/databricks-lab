"""
DV 2.0 Generator — Documentation & Diagram Generator (TASK_08)

Generates two human-readable artifacts from the final DVModel:
1. A draw.io-compatible mxGraph XML diagram (04_diagram.drawio)
2. A markdown documentation file (04_documentation.md)
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from ..models import DVModel
from ..session import Session


class DocGenerator:
    """Produces draw.io XML and markdown documentation from a DVModel."""

    HUB_STYLE    = "rounded=1;fillColor=#FFE6CC;strokeColor=#d6b656;"
    LINK_STYLE   = "fillColor=#DAE8FC;strokeColor=#6c8ebf;"
    SAT_STYLE    = "ellipse;fillColor=#D5E8D4;strokeColor=#82b366;"
    PIT_STYLE    = "ellipse;fillColor=#E1D5E7;strokeColor=#9673a6;"
    BRIDGE_STYLE = "fillColor=#F8CECC;strokeColor=#b85450;"

    # Cell dimensions
    HUB_W  = 140
    HUB_H  = 60
    LINK_W = 160
    LINK_H = 50
    SAT_W  = 160
    SAT_H  = 50

    def __init__(self, model: DVModel, session: Session) -> None:
        self.model = model
        self.session = session

    def run(self) -> None:
        """Generate diagram and docs (or skip if already done)."""
        if self.session.is_step_done("step4_doc_gen"):
            return
        self._write_diagram()
        self._write_markdown()
        self.session.mark_step_done("step4_doc_gen")

    # ------------------------------------------------------------------
    # Layout helpers
    # ------------------------------------------------------------------

    def _hub_position(self, index: int) -> tuple[int, int]:
        """Grid layout: 3 columns, 220px wide, 160px tall spacing."""
        col = index % 3
        row = index // 3
        return (200 + col * 220, 100 + row * 160)

    def _sat_position(self, hub_pos: tuple, sat_index: int) -> tuple[int, int]:
        """Offset satellites to the right of parent hub."""
        return (hub_pos[0] + 180, hub_pos[1] + sat_index * 100)

    def _link_position(self, hub_positions: dict, hub_refs: list) -> tuple[int, int]:
        """Position link midway between its connected hubs."""
        if not hub_refs:
            return (600, 300)
        xs = [hub_positions[r.hub][0] for r in hub_refs if r.hub in hub_positions]
        ys = [hub_positions[r.hub][1] for r in hub_refs if r.hub in hub_positions]
        if not xs:
            return (600, 300)
        return (int(sum(xs) / len(xs)), int(sum(ys) / len(ys)))

    # ------------------------------------------------------------------
    # Diagram generator
    # ------------------------------------------------------------------

    def _write_diagram(self) -> None:
        """Build mxGraph XML tree and write 04_diagram.drawio."""
        root_elem = ET.Element("mxGraphModel")
        root_cell = ET.SubElement(root_elem, "root")

        # Required mxGraph base cells
        ET.SubElement(root_cell, "mxCell", id="0")
        ET.SubElement(root_cell, "mxCell", id="1", parent="0")

        cell_id = 2  # incrementing cell id counter

        # --- Hubs ---
        hub_positions: dict[str, tuple[int, int]] = {}
        hub_cell_ids: dict[str, str] = {}

        for idx, hub in enumerate(self.model.hubs):
            x, y = self._hub_position(idx)
            hub_positions[hub.name] = (x, y)
            cid = str(cell_id)
            hub_cell_ids[hub.name] = cid
            cell_id += 1

            bk_label = "\\n".join(hub.business_key_columns)
            label = f"{hub.name}\\n{bk_label}"

            mc = ET.SubElement(
                root_cell, "mxCell",
                id=cid,
                value=label,
                style=self.HUB_STYLE,
                vertex="1",
                parent="1",
            )
            ET.SubElement(
                mc, "mxGeometry",
                x=str(x), y=str(y),
                width=str(self.HUB_W), height=str(self.HUB_H),
                **{"as": "geometry"},
            )

        # --- Satellites ---
        # Group satellites by parent hub to position them in stacks
        sat_by_hub: dict[str, list] = {}
        for sat in self.model.satellites:
            sat_by_hub.setdefault(sat.parent_hub, []).append(sat)

        sat_cell_ids: dict[str, str] = {}

        for hub_name, sats in sat_by_hub.items():
            hub_pos = hub_positions.get(hub_name, (200, 200))
            for sat_idx, sat in enumerate(sats):
                x, y = self._sat_position(hub_pos, sat_idx)
                cid = str(cell_id)
                sat_cell_ids[sat.name] = cid
                cell_id += 1

                tracked_label = "\\n".join(sat.tracked_columns[:5])  # max 5 cols in label
                if len(sat.tracked_columns) > 5:
                    tracked_label += f"\\n(+{len(sat.tracked_columns) - 5} more)"
                label = f"{sat.name}\\n{tracked_label}"

                mc = ET.SubElement(
                    root_cell, "mxCell",
                    id=cid,
                    value=label,
                    style=self.SAT_STYLE,
                    vertex="1",
                    parent="1",
                )
                ET.SubElement(
                    mc, "mxGeometry",
                    x=str(x), y=str(y),
                    width=str(self.SAT_W), height=str(self.SAT_H),
                    **{"as": "geometry"},
                )

                # Edge: Hub → Satellite
                if hub_name in hub_cell_ids:
                    ecid = str(cell_id)
                    cell_id += 1
                    edge = ET.SubElement(
                        root_cell, "mxCell",
                        id=ecid,
                        style="edgeStyle=orthogonalEdgeStyle;",
                        edge="1",
                        source=hub_cell_ids[hub_name],
                        target=cid,
                        parent="1",
                    )
                    ET.SubElement(edge, "mxGeometry", relative="1", **{"as": "geometry"})

        # --- Links ---
        link_cell_ids: dict[str, str] = {}

        for lnk in self.model.links:
            x, y = self._link_position(hub_positions, lnk.hub_references)
            cid = str(cell_id)
            link_cell_ids[lnk.name] = cid
            cell_id += 1

            mc = ET.SubElement(
                root_cell, "mxCell",
                id=cid,
                value=lnk.name,
                style=self.LINK_STYLE,
                vertex="1",
                parent="1",
            )
            ET.SubElement(
                mc, "mxGeometry",
                x=str(x), y=str(y),
                width=str(self.LINK_W), height=str(self.LINK_H),
                **{"as": "geometry"},
            )

            # Edges: each referenced hub → link
            for ref in lnk.hub_references:
                if ref.hub in hub_cell_ids:
                    ecid = str(cell_id)
                    cell_id += 1
                    edge = ET.SubElement(
                        root_cell, "mxCell",
                        id=ecid,
                        style="edgeStyle=orthogonalEdgeStyle;",
                        edge="1",
                        source=hub_cell_ids[ref.hub],
                        target=cid,
                        parent="1",
                    )
                    ET.SubElement(edge, "mxGeometry", relative="1", **{"as": "geometry"})

        # --- PIT Tables ---
        pit_start_x = 1000
        for pit_idx, pit in enumerate(self.model.pit_tables):
            x = pit_start_x
            y = 100 + pit_idx * 100
            cid = str(cell_id)
            cell_id += 1

            sats_label = ", ".join(pit.satellites)
            label = f"{pit.name}\\n{sats_label}"

            mc = ET.SubElement(
                root_cell, "mxCell",
                id=cid,
                value=label,
                style=self.PIT_STYLE,
                vertex="1",
                parent="1",
            )
            ET.SubElement(
                mc, "mxGeometry",
                x=str(x), y=str(y),
                width="200", height="60",
                **{"as": "geometry"},
            )

        # --- Bridge Tables ---
        brg_start_x = 1250
        for brg_idx, brg in enumerate(self.model.bridge_tables):
            x = brg_start_x
            y = 100 + brg_idx * 100
            cid = str(cell_id)
            cell_id += 1

            path_label = " → ".join(brg.path)
            label = f"{brg.name}\\n{path_label}"

            mc = ET.SubElement(
                root_cell, "mxCell",
                id=cid,
                value=label,
                style=self.BRIDGE_STYLE,
                vertex="1",
                parent="1",
            )
            ET.SubElement(
                mc, "mxGeometry",
                x=str(x), y=str(y),
                width="200", height="60",
                **{"as": "geometry"},
            )

        # Serialise
        xml_str = ET.tostring(root_elem, encoding="unicode", xml_declaration=False)
        out_path = self.session.session_dir / "04_diagram.drawio"
        out_path.write_text('<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str)

    # ------------------------------------------------------------------
    # Markdown documentation
    # ------------------------------------------------------------------

    def _write_markdown(self) -> None:
        """Render 04_documentation.md with tables for each entity type."""
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        session_id = self.session.state.session_id

        lines: list[str] = [
            "# Data Vault 2.0 Model — dvdrental",
            "",
            f"Generated: {ts}",
            f"Source: {session_id}",
            "",
            "## Summary",
            "",
            f"- Hubs: {len(self.model.hubs)}",
            f"- Links: {len(self.model.links)}",
            f"- Satellites: {len(self.model.satellites)}",
            f"- PIT tables: {len(self.model.pit_tables)}",
            f"- Bridge tables: {len(self.model.bridge_tables)}",
            "",
        ]

        # --- Hubs ---
        lines += [
            "## Hubs",
            "",
            "| Hub | Business Key | Source Table | Record Source |",
            "|-----|-------------|--------------|---------------|",
        ]
        for hub in self.model.hubs:
            bk = ", ".join(hub.business_key_columns)
            conf_flag = " ⚠️" if hub.confidence.value == "LOW" else ""
            lines.append(
                f"| {hub.name}{conf_flag} | {bk} | {hub.source_table} | {hub.record_source} |"
            )
        lines.append("")

        # --- Links ---
        lines += [
            "## Links",
            "",
            "| Link | Hubs Connected | Source Table | Confidence |",
            "|------|---------------|--------------|-----------|",
        ]
        for lnk in self.model.links:
            hubs_conn = ", ".join(r.hub for r in lnk.hub_references)
            conf_flag = " ⚠️" if lnk.confidence.value == "LOW" else ""
            lines.append(
                f"| {lnk.name}{conf_flag} | {hubs_conn} | {lnk.source_table} | {lnk.confidence.value} |"
            )
        lines.append("")

        # --- Satellites ---
        lines += [
            "## Satellites",
            "",
            "| Satellite | Parent Hub | Tracked Columns | Split Reason |",
            "|-----------|-----------|-----------------|-------------|",
        ]
        for sat in self.model.satellites:
            tracked = ", ".join(sat.tracked_columns)
            split = sat.split_reason or ""
            conf_flag = " ⚠️" if sat.confidence.value == "LOW" else ""
            lines.append(
                f"| {sat.name}{conf_flag} | {sat.parent_hub} | {tracked} | {split} |"
            )
        lines.append("")

        # --- PIT Tables ---
        lines += [
            "## PIT Tables",
            "",
            "| PIT Table | Hub | Satellites | Snapshot Grain |",
            "|-----------|-----|-----------|---------------|",
        ]
        for pit in self.model.pit_tables:
            sats = ", ".join(pit.satellites)
            lines.append(f"| {pit.name} | {pit.hub} | {sats} | {pit.snapshot_grain} |")
        lines.append("")

        # --- Bridge Tables ---
        lines += [
            "## Bridge Tables",
            "",
            "| Bridge Table | Path |",
            "|-------------|------|",
        ]
        for brg in self.model.bridge_tables:
            path_str = " → ".join(brg.path)
            lines.append(f"| {brg.name} | {path_str} |")
        lines.append("")

        # --- Flagged items ---
        try:
            from ..decision_logger import DecisionLogger
            logger = DecisionLogger(str(self.session.session_dir))
            flagged = logger.read_flagged()
        except Exception:
            flagged = []

        if flagged:
            lines += [
                "## Entity Relationship Notes",
                "",
                "The following items were flagged as LOW confidence and require human review:",
                "",
                "| Step | Entity | Rule | Reason |",
                "|------|--------|------|--------|",
            ]
            for entry in flagged:
                lines.append(
                    f"| {entry.step} | {entry.entity} | {entry.rule} | {entry.reason} |"
                )
            lines.append("")
        else:
            lines += [
                "## Entity Relationship Notes",
                "",
                "No LOW-confidence items flagged. All classifications are HIGH confidence.",
                "",
            ]

        out_path = self.session.session_dir / "04_documentation.md"
        out_path.write_text("\n".join(lines))
