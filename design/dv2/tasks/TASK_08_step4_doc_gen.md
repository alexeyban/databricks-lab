# TASK_08: step4_doc_gen.py — Documentation & Diagram Generator

## File
`generators/dv_generator/steps/step4_doc_gen.py`

## Purpose
Generates two human-readable artifacts from the final DVModel: (1) a draw.io-compatible mxGraph XML diagram showing all hubs, links, and satellites with their relationships, and (2) a markdown documentation file describing the full vault model with tables for each entity type.

## Depends on
- `TASK_01: models.py` — `DVModel`, `HubDef`, `LinkDef`, `SatDef`
- `TASK_03: session.py` — `Session`

## Inputs
- `DVModel` (from `03_dv_model_draft.json`)
- `Session` instance

## Outputs
- `{session_dir}/04_diagram.drawio` — raw mxGraph XML importable into diagrams.net
- `{session_dir}/04_documentation.md` — full markdown doc of the vault model

## mxGraph XML structure

```xml
<mxGraphModel>
  <root>
    <mxCell id="0"/>
    <mxCell id="1" parent="0"/>
    <!-- Hub: yellow rounded rectangle -->
    <mxCell id="hub_film" value="HUB_FILM\nfilm_id" style="rounded=1;fillColor=#FFE6CC;strokeColor=#d6b656;" vertex="1" parent="1">
      <mxGeometry x="100" y="100" width="120" height="60" as="geometry"/>
    </mxCell>
    <!-- Link: blue square -->
    <mxCell id="lnk_rental_customer" value="LNK_RENTAL_CUSTOMER" style="fillColor=#DAE8FC;strokeColor=#6c8ebf;" vertex="1" parent="1">
      <mxGeometry .../>
    </mxCell>
    <!-- Satellite: green ellipse -->
    <mxCell id="sat_film_pricing" value="SAT_FILM_PRICING\nrental_rate\nrental_duration\nreplacement_cost" style="ellipse;fillColor=#D5E8D4;strokeColor=#82b366;" vertex="1" parent="1">
      <mxGeometry .../>
    </mxCell>
    <!-- Edge: Hub → Link -->
    <mxCell edge="1" source="hub_film" target="lnk_film_language" .../>
    <!-- Edge: Hub → Satellite -->
    <mxCell edge="1" source="hub_film" target="sat_film_pricing" .../>
  </root>
</mxGraphModel>
```

### Layout algorithm
- Hubs: arranged in a grid (3 columns), starting at x=200, y=200, spacing 200px
- Links: positioned midway between their connected hubs (average of hub coordinates)
- Satellites: offset 150px to the right of their parent hub

### Style constants
| Entity | Fill | Border |
|--------|------|--------|
| Hub | `#FFE6CC` | `#d6b656` |
| Link | `#DAE8FC` | `#6c8ebf` |
| Satellite | `#D5E8D4` | `#82b366` |
| PIT | `#E1D5E7` | `#9673a6` |
| Bridge | `#F8CECC` | `#b85450` |

## `04_documentation.md` structure

```markdown
# Data Vault 2.0 Model — dvdrental

Generated: {timestamp}
Source: {session_id}

## Summary
- Hubs: 13
- Links: 17
- Satellites: 14
- PIT tables: 4
- Bridge tables: 2

## Hubs
| Hub | Business Key | Source Table | Record Source |
|-----|-------------|--------------|---------------|
| HUB_FILM | film_id | silver.silver_film | cdc.dvdrental.film |
...

## Links
| Link | Hubs Connected | Source Table |
...

## Satellites
| Satellite | Parent Hub | Tracked Columns | Split Reason |
...

## PIT Tables
...

## Bridge Tables
...

## Entity Relationship Notes
(decision log summary — LOW confidence items flagged for review)
```

## Key classes / functions

```python
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from ..models import DVModel
from ..session import Session

class DocGenerator:
    """Produces draw.io XML and markdown documentation from a DVModel."""

    HUB_STYLE   = "rounded=1;fillColor=#FFE6CC;strokeColor=#d6b656;"
    LINK_STYLE  = "fillColor=#DAE8FC;strokeColor=#6c8ebf;"
    SAT_STYLE   = "ellipse;fillColor=#D5E8D4;strokeColor=#82b366;"
    PIT_STYLE   = "ellipse;fillColor=#E1D5E7;strokeColor=#9673a6;"
    BRIDGE_STYLE= "fillColor=#F8CECC;strokeColor=#b85450;"

    def __init__(self, model: DVModel, session: Session):
        self.model = model
        self.session = session

    def run(self) -> None:
        if self.session.is_step_done("step4_doc_gen"):
            return
        self._write_diagram()
        self._write_markdown()
        self.session.mark_step_done("step4_doc_gen")

    def _write_diagram(self) -> None:
        """Build mxGraph XML tree and write 04_diagram.drawio."""

    def _write_markdown(self) -> None:
        """Render 04_documentation.md with tables for each entity type."""

    def _hub_position(self, index: int) -> tuple[int, int]:
        """Grid layout: 3 columns, 200px spacing."""
        col = index % 3
        row = index // 3
        return (200 + col * 220, 100 + row * 160)

    def _sat_position(self, hub_pos: tuple, sat_index: int) -> tuple[int, int]:
        """Offset satellites to the right of parent hub."""
        return (hub_pos[0] + 180, hub_pos[1] + sat_index * 100)
```

## Logic walkthrough
1. **Diagram**: Build an `mxGraphModel` XML tree. Add one `mxCell` per hub (grid layout), one per link (midpoint of connected hubs), one per satellite (offset from parent hub). Add edge cells connecting hubs to their links and satellites. Serialise with `ET.tostring(root, encoding="unicode")`.
2. **Markdown**: Render header + summary stats, then one table section per entity type. Append a "Flagged Items" section from `decision_logger.read_flagged()` if any LOW-confidence decisions exist.

## Acceptance criteria
- `04_diagram.drawio` is valid XML parseable by `xml.etree.ElementTree`
- File contains `<mxCell>` elements for all 13 hubs, 17 links, 14 satellites
- Hub cells use yellow fill; link cells use blue fill; satellite cells use green fill
- Opening the file in diagrams.net renders a connected graph without errors
- `04_documentation.md` contains one row per hub in the Hubs table
- All 14 satellite rows appear in the Satellites table with tracked columns listed
