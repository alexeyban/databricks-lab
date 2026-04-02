# TASK_05: step2_rule_engine.py — DV 2.0 Classification Rule Engine

## File
`generators/dv_generator/steps/step2_rule_engine.py`

## Purpose
Applies a fixed set of DV 2.0 classification rules to the schema analysis output to determine which Silver tables become Hubs, which FK relationships become Links, and how columns split across Satellites. Logs every rule that fires with its confidence level. Writes `02_classification.json` containing fully-populated `HubDef`, `LinkDef`, `SatDef` instances (PIT/Bridge are scaffolded in step3).

## Depends on
- `TASK_01: models.py` — `HubDef`, `LinkDef`, `SatDef`, `ConfidenceLevel`, `DVModel`
- `TASK_02: decision_logger.py` — `DecisionLogger`
- `TASK_03: session.py` — `Session`
- `TASK_04: step1_analyzer.py` — consumes `list[TableDef]` from `01_schema_analysis.json`

## Inputs
- `list[TableDef]` from `01_schema_analysis.json`
- `Session` + `DecisionLogger` instances

## Outputs
- `{session_dir}/02_classification.json` — serialised `DVModel` (hubs, links, satellites only; pit/bridge empty at this stage)

## Classification Rules

### Hub rules
| ID | Rule | Confidence |
|----|------|-----------|
| R1 | Table has a single-column integer PK (ends in `_id`) → one Hub per table | HIGH |
| R2 | Table is a junction table (two FKs, no meaningful non-FK columns) → **not** a Hub, becomes a Link only | HIGH |
| R3 | Table has no PK or composite PK → flag for human review | LOW |
| R4 | Table name matches known reference pattern (`country`, `city`, `language`, `category`, `actor`, `store`, `staff`, `address`) → Hub regardless of column count | HIGH |

### Link rules
| ID | Rule | Confidence |
|----|------|-----------|
| L1 | Any FK column → one Link per (source_table, FK_col) pair, connecting source Hub to target Hub | HIGH |
| L2 | Junction table (film_actor, film_category) → single Link with 2 hub_references | HIGH |
| L3 | Table has 3+ FK columns (e.g. rental: inventory_id, customer_id, staff_id) → generate one Link per FK pair or one multi-hub Link (configurable; default: one Link per FK) | MEDIUM |

### Satellite split rules (S2 heuristic — column name patterns)
| ID | Rule | Confidence |
|----|------|-----------|
| S1 | All non-PK, non-FK, non-audit columns belong to one satellite by default | HIGH |
| S2 | Columns matching pricing patterns (`rate`, `cost`, `price`, `amount`, `fee`) → split into a separate `_PRICING` or `_DETAIL` satellite | MEDIUM |
| S3 | Columns matching status patterns (`active`, `status`, `flag`, `enabled`) → split into a separate `_STATUS` satellite if table also has other attributes | MEDIUM |
| S4 | Audit-only columns (`last_update`, `last_updated_dt`) → go into the satellite alongside the most relevant payload; if table has only audit columns → create marker satellite | HIGH |

## Key classes / functions

```python
from ..models import TableDef, HubDef, LinkDef, SatDef, DVModel, ConfidenceLevel, LinkRef
from ..decision_logger import DecisionLogger
from ..session import Session

PRICING_KEYWORDS = {"rate", "cost", "price", "amount", "fee", "salary"}
STATUS_KEYWORDS  = {"active", "status", "flag", "enabled", "activebool"}
AUDIT_COLUMNS    = {"last_update", "last_updated_dt", "created_at", "updated_at"}
JUNCTION_TABLES  = {"silver_film_actor", "silver_film_category"}
REFERENCE_TABLES = {"silver_country", "silver_city", "silver_language", "silver_category",
                    "silver_actor", "silver_store", "silver_staff", "silver_address"}

class RuleEngine:
    """Classifies Silver tables into DV 2.0 entities.

    Usage:
        engine = RuleEngine(tables, session, logger)
        model = engine.run()   # returns DVModel with hubs/links/sats populated
    """

    def __init__(self, tables: list[TableDef], session: Session, logger: DecisionLogger):
        self.tables = {t.name: t for t in tables}
        self.session = session
        self.logger = logger
        self.output_path = session.session_dir / "02_classification.json"

    def run(self) -> DVModel:
        if self.session.is_step_done("step2_rule_engine"):
            return self._load_cached()

        model = DVModel()
        for table in self.tables.values():
            self._classify_hub(table, model)
        for table in self.tables.values():
            self._classify_links(table, model)
        for table in self.tables.values():
            self._classify_satellites(table, model)

        self._save(model)
        self.session.mark_step_done("step2_rule_engine", metadata={
            "hubs": len(model.hubs),
            "links": len(model.links),
            "satellites": len(model.satellites),
        })
        return model

    def _classify_hub(self, table: TableDef, model: DVModel) -> None:
        """Apply R1–R4. Appends HubDef to model.hubs if table qualifies."""

    def _classify_links(self, table: TableDef, model: DVModel) -> None:
        """Apply L1–L3. Appends LinkDef entries to model.links."""

    def _classify_satellites(self, table: TableDef, model: DVModel) -> None:
        """Apply S1–S4. Appends SatDef entries to model.satellites."""

    def _hub_name(self, table_name: str) -> str:
        """Convert 'silver_film' → 'HUB_FILM'."""
        return "HUB_" + table_name.replace("silver_", "").upper()

    def _link_name(self, source: str, target: str) -> str:
        """Convert (silver_rental, silver_customer) → 'LNK_RENTAL_CUSTOMER'."""

    def _sat_name(self, hub_name: str, suffix: str) -> str:
        """Convert ('HUB_FILM', 'PRICING') → 'SAT_FILM_PRICING'."""

    def _is_junction(self, table: TableDef) -> bool:
        """True if table has exactly 2 FK cols and no non-FK non-audit payload columns."""

    def _split_columns(self, non_pk_non_fk_cols: list[str]) -> dict[str, list[str]]:
        """
        Apply S2/S3 heuristic. Returns dict of {suffix: [col_list]}.
        Example: {"CORE": ["title", "description"], "PRICING": ["rental_rate", "rental_duration"]}
        """

    def _load_cached(self) -> DVModel: ...
    def _save(self, model: DVModel) -> None: ...
```

## Logic walkthrough
1. **Hub pass**: iterate all tables. Apply R2 first (junction tables → skip hub). Apply R1/R4 for all others → create `HubDef`. Log rule fired.
2. **Link pass**: iterate all tables. For each FK hint in `table.fk_hints`, create a `LinkDef` connecting source hub to target hub via `LinkRef`. Junction tables (L2) produce a single `LinkDef` with 2 `hub_references`.
3. **Satellite pass**: for each non-junction table, collect non-PK, non-FK, non-audit columns. Call `_split_columns()`. For each bucket, create one `SatDef` with `tracked_columns` set to that bucket.
4. **Naming convention**: `HUB_FILM`, `LNK_RENTAL_CUSTOMER`, `SAT_FILM_PRICING` etc. — all uppercase.
5. Every rule application is logged via `DecisionLogger` with rule ID + reason.

## Expected output for dvdrental (partial)
- 13 Hubs (one per non-junction table)
- 17 Links
- 14 Satellites (film splits into CORE + PRICING; others are single satellites)

## Acceptance criteria
- `model.hubs` count = 13 for dvdrental
- `model.links` count = 17
- `model.satellites` count = 14 (film has 2 satellites)
- Junction tables (film_actor, film_category) produce Links but NOT Hubs
- `SAT_FILM_PRICING.tracked_columns` contains `rental_rate`, `rental_duration`, `replacement_cost`
- `SAT_FILM_CORE.tracked_columns` does NOT contain any pricing columns
- Every rule application appears in `decisions.log`
- `02_classification.json` is valid JSON that round-trips through `_load_cached()`
