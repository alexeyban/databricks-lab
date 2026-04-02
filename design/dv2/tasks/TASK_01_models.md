# TASK_01: models.py — Core Dataclasses

## File
`generators/dv_generator/models.py`

## Purpose
Defines all data-transfer objects shared across every module in the generator. Acts as the single schema contract: every step reads and writes instances of these classes, never raw dicts. Ensures type safety and IDE completion throughout the codebase.

## Depends on
Nothing — this is the foundation module with zero internal imports.

## Inputs
None at runtime. Imported by all other modules.

## Outputs
Python module exporting dataclasses and enums.

## Key classes / functions

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

class DVObjectType(Enum):
    HUB = "hub"
    LINK = "link"
    SATELLITE = "satellite"
    PIT = "pit"
    BRIDGE = "bridge"

class ConfidenceLevel(Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

@dataclass
class ColumnDef:
    """Represents one column from a Silver source table."""
    name: str
    data_type: str                  # e.g. "integer", "varchar", "numeric", "timestamp"
    nullable: bool
    is_pk: bool = False
    is_fk: bool = False
    fk_table: Optional[str] = None  # target table name if is_fk=True
    fk_column: Optional[str] = None

@dataclass
class TableDef:
    """Represents one Silver source table as parsed by the analyzer."""
    name: str                       # e.g. "silver_film"
    source_table: str               # fully-qualified: "silver.silver_film"
    columns: list[ColumnDef] = field(default_factory=list)
    pk_columns: list[str] = field(default_factory=list)
    fk_hints: dict[str, str] = field(default_factory=dict)  # col_name → target_table

@dataclass
class LinkRef:
    """One FK reference within a Link definition."""
    hub: str           # e.g. "HUB_FILM"
    source_column: str # e.g. "film_id"

@dataclass
class HubDef:
    """Data Vault 2.0 Hub definition."""
    name: str                           # e.g. "HUB_FILM"
    target_table: str                   # e.g. "vault.hub_film"
    source_table: str                   # e.g. "silver.silver_film"
    business_key_columns: list[str]
    load_date_column: str               # watermark column in source
    record_source: str                  # e.g. "cdc.dvdrental.film"
    enabled: bool = True
    confidence: ConfidenceLevel = ConfidenceLevel.HIGH
    rules_fired: list[str] = field(default_factory=list)

@dataclass
class LinkDef:
    """Data Vault 2.0 Link definition."""
    name: str                           # e.g. "LNK_RENTAL_CUSTOMER"
    target_table: str
    source_table: str
    hub_references: list[LinkRef]       # ordered list of FK refs
    load_date_column: str
    record_source: str
    enabled: bool = True
    confidence: ConfidenceLevel = ConfidenceLevel.HIGH
    rules_fired: list[str] = field(default_factory=list)

@dataclass
class SatDef:
    """Data Vault 2.0 Satellite definition."""
    name: str                           # e.g. "SAT_FILM_PRICING"
    target_table: str
    parent_hub: str                     # e.g. "HUB_FILM"
    source_table: str
    hub_key_source_column: str          # PK column in source that maps to hub BK
    tracked_columns: list[str]          # payload columns to DIFF_HASH
    load_date_column: str
    record_source: str
    split_reason: Optional[str] = None  # why this sat was split from another
    enabled: bool = True
    confidence: ConfidenceLevel = ConfidenceLevel.HIGH
    rules_fired: list[str] = field(default_factory=list)

@dataclass
class PitDef:
    """Point-in-Time table definition."""
    name: str                           # e.g. "PIT_FILM"
    target_table: str
    hub: str                            # e.g. "HUB_FILM"
    satellites: list[str]               # list of SatDef.name
    snapshot_grain: str = "daily"       # "daily" | "hourly"
    enabled: bool = True

@dataclass
class BridgeDef:
    """Bridge table definition."""
    name: str                           # e.g. "BRG_RENTAL_FILM"
    target_table: str
    path: list[str]                     # alternating Hub/Link names: [HUB_A, LNK_AB, HUB_B, ...]
    enabled: bool = True

@dataclass
class DecisionEntry:
    """One logged decision from the rule engine or analyzer."""
    step: str           # e.g. "step2_rule_engine"
    entity: str         # table or object name
    rule: str           # rule ID + description, e.g. "R1: single integer PK → Hub"
    confidence: ConfidenceLevel
    reason: str         # human-readable explanation
    timestamp: str      # ISO 8601

@dataclass
class DVModel:
    """Complete Data Vault 2.0 model — the central output of the generator."""
    hubs: list[HubDef] = field(default_factory=list)
    links: list[LinkDef] = field(default_factory=list)
    satellites: list[SatDef] = field(default_factory=list)
    pit_tables: list[PitDef] = field(default_factory=list)
    bridge_tables: list[BridgeDef] = field(default_factory=list)
```

## Logic walkthrough
Pure data definitions — no logic. All fields use default_factory where mutable defaults would otherwise be needed. `DVObjectType` and `ConfidenceLevel` are enums used throughout the rule engine and decision logger.

## Acceptance criteria
- `from generators.dv_generator.models import HubDef, LinkDef, SatDef, PitDef, BridgeDef, DVModel, DecisionEntry` succeeds
- `HubDef(name="HUB_FILM", ...)` is instantiable with all required fields
- `DVModel()` produces an empty model with empty lists (not shared mutable state)
- All dataclasses are serialisable via `dataclasses.asdict()`
