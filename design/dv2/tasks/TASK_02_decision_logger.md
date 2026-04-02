# TASK_02: decision_logger.py — Audit Trail Logger

## File
`generators/dv_generator/decision_logger.py`

## Purpose
Provides an append-only audit trail of every classification and mapping decision made during the generator run. Written to `decisions.log` in the session folder. Human-reviewable after the run; also the basis for the human review notebook's "flagged items with reasoning" section.

## Depends on
- `TASK_01: models.py` — imports `DecisionEntry`, `ConfidenceLevel`

## Inputs
- `session_dir: str` — path to the current session folder (e.g. `generated/dv_sessions/20260402_143000/`)
- Individual `log()` calls from every step

## Outputs
- `{session_dir}/decisions.log` — newline-delimited JSON, one `DecisionEntry` per line

## Key classes / functions

```python
import json
from datetime import datetime, timezone
from pathlib import Path
from .models import DecisionEntry, ConfidenceLevel

class DecisionLogger:
    """Append-only logger for DV 2.0 generator decisions.

    Usage:
        logger = DecisionLogger(session_dir="/path/to/session")
        logger.log(
            step="step2_rule_engine",
            entity="silver_film",
            rule="R1: single integer PK → Hub",
            confidence=ConfidenceLevel.HIGH,
            reason="film_id is the sole integer PK; no composite key."
        )
    """

    def __init__(self, session_dir: str):
        """Open (or create) decisions.log in append mode."""
        self.log_path = Path(session_dir) / "decisions.log"
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        step: str,
        entity: str,
        rule: str,
        confidence: ConfidenceLevel,
        reason: str,
    ) -> DecisionEntry:
        """Append one decision to the log file and return the entry."""
        entry = DecisionEntry(
            step=step,
            entity=entity,
            rule=rule,
            confidence=confidence,
            reason=reason,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(self._to_dict(entry)) + "\n")
        return entry

    def read_all(self) -> list[DecisionEntry]:
        """Read all entries from decisions.log (for review/validation steps)."""
        if not self.log_path.exists():
            return []
        entries = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    d = json.loads(line)
                    entries.append(DecisionEntry(**{
                        **d,
                        "confidence": ConfidenceLevel(d["confidence"])
                    }))
        return entries

    def read_flagged(self) -> list[DecisionEntry]:
        """Return only LOW-confidence entries — these require human review."""
        return [e for e in self.read_all() if e.confidence == ConfidenceLevel.LOW]

    @staticmethod
    def _to_dict(entry: DecisionEntry) -> dict:
        return {
            "step": entry.step,
            "entity": entry.entity,
            "rule": entry.rule,
            "confidence": entry.confidence.value,
            "reason": entry.reason,
            "timestamp": entry.timestamp,
        }
```

## Logic walkthrough
1. `__init__` resolves the log path and ensures the parent directory exists.
2. `log()` constructs a `DecisionEntry`, serialises it to JSON, and appends a line to `decisions.log`. Returns the entry so callers can chain or inspect.
3. `read_all()` re-reads the entire log — used by `step5_review.py` and `step6_validator.py`.
4. `read_flagged()` filters to `LOW` confidence — surfaces items needing human attention in the review notebook.

## Acceptance criteria
- After 3 `log()` calls, `decisions.log` contains exactly 3 JSON lines
- `read_all()` returns the same 3 entries in order
- `read_flagged()` returns only entries with `confidence=LOW`
- Calling `log()` on an existing session file appends without overwriting prior entries
- `timestamp` is a valid ISO 8601 UTC string
