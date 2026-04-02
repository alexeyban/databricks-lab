"""
DV 2.0 Generator — Decision Logger (TASK_02)

Append-only audit trail of every classification and mapping decision made
during a generator run. Written to decisions.log in the session folder.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .models import ConfidenceLevel, DecisionEntry


class DecisionLogger:
    """Append-only logger for DV 2.0 generator decisions.

    Usage::

        logger = DecisionLogger(session_dir="/path/to/session")
        logger.log(
            step="step2_rule_engine",
            entity="silver_film",
            rule="R1: single integer PK → Hub",
            confidence=ConfidenceLevel.HIGH,
            reason="film_id is the sole integer PK; no composite key.",
        )
    """

    LOG_FILE = "decisions.log"

    def __init__(self, session_dir: str) -> None:
        """Open (or create) decisions.log in append mode."""
        self.log_path = Path(session_dir) / self.LOG_FILE
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
        """Read all entries from decisions.log."""
        if not self.log_path.exists():
            return []
        entries: list[DecisionEntry] = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    d = json.loads(line)
                    entries.append(
                        DecisionEntry(
                            step=d["step"],
                            entity=d["entity"],
                            rule=d["rule"],
                            confidence=ConfidenceLevel(d["confidence"]),
                            reason=d["reason"],
                            timestamp=d["timestamp"],
                        )
                    )
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
