"""
DV 2.0 Generator — Session State Manager (TASK_03)

Creates/resumes a generator session, persists step completion state to
00_session_state.json, supports resuming interrupted runs from any step.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Ordered list of all pipeline steps — used for resume/skip logic
STEPS: list[str] = [
    "step1_analyzer",
    "step2_rule_engine",
    "step2b_ai_classifier",
    "step3_artifact_gen",
    "step3b_notebook_gen",
    "step4_doc_gen",
    "step5_review",
    "step6_validator",
    "step7_applier",
]


@dataclass
class SessionState:
    session_id: str
    session_dir: str
    created_at: str
    completed_steps: list[str] = field(default_factory=list)
    current_step: str = ""
    status: str = "in_progress"  # "in_progress"|"awaiting_review"|"completed"|"failed"
    metadata: dict = field(default_factory=dict)


class Session:
    """Creates or resumes a DV generator session.

    Usage — new session::

        session = Session()
        print(session.session_dir)  # generated/dv_sessions/20260402_143000

    Usage — resume::

        session = Session(session_id="20260402_143000")
    """

    STATE_FILE = "00_session_state.json"

    def __init__(
        self,
        session_id: str | None = None,
        base_dir: str = "generated/dv_sessions",
    ) -> None:
        self.base_dir = Path(base_dir)
        if session_id:
            self.state = self._load(session_id)
        else:
            self.state = self._create()

    @property
    def session_dir(self) -> Path:
        return Path(self.state.session_dir)

    def mark_step_done(self, step: str, metadata: dict | None = None) -> None:
        """Record a step as completed and persist state."""
        if step not in self.state.completed_steps:
            self.state.completed_steps.append(step)
        self.state.current_step = step
        if metadata:
            self.state.metadata[step] = metadata
        self._save()

    def is_step_done(self, step: str) -> bool:
        """True if this step already completed (skip on resume)."""
        return step in self.state.completed_steps

    def set_status(self, status: str) -> None:
        self.state.status = status
        self._save()

    def next_pending_step(self) -> str | None:
        """Return the first step not yet completed, or None if all done."""
        for step in STEPS:
            if step not in self.state.completed_steps:
                return step
        return None

    def reset_from_step(self, from_step: str) -> None:
        """Remove from_step and all later steps so they re-execute."""
        if from_step not in STEPS:
            raise ValueError(
                f"Unknown step: {from_step!r}. Valid steps: {STEPS}"
            )
        idx = STEPS.index(from_step)
        self.state.completed_steps = [
            s for s in self.state.completed_steps
            if s in STEPS and STEPS.index(s) < idx
        ]
        self._save()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _create(self) -> SessionState:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        session_dir = self.base_dir / ts
        session_dir.mkdir(parents=True, exist_ok=True)
        state = SessionState(
            session_id=ts,
            session_dir=str(session_dir),
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        self._save_state(state)
        return state

    def _load(self, session_id: str) -> SessionState:
        state_path = self.base_dir / session_id / self.STATE_FILE
        if not state_path.exists():
            raise FileNotFoundError(
                f"Session not found: {session_id!r} "
                f"(looked for {state_path})"
            )
        with state_path.open() as f:
            d = json.load(f)
        return SessionState(**d)

    def _save(self) -> None:
        self._save_state(self.state)

    def _save_state(self, state: SessionState) -> None:
        state_path = Path(state.session_dir) / self.STATE_FILE
        with state_path.open("w") as f:
            json.dump(asdict(state), f, indent=2)
