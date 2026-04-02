# TASK_03: session.py — Session State Manager

## File
`generators/dv_generator/session.py`

## Purpose
Manages the lifecycle of a generator run: creates a timestamped session folder, persists step completion state to `00_session_state.json`, and supports resuming interrupted runs from any step. Every other module calls `session.mark_step_done()` on completion and checks `session.is_step_done()` before re-running.

## Depends on
- `TASK_01: models.py` — no direct import, but `DVModel` is serialised here
- Standard library only (`pathlib`, `json`, `datetime`)

## Inputs
- `base_dir: str` — root for session folders (default: `generated/dv_sessions/`)
- `session_id: str | None` — if provided, resumes existing session; if None, creates new

## Outputs
- `generated/dv_sessions/YYYYMMDD_HHMMSS/` — new session folder (on create)
- `{session_dir}/00_session_state.json` — read/written on every step transition

## Key classes / functions

```python
import json
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, asdict, field

STEPS = [
    "step1_analyzer",
    "step2_rule_engine",
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
    status: str = "in_progress"   # "in_progress" | "awaiting_review" | "completed" | "failed"
    metadata: dict = field(default_factory=dict)  # free-form per-step metadata

class Session:
    """Creates or resumes a DV generator session.

    Usage — new session:
        session = Session()
        print(session.session_dir)   # generated/dv_sessions/20260402_143000

    Usage — resume:
        session = Session(session_id="20260402_143000")
    """

    STATE_FILE = "00_session_state.json"

    def __init__(self, session_id: str = None, base_dir: str = "generated/dv_sessions"):
        self.base_dir = Path(base_dir)
        if session_id:
            self.state = self._load(session_id)
        else:
            self.state = self._create()

    @property
    def session_dir(self) -> Path:
        return Path(self.state.session_dir)

    def mark_step_done(self, step: str, metadata: dict = None) -> None:
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
            raise FileNotFoundError(f"Session not found: {session_id}")
        with state_path.open() as f:
            d = json.load(f)
        return SessionState(**d)

    def _save(self) -> None:
        self._save_state(self.state)

    def _save_state(self, state: SessionState) -> None:
        state_path = Path(state.session_dir) / self.STATE_FILE
        with state_path.open("w") as f:
            json.dump(asdict(state), f, indent=2)
```

## Logic walkthrough
1. **New session**: `__init__` with no `session_id` calls `_create()`, which generates a `YYYYMMDD_HHMMSS` timestamp, creates the directory, writes `00_session_state.json`, returns a `SessionState`.
2. **Resume**: `__init__` with `session_id` calls `_load()`, reads and parses `00_session_state.json`, restores `SessionState`. Steps already in `completed_steps` will be skipped by callers.
3. **Step gating**: every step module calls `session.is_step_done(step_name)` at entry. If True, load artifacts from disk and return immediately without recomputing.
4. **Step completion**: every step module calls `session.mark_step_done(step_name, metadata={...})` before exiting.
5. **`next_pending_step()`**: used by `main.py --resume` to find where to restart.

## Acceptance criteria
- `Session()` creates `generated/dv_sessions/YYYYMMDD_HHMMSS/00_session_state.json`
- `Session(session_id=<id>)` loads the state without re-creating the folder
- `mark_step_done("step1_analyzer")` → `is_step_done("step1_analyzer")` returns True and `completed_steps` contains it
- State file is valid JSON after each `mark_step_done` call
- `next_pending_step()` returns the first step not in `completed_steps`
- Re-running `mark_step_done` with the same step does not duplicate entries in `completed_steps`
