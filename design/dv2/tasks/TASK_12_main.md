# TASK_12: main.py — CLI Orchestrator

## File
`generators/dv_generator/main.py`

## Purpose
Entry point for the DV 2.0 generator. Provides a CLI with three modes: `--analyze` (full fresh run from step1), `--resume <session_id>` (continue an interrupted session from where it left off), and `--from-step <step_name>` (re-run from a specific step, skipping prior ones). Wires all modules together in the correct order and handles the pause after step5 (awaiting human review).

## Depends on
All other tasks (TASK_01 through TASK_11).

## Inputs (CLI arguments)
```
usage: python -m generators.dv_generator.main [-h]
       [--analyze]
       [--resume SESSION_ID]
       [--from-step STEP_NAME]
       [--config-dir CONFIG_DIR]
       [--repo-root REPO_ROOT]
       [--base-dir BASE_DIR]
       [--dry-run]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--analyze` | — | Start a fresh analysis run (creates new session) |
| `--resume SESSION_ID` | — | Resume session `SESSION_ID` from its last completed step |
| `--from-step STEP_NAME` | — | Combined with `--resume`: skip to a specific step, re-running it even if previously done |
| `--config-dir` | `pipeline_configs/silver` | Directory containing Silver JSON configs |
| `--repo-root` | `.` | Project root for final artifact placement |
| `--base-dir` | `generated/dv_sessions` | Where session folders are created |
| `--dry-run` | False | Run all steps but don't write to repo (step7 skips the copy) |

## Step execution order
```
step1_analyzer
  ↓
step2_rule_engine
  ↓
step3_artifact_gen
  ↓
step3b_notebook_gen
  ↓
step4_doc_gen
  ↓
step5_review        ← PAUSE: prints instructions, exits with code 0
  [human edits 05_review_notebook.ipynb and runs save cell]
  [human runs: python -m generators.dv_generator.main --resume <id> --from-step step6_validator]
  ↓
step6_validator
  ↓
step7_applier       ← PASS: writes to repo / FAIL: writes code_review.md
```

## Key classes / functions

```python
import argparse
import sys
from pathlib import Path

from .session import Session, STEPS
from .decision_logger import DecisionLogger
from .steps.step1_analyzer import SchemaAnalyzer
from .steps.step2_rule_engine import RuleEngine
from .steps.step3_artifact_gen import ArtifactGenerator
from .steps.step3b_notebook_gen import NotebookGenerator
from .steps.step4_doc_gen import DocGenerator
from .steps.step5_review import ReviewGenerator
from .steps.step6_validator import Validator
from .steps.step7_applier import Applier

def main() -> None:
    args = _parse_args()

    if args.analyze:
        session = Session(base_dir=args.base_dir)
    elif args.resume:
        session = Session(session_id=args.resume, base_dir=args.base_dir)
    else:
        print("Error: specify --analyze or --resume <session_id>")
        sys.exit(1)

    # If --from-step: remove that step and all subsequent from completed_steps
    # so they re-execute even if previously done
    if args.from_step:
        _reset_from_step(session, args.from_step)

    logger = DecisionLogger(str(session.session_dir))
    _run_pipeline(session, logger, args)

def _run_pipeline(session: Session, logger: DecisionLogger, args) -> None:
    # Step 1
    analyzer = SchemaAnalyzer(args.config_dir, session, logger)
    tables = analyzer.run()

    # Step 2
    engine = RuleEngine(tables, session, logger)
    model = engine.run()

    # Step 3
    artifact_gen = ArtifactGenerator(model, session)
    model = artifact_gen.run()

    # Step 3b
    nb_gen = NotebookGenerator(model, session)
    nb_gen.run()

    # Step 4
    doc_gen = DocGenerator(model, session)
    doc_gen.run()

    # Step 5 — pause for human review
    review_gen = ReviewGenerator(model, logger, session)
    review_gen.run()
    if session.state.status == "awaiting_review":
        # Normal pause point — human must edit and re-run --from-step step6_validator
        sys.exit(0)

    # Step 6
    validator = Validator(session)
    report = validator.run()

    # Step 7
    applier = Applier(session, repo_root=args.repo_root)
    success = applier.run()
    sys.exit(0 if success else 1)

def _reset_from_step(session: Session, from_step: str) -> None:
    """Remove from_step and all later steps from completed_steps so they re-run."""
    if from_step not in STEPS:
        print(f"Unknown step: {from_step}. Valid steps: {STEPS}")
        sys.exit(1)
    idx = STEPS.index(from_step)
    session.state.completed_steps = [s for s in session.state.completed_steps if STEPS.index(s) < idx]
    session._save()

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DV 2.0 model generator for Databricks CDC pipelines")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--analyze", action="store_true")
    group.add_argument("--resume", metavar="SESSION_ID")
    parser.add_argument("--from-step", metavar="STEP_NAME", dest="from_step")
    parser.add_argument("--config-dir", default="pipeline_configs/silver")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--base-dir", default="generated/dv_sessions")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()

if __name__ == "__main__":
    main()
```

## Usage examples

```bash
# Full fresh run (steps 1-5, then pauses)
python -m generators.dv_generator.main --analyze

# Resume after human review (steps 6-7)
python -m generators.dv_generator.main --resume 20260402_143000 --from-step step6_validator

# Re-run everything from step3 (e.g. after changing classification rules)
python -m generators.dv_generator.main --resume 20260402_143000 --from-step step3_artifact_gen

# Dry run (don't write to repo)
python -m generators.dv_generator.main --analyze --dry-run
```

## `__init__.py` required
```python
# generators/dv_generator/__init__.py  (empty or with version)
# generators/dv_generator/steps/__init__.py  (empty)
```

## Acceptance criteria
- `python -m generators.dv_generator.main --analyze` creates a new session folder and runs steps 1–5
- After step5, process exits with code 0 and prints resume instructions
- `python -m generators.dv_generator.main --resume <id> --from-step step6_validator` re-runs only steps 6–7
- `--from-step step3_artifact_gen` causes steps 3, 3b, 4, 5, 6, 7 to re-run (removes them from completed_steps)
- `--analyze` and `--resume` are mutually exclusive (argparse enforces)
- Unknown `--from-step` value exits with a helpful error message
- Exit code 0 on success, 1 on validation failure in step7
