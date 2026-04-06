"""
DV 2.0 Generator — CLI Orchestrator (TASK_12)

Entry point for the DV 2.0 auto-generator tool.

Usage:
    python -m generators.dv_generator.main --analyze
    python -m generators.dv_generator.main --resume <session_id> --from-step step6_validator
    python -m generators.dv_generator.main --analyze --dry-run
"""
from __future__ import annotations

import argparse
import sys

from .decision_logger import DecisionLogger
from .session import Session, STEPS
from .steps.step1_analyzer import SchemaAnalyzer
from .steps.step2_rule_engine import RuleEngine
from .steps.step2b_ai_classifier import AIClassifier
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
        print(f"New session: {session.state.session_id}  ({session.session_dir})")
    else:
        session = Session(session_id=args.resume, base_dir=args.base_dir)
        print(f"Resumed session: {session.state.session_id}  ({session.session_dir})")

    if args.from_step:
        if args.from_step not in STEPS:
            print(f"Error: unknown step '{args.from_step}'. Valid steps:\n  " + "\n  ".join(STEPS))
            sys.exit(1)
        session.reset_from_step(args.from_step)
        print(f"Reset to step: {args.from_step}")

    logger = DecisionLogger(str(session.session_dir))
    _run_pipeline(session, logger, args)


def _run_pipeline(session: Session, logger: DecisionLogger, args: argparse.Namespace) -> None:
    # ------------------------------------------------------------------ #
    # Step 1 — Schema analysis                                            #
    # ------------------------------------------------------------------ #
    analyzer = SchemaAnalyzer(args.config_dir, session, logger)
    tables = analyzer.run()
    print(f"[step1] Analyzed {len(tables)} tables")

    # ------------------------------------------------------------------ #
    # Step 2 — Heuristic rule engine                                     #
    # ------------------------------------------------------------------ #
    engine = RuleEngine(tables, session, logger)
    model = engine.run()
    print(
        f"[step2] Classified: {len(model.hubs)} hubs, "
        f"{len(model.links)} links, {len(model.satellites)} satellites"
    )

    # ------------------------------------------------------------------ #
    # Step 2b — AI semantic classifier (optional, skipped with --no-ai)  #
    # ------------------------------------------------------------------ #
    if not args.no_ai:
        try:
            ai_classifier = AIClassifier(tables, model, session, logger,
                                          genie_space_id=args.genie_space_id)
            model = ai_classifier.run()
            print(
                f"[step2b] AI merge: {len(model.hubs)} hubs, "
                f"{len(model.links)} links, {len(model.satellites)} satellites"
            )
        except (EnvironmentError, ValueError) as exc:
            print(f"[step2b] Skipped (env not configured): {exc}")
    else:
        print("[step2b] Skipped (--no-ai)")

    # ------------------------------------------------------------------ #
    # Step 3 — Artifact generation (dv_model_draft.json + templates)     #
    # ------------------------------------------------------------------ #
    artifact_gen = ArtifactGenerator(model, session)
    model = artifact_gen.run()
    print(f"[step3] Model draft + query templates written")

    # ------------------------------------------------------------------ #
    # Step 3b — Notebook generation                                       #
    # ------------------------------------------------------------------ #
    nb_gen = NotebookGenerator(model, session)
    nb_gen.run()
    print(f"[step3b] Vault notebooks generated")

    # ------------------------------------------------------------------ #
    # Step 4 — Documentation + draw.io diagram                           #
    # ------------------------------------------------------------------ #
    doc_gen = DocGenerator(model, session)
    doc_gen.run()
    print(f"[step4] Documentation + diagram written")

    # ------------------------------------------------------------------ #
    # Step 5 — Human review notebook                                     #
    # ------------------------------------------------------------------ #
    step5_was_done = session.is_step_done("step5_review")
    review_gen = ReviewGenerator(model, logger, session)
    review_gen.run()

    if not step5_was_done and session.state.status == "awaiting_review":
        # Normal pause — step5 just ran; human must open the notebook and re-run
        sys.exit(0)

    # ------------------------------------------------------------------ #
    # Step 6 — Validation                                                 #
    # ------------------------------------------------------------------ #
    validator = Validator(session)
    report = validator.run()
    status = "PASSED" if report.passed else "FAILED"
    print(
        f"[step6] Validation {status}: "
        f"{report.error_count} error(s), {report.warning_count} warning(s)"
    )

    # ------------------------------------------------------------------ #
    # Step 7 — Apply (or code review)                                    #
    # ------------------------------------------------------------------ #
    applier = Applier(session, repo_root=args.repo_root if not args.dry_run else "/dev/null")
    success = applier.run()
    sys.exit(0 if success else 1)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m generators.dv_generator.main",
        description="DV 2.0 model generator for Databricks CDC pipelines",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--analyze",
        action="store_true",
        help="Start a fresh analysis run (creates new session)",
    )
    group.add_argument(
        "--resume",
        metavar="SESSION_ID",
        help="Resume an existing session from its last completed step",
    )
    parser.add_argument(
        "--from-step",
        metavar="STEP_NAME",
        dest="from_step",
        help=f"Re-run from this step (one of: {', '.join(STEPS)})",
    )
    parser.add_argument(
        "--config-dir",
        default="pipeline_configs/silver",
        help="Directory containing Silver JSON configs (default: pipeline_configs/silver)",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Project root for final artifact placement (default: .)",
    )
    parser.add_argument(
        "--base-dir",
        default="generated/dv_sessions",
        help="Where session folders are created (default: generated/dv_sessions)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all steps but skip writing artifacts to repo in step7",
    )
    parser.add_argument(
        "--no-ai",
        action="store_true",
        dest="no_ai",
        help="Skip step2b AI classifier and use heuristic-only output",
    )
    parser.add_argument(
        "--genie-space-id",
        metavar="SPACE_ID",
        dest="genie_space_id",
        default=None,
        help="Databricks Genie space ID for semantic table enrichment (overrides GENIE_SPACE_ID env var)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
