# AGENTS.md

## Workflow for Changes

**Before making any changes to this repository:**

1. Create a new branch for your changes
2. Make your changes on that branch
3. Create a pull request
4. **Wait for human approval** before merging

```bash
# Example workflow
git checkout -b feature/your-feature-name
# ... make your changes ...
git add .
git commit -m "Description of changes"
git push -u origin feature/your-feature-name
# Then create PR via GitHub UI or: gh pr create --title "..." --body "..."
```

Do NOT commit directly to main/master.

## Purpose
This repository is a Databricks CDC lakehouse lab with:
- local Docker infrastructure
- Python automation and generator scripts
- Databricks notebook utilities
- a dbt project in `cdc_gold/`
Use this file as the repo-specific guide for coding agents.

## Rule Files Checked
- `.cursor/rules/`: not present
- `.cursorrules`: not present
- `.github/copilot-instructions.md`: not present
No extra Cursor or Copilot rules were found, so follow this file and nearby code.

## Repo Layout
- `runtime/`: Databricks helper modules and notebook normalization
- `generators/`: PostgreSQL data mutation generators
- `Agents/`: local specialist agent definitions and role prompts
- `skills/`: local skill activators and repo-specific operational workflows
- `pipeline_configs/silver/`: per-table JSON configs for generic Silver pipelines
- `dq_queries/silver/`: stored SQL data quality checks executed after Silver updates
- `skills/docker-databricks-lab-ops/scripts/`: operational smoke-test scripts
- `cdc_gold/`: dbt project for Gold-layer models and tests
- `docker-compose.yml`: local Postgres/Kafka/Debezium stack

## Local Agents And Skills
This repo includes a local agent/skill catalog. Use it as part of normal work.

### How To Use Them
- Check `skills/` before non-trivial Databricks, CDC, dbt, notebook, or ops work.
- For role-based skills, read the matching file in `Agents/` before substantive implementation.
- If multiple roles apply, state the sequence you are using and keep responsibilities separated.
- Prefer the local repo-specific workflow skill over ad hoc commands when operating the stack.

### Most Relevant Local Agents
- `Agents/engineering-data-engineer.md`: best default for CDC, dbt, pipeline, and data reliability work
- `Agents/databricks_architect.md`: architecture choices for Databricks and lakehouse topology
- `Agents/lakehouse_data_architect.md`: Bronze/Silver/Gold contracts and model boundaries
- `Agents/databricks-notebook-publisher.md`: publish local notebooks into Databricks workspaces
- `Agents/databricks-job-operator.md`: run jobs, capture `run_id`, and track terminal state
- `Agents/databricks-notebook-remediator.md`: diagnose failed notebook runs and fix forward
- `Agents/databricks-data-quality-analyst.md`: validate output quality beyond job success
- `Agents/databricks-dq-automation.md`: run stored SQL DQ checks after pipeline changes
- `Agents/spark_performance_engineer.md`: Spark tuning and execution-performance review
- `Agents/evidenceqa.md` and `Agents/testing-reality-checker.md`: evidence-based QA and final readiness checks
- `Agents/confluence-documentation-generator.md`: generate Confluence-ready docs with Mermaid diagrams

### Most Relevant Local Skills
- `skills/docker-databricks-lab-ops/`: primary repo-specific operational workflow; use for stack bring-up, connector registration, generators, ngrok/Kafka wiring, Databricks runs, and smoke tests
- `skills/engineering-data-engineer/`: activate the repo's data-engineer specialization
- `skills/lakehouse-data-architect/`: use for medallion design and CDC model decisions
- `skills/databricks-notebook-publisher/`: use before publishing notebooks to the workspace
- `skills/databricks-job-operator/`: use when executing and monitoring Databricks jobs
- `skills/databricks-notebook-remediator/`: use when Databricks runs fail
- `skills/databricks-data-quality-analyst/`: use when validating table or notebook outputs
- `skills/databricks-dq-automation/`: run repo-managed SQL checks in `dq_queries/silver/` after pipeline updates
- `skills/testing-reality-checker/`: use for final confidence checks before calling work done
- `skills/confluence-documentation-generator/`: use to generate Confluence-ready docs with Mermaid diagrams

### Recommended Sequencing
- Delivery flow: `engineering-data-engineer` -> `databricks-notebook-publisher` -> `databricks-job-operator`
- Failure flow: `databricks-job-operator` -> `databricks-notebook-remediator` -> `databricks-job-operator`
- Validation flow: `databricks-data-quality-analyst` -> `testing-reality-checker`
- Automated DQ flow: `databricks-dq-automation` after successful Silver or workflow updates
- Documentation flow: `confluence-documentation-generator` to generate Confluence-ready docs
- End-to-end ops flow: consult `skills/docker-databricks-lab-ops/` first

### Notes
- `Agents/agents-orchestrator.md` describes a broader multi-agent pipeline and can be used when a task spans planning, implementation, remediation, and QA.
- Some agent files are generic imports from a wider catalog; prioritize the Databricks, lakehouse, data engineering, and QA roles for this repository.
- Adding new agents or skills is allowed when repeated workflows appear that are not well covered by the current catalog.

## Setup Commands
- Install Python dependencies:
  - `python3 -m pip install -r requirements.txt`
- Create local env file:
  - `cp .envexample .env`
- Do not commit `.env`, `.databrickscfg`, or generated logs.

## Session Logging
- Use `OPENCODE_LOG.md` as the local handoff and activity log for repository work.
- At the start of every OpenCode session, read `OPENCODE_LOG.md` first and use it to restore prior context before making changes.
- During meaningful work, append concise entries covering what changed, what was validated, what failed, and any active follow-up items.
- Keep entries factual and scannable so the next session can resume quickly.

## Build / Run Commands
There is no top-level `Makefile`, `tox`, `nox`, or other unified task runner.
Use the native commands below.

### Infrastructure
- Start local services:
  - `docker compose up -d`
- Stop local services:
  - `docker compose down`
- Initialize the source schema:
  - `psql -h localhost -U postgres -d demo -f init-db.sql`
- Migrate older schema versions:
  - `psql -h localhost -U postgres -d demo -f migrate_schema.sql`
- Register the Debezium connector:
  - `curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' --data @postgres-connector.json`

### Python Scripts
- Run products generator:
  - `python3 generators/load_products_generator.py`
- Run orders generator:
  - `python3 generators/load_generator.py`
- Normalize notebooks after JSON edits:
  - `python3 runtime/normalize_notebooks.py <notebook> [<notebook> ...]`
- Databricks connectivity smoke script:
  - `python3 test_databricks.py`
- End-to-end notebook smoke test:
  - `python3 skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py`
- Generate Confluence documentation:
  - `python3 runtime/confluence_doc_generator.py [output_dir]`
  - Outputs: `docs/confluence_html.html`, `docs/confluence_markdown.md`, `docs/diagrams/*.mmd`

### dbt
Run these from `cdc_gold/`.
- Check profile/connectivity:
  - `dbt debug`
- Build models and run tests:
  - `dbt build`
- Run tests only:
  - `dbt test`
- Clean dbt artifacts:
  - `dbt clean`

## Lint / Format / Type Check
No linter, formatter, or type-checker config is currently committed.
- No verified `ruff`, `flake8`, `black`, `isort`, `mypy`, or `pyright` command exists.
- Do not invent a standard lint command in code changes or docs.
- If one of these tools is added, update this file with the exact command.

## Test Commands
There is no formal `pytest` or `unittest` suite configured today.
- Python smoke test:
  - `python3 test_databricks.py`
- End-to-end integration smoke test:
  - `python3 skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py`
- dbt tests:
  - `cd cdc_gold && dbt test`

## Running A Single Test
### Python
- No verified single-test `pytest` command exists yet.
- The only standalone Python test-like file is:
  - `python3 test_databricks.py`

### dbt
- Test one model:
  - `cd cdc_gold && dbt test --select total_products_order`
- Build and test one model:
  - `cd cdc_gold && dbt build --select total_products_order`
- Target a narrower selector when debugging source/model issues:
  - `cd cdc_gold && dbt test --select source:*`
If a Python test runner is added later, document both single-file and single-test-name commands here.

## Command Selection Guidance
- Prefer repo-documented commands over ad hoc shell pipelines.
- Prefer `python3` over `python` unless the repo standard changes.
- Prefer targeted dbt selectors before running the full dbt project.
- Use `dbt build` when both execution and tests are needed.
- Use the smoke-test script for full-flow validation, not for quick checks.

## Code Style Guidelines
These conventions are inferred from the current Python codebase.

### Imports
- Order imports as: standard library, third-party, local modules.
- Separate groups with one blank line.
- Prefer explicit imports.
- Remove unused imports.
- Use local imports like `from runtime.databricks_client import get_client`.

### Formatting
- Follow PEP 8 style.
- Use 4-space indentation.
- Keep top-level functions separated by blank lines.
- Keep formatting consistent with surrounding code.
- Avoid unrelated reformatting in targeted changes.

### Types
- Add type hints on new functions and helpers.
- Match existing Python 3.10+ syntax: `int | None`, `list[str]`.
- Prefer precise types at module boundaries and helper APIs.
- Avoid broad untyped containers when a clearer type is practical.

### Naming
- Use `snake_case` for functions, variables, and modules.
- Use `UPPER_CASE` for module-level constants.
- Prefer descriptive names over abbreviations.
- Reserve one-letter names for very short-lived locals only.

### Structure
- Keep functions focused and single-purpose.
- Prefer small helper functions for repeated environment parsing or command execution.
- Use `main() -> int` for CLI scripts when appropriate.
- Use `if __name__ == "__main__":` guards on executable modules.
- Return exit codes from `main()` and call `sys.exit(main())` when needed.

### Error Handling
- Raise explicit exceptions for invalid environment, infrastructure, or job state.
- Include actionable context in error messages.
- Do not swallow exceptions silently.
- Catch broad exceptions only at script boundaries that convert failures into stable output.
- Keep `subprocess.run(..., check=True, text=True)` unless failure should be handled manually.

### Output And Automation
- Use simple `print(...)` output for scripts unless a logger is introduced.
- Emit JSON for automation-facing results when downstream tooling may parse them.
- Keep operational output concise and useful.

### SQL / External Calls
- Parameterize SQL values; do not interpolate user-provided values directly into SQL.
- If SQL identifiers are dynamic, keep them tightly controlled and trusted.
- Centralize environment variable parsing in helpers when conversion is needed.
- Prefer `pathlib.Path` for filesystem work in newer scripts.

### Databricks Patterns
- Fail fast if required env vars like `DATABRICKS_HOST` or `DATABRICKS_TOKEN` are missing.
- Centralize Databricks client creation in helper functions.
- Poll jobs with explicit timeout handling.
- Return structured results from long-running operations when practical.

## Editing Guidance
- Keep changes narrow; the repo mixes infra, Python automation, notebooks, and dbt.
- Preserve existing behavior unless the task explicitly changes it.
- Update this file when adding new tooling, commands, or repo rules.
- Check `README.md`, `requirements.txt`, `runtime/`, and `cdc_gold/dbt_project.yml` before large changes.

## Current Gaps
- No formal Python unit-test framework is configured.
- No committed lint/type-check toolchain is configured.
- No root task runner exists.
When those are added, update this file immediately with exact commands and single-test examples.
