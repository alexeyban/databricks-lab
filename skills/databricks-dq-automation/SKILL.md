---
name: databricks-dq-automation
description: Run repository-managed Databricks SQL data quality checks after pipeline changes and turn results into a PASS/FAIL report.
---

# databricks-dq-automation

Use this skill when pipeline work changes Bronze, Silver, Gold, or notebook orchestration and you need automated post-update DQ validation.

## Workflow

1. Read [`Agents/databricks-dq-automation.md`](../../Agents/databricks-dq-automation.md) before substantive work.
2. Prefer stored SQL in `dq_queries/` over handwritten one-off checks.
3. Run `skills/databricks-dq-automation/scripts/run_silver_dq.py` after notebook or workflow updates that touch Silver outputs.
4. Treat any failed DQ check as a blocker until the cause is understood or fixed.

## Repository Mapping

- Agent definition: `Agents/databricks-dq-automation.md`
- Query folder: `dq_queries/silver/`
- Automation script: `skills/databricks-dq-automation/scripts/run_silver_dq.py`
