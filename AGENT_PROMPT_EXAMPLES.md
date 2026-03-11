# Agent Prompt Examples

This file contains example prompts for the agent roles defined in [Agents](/home/legion/PycharmProjects/gitlab/databricks-lab/Agents).

The agent-catalog idea used in this repository was inspired by [agency-agents](https://github.com/msitarzewski/agency-agents/).

## Core Orchestration

### Agents Orchestrator

```text
Use the Agents Orchestrator to run the end-to-end implementation workflow for this repository. Start from the current README and notebooks, coordinate the required agents, and return a phase-by-phase execution plan with clear handoffs.
```

### Senior Project Manager

```text
Act as Senior Project Manager. Read README.md and convert the current lakehouse lab into a concrete task list for adding a new CDC source table called customers. Keep scope realistic and define acceptance criteria per task.
```

### ArchitectUX

```text
Act as ArchitectUX. Create an implementation-ready architecture for extending this lab with a monitoring dashboard for Bronze, Silver, and Gold freshness, including pages, states, navigation, and backend/frontend boundaries.
```

## Engineering

### Frontend Developer

```text
Act as Frontend Developer. Implement a small status UI for this project that shows Docker service health, Kafka connector status, and the latest Databricks smoke test result.
```

### Backend Architect

```text
Act as Backend Architect. Design the API and service structure for exposing smoke-test results, connector status, and notebook run history to a frontend dashboard.
```

### engineering-senior-developer

```text
Act as engineering-senior-developer. Implement a pragmatic full-stack improvement that stores smoke-test run summaries in JSON and renders the last successful run in a local status page.
```

### Data Engineer

```text
Act as Data Engineer. Extend the medallion pipeline to support a new PostgreSQL source table called customers, including source schema, generators, Bronze ingestion, Silver merge logic, and Gold-ready output contracts.
```

### DevOps Automator

```text
Act as DevOps Automator. Automate the smoke-test workflow so it can run in CI with Docker services, ngrok bootstrap discovery, and Databricks notebook verification.
```

## Databricks and Lakehouse

### Databricks Architect

```text
Act as Databricks Architect. Review this repository and propose a production-grade Databricks architecture for running the CDC lab across dev, staging, and prod with Unity Catalog, Jobs, and checkpoint isolation.
```

### Databricks Platform Engineer

```text
Act as Databricks Platform Engineer. Define how to provision and manage Databricks workspaces, jobs, access control, and CI/CD for this lab at team scale.
```

### Lakehouse Data Architect

```text
Act as Lakehouse Data Architect. Design the Bronze, Silver, and Gold data models for adding customers and customer-order analytics to this project.
```

### Spark Performance Engineer

```text
Act as Spark Performance Engineer. Analyze the Bronze and Silver notebooks in this repository and recommend concrete performance optimizations for Kafka ingest, merge logic, file sizing, and checkpoint behavior.
```

## Databricks Notebook Operations

### databricks-notebook-publisher

```text
Act as databricks-notebook-publisher. Push notebooks/bronze/NB_ingest_to_bronze.ipynb and notebooks/silver/NB_process_to_silver.ipynb into the target Databricks workspace path and report the publish result.
```

### databricks-job-operator

```text
Act as databricks-job-operator. Run the Orders ingest job in Databricks with the current ngrok Kafka bootstrap and return run_id, lifecycle state, result state, and task-level statuses.
```

### databricks-notebook-remediator

```text
Act as databricks-notebook-remediator. Investigate the latest failed Databricks run for the Bronze or Silver notebook, identify the root cause, and propose the smallest safe fix.
```

### databricks-data-quality-analyst

```text
Act as databricks-data-quality-analyst. Validate Bronze, Silver, and Gold outputs for this repository, including freshness, completeness, duplicates, referential integrity, and business rules, then produce a DQ report.
```

### databricks-notebook-algorithm-auditor

```text
Act as databricks-notebook-algorithm-auditor. Review NB_ingest_to_bronze.ipynb and NB_process_to_silver.ipynb for logic correctness, CDC semantics, idempotency, and scaling risks.
```

### drawio-architecture-architect

```text
Act as drawio-architecture-architect. Create a draw.io architecture package for this repository, including a system overview and a detailed diagram of PostgreSQL, Debezium, Kafka, Databricks notebooks, Delta tables, dbt Gold models, and smoke-test automation.
```

## Quality, Analysis, and Reporting

### EvidenceQA

```text
Act as EvidenceQA. Validate the latest smoke-test workflow changes in this repository and return PASS or FAIL with evidence, exact reproduction steps, and concrete findings only.
```

### testing-reality-checker

```text
Act as testing-reality-checker. Perform a final readiness review of this repository for public release, focusing on secret hygiene, smoke-test reliability, notebook execution evidence, and operational gaps.
```

### Data Analytics Reporter

```text
Act as Data Analytics Reporter. Build an executive summary of pipeline health for this project from available notebook results, source mutation counts, and Gold model outputs.
```

### Data Consolidation Agent

```text
Act as Data Consolidation Agent. Consolidate current sales-like outputs in Gold into dashboard-friendly aggregates by product, price band, and pipeline freshness.
```

## Practical Multi-Agent Prompts

### Public Release Readiness

```text
Use Agents Orchestrator to prepare this repository for public release. Coordinate testing-reality-checker, databricks-data-quality-analyst, EvidenceQA, and DevOps Automator. Return a release checklist, blocking issues, and exact remediation steps.
```

### New Source Onboarding

```text
Use Agents Orchestrator to extend this lab with a new customers source. Coordinate Senior Project Manager, Lakehouse Data Architect, Data Engineer, Databricks Architect, and databricks-data-quality-analyst. Produce a task plan and implementation sequence.
```

### Databricks Runtime Incident

```text
Use Agents Orchestrator to investigate a failed Databricks pipeline run. Coordinate databricks-job-operator, databricks-notebook-remediator, Spark Performance Engineer, and databricks-notebook-algorithm-auditor. Return root cause, fix plan, and rerun criteria.
```
