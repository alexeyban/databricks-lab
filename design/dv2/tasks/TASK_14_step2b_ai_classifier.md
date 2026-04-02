# TASK_14: step2b_ai_classifier.py — AI Semantic Classifier + Merge

## File
`generators/dv_generator/steps/step2b_ai_classifier.py`

## Purpose
Runs an LLM-powered classification of Silver table schemas **in parallel** with the heuristic rule engine output. Fetches 50–100 data sample rows from each Silver table via Databricks SQL to give the AI semantic context (actual values, not just column names). Calls the LLM with a tool that accepts a full `DVModel` JSON schema, forcing structured output. Then merges the AI model with the heuristic model — agreements become HIGH confidence, disagreements become LOW confidence and are flagged in `decisions.log` for the human reviewer in step5.

## Depends on
- `TASK_01: models.py` — `DVModel`, `HubDef`, `LinkDef`, `SatDef`, `ConfidenceLevel`
- `TASK_02: decision_logger.py` — `DecisionLogger`
- `TASK_03: session.py` — `Session`
- `TASK_13: llm_client.py` — `LLMClient`, `Message`
- `databricks-sdk` (already in requirements.txt) — for SQL execution

## Inputs
- `list[TableDef]` from `01_schema_analysis.json`
- `DVModel` (heuristic) from `02_classification.json`
- Databricks SQL warehouse (env: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`)
- `LLMClient` configured via env vars (see TASK_13)
- `Session` + `DecisionLogger`

## Outputs
- `{session_dir}/data_samples/{table_name}_sample.json` — 50–100 rows per table (cached)
- `{session_dir}/02b_ai_classification.json` — raw AI tool call output (DVModel before merge)
- `{session_dir}/02b_merged_classification.json` — final merged DVModel (fed to step3)

## Databricks SQL data sampling

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

def fetch_table_sample(table: str, warehouse_id: str, limit: int = 100) -> list[dict]:
    """Execute SELECT * LIMIT {limit} on a Silver table and return rows as list of dicts."""
    client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )
    statement = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"SELECT * FROM {table} LIMIT {limit}",
        wait_timeout="30s",
    )
    columns = [c.name for c in statement.manifest.schema.columns]
    rows = []
    for row in (statement.result.data_array or []):
        rows.append(dict(zip(columns, row)))
    return rows
```

## AI tool definition (DVModel tool schema)

```python
DV_CLASSIFY_TOOL = {
    "name": "classify_dv_model",
    "description": (
        "Classify the provided Silver tables into a Data Vault 2.0 model. "
        "Return hubs (one per unique business entity), links (one per FK relationship "
        "or many-to-many junction), and satellites (grouped by change rate / business theme). "
        "Use the actual data samples to infer business semantics."
    ),
    "input_schema": {
        "type": "object",
        "required": ["hubs", "links", "satellites"],
        "properties": {
            "hubs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "source_table", "business_key_columns", "reasoning"],
                    "properties": {
                        "name":                  {"type": "string", "description": "e.g. HUB_FILM"},
                        "source_table":          {"type": "string", "description": "e.g. silver.silver_film"},
                        "business_key_columns":  {"type": "array", "items": {"type": "string"}},
                        "reasoning":             {"type": "string", "description": "Why this is a hub"},
                    }
                }
            },
            "links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "source_table", "hub_references", "reasoning"],
                    "properties": {
                        "name":           {"type": "string"},
                        "source_table":   {"type": "string"},
                        "hub_references": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "hub":           {"type": "string"},
                                    "source_column": {"type": "string"},
                                }
                            }
                        },
                        "reasoning": {"type": "string"},
                    }
                }
            },
            "satellites": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "parent_hub", "source_table", "tracked_columns", "split_reason", "reasoning"],
                    "properties": {
                        "name":            {"type": "string"},
                        "parent_hub":      {"type": "string"},
                        "source_table":    {"type": "string"},
                        "tracked_columns": {"type": "array", "items": {"type": "string"}},
                        "split_reason":    {"type": "string", "description": "Why columns were split into this satellite"},
                        "reasoning":       {"type": "string"},
                    }
                }
            }
        }
    }
}
```

## System prompt

```python
SYSTEM_PROMPT = """You are a Data Vault 2.0 architect with deep expertise in CDC pipelines.

Rules to follow:
1. HUB: one per distinct business entity (person, place, thing, event). Each hub has exactly one business key.
2. LINK: one per meaningful relationship between hubs. Junction tables (film_actor, film_category) become links with 2 hub_references.
3. SATELLITE: one or more per hub, grouping descriptive attributes by change rate and business theme.
   - Split attributes that change at very different rates into separate satellites (e.g. pricing vs core metadata).
   - A satellite with only audit columns (last_update) is a valid "marker" satellite.
4. Do NOT create a hub for junction/association tables — they become links only.
5. Use the data samples to understand actual business semantics, not just column names."""
```

## User prompt construction

```python
def build_prompt(tables: list[TableDef], samples: dict[str, list[dict]]) -> str:
    """Build the classification prompt from schemas + data samples."""
    sections = ["Classify the following Silver tables into a Data Vault 2.0 model.\n"]
    for table in tables:
        sections.append(f"## Table: {table.source_table}")
        sections.append(f"Primary key: {table.pk_columns}")
        sections.append(f"FK hints: {table.fk_hints}")
        sections.append("Columns:")
        for col in table.columns:
            sections.append(f"  - {col.name} ({col.data_type}, nullable={col.nullable})")
        sample = samples.get(table.name, [])
        if sample:
            sections.append(f"Sample rows ({len(sample)} rows):")
            sections.append(json.dumps(sample[:5], default=str, indent=2))  # show first 5 in prompt
        sections.append("")
    return "\n".join(sections)
```

## Key classes / functions

```python
import json, os
from pathlib import Path
from ..models import TableDef, DVModel, HubDef, LinkDef, SatDef, ConfidenceLevel, LinkRef
from ..decision_logger import DecisionLogger
from ..session import Session
from ..llm_client import LLMClient, Message

class AIClassifier:
    """Runs AI-powered DV 2.0 classification and merges with heuristic output."""

    SAMPLE_LIMIT = 100
    SAMPLES_DIR = "data_samples"

    def __init__(
        self,
        tables: list[TableDef],
        heuristic_model: DVModel,
        session: Session,
        logger: DecisionLogger,
    ):
        self.tables = tables
        self.heuristic_model = heuristic_model
        self.session = session
        self.logger = logger
        self.warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        self.llm = LLMClient()

    def run(self) -> DVModel:
        """Run AI classification and return merged DVModel."""
        if self.session.is_step_done("step2b_ai_classifier"):
            return self._load_merged()

        samples = self._fetch_all_samples()
        ai_model = self._classify_with_ai(samples)
        merged_model = self._merge(ai_model)

        self._save_ai_model(ai_model)
        self._save_merged(merged_model)
        self.session.mark_step_done("step2b_ai_classifier", metadata={
            "ai_hubs": len(ai_model.hubs),
            "ai_links": len(ai_model.links),
            "ai_satellites": len(ai_model.satellites),
        })
        return merged_model

    def _fetch_all_samples(self) -> dict[str, list[dict]]:
        """Fetch data samples for all tables. Cache in {session_dir}/data_samples/."""
        samples_dir = self.session.session_dir / self.SAMPLES_DIR
        samples_dir.mkdir(exist_ok=True)
        samples = {}
        for table in self.tables:
            cache_file = samples_dir / f"{table.name}_sample.json"
            if cache_file.exists():
                samples[table.name] = json.loads(cache_file.read_text())
            else:
                rows = self._fetch_table_sample(table.source_table)
                cache_file.write_text(json.dumps(rows, default=str, indent=2))
                samples[table.name] = rows
        return samples

    def _fetch_table_sample(self, source_table: str) -> list[dict]:
        """Execute SELECT * LIMIT N on a Silver Delta table via Databricks SQL."""

    def _classify_with_ai(self, samples: dict[str, list[dict]]) -> DVModel:
        """Build prompt, call LLM with tool, parse response into DVModel."""
        prompt = build_prompt(self.tables, samples)
        result = self.llm.complete_with_tools(
            messages=[Message(role="user", content=prompt)],
            tools=[DV_CLASSIFY_TOOL],
            system=SYSTEM_PROMPT,
        )
        return self._parse_tool_result(result.tool_input)

    def _parse_tool_result(self, tool_input: dict) -> DVModel:
        """Convert AI tool call JSON into DVModel instances."""

    def _merge(self, ai_model: DVModel) -> DVModel:
        """Compare AI model with heuristic model. Return merged DVModel with confidence."""

    def _merge_hubs(self, merged: DVModel, ai_model: DVModel) -> None:
        """Hub merge logic — see merge table in design."""

    def _merge_links(self, merged: DVModel, ai_model: DVModel) -> None:
        """Link merge logic."""

    def _merge_satellites(self, merged: DVModel, ai_model: DVModel) -> None:
        """Satellite merge logic — AI split wins on disagreement."""

    def _save_ai_model(self, model: DVModel) -> None: ...
    def _save_merged(self, model: DVModel) -> None: ...
    def _load_merged(self) -> DVModel: ...
```

## Merge logic detail

```python
def _merge_hubs(self, merged: DVModel, ai_model: DVModel) -> None:
    heuristic_hub_names = {h.name for h in self.heuristic_model.hubs}
    ai_hub_names = {h.name for h in ai_model.hubs}

    for hub in self.heuristic_model.hubs:
        if hub.name in ai_hub_names:
            # Agreement → HIGH confidence
            hub.confidence = ConfidenceLevel.HIGH
            hub.rules_fired.append("MERGE:both_agree")
            merged.hubs.append(hub)
        else:
            # Heuristic only → LOW confidence
            hub.confidence = ConfidenceLevel.LOW
            hub.rules_fired.append("MERGE:heuristic_only")
            merged.hubs.append(hub)
            self.logger.log("step2b_ai_classifier", hub.name,
                "MERGE:heuristic_only — AI did not classify this as a Hub",
                ConfidenceLevel.LOW, "Review: AI may have seen it as a link or attribute")

    for hub in ai_model.hubs:
        if hub.name not in heuristic_hub_names:
            # AI only → LOW confidence (AI may have found a semantically valid hub)
            hub.confidence = ConfidenceLevel.LOW
            hub.rules_fired.append("MERGE:ai_only")
            merged.hubs.append(hub)
            self.logger.log("step2b_ai_classifier", hub.name,
                f"MERGE:ai_only — AI found hub not detected by heuristics. Reasoning: {hub.rules_fired}",
                ConfidenceLevel.LOW, "Review: confirm this is a genuine business entity")
```

## Env vars required
(add to `.envexample`)
```
LLM_PROVIDER=claude          # or openai_compatible
LLM_API_KEY=your_key_here
LLM_BASE_URL=                # optional, for openai_compatible local endpoints
LLM_MODEL=                   # optional override
LLM_MAX_TOKENS=4096
```

## Updates required in other modules
- `session.py` — add `"step2b_ai_classifier"` to `STEPS` list between `step2_rule_engine` and `step3_artifact_gen`
- `steps/step3_artifact_gen.py` — load model from `02b_merged_classification.json` if it exists, else fall back to `02_classification.json`
- `main.py` — after `engine.run()`, run `AIClassifier(...).run()` and pass merged model to subsequent steps
- `requirements.txt` — add `anthropic>=0.40.0`
- `.envexample` — add LLM env vars

## Acceptance criteria
- `_fetch_all_samples()` creates `data_samples/silver_film_sample.json` etc. and does not re-fetch on second call (cache hit)
- If `DATABRICKS_WAREHOUSE_ID` is not set, `_fetch_table_sample()` raises a descriptive error (not a cryptic SDK error)
- AI model is stored in `02b_ai_classification.json`; merged model in `02b_merged_classification.json`
- For dvdrental: merged model has 0 LOW-confidence entities (AI and heuristics should fully agree)
- Entities present in both models have `confidence = HIGH`
- Entities only in one model have `confidence = LOW` and appear in `decisions.log`
- `merged_model` fed to step3 produces the same final vault model as the heuristic-only path when both agree
- Setting `LLM_PROVIDER=openai_compatible` + `LLM_BASE_URL=http://localhost:11434/v1` works without code changes
