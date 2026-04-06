"""
DV 2.0 Generator — AI Semantic Classifier + Merge (TASK_14)

Runs an LLM-powered classification of Silver table schemas, optionally enriched
with live data samples from Databricks SQL and/or Genie semantic insights, then
merges the AI model with the heuristic rule engine output.

Agreement between AI + heuristics → HIGH confidence.
Disagreement (one side only) → LOW confidence + logged in decisions.log for human review.

Outputs:
  {session_dir}/data_samples/{table}_sample.json   — cached sample rows per table
  {session_dir}/genie_insights/{table}_genie.json  — cached Genie enrichment (optional)
  {session_dir}/02b_ai_classification.json         — raw AI DVModel (before merge)
  {session_dir}/02b_merged_classification.json     — final merged DVModel (fed to step3)

Optional env vars:
  GENIE_SPACE_ID — Databricks Genie space ID; if set, per-table semantic enrichment is added
                   to the classification prompt. Requires a pre-configured Genie space with
                   Silver tables registered and Databricks Assistant enabled.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from ..decision_logger import DecisionLogger
from ..llm_client import LLMClient, Message
from ..models import (
    ConfidenceLevel,
    DVModel,
    HubDef,
    LinkDef,
    LinkRef,
    SatDef,
    TableDef,
)
from ..session import Session
from .step2_rule_engine import _model_from_dict, _model_to_dict

# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------

DV_CLASSIFY_TOOL: dict = {
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
                        "name": {"type": "string", "description": "e.g. HUB_FILM"},
                        "source_table": {"type": "string", "description": "e.g. silver.silver_film"},
                        "business_key_columns": {"type": "array", "items": {"type": "string"}},
                        "reasoning": {"type": "string", "description": "Why this is a hub"},
                    },
                },
            },
            "links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "source_table", "hub_references", "reasoning"],
                    "properties": {
                        "name": {"type": "string"},
                        "source_table": {"type": "string"},
                        "hub_references": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "hub": {"type": "string"},
                                    "source_column": {"type": "string"},
                                },
                            },
                        },
                        "reasoning": {"type": "string"},
                    },
                },
            },
            "satellites": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "name", "parent_hub", "source_table",
                        "tracked_columns", "split_reason", "reasoning",
                    ],
                    "properties": {
                        "name": {"type": "string"},
                        "parent_hub": {"type": "string"},
                        "source_table": {"type": "string"},
                        "tracked_columns": {"type": "array", "items": {"type": "string"}},
                        "split_reason": {
                            "type": "string",
                            "description": "Why columns were split into this satellite",
                        },
                        "reasoning": {"type": "string"},
                    },
                },
            },
        },
    },
}

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Data Vault 2.0 architect with deep expertise in CDC pipelines.

Rules to follow:
1. HUB: one per distinct business entity (person, place, thing, event). Each hub has exactly one business key.
2. LINK: one per meaningful relationship between hubs. Junction tables (film_actor, film_category) become links with 2 hub_references.
3. SATELLITE: one or more per hub, grouping descriptive attributes by change rate and business theme.
   - Split attributes that change at very different rates into separate satellites (e.g. pricing vs core metadata).
   - A satellite with only audit columns (last_update) is a valid "marker" satellite.
4. Do NOT create a hub for junction/association tables — they become links only.
5. Use the data samples to understand actual business semantics, not just column names."""


def _build_prompt(
    tables: list[TableDef],
    samples: dict[str, list[dict]],
    genie_insights: dict[str, str] | None = None,
) -> str:
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
            sections.append(f"Sample rows ({len(sample)} rows, showing first 5):")
            sections.append(json.dumps(sample[:5], default=str, indent=2))
        if genie_insights and table.name in genie_insights:
            sections.append("### Genie Insight (Databricks AI/BI analysis):")
            sections.append(genie_insights[table.name])
        sections.append("")
    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Genie enrichment (optional — requires GENIE_SPACE_ID env var)
# ---------------------------------------------------------------------------

class GenieEnricher:
    """Ask Databricks Genie semantic questions about each Silver table.

    Genie provides AI-powered natural-language analysis of Unity Catalog tables.
    Its answers enrich the LLM classification prompt, improving accuracy without
    replacing the structured tool-call classification step.

    Prerequisites:
    - A Genie space must be pre-configured in the Databricks workspace with the
      Silver tables registered and Databricks Assistant enabled.
    - A Pro or Serverless SQL warehouse associated with the space.

    Results are cached per table in {session_dir}/genie_insights/ so subsequent
    runs do not repeat the Genie API calls.
    """

    GENIE_INSIGHTS_DIR = "genie_insights"

    # Questions asked per table in a single conversation thread
    QUESTIONS = [
        "In one sentence, what business entity or concept does this table represent?",
        "Which column is the natural business key — the identifier a business person would use to refer to this entity?",
        "Which columns change frequently (transactional) vs rarely (reference/descriptive)?",
    ]

    def __init__(self, space_id: str) -> None:
        self.space_id = space_id

    def enrich_all(
        self, tables: list[TableDef], cache_dir: Path
    ) -> dict[str, str]:
        """Return {table_name: enrichment_text} for all tables, using cache when available."""
        insights_dir = cache_dir / self.GENIE_INSIGHTS_DIR
        insights_dir.mkdir(exist_ok=True)

        try:
            from databricks.sdk import WorkspaceClient
            client = WorkspaceClient(
                host=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN"),
            )
        except ImportError as exc:
            raise EnvironmentError(
                "databricks-sdk is not installed. Run: pip install databricks-sdk"
            ) from exc

        insights: dict[str, str] = {}
        for table in tables:
            cache_file = insights_dir / f"{table.name}_genie.json"
            if cache_file.exists():
                insights[table.name] = json.loads(cache_file.read_text())
            else:
                try:
                    text = self._ask_table(client, table)
                    cache_file.write_text(json.dumps(text))
                    insights[table.name] = text
                except Exception as exc:
                    # Non-fatal: log and continue without Genie insight for this table
                    print(f"  [genie] Warning: could not enrich {table.name}: {exc}")
        return insights

    def _ask_table(self, client, table: TableDef) -> str:
        """Open a Genie conversation for one table, ask all QUESTIONS, return combined answer."""
        from databricks.sdk.service.dashboards import MessageStatus

        first_q = f"I am looking at the table `{table.source_table}`. {self.QUESTIONS[0]}"

        # Start conversation with first question
        response = client.genie.start_conversation_and_wait(
            space_id=self.space_id,
            content=first_q,
        )
        conversation_id = response.conversation_id
        answers: list[str] = [self._extract_text(response)]

        # Ask remaining questions in the same conversation thread
        for question in self.QUESTIONS[1:]:
            msg = client.genie.create_message_and_wait(
                space_id=self.space_id,
                conversation_id=conversation_id,
                content=question,
            )
            answers.append(self._extract_text(msg))

        return "\n".join(
            f"Q: {q}\nA: {a}"
            for q, a in zip(self.QUESTIONS, answers)
            if a
        )

    @staticmethod
    def _extract_text(message) -> str:
        """Pull the text content from a GenieMessage's attachments."""
        for attachment in message.attachments or []:
            if attachment.text and attachment.text.content:
                return attachment.text.content.strip()
        return ""


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class AIClassifier:
    """Runs AI-powered DV 2.0 classification and merges with heuristic output."""

    SAMPLE_LIMIT = 100
    SAMPLES_DIR = "data_samples"
    STEP_NAME = "step2b_ai_classifier"

    def __init__(
        self,
        tables: list[TableDef],
        heuristic_model: DVModel,
        session: Session,
        logger: DecisionLogger,
        genie_space_id: str | None = None,
    ) -> None:
        self.tables = tables
        self.heuristic_model = heuristic_model
        self.session = session
        self.logger = logger
        self.warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        self.llm = LLMClient()
        space_id = genie_space_id or os.getenv("GENIE_SPACE_ID")
        self.genie_enricher = GenieEnricher(space_id) if space_id else None

    def run(self) -> DVModel:
        """Run AI classification and return merged DVModel."""
        if self.session.is_step_done(self.STEP_NAME):
            return self._load_merged()

        samples = self._fetch_all_samples()
        genie_insights = self._fetch_genie_insights()
        ai_model = self._classify_with_ai(samples, genie_insights)
        merged_model = self._merge(ai_model)

        self._save_ai_model(ai_model)
        self._save_merged(merged_model)
        self.session.mark_step_done(
            self.STEP_NAME,
            metadata={
                "ai_hubs": len(ai_model.hubs),
                "ai_links": len(ai_model.links),
                "ai_satellites": len(ai_model.satellites),
                "merged_hubs": len(merged_model.hubs),
                "merged_links": len(merged_model.links),
                "merged_satellites": len(merged_model.satellites),
            },
        )
        return merged_model

    # ------------------------------------------------------------------
    # Data sampling
    # ------------------------------------------------------------------

    def _fetch_all_samples(self) -> dict[str, list[dict]]:
        """Fetch data samples for all tables; cache to {session_dir}/data_samples/."""
        samples_dir = self.session.session_dir / self.SAMPLES_DIR
        samples_dir.mkdir(exist_ok=True)
        samples: dict[str, list[dict]] = {}
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
        if not self.warehouse_id:
            raise EnvironmentError(
                "DATABRICKS_WAREHOUSE_ID is not set. "
                "Export it before running the AI classifier, e.g.:\n"
                "  export DATABRICKS_WAREHOUSE_ID=53165753164ae80e"
            )
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError as exc:
            raise EnvironmentError(
                "databricks-sdk is not installed. Run: pip install databricks-sdk"
            ) from exc

        client = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN"),
        )
        statement = client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=f"SELECT * FROM {source_table} LIMIT {self.SAMPLE_LIMIT}",
            wait_timeout="30s",
        )
        columns = [c.name for c in statement.manifest.schema.columns]
        rows = []
        for row in (statement.result.data_array or []):
            rows.append(dict(zip(columns, row)))
        return rows

    # ------------------------------------------------------------------
    # Genie enrichment
    # ------------------------------------------------------------------

    def _fetch_genie_insights(self) -> dict[str, str] | None:
        """Fetch Genie semantic insights for all tables (cached). Returns None if no enricher."""
        if not self.genie_enricher:
            return None
        print("  [step2b] Fetching Genie insights...")
        try:
            insights = self.genie_enricher.enrich_all(
                self.tables, self.session.session_dir
            )
            print(f"  [step2b] Genie enriched {len(insights)}/{len(self.tables)} tables")
            return insights
        except Exception as exc:
            print(f"  [step2b] Genie enrichment failed, continuing without it: {exc}")
            return None

    # ------------------------------------------------------------------
    # AI classification
    # ------------------------------------------------------------------

    def _classify_with_ai(
        self,
        samples: dict[str, list[dict]],
        genie_insights: dict[str, str] | None = None,
    ) -> DVModel:
        """Build prompt, call LLM with tool, parse tool response into DVModel."""
        prompt = _build_prompt(self.tables, samples, genie_insights)
        result = self.llm.complete_with_tools(
            messages=[Message(role="user", content=prompt)],
            tools=[DV_CLASSIFY_TOOL],
            system=SYSTEM_PROMPT,
        )
        return self._parse_tool_result(result.tool_input)

    def _parse_tool_result(self, tool_input: dict) -> DVModel:
        """Convert AI tool call JSON into DVModel instances."""
        model = DVModel()

        for h in tool_input.get("hubs", []):
            name = h.get("name", "").upper()
            source = h.get("source_table", "")
            short = name.replace("HUB_", "", 1).lower()
            model.hubs.append(
                HubDef(
                    name=name,
                    target_table=f"vault.hub_{short}",
                    source_table=source,
                    business_key_columns=h.get("business_key_columns", []),
                    record_source=f"cdc.dvdrental.{source.split('.')[-1].replace('silver_', '')}",
                    rules_fired=[f"AI:{h.get('reasoning', '')[:120]}"],
                    confidence=ConfidenceLevel.HIGH,
                )
            )

        for lk in tool_input.get("links", []):
            name = lk.get("name", "").upper()
            source = lk.get("source_table", "")
            short = name.replace("LNK_", "", 1).lower()
            refs = [
                LinkRef(hub=r.get("hub", "").upper(), source_column=r.get("source_column", ""))
                for r in lk.get("hub_references", [])
            ]
            model.links.append(
                LinkDef(
                    name=name,
                    target_table=f"vault.lnk_{short}",
                    source_table=source,
                    hub_references=refs,
                    record_source=f"cdc.dvdrental.{source.split('.')[-1].replace('silver_', '')}",
                    rules_fired=[f"AI:{lk.get('reasoning', '')[:120]}"],
                    confidence=ConfidenceLevel.HIGH,
                )
            )

        for s in tool_input.get("satellites", []):
            name = s.get("name", "").upper()
            source = s.get("source_table", "")
            parent = s.get("parent_hub", "").upper()
            short = name.replace("SAT_", "", 1).lower()
            # Derive hub_key_source_column from parent hub BK (best effort)
            hub_key_col = _guess_hub_key_col(source)
            model.satellites.append(
                SatDef(
                    name=name,
                    target_table=f"vault.sat_{short}",
                    parent_hub=parent,
                    source_table=source,
                    hub_key_source_column=hub_key_col,
                    tracked_columns=s.get("tracked_columns", []),
                    split_reason=s.get("split_reason"),
                    record_source=f"cdc.dvdrental.{source.split('.')[-1].replace('silver_', '')}",
                    rules_fired=[f"AI:{s.get('reasoning', '')[:120]}"],
                    confidence=ConfidenceLevel.HIGH,
                )
            )

        return model

    # ------------------------------------------------------------------
    # Merge logic
    # ------------------------------------------------------------------

    def _merge(self, ai_model: DVModel) -> DVModel:
        """Compare AI model with heuristic model; return merged DVModel with confidence."""
        merged = DVModel()
        self._merge_hubs(merged, ai_model)
        self._merge_links(merged, ai_model)
        self._merge_satellites(merged, ai_model)
        return merged

    def _merge_hubs(self, merged: DVModel, ai_model: DVModel) -> None:
        heuristic_names = {h.name for h in self.heuristic_model.hubs}
        ai_names = {h.name for h in ai_model.hubs}
        ai_map = {h.name: h for h in ai_model.hubs}

        for hub in self.heuristic_model.hubs:
            if hub.name in ai_names:
                hub.confidence = ConfidenceLevel.HIGH
                hub.rules_fired.append("MERGE:both_agree")
            else:
                hub.confidence = ConfidenceLevel.LOW
                hub.rules_fired.append("MERGE:heuristic_only")
                self.logger.log(
                    self.STEP_NAME, hub.name,
                    "MERGE:heuristic_only — AI did not classify this as a Hub",
                    ConfidenceLevel.LOW,
                    "Review: AI may have seen it as a link or attribute",
                )
            merged.hubs.append(hub)

        for hub in ai_model.hubs:
            if hub.name not in heuristic_names:
                hub.confidence = ConfidenceLevel.LOW
                hub.rules_fired.append("MERGE:ai_only")
                self.logger.log(
                    self.STEP_NAME, hub.name,
                    "MERGE:ai_only — AI found hub not detected by heuristics",
                    ConfidenceLevel.LOW,
                    "Review: confirm this is a genuine business entity",
                )
                merged.hubs.append(hub)

    def _merge_links(self, merged: DVModel, ai_model: DVModel) -> None:
        heuristic_names = {lk.name for lk in self.heuristic_model.links}
        ai_names = {lk.name for lk in ai_model.links}

        for lk in self.heuristic_model.links:
            if lk.name in ai_names:
                lk.confidence = ConfidenceLevel.HIGH
                lk.rules_fired.append("MERGE:both_agree")
            else:
                lk.confidence = ConfidenceLevel.LOW
                lk.rules_fired.append("MERGE:heuristic_only")
                self.logger.log(
                    self.STEP_NAME, lk.name,
                    "MERGE:heuristic_only — AI did not classify this as a Link",
                    ConfidenceLevel.LOW,
                    "Review: check if the FK relationship is correctly modelled",
                )
            merged.links.append(lk)

        for lk in ai_model.links:
            if lk.name not in heuristic_names:
                lk.confidence = ConfidenceLevel.LOW
                lk.rules_fired.append("MERGE:ai_only")
                self.logger.log(
                    self.STEP_NAME, lk.name,
                    "MERGE:ai_only — AI found link not detected by heuristics",
                    ConfidenceLevel.LOW,
                    "Review: confirm the relationship and hub references are correct",
                )
                merged.links.append(lk)

    def _merge_satellites(self, merged: DVModel, ai_model: DVModel) -> None:
        """AI split wins on disagreement — AI is better at semantic grouping."""
        heuristic_names = {s.name for s in self.heuristic_model.satellites}
        ai_names = {s.name for s in ai_model.satellites}

        for sat in self.heuristic_model.satellites:
            if sat.name in ai_names:
                sat.confidence = ConfidenceLevel.HIGH
                sat.rules_fired.append("MERGE:both_agree")
            else:
                sat.confidence = ConfidenceLevel.LOW
                sat.rules_fired.append("MERGE:heuristic_only")
                self.logger.log(
                    self.STEP_NAME, sat.name,
                    "MERGE:heuristic_only — AI split satellites differently",
                    ConfidenceLevel.LOW,
                    "Review: AI may have grouped these columns differently",
                )
            merged.satellites.append(sat)

        for sat in ai_model.satellites:
            if sat.name not in heuristic_names:
                sat.confidence = ConfidenceLevel.LOW
                sat.rules_fired.append("MERGE:ai_only")
                self.logger.log(
                    self.STEP_NAME, sat.name,
                    "MERGE:ai_only — AI created a satellite not in heuristic model",
                    ConfidenceLevel.LOW,
                    "Review: confirm this satellite split is intentional",
                )
                merged.satellites.append(sat)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_ai_model(self, model: DVModel) -> None:
        path = self.session.session_dir / "02b_ai_classification.json"
        path.write_text(json.dumps(_model_to_dict(model), indent=2))

    def _save_merged(self, model: DVModel) -> None:
        path = self.session.session_dir / "02b_merged_classification.json"
        path.write_text(json.dumps(_model_to_dict(model), indent=2))

    def _load_merged(self) -> DVModel:
        path = self.session.session_dir / "02b_merged_classification.json"
        return _model_from_dict(json.loads(path.read_text()))


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _guess_hub_key_col(source_table: str) -> str:
    """Derive a likely hub business key column name from the source table name.

    e.g. silver.silver_film → film_id
    """
    table_suffix = source_table.split(".")[-1].replace("silver_", "")
    return f"{table_suffix}_id"
