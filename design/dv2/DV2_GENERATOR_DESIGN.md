---
name: DV 2.0 Generator — design state and todo steps
description: Full context and todo list for the DV 2.0 auto-generator tool (schema analyzer → classifier → notebook + config generator → Jupyter review → validator → applier)
type: project
---

## What This Is

A **meta-tool** (`generators/dv_generator/`) that automates the creation of a Data Vault 2.0 model from any Silver layer schema. It replaces manual DV model design with a 7-step pipeline that:
1. Analyzes Silver tables offline
2. Classifies entities into Hubs/Links/Satellites
3. Generates `dv_model.json` config + PySpark query snippets + `.ipynb` vault notebooks
4. Produces draw.io diagram + markdown documentation
5. Opens a Jupyter review notebook for human correction
6. Validates corrections
7. Applies the final model (or provides a code review if invalid)

**Why:** The DV 2.0 vault layer (13 hubs, 17 links, 14 satellites) was manually designed. This generator makes the same process repeatable for any new source schema.

---

## Plan File Location

Full architectural plan with all design decisions:
`/home/legion/.claude/plans/dv2-generator-design.md`

DV 2.0 vault layer plan (what the generator will produce):
`/home/legion/.claude/plans/enumerated-pondering-fox.md`

---

## Locked Design Decisions

| # | Topic | Decision |
|---|-------|---------|
| Q1 | Schema input | **Offline** — reads `pipeline_configs/silver/*.json` + schema contract notebooks |
| Q2 | Human review UI | **Jupyter notebook** — generated review notebook with editable Python dicts inline |
| Q3 | Generator scope | **Both** `dv_model.json` config AND full `.ipynb` vault notebooks (NB_2–NB_5) |
| Q4 | draw.io format | **Raw mxGraph XML** — importable into diagrams.net, no CLI rendering |
| Q5 | Satellite split rule | **S2 heuristic only** (column name patterns) — no live data frequency analysis |

---

## Implementation Todo (ordered)

### Not started — all 12 modules in `generators/dv_generator/`

- [ ] **1. `models.py`** — dataclasses: HubDef, LinkDef, SatDef, PitDef, BridgeDef, LinkRef, DecisionEntry
- [ ] **2. `decision_logger.py`** — DecisionLogger class; writes append-only `decisions.log` with step/entity/rule/confidence/reason
- [ ] **3. `session.py`** — SessionState, timestamped folder creation (`generated/dv_sessions/YYYYMMDD_HHMMSS/`), `00_session_state.json` read/write, resume logic
- [ ] **4. `steps/step1_analyzer.py`** — reads `pipeline_configs/silver/*.json` + `NB_schema_contracts` exports; builds column/type/nullability/FK-hint map; writes `01_schema_analysis.json`
- [ ] **5. `steps/step2_rule_engine.py`** — DV 2.0 classification rules (R1-R4 for hubs, L1-L3 for links, S2 heuristic for satellite splitting); writes `02_classification.json`; logs every rule that fires
- [ ] **6. `steps/step3_artifact_gen.py`** — generates `03_dv_model_draft.json` (full config) + `query_templates/*.py` (SHA-256 hash formulas, DIFF_HASH, DDL)
- [ ] **7. `steps/step3b_notebook_gen.py`** — generates `.ipynb` files for all 5 vault notebooks (NB_dv_metadata, NB_ingest_to_hubs, NB_ingest_to_links, NB_ingest_to_satellites, NB_dv_business_vault) templated from config
- [ ] **8. `steps/step4_doc_gen.py`** — mxGraph XML draw.io diagram (Hubs=circles/yellow, Links=squares/blue, Sats=ellipses) + `04_documentation.md`
- [ ] **9. `steps/step5_review.py`** — generates `05_review_notebook.ipynb` with: intro cell, summary DataFrame, editable hub/link/sat dicts, flagged items with reasoning, save+validate cell
- [ ] **10. `steps/step6_validator.py`** — structural checks V1-V9 (hub references, no orphan links, etc.) + DV 2.0 compliance + warnings W1-W4; writes `06_validation_report.json`
- [ ] **11. `steps/step7_applier.py`** — on valid: writes `pipeline_configs/datavault/dv_model.json` + final notebooks; on invalid: generates `07_code_review.md` with per-error explanation + fix options
- [ ] **12. `main.py`** — CLI orchestrator: `--analyze`, `--resume <session_id>`, `--from-step <n>`, ties all steps together

---

## Session Output Structure

```
generated/dv_sessions/YYYYMMDD_HHMMSS/
  00_session_state.json       ← step tracker, resume pointer
  01_schema_analysis.json     ← offline Silver introspection
  02_classification.json      ← Hub/Link/Sat with confidence + rules fired
  decisions.log               ← append-only decision audit trail
  03_dv_model_draft.json      ← auto-generated DV 2.0 config
  query_templates/            ← per-entity PySpark snippets
  04_documentation.md
  04_diagram.drawio           ← mxGraph XML
  05_review_notebook.ipynb    ← Jupyter review for human
  05_human_review_template.json
  05_human_review.json        ← human-modified version (diff tracked)
  06_validation_report.json
  07_final_model.json         ← approved → pipeline_configs/datavault/dv_model.json
  notebooks/                  ← generated vault notebooks
    NB_dv_metadata.ipynb
    NB_ingest_to_hubs.ipynb
    NB_ingest_to_links.ipynb
    NB_ingest_to_satellites.ipynb
    NB_dv_business_vault.ipynb
```

---

## Key Technical Specs to Remember

**Hash key formula:**
```python
sha2(concat_ws("||", upper(trim(col("film_id").cast("string")))), 256)
# NULL handling: coalesce(col, lit("NULL")) before hashing
```

**Satellite DIFF_HASH (change detection):**
```python
sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in tracked_cols]), 256)
```

**Satellite insert condition (append-only):**
```python
# LEFT JOIN latest DIFF_HK per hub key; insert where DIFF_HK changed or new
```

**DV 2.0 standard columns:**
- Hub: `HK_{name}`, `BK_{col}`, `LOAD_DATE`, `RECORD_SOURCE`
- Link: `HK_{name}`, `HK_{hub1}`, `HK_{hub2}`, `LOAD_DATE`, `RECORD_SOURCE`
- Satellite: `HK_{parent}`, `LOAD_DATE`, `DIFF_HK`, `RECORD_SOURCE`, + payload cols

---

## What To Do Next (resume instructions)

When picking this up in a new session:
1. Read `/home/legion/.claude/plans/dv2-generator-design.md` — full design
2. Read `/home/legion/.claude/plans/enumerated-pondering-fox.md` — the target DV model (what the generator should produce for dvdrental)
3. Start with `models.py` → `decision_logger.py` → `session.py` (foundation modules)
4. Then implement steps 1–7 in order
5. Test by running the generator against the existing dvdrental Silver config and verifying output matches the manually designed vault model

**How:** The user said "let's go" / approved the plan — just start implementing in order.

---

## Status

**As of 2026-04-01**: Plan designed and approved. Implementation not started. Waiting to begin.
