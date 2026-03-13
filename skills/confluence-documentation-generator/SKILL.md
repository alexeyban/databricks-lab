---
name: confluence-documentation-generator
description: Use this skill to generate Confluence-ready documentation with Mermaid diagrams for CDC lakehouse pipelines.
---

# confluence-documentation-generator

Use this skill when the user wants to generate Confluence documentation, including architecture diagrams, data flow diagrams, ER diagrams, and operational guides.

## Workflow

1. Read [`Agents/confluence-documentation-generator.md`](../../Agents/confluence-documentation-generator.md) for the agent definition.
2. Adopt the role, expertise, and capabilities defined in that agent file.
3. Execute the documentation generator:
   - **Local**: Run `python3 runtime/confluence_doc_generator.py`
   - **Databricks**: Run `notebooks/helpers/NB_confluence_generator.ipynb`
4. Return the generated documentation paths and summary of contents.

## Repository Mapping

- Agent definition: `Agents/confluence-documentation-generator.md`
- Core module: `runtime/confluence_doc_generator.py`
- Notebook: `notebooks/helpers/NB_confluence_generator.ipynb`
- Output: `docs/confluence_html.html`, `docs/confluence_markdown.md`

## Usage Examples

```
Generate Confluence documentation
Build documentation for the CDC pipeline
Create HTML and Markdown docs with Mermaid diagrams
Export documentation for Confluence
```

## Notes

- The generator creates both HTML (inline styles) and Markdown formats
- Mermaid diagrams are embedded and work natively in Confluence Cloud
- Output files are written to `docs/` directory (gitignored)
- The notebook version requires DBFS access for file output
