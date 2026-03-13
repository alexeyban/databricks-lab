#!/usr/bin/env python3
"""
Confluence Documentation Generator

Generates comprehensive, Confluence-ready documentation with Mermaid diagrams,
supporting both HTML and Markdown output formats.
"""

import json
import re
import yaml
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).parent.parent


@dataclass
class TableSchema:
    name: str
    columns: list[dict[str, Any]]
    primary_key: str | None = None
    foreign_keys: list[dict[str, str]] = field(default_factory=list)


@dataclass
class JobTask:
    task_key: str
    depends_on: list[str]
    notebook_path: str | None = None
    dbt_command: str | None = None


@dataclass
class JobConfig:
    name: str
    tasks: list[JobTask]
    schedule: str | None = None


def collect_source_schema() -> list[TableSchema]:
    sql_path = REPO_ROOT / "init-db.sql"
    content = sql_path.read_text()
    tables = []
    current_table: dict[str, Any] | None = None
    columns: list[dict[str, Any]] = []

    for line in content.splitlines():
        line = line.strip()
        if line.upper().startswith("CREATE TABLE"):
            match = re.search(r"CREATE TABLE.*?(\w+)\s*\(", line, re.IGNORECASE)
            if match:
                if current_table:
                    tables.append(
                        TableSchema(
                            name=current_table["name"],
                            columns=columns.copy(),
                            primary_key=current_table.get("pk"),
                        )
                    )
                current_table = {"name": match.group(1)}
                columns = []
        elif line.upper().startswith("PRIMARY KEY"):
            match = re.search(r"PRIMARY KEY\s*\((.*?)\)", line, re.IGNORECASE)
            if match and current_table:
                current_table["pk"] = match.group(1)
        elif line.upper().startswith("FOREIGN KEY"):
            match = re.search(
                r"FOREIGN KEY\s*\((.*?)\)\s*REFERENCES\s+(\w+)\s*\((.*?)\)",
                line,
                re.IGNORECASE,
            )
            if match and current_table:
                if "fks" not in current_table:
                    current_table["fks"] = []
                current_table["fks"].append(
                    {
                        "column": match.group(1),
                        "references": f"{match.group(2)}({match.group(3)})",
                    }
                )
        elif (
            line
            and not line.startswith("DO $$")
            and not line.startswith("BEGIN")
            and not line.startswith("END")
        ):
            col_match = re.match(r"(\w+)\s+(\w+(?:\([^)]+\))?)", line)
            if col_match and current_table and not line.startswith(")"):
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                is_pk = "PRIMARY KEY" in line.upper()
                is_not_null = "NOT NULL" in line.upper()
                default_match = re.search(r"DEFAULT\s+(\w+)", line, re.IGNORECASE)
                default = default_match.group(1) if default_match else None
                columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "nullable": not is_not_null,
                        "default": default,
                        "primary_key": is_pk,
                    }
                )

    if current_table:
        tables.append(
            TableSchema(
                name=current_table["name"],
                columns=columns.copy(),
                primary_key=current_table.get("pk"),
                foreign_keys=current_table.get("fks", []),
            )
        )

    return tables


def parse_job_config() -> JobConfig:
    yaml_path = REPO_ROOT / "Orders-ingest-job.yaml"
    content = yaml.safe_load(yaml_path.read_text())

    job = content.get("resources", {}).get("jobs", {})
    job_name = list(job.keys())[0] if job else "unknown"
    job_data = job[job_name] if job else {}

    tasks = []
    for task in job_data.get("tasks", []):
        task_key = task.get("task_key", "")
        depends_on = [d.get("task_key") for d in task.get("depends_on", [])]

        notebook_task = task.get("notebook_task", {})
        notebook_path = notebook_task.get("notebook_path")

        dbt_task = task.get("dbt_task", {})
        dbt_command = (
            dbt_task.get("commands", [""])[0] if dbt_task.get("commands") else None
        )

        tasks.append(
            JobTask(
                task_key=task_key,
                depends_on=depends_on,
                notebook_path=notebook_path,
                dbt_command=dbt_command,
            )
        )

    schedule_data = job_data.get("schedule", {})
    schedule = schedule_data.get("quartz_cron_expression") if schedule_data else None

    return JobConfig(name=job_name, tasks=tasks, schedule=schedule)


def generate_architecture_mermaid() -> str:
    return """```mermaid
flowchart LR
    subgraph Source["Source Systems"]
        PG[(PostgreSQL<br/>orders, products)]
    end
    
    subgraph CDC["CDC Layer"]
        DBZ[Debezium<br/>Connect]
        KB[Kafka<br/>cdc.public.*]
        SR[Schema<br/>Registry]
    end
    
    subgraph Databricks["Databricks Lakehouse"]
        subgraph Bronze["Bronze Layer"]
            BO[workspace.bronze<br/>orders]
            BP[workspace.bronze<br/>products]
        end
        
        subgraph Silver["Silver Layer"]
            SO[workspace.silver<br/>silver_orders]
            SP[workspace.silver<br/>silver_products]
        end
        
        subgraph Gold["Gold Layer"]
            GO[workspace.gold<br/>total_products_order]
        end
    end
    
    subgraph dbt["dbt Gold"]
        DBT[dbt-core<br/>dbt-databricks]
    end
    
    PG -->|WAL/CDC| DBZ
    DBZ -->|CDC Events| KB
    KB -->|Stream| BO
    KB -->|Stream| BP
    BO -->|Merge| SO
    BP -->|Merge| SP
    SO -->|Query| DBT
    SP -->|Query| DBT
    DBT -->|Build + Tests| GO
    
    style Source fill:#e3f2fd,stroke:#1976d2
    style CDC fill:#fff3e0,stroke:#f57c00
    style Bronze fill:#e8f5e9,stroke:#388e3c
    style Silver fill:#f3e5f5,stroke:#7b1fa2
    style Gold fill:#fce4ec,stroke:#c2185b
    style dbt fill:#fafafa,stroke:#616161
```"""


def generate_dataflow_mermaid() -> str:
    return """```mermaid
sequenceDiagram
    participant P as PostgreSQL
    participant D as Debezium
    participant K as Kafka
    participant B as Bronze
    participant S as Silver
    participant G as Gold
    
    Note over P: INSERT/UPDATE/DELETE<br/>on orders/products
    
    P->>D: WAL Change Event
    D->>K: Serialize CDC Payload
    K->>B: Spark Structured Streaming
    
    loop Bronze Ingestion
        B->>B: Parse Debezium JSON
        B->>B: Write raw to Delta
    end
    
    B->>S: Read Bronze micro-batch
    
    loop Silver Merge
        S->>S: Deduplicate by key
        S->>S: Merge to current state
        S->>S: Schema evolution
    end
    
    S->>G: dbt source query
    
    loop Gold Build
        G->>G: Run dbt models
        G->>G: Execute DQ tests
    end
    
    Note over G: Total products<br/>by color
```"""


def generate_erdiagram_mermaid() -> str:
    tables = collect_source_schema()

    erd = ["```mermaid", "erDiagram"]
    erd.append("    PRODUCTS {")
    products_table = next((t for t in tables if t.name == "products"), None)
    if products_table:
        for col in products_table.columns:
            pk_marker = "PK" if col.get("primary_key") else ""
            null_marker = "NULL" if col.get("nullable") else "NOT NULL"
            erd.append(f"        {col['name']} {col['type']} {pk_marker} {null_marker}")
    erd.append("    }")

    erd.append("    ORDERS {")
    orders_table = next((t for t in tables if t.name == "orders"), None)
    if orders_table:
        for col in orders_table.columns:
            pk_marker = "PK" if col.get("primary_key") else ""
            null_marker = "NULL" if col.get("nullable") else "NOT NULL"
            erd.append(f"        {col['name']} {col['type']} {pk_marker} {null_marker}")
    erd.append("    }")

    erd.append('    PRODUCTS ||--o{ ORDERS : "product_id"')
    erd.append("```")

    return "\n".join(erd)


def generate_job_mermaid() -> str:
    job = parse_job_config()

    lines = ["```mermaid", "graph TD"]

    for task in job.tasks:
        if task.notebook_path:
            label = task.notebook_path.split("/")[-1].replace("_", " ")
            lines.append(f'    {task.task_key}["{label}"]')
        elif task.dbt_command:
            label = task.dbt_command.replace("--", " ").replace("_", " ")
            lines.append(f'    {task.task_key}["{label}"]')

    for task in job.tasks:
        for dep in task.depends_on:
            lines.append(f"    {dep} --> {task.task_key}")

    lines.append("```")

    return "\n".join(lines)


def generate_html(title: str, sections: dict[str, str]) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        :root {{
            --primary-color: #0052cc;
            --secondary-color: #172b4d;
            --accent-color: #00875a;
            --bg-color: #ffffff;
            --border-color: #dfe1e6;
            --code-bg: #f4f5f7;
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: var(--secondary-color);
            background: var(--bg-color);
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        h1 {{
            color: var(--primary-color);
            border-bottom: 3px solid var(--primary-color);
            padding-bottom: 10px;
            margin-bottom: 20px;
        }}
        
        h2 {{
            color: var(--primary-color);
            border-left: 4px solid var(--accent-color);
            padding-left: 12px;
            margin: 30px 0 15px 0;
        }}
        
        h3 {{
            color: var(--secondary-color);
            margin: 20px 0 10px 0;
        }}
        
        p {{
            margin-bottom: 15px;
        }}
        
        code {{
            background: var(--code-bg);
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 0.9em;
        }}
        
        pre {{
            background: var(--code-bg);
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            margin: 15px 0;
            border: 1px solid var(--border-color);
        }}
        
        pre code {{
            background: none;
            padding: 0;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        
        th, td {{
            padding: 12px;
            text-align: left;
            border: 1px solid var(--border-color);
        }}
        
        th {{
            background: var(--primary-color);
            color: white;
            font-weight: 600;
        }}
        
        tr:nth-child(even) {{
            background: #f9f9f9;
        }}
        
        .toc {{
            background: #f4f5f7;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 30px;
        }}
        
        .toc h3 {{
            margin-top: 0;
        }}
        
        .toc ul {{
            list-style: none;
            padding-left: 0;
        }}
        
        .toc li {{
            margin: 8px 0;
        }}
        
        .toc a {{
            color: var(--primary-color);
            text-decoration: none;
        }}
        
        .toc a:hover {{
            text-decoration: underline;
        }}
        
        .info-box {{
            background: #e3f2fd;
            border-left: 4px solid #1976d2;
            padding: 15px;
            margin: 20px 0;
            border-radius: 0 5px 5px 0;
        }}
        
        .warning-box {{
            background: #fff3e0;
            border-left: 4px solid #f57c00;
            padding: 15px;
            margin: 20px 0;
            border-radius: 0 5px 5px 0;
        }}
        
        .success-box {{
            background: #e8f5e9;
            border-left: 4px solid #388e3c;
            padding: 15px;
            margin: 20px 0;
            border-radius: 0 5px 5px 0;
        }}
        
        .footer {{
            margin-top: 50px;
            padding-top: 20px;
            border-top: 1px solid var(--border-color);
            font-size: 0.85em;
            color: #6b778c;
            text-align: center;
        }}
        
        .metadata-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        
        .metadata-item {{
            background: #f4f5f7;
            padding: 15px;
            border-radius: 5px;
        }}
        
        .metadata-label {{
            font-size: 0.85em;
            color: #6b778c;
            margin-bottom: 5px;
        }}
        
        .metadata-value {{
            font-weight: 600;
            color: var(--secondary-color);
        }}
        
        @media print {{
            body {{
                max-width: 100%;
                padding: 10px;
            }}
            
            .toc {{
                break-inside: avoid;
            }}
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    
    <div class="metadata-grid">
        <div class="metadata-item">
            <div class="metadata-label">Generated</div>
            <div class="metadata-value">{timestamp}</div>
        </div>
        <div class="metadata-item">
            <div class="metadata-label">Version</div>
            <div class="metadata-value">1.0.0</div>
        </div>
        <div class="metadata-item">
            <div class="metadata-label">Pipeline</div>
            <div class="metadata-value">CDC Lakehouse</div>
        </div>
    </div>
    
    <div class="toc">
        <h3>Table of Contents</h3>
        <ul>
"""

    for i, section_title in enumerate(sections.keys(), 1):
        anchor = section_title.lower().replace(" ", "-")
        html += f'            <li><a href="#{anchor}">{i}. {section_title}</a></li>\n'

    html += """        </ul>
    </div>
"""

    for i, (section_title, section_content) in enumerate(sections.items(), 1):
        anchor = section_title.lower().replace(" ", "-")
        html += f'    <h2 id="{anchor}">{i}. {section_title}</h2>\n'
        html += f"    {section_content}\n"

    html += f"""
    <div class="footer">
        <p>Generated by Confluence Documentation Generator | CDC Lakehouse Lab</p>
    </div>
    
    <script>
        mermaid.initialize({{ 
            startOnLoad: true,
            theme: 'default',
            securityLevel: 'loose',
            flowchart: {{ 
                useMaxWidth: true,
                htmlLabels: true,
                curve: 'basis'
            }},
            sequence: {{
                actorMargin: 50,
                boxMargin: 10,
                boxTextMargin: 5,
                noteMargin: 10,
                messageMargin: 35
            }}
        }});
    </script>
</body>
</html>"""

    return html


def generate_markdown(title: str, sections: dict[str, str]) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    md = f"# {title}\n\n"
    md += f"| Property | Value |\n|----------|-------|\n"
    md += f"| Generated | {timestamp} |\n"
    md += f"| Version | 1.0.0 |\n"
    md += f"| Pipeline | CDC Lakehouse |\n\n"

    md += "## Table of Contents\n\n"
    for i, section_title in enumerate(sections.keys(), 1):
        anchor = section_title.lower().replace(" ", "-")
        md += f"{i}. [{section_title}](#{anchor})\n"

    md += "\n---\n\n"

    for i, (section_title, section_content) in enumerate(sections.items(), 1):
        anchor = section_title.lower().replace(" ", "-")
        md += f"## {i}. {section_title} {{#{anchor}}}\n\n"

        content = section_content
        content = content.replace('<pre class="mermaid">', "```mermaid\n")
        content = content.replace("</pre>", "\n```")

        content = content.replace("<table>", "|")
        content = content.replace("</table>", "")
        content = content.replace("<thead>", "|")
        content = content.replace("</thead>", "\n|---|\n")
        content = content.replace("<tbody>", "|")
        content = content.replace("</tbody>", "")
        content = content.replace("<tr>", "|")
        content = content.replace("</tr>", "\n")
        content = content.replace("<th>", "|")
        content = content.replace("</th>", "|")
        content = content.replace("<td>", "|")
        content = content.replace("</td>", "|")
        content = content.replace("<br/>", "  \n")
        content = content.replace("<strong>", "**")
        content = content.replace("</strong>", "**")
        content = content.replace("<em>", "*")
        content = content.replace("</em>", "*")

        md += content + "\n\n"

    md += "---\n\n"
    md += "*Generated by Confluence Documentation Generator | CDC Lakehouse Lab*\n"

    return md


def build_sections() -> dict[str, str]:
    tables = collect_source_schema()
    job = parse_job_config()

    sections = {}

    sections["Overview"] = """<div class="info-box">
This documentation describes the end-to-end CDC (Change Data Capture) pipeline from PostgreSQL through Debezium and Kafka into Databricks Lakehouse with dbt Gold transformations.
</div>

### Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Source Database | PostgreSQL | 15 |
| CDC Capture | Debezium | 2.5 |
| Message Broker | Apache Kafka | 7.6.0 |
| Lakehouse Platform | Databricks | Latest |
| Gold Transformation | dbt | 1.x |
| Storage | Delta Lake | - |
| Catalog | Unity Catalog | - |"""

    sections["Architecture"] = (
        generate_architecture_mermaid()
        + """

### Architecture Overview

The pipeline follows a **medallion architecture** pattern with three main layers:

1. **Bronze Layer**: Raw CDC events ingested from Kafka
2. **Silver Layer**: Current-state tables with deduplication and schema evolution
3. **Gold Layer**: Business-ready aggregates built with dbt

Data flows from PostgreSQL → Debezium → Kafka → Databricks (Bronze → Silver) → dbt Gold."""
    )

    sections["Data Flow"] = (
        generate_dataflow_mermaid()
        + """

### Pipeline Execution Flow

1. **Source Changes**: Transactions on `orders` and `products` tables in PostgreSQL generate WAL entries
2. **CDC Capture**: Debezium Connect captures changes via PostgreSQL logical replication
3. **Event Streaming**: CDC events are published to Kafka topics (`cdc.public.orders`, `cdc.public.products`)
4. **Bronze Ingestion**: Databricks Spark Structured Streaming reads Kafka and writes raw JSON to Delta
5. **Silver Processing**: Streaming MERGE operations deduplicate and maintain current state
6. **Gold Transformation**: dbt reads Silver tables, applies business logic, and runs DQ tests"""
    )

    erd = generate_erdiagram_mermaid()
    sections["Source Schema"] = f"""{erd}

### Table Definitions

"""
    for table in tables:
        sections["Source Schema"] += f"#### {table.name.upper()}\n\n"
        sections["Source Schema"] += "| Column | Type | Nullable | Key |\n"
        sections["Source Schema"] += "|--------|------|----------|-----|\n"
        for col in table.columns:
            key = "PK" if col.get("primary_key") else ""
            nullable = "YES" if col.get("nullable") else "NO"
            sections["Source Schema"] += (
                f"| {col['name']} | {col['type']} | {nullable} | {key} |\n"
            )
        sections["Source Schema"] += "\n"

    sections["Medallion Architecture"] = """### Bronze Layer

| Schema | Table | Description |
|--------|-------|-------------|
| workspace.bronze | orders | Raw Debezium CDC events for orders |
| workspace.bronze | products | Raw Debezium CDC events for products |

### Silver Layer

| Schema | Table | Description |
|--------|-------|-------------|
| workspace.silver | silver_orders | Current-state orders with schema evolution |
| workspace.silver | silver_products | Current-state products with schema evolution |

### Gold Layer

| Schema | Table | Description |
|--------|-------|-------------|
| workspace.gold | total_products_order | Aggregate: total amount by product name and color |

### Schema Evolution

The Silver layer supports **schema evolution**:
- New columns added to source are automatically propagated
- Legacy fields are preserved during transitions
- Dynamic MERGE adapts to runtime schema"""

    sections["Job Configuration"] = f"""### Job: {job.name}

| Property | Value |
|----------|-------|
| Job ID | 574281734474239 |
| Schedule | {job.schedule or "Paused"} |

### Tasks

"""
    for task in job.tasks:
        sections["Job Configuration"] += f"#### {task.task_key}\n\n"
        if task.notebook_path:
            sections["Job Configuration"] += f"- **Type**: Notebook\n"
            sections["Job Configuration"] += f"- **Path**: `{task.notebook_path}`\n"
        if task.dbt_command:
            sections["Job Configuration"] += f"- **Type**: dbt Task\n"
            sections["Job Configuration"] += f"- **Command**: `{task.dbt_command}`\n"
        if task.depends_on:
            sections["Job Configuration"] += (
                f"- **Depends on**: {', '.join(task.depends_on)}\n"
            )
        sections["Job Configuration"] += "\n"

    sections["Job Configuration"] += generate_job_mermaid()

    sections["Data Quality"] = """### dbt Tests

The Gold layer includes data quality tests:

- **NOT NULL validation**: Ensures critical columns have values
- **Uniqueness checks**: Validates primary key integrity
- **Positive totals**: Ensures business rules are met

### DQ Queries

Stored SQL checks in `dq_queries/silver/`:
- `orders.sql`: Completeness and referential integrity
- `products.sql`: Data consistency validation

### Schema Drift Detection

| Policy | Behavior |
|--------|----------|
| strict | Block any schema change |
| additive_only | Allow new columns only (default) |
| permissive | Log drift but never block |"""

    sections["Operational Guide"] = """### Starting the Pipeline

```bash
# Start local infrastructure
docker compose up -d

# Register Debezium connector
curl -X POST http://localhost:8083/connectors \\
  -H 'Content-Type: application/json' \\
  --data @postgres-connector.json

# Generate test data
python3 generators/load_products_generator.py
python3 generators/load_generator.py

# Run Databricks job
python3 skills/docker-databricks-lab-ops/scripts/smoke_test_notebooks.py
```

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| DATABRICKS_HOST | Databricks workspace URL | https://dbc-*.cloud.databricks.com |
| DATABRICKS_TOKEN | API authentication token | dapi* |
| KAFKA_BOOTSTRAP | Kafka connection (ngrok) | *.tcp.ngrok.io:***** |

### Checkpoints

| Layer | Path |
|-------|------|
| Bronze | `dbfs:/pipelines/bronze/checkpoints/*` |
| Silver | `dbfs:/pipelines/silver/checkpoints/*` |"""

    sections["Troubleshooting"] = """### Common Issues

| Issue | Cause | Resolution |
|-------|-------|------------|
| dbt command not found | Missing dependencies | Install dbt-core and dbt-databricks in job environment |
| Kafka connection timeout | ngrok tunnel expired | Restart ngrok and update KAFKA_BOOTSTRAP |
| Schema drift errors | Source schema changed | Review and update Silver metadata |
| Job task skipped | Upstream failure | Check earlier task logs |

### Debug Commands

```sql
-- Check Bronze data
SELECT * FROM workspace.bronze.orders LIMIT 10;

-- Check Silver deduplication
SELECT id, COUNT(*) as cnt 
FROM workspace.silver.silver_orders 
GROUP BY id 
HAVING cnt > 1;

-- Verify Gold output
SELECT * FROM workspace.gold.total_products_order;

-- Check schema drift log
SELECT * FROM workspace.monitoring.schema_drift_log 
ORDER BY detected_at DESC 
LIMIT 10;
```"""

    return sections


def generate_documentation(output_dir: Path | None = None) -> tuple[Path, Path]:
    if output_dir is None:
        output_dir = REPO_ROOT / "docs"

    output_dir.mkdir(exist_ok=True)
    (output_dir / "diagrams").mkdir(exist_ok=True)

    sections = build_sections()

    html_content = generate_html("CDC Lakehouse Pipeline Documentation", sections)
    md_content = generate_markdown("CDC Lakehouse Pipeline Documentation", sections)

    html_path = output_dir / "confluence_html.html"
    md_path = output_dir / "confluence_markdown.md"

    html_path.write_text(html_content)
    md_path.write_text(md_content)

    (output_dir / "diagrams" / "architecture.mmd").write_text(
        generate_architecture_mermaid()
    )
    (output_dir / "diagrams" / "dataflow.mmd").write_text(generate_dataflow_mermaid())
    (output_dir / "diagrams" / "erdiagram.mmd").write_text(generate_erdiagram_mermaid())
    (output_dir / "diagrams" / "job-structure.mmd").write_text(generate_job_mermaid())

    print(f"Generated documentation:")
    print(f"  - {html_path}")
    print(f"  - {md_path}")
    print(f"  - {output_dir / 'diagrams'}")

    return html_path, md_path


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
    else:
        output = None
    generate_documentation(output)
