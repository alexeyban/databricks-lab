#!/usr/bin/env python3
"""Generate dbt vault models from config/datavault/dv_model.json."""
import json
import os
import glob

with open("config/datavault/dv_model.json") as f:
    model = json.load(f)

DBT_MODELS_DIR = "transformation/dbt_project/models"


def makedirs(path):
    os.makedirs(path, exist_ok=True)


makedirs(f"{DBT_MODELS_DIR}/vault/hubs")
makedirs(f"{DBT_MODELS_DIR}/vault/links")
makedirs(f"{DBT_MODELS_DIR}/vault/satellites")
makedirs(f"{DBT_MODELS_DIR}/vault/pit")
makedirs(f"{DBT_MODELS_DIR}/vault/bridge")


def hub_hk(entity):
    return f"{entity.upper()}_HK"


def lnk_hk(lnk_entity):
    return f"{lnk_entity.upper()}_HK"


def diff_hk(sat_short):
    return f"{sat_short.upper()}_DIFF_HK"


def src_ref(src_table):
    parts = src_table.split(".")
    return "{{ source('" + parts[0] + "', '" + parts[1] + "') }}"


# ---------------------------------------------------------------
# HUBS
# ---------------------------------------------------------------
for hub in model["hubs"]:
    entity = hub["name"].replace("HUB_", "")
    hk = hub_hk(entity)
    bk_cols = hub["business_key_columns"]
    bk_cast = " || '|' || ".join([f"CAST({c} AS STRING)" for c in bk_cols])
    src = src_ref(hub["source_table"])
    tbl_name = hub["name"].lower()

    lines = [
        "{{ config(",
        "    materialized='incremental',",
        f"    unique_key='{hk}',",
        "    on_schema_change='append_new_columns',",
        "    incremental_strategy='merge'",
        ") }}",
        "",
        "SELECT",
        f"    SHA2({bk_cast}, 256)    AS {hk},",
        "    CURRENT_TIMESTAMP()      AS LOAD_DATE,",
        f"    '{hub['record_source']}' AS RECORD_SOURCE,",
    ]
    for col in bk_cols:
        lines.append(f"    {col}")
    lines[-1] = lines[-1].rstrip(",")
    lines += [
        f"FROM {src}",
        "{%- if is_incremental() %}",
        f"WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{{{ this }}}})",
        "{%- endif %}",
    ]
    with open(f"{DBT_MODELS_DIR}/vault/hubs/{tbl_name}.sql", "w") as f:
        f.write("\n".join(lines) + "\n")

print(f"Generated {len(model['hubs'])} hub models")

# ---------------------------------------------------------------
# LINKS
# ---------------------------------------------------------------
for lnk in model["links"]:
    lnk_entity = lnk["name"].replace("LNK_", "")
    lnk_hk_col = lnk_hk(lnk_entity)
    refs = lnk["hub_references"]
    src = src_ref(lnk["source_table"])
    tbl_name = lnk["name"].lower()

    hk_parts = " || '|' || ".join(
        [f"SHA2(CAST({r['source_column']} AS STRING), 256)" for r in refs]
    )

    lines = [
        "{{ config(",
        "    materialized='incremental',",
        f"    unique_key='{lnk_hk_col}',",
        "    on_schema_change='append_new_columns',",
        "    incremental_strategy='merge'",
        ") }}",
        "",
        "SELECT",
        f"    SHA2({hk_parts}, 256)  AS {lnk_hk_col},",
    ]
    for r in refs:
        h_entity = r["hub"].replace("HUB_", "")
        lines.append(f"    SHA2(CAST({r['source_column']} AS STRING), 256)  AS {hub_hk(h_entity)},")
    lines += [
        "    CURRENT_TIMESTAMP()     AS LOAD_DATE,",
        f"    '{lnk['record_source']}' AS RECORD_SOURCE,",
    ]
    for r in refs:
        lines.append(f"    {r['source_column']},")
    lines[-1] = lines[-1].rstrip(",")
    lines += [
        f"FROM {src}",
        "{%- if is_incremental() %}",
        f"WHERE CURRENT_TIMESTAMP() > (SELECT COALESCE(MAX(LOAD_DATE), '1970-01-01') FROM {{{{ this }}}})",
        "{%- endif %}",
    ]
    with open(f"{DBT_MODELS_DIR}/vault/links/{tbl_name}.sql", "w") as f:
        f.write("\n".join(lines) + "\n")

print(f"Generated {len(model['links'])} link models")

# ---------------------------------------------------------------
# SATELLITES
# ---------------------------------------------------------------
for sat in model["satellites"]:
    parent_hub = sat["parent_hub"]
    entity = parent_hub.replace("HUB_", "")
    hk_col = hub_hk(entity)
    bk_col = sat["hub_key_source_column"]
    tracked = sat["tracked_columns"]
    src = src_ref(sat["source_table"])
    tbl_name = sat["name"].lower()
    diff_col = diff_hk(sat["name"].replace("SAT_", ""))

    tracked_concat = " || '|' || ".join([f"CAST({c} AS STRING)" for c in tracked])

    lines = [
        "{{ config(",
        "    materialized='incremental',",
        f"    unique_key=['{hk_col}', 'LOAD_DATE'],",
        "    on_schema_change='append_new_columns',",
        "    incremental_strategy='merge'",
        ") }}",
        "",
        "WITH source AS (",
        "    SELECT",
        f"        SHA2(CAST({bk_col} AS STRING), 256)  AS {hk_col},",
        "        CURRENT_TIMESTAMP()                    AS LOAD_DATE,",
        f"        SHA2({tracked_concat}, 256)           AS {diff_col},",
        f"        '{sat['record_source']}'              AS RECORD_SOURCE,",
    ]
    for col in tracked:
        lines.append(f"        {col},")
    lines[-1] = lines[-1].rstrip(",")
    lines += [
        f"    FROM {src}",
        ")",
        "SELECT s.*",
        "FROM source s",
        "{%- if is_incremental() %}",
        "WHERE NOT EXISTS (",
        "    SELECT 1",
        "    FROM {{ this }} t",
        f"    WHERE t.{hk_col} = s.{hk_col}",
        f"      AND t.{diff_col} = s.{diff_col}",
        ")",
        "{%- endif %}",
    ]
    with open(f"{DBT_MODELS_DIR}/vault/satellites/{tbl_name}.sql", "w") as f:
        f.write("\n".join(lines) + "\n")

print(f"Generated {len(model['satellites'])} satellite models")

# ---------------------------------------------------------------
# PIT TABLES
# ---------------------------------------------------------------
for pit in model["pit_tables"]:
    hub_name = pit["hub"]
    entity = hub_name.replace("HUB_", "")
    hk_col = hub_hk(entity)
    sats = pit["satellites"]
    tbl_name = pit["name"].lower()

    hub_ref = "{{ ref('" + hub_name.lower() + "') }}"

    sat_select_lines = []
    sat_join_lines = []
    for idx, sat_name in enumerate(sats):
        sat_def = next(s for s in model["satellites"] if s["name"] == sat_name)
        sat_ref = "{{ ref('" + sat_name.lower() + "') }}"
        alias = f"s{idx}"
        diff_col = diff_hk(sat_name.replace("SAT_", ""))
        sat_select_lines.append(f"    {alias}.LOAD_DATE  AS {sat_name}_LOAD_DATE,")
        sat_select_lines.append(f"    {alias}.{diff_col}  AS {sat_name}_DIFF_HK,")
        sat_join_lines.append(f"LEFT JOIN {sat_ref} {alias}")
        sat_join_lines.append(f"    ON hub.{hk_col} = {alias}.{hk_col}")
        sat_join_lines.append(f"    AND {alias}.LOAD_DATE = (")
        sat_join_lines.append(f"        SELECT MAX(LOAD_DATE) FROM {sat_ref} s_{idx}")
        sat_join_lines.append(f"        WHERE s_{idx}.{hk_col} = hub.{hk_col}")
        sat_join_lines.append(f"        AND s_{idx}.LOAD_DATE <= snap.snapshot_date")
        sat_join_lines.append("    )")

    lines = [
        "{{ config(materialized='table') }}",
        "",
        "WITH snapshot_dates AS (",
        "    SELECT EXPLODE(SEQUENCE(",
        "        DATE('2020-01-01'),",
        "        CURRENT_DATE(),",
        "        INTERVAL 1 DAY",
        "    )) AS snapshot_date",
        "),",
        "hub AS (",
        f"    SELECT DISTINCT {hk_col}",
        f"    FROM {hub_ref}",
        ")",
        "",
        "SELECT",
        f"    hub.{hk_col},",
        "    snap.snapshot_date,",
        "    CURRENT_TIMESTAMP() AS LOAD_DATE,",
    ] + sat_select_lines
    lines[-1] = lines[-1].rstrip(",")
    lines += [
        "FROM hub",
        "CROSS JOIN snapshot_dates snap",
    ] + sat_join_lines

    with open(f"{DBT_MODELS_DIR}/vault/pit/{tbl_name}.sql", "w") as f:
        f.write("\n".join(lines) + "\n")

print(f"Generated {len(model['pit_tables'])} PIT models")

# ---------------------------------------------------------------
# BRIDGE TABLES
# ---------------------------------------------------------------
for brg in model["bridge_tables"]:
    path = brg["path"]
    tbl_name = brg["name"].lower()

    from_hub = path[0]
    entity = from_hub.replace("HUB_", "")
    from_hk = hub_hk(entity)
    hub_ref = "{{ ref('" + from_hub.lower() + "') }}"

    select_lines = [f"    h0.{from_hk}"]
    join_lines = []
    prev_alias = "h0"
    prev_hk = from_hk

    i = 1
    while i < len(path):
        lnk_name = path[i]
        lnk_def = next(l for l in model["links"] if l["name"] == lnk_name)
        lnk_alias = f"l{i}"
        lnk_ref = "{{ ref('" + lnk_name.lower() + "') }}"
        lnk_hk_col = lnk_hk(lnk_name.replace("LNK_", ""))

        join_lines.append(f"JOIN {lnk_ref} {lnk_alias}")
        join_lines.append(f"    ON {prev_alias}.{prev_hk} = {lnk_alias}.{prev_hk}")
        select_lines.append(f"    {lnk_alias}.{lnk_hk_col}")

        if i + 1 < len(path):
            next_hub = path[i + 1]
            next_entity = next_hub.replace("HUB_", "")
            next_hk = hub_hk(next_entity)
            hub_alias = f"h{i}"
            hub_ref2 = "{{ ref('" + next_hub.lower() + "') }}"
            join_lines.append(f"JOIN {hub_ref2} {hub_alias}")
            join_lines.append(f"    ON {lnk_alias}.{next_hk} = {hub_alias}.{next_hk}")
            select_lines.append(f"    {hub_alias}.{next_hk}")
            prev_alias = hub_alias
            prev_hk = next_hk
        i += 2

    select_lines.append("    CURRENT_TIMESTAMP() AS LOAD_DATE")

    lines = [
        "{{ config(materialized='table') }}",
        "",
        "SELECT",
    ]
    for idx, s in enumerate(select_lines):
        comma = "," if idx < len(select_lines) - 1 else ""
        lines.append(s + comma)
    lines += [f"FROM {hub_ref} h0"] + join_lines

    with open(f"{DBT_MODELS_DIR}/vault/bridge/{tbl_name}.sql", "w") as f:
        f.write("\n".join(lines) + "\n")

print(f"Generated {len(model['bridge_tables'])} bridge models")

# Summary
files = glob.glob(f"{DBT_MODELS_DIR}/vault/**/*.sql", recursive=True)
print(f"\nTotal vault model files: {len(files)}")
for fp in sorted(files):
    print(f"  {fp}")
