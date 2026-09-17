# Data Vault 2.0 Architectural Plan — dvdrental CDC Lakehouse

## Context

The current pipeline captures CDC events from PostgreSQL dvdrental into Bronze (raw Debezium envelopes) → Silver (current-state MERGE, 1:1 with source tables). The goal is to add a **Data Vault 2.0 layer** (workspace.vault.*) above Silver that provides a fully historized, audit-ready, integration-ready store covering **all 15 Debezium-captured tables** in Phase 1.

**Design decisions locked:**
- Scope: all 15 tables immediately
- Satellite strategy: **pure append-only** (no end-dating; PIT tables handle time-travel queries)
- Hash key: **SHA-256, hex string, 64 chars** via Databricks `sha2(col, 256)`
- Business Vault: **in scope** (PIT + Bridge tables, NB_5)
- Execution: **micro-batch**, triggered after Silver tasks complete

---

## Source Schema — All 15 Tables

```
REFERENCE / DIMENSION
  country (country_id)
  city (city_id) ──► country
  address (address_id) ──► city
  language (language_id)
  category (category_id)
  actor (actor_id)
  store (store_id)
  staff (staff_id) ──► address, store
  customer (customer_id) ──► address, store

TRANSACTION / FACT
  film (film_id) ──► language
  film_actor (film_id, actor_id)       [junction]
  film_category (film_id, category_id) [junction]
  inventory (inventory_id) ──► film, store
  rental (rental_id) ──► inventory, customer, staff
  payment (payment_id) ──► rental, customer, staff
```

---

## DV 2.0 Entity Model (Full Scope)

### Hubs — 13 tables (one per business entity)

| Hub | Business Key | Source Table |
|-----|-------------|--------------|
| HUB_FILM | film_id | silver.silver_film |
| HUB_RENTAL | rental_id | silver.silver_rental |
| HUB_PAYMENT | payment_id | silver.silver_payment |
| HUB_CUSTOMER | customer_id | silver.silver_customer |
| HUB_INVENTORY | inventory_id | silver.silver_inventory |
| HUB_ACTOR | actor_id | silver.silver_actor |
| HUB_STAFF | staff_id | silver.silver_staff |
| HUB_STORE | store_id | silver.silver_store |
| HUB_ADDRESS | address_id | silver.silver_address |
| HUB_CITY | city_id | silver.silver_city |
| HUB_COUNTRY | country_id | silver.silver_country |
| HUB_LANGUAGE | language_id | silver.silver_language |
| HUB_CATEGORY | category_id | silver.silver_category |

### Links — 17 relationships

| Link | Hubs | Source Table |
|------|------|-------------|
| LNK_RENTAL_CUSTOMER | RENTAL + CUSTOMER | silver_rental |
| LNK_RENTAL_INVENTORY | RENTAL + INVENTORY | silver_rental |
| LNK_RENTAL_STAFF | RENTAL + STAFF | silver_rental |
| LNK_PAYMENT_RENTAL | PAYMENT + RENTAL | silver_payment |
| LNK_PAYMENT_CUSTOMER | PAYMENT + CUSTOMER | silver_payment |
| LNK_PAYMENT_STAFF | PAYMENT + STAFF | silver_payment |
| LNK_INVENTORY_FILM | INVENTORY + FILM | silver_inventory |
| LNK_INVENTORY_STORE | INVENTORY + STORE | silver_inventory |
| LNK_FILM_ACTOR | FILM + ACTOR | silver_film_actor |
| LNK_FILM_CATEGORY | FILM + CATEGORY | silver_film_category |
| LNK_FILM_LANGUAGE | FILM + LANGUAGE | silver_film |
| LNK_CUSTOMER_ADDRESS | CUSTOMER + ADDRESS | silver_customer |
| LNK_CUSTOMER_STORE | CUSTOMER + STORE | silver_customer |
| LNK_STAFF_ADDRESS | STAFF + ADDRESS | silver_staff |
| LNK_STAFF_STORE | STAFF + STORE | silver_staff |
| LNK_ADDRESS_CITY | ADDRESS + CITY | silver_address |
| LNK_CITY_COUNTRY | CITY + COUNTRY | silver_city |

### Satellites — 14 (split by change rate)

| Satellite | Parent Hub | Tracked Columns | Change Rate |
|-----------|-----------|-----------------|-------------|
| SAT_FILM_CORE | HUB_FILM | title, description, release_year, rating, length | Low |
| SAT_FILM_PRICING | HUB_FILM | rental_rate, rental_duration, replacement_cost | High (generator) |
| SAT_RENTAL_STATUS | HUB_RENTAL | rental_date, return_date, last_update | Medium |
| SAT_PAYMENT_DETAIL | HUB_PAYMENT | amount, payment_date | Insert-only |
| SAT_CUSTOMER_PROFILE | HUB_CUSTOMER | first_name, last_name, email, activebool, active | Low |
| SAT_ACTOR_NAME | HUB_ACTOR | first_name, last_name | Very low |
| SAT_STAFF_PROFILE | HUB_STAFF | first_name, last_name, email, active, username | Low |
| SAT_STORE_INFO | HUB_STORE | manager_staff_id | Very low |
| SAT_ADDRESS_DETAIL | HUB_ADDRESS | address, address2, district, postal_code, phone | Low |
| SAT_CITY_NAME | HUB_CITY | city | Very low |
| SAT_COUNTRY_NAME | HUB_COUNTRY | country | Very low |
| SAT_LANGUAGE_NAME | HUB_LANGUAGE | name | Very low |
| SAT_CATEGORY_NAME | HUB_CATEGORY | name | Very low |
| SAT_INVENTORY (marker) | HUB_INVENTORY | last_update | Low |

### PIT Tables (Business Vault — NB_5)

One PIT per hub. Snapshot the latest active satellite `LOAD_DATE` as of each `SNAPSHOT_DATE`.
Priority PITs for query performance:

| PIT Table | Hub | Satellites Snapshotted |
|-----------|-----|----------------------|
| PIT_FILM | HUB_FILM | SAT_FILM_CORE, SAT_FILM_PRICING |
| PIT_RENTAL | HUB_RENTAL | SAT_RENTAL_STATUS |
| PIT_CUSTOMER | HUB_CUSTOMER | SAT_CUSTOMER_PROFILE |
| PIT_PAYMENT | HUB_PAYMENT | SAT_PAYMENT_DETAIL |

### Bridge Tables (Business Vault — NB_5)

Pre-joined many-to-many structures for downstream Gold/reporting:

| Bridge | Resolves |
|--------|---------|
| BRG_RENTAL_FILM | rental → inventory → film path |
| BRG_FILM_CAST | film + actor (via LNK_FILM_ACTOR + SAT_ACTOR_NAME) |

---

## Notebooks (5 total)

### NB_1: `NB_dv_metadata.ipynb` — Config & Shared Helpers
**Purpose**: Single source of truth for all DV config + utility functions used by NB_2–5.

Responsibilities:
- Parse `pipeline_configs/datavault/dv_model.json` at runtime
- `generate_hash_key(cols, algo="sha2_256")` → `sha2(concat_ws("||", UPPER(TRIM(col1)), ...), 256)`
- `generate_diff_hash(cols)` → `sha2(concat_ws("||", coalesce(col, "NULL"), ...), 256)` for satellite change detection
- `create_hub_ddl(hub_config)`, `create_link_ddl(link_config)`, `create_sat_ddl(sat_config)`
- `create_pit_ddl(pit_config)`, `create_bridge_ddl(bridge_config)`
- `get_latest_satellite_hash(sat_table, hub_key)` — fetch DIFF_HK for last row per hub key
- Reuses patterns from `notebooks/helpers/NB_catalog_helpers.ipynb`

Standard DV 2.0 columns added automatically:
- Hubs: `HK_{name}` (SHA-256), `BK_{col}` (business key), `LOAD_DATE`, `RECORD_SOURCE`
- Links: `HK_{name}`, each `HK_{hub}` FK, `LOAD_DATE`, `RECORD_SOURCE`
- Satellites: `HK_{parent}`, `LOAD_DATE`, `DIFF_HK`, `RECORD_SOURCE`, + payload columns

### NB_2: `NB_ingest_to_hubs.ipynb` — Silver → Hubs
**Purpose**: Extract business keys from all enabled Silver tables and insert into Hub tables.

Logic per hub:
1. Read Silver table (batch, watermarked on `last_updated_dt` since last run)
2. Compute `HK_*` = `sha2(concat_ws("||", UPPER(TRIM(BK_col))), 256)`
3. MERGE ON `HK = target.HK` → `WHEN NOT MATCHED THEN INSERT` only
4. Log count of new keys inserted

**Execution**: All 13 hubs can load **in parallel** (no dependencies between them).

### NB_3: `NB_ingest_to_links.ipynb` — Silver → Links
**Purpose**: Extract FK relationships and insert into Link tables.

Logic per link:
1. Read source Silver table
2. Resolve `HK_*` for each FK column (same SHA-256 formula, must match Hub computation)
3. Compute composite `HK_LINK` = `sha2(concat_ws("||", HK_1, HK_2 [, HK_3]), 256)`
4. MERGE ON `HK_LINK = target.HK_LINK` → `WHEN NOT MATCHED THEN INSERT` only

**Depends on**: NB_2 must complete first (Hubs must exist before Links reference them).
**Execution**: All 17 links can load **in parallel**.

### NB_4: `NB_ingest_to_satellites.ipynb` — Silver → Satellites (append-only)
**Purpose**: Detect attribute changes via DIFF_HASH and append new rows. No end-dating.

Logic per satellite:
1. Read Silver table (all rows or watermarked batch)
2. Compute `DIFF_HK` = `sha2(concat_ws("||", coalesce(col, "NULL") for each tracked col), 256)`
3. LEFT JOIN against latest `DIFF_HK` per `HK_{parent}` currently in satellite
4. INSERT only rows where `DIFF_HK` changed or hub key is new
5. Multiple rows per `HK_{parent}` accumulate over time — **no updates, no deletes**

**Query pattern for current state**: `WHERE LOAD_DATE = (SELECT MAX(LOAD_DATE) FROM sat WHERE HK = x)`
**Query pattern for history**: scan all rows for `HK` ordered by `LOAD_DATE`

**Depends on**: NB_2 (Hubs must exist).
**Execution**: Satellites within a hub are independent and can load **in parallel**.

### NB_5: `NB_dv_business_vault.ipynb` — PIT & Bridge Tables
**Purpose**: Query-acceleration structures that denormalize the vault for downstream consumers.

**PIT logic**:
1. Generate `snapshot_date` spine (daily or configurable)
2. For each snapshot date, for each hub key: find the satellite row with the highest `LOAD_DATE` ≤ `snapshot_date`
3. Write one PIT row per (hub_key, snapshot_date) containing the matched `LOAD_DATE` pointers
4. Downstream joins: `PIT.LOAD_DATE_SAT_FILM_PRICING = SAT_FILM_PRICING.LOAD_DATE`

**Bridge logic**:
1. Join Hub → Link → Hub chains via hash keys
2. Optionally enrich with current satellite attributes
3. Persist as Delta table for Gold/dbt consumption

**Depends on**: NB_4 (Satellites must be populated).
**Replaces**: Current Gold-layer aggregations can reference PIT/Bridge instead of Silver directly.

---

## Metadata Config Schema (`pipeline_configs/datavault/dv_model.json`)

```json
{
  "hubs": [
    {
      "name": "HUB_FILM",
      "target_table": "vault.hub_film",
      "source_table": "silver.silver_film",
      "business_key_columns": ["film_id"],
      "load_date_column": "last_updated_dt",
      "record_source": "cdc.dvdrental.film",
      "enabled": true
    }
  ],
  "links": [
    {
      "name": "LNK_RENTAL_CUSTOMER",
      "target_table": "vault.lnk_rental_customer",
      "source_table": "silver.silver_rental",
      "hub_references": [
        {"hub": "HUB_RENTAL",   "source_column": "rental_id"},
        {"hub": "HUB_CUSTOMER", "source_column": "customer_id"}
      ],
      "load_date_column": "last_updated_dt",
      "record_source": "cdc.dvdrental.rental",
      "enabled": true
    }
  ],
  "satellites": [
    {
      "name": "SAT_FILM_PRICING",
      "target_table": "vault.sat_film_pricing",
      "parent_hub": "HUB_FILM",
      "source_table": "silver.silver_film",
      "hub_key_source_column": "film_id",
      "tracked_columns": ["rental_rate", "rental_duration", "replacement_cost"],
      "load_date_column": "last_updated_dt",
      "record_source": "cdc.dvdrental.film",
      "enabled": true
    }
  ],
  "pit_tables": [
    {
      "name": "PIT_FILM",
      "target_table": "vault.pit_film",
      "hub": "HUB_FILM",
      "satellites": ["SAT_FILM_CORE", "SAT_FILM_PRICING"],
      "snapshot_grain": "daily",
      "enabled": true
    }
  ],
  "bridge_tables": [
    {
      "name": "BRG_RENTAL_FILM",
      "target_table": "vault.brg_rental_film",
      "path": ["HUB_RENTAL", "LNK_RENTAL_INVENTORY", "HUB_INVENTORY", "LNK_INVENTORY_FILM", "HUB_FILM"],
      "enabled": true
    }
  ]
}
```

---

## Databricks Catalog Layout (new)

```
workspace/
  └─ vault/
     ├─ hub_film          ├─ hub_rental         ├─ hub_payment
     ├─ hub_customer      ├─ hub_inventory       ├─ hub_actor
     ├─ hub_staff         ├─ hub_store           ├─ hub_address
     ├─ hub_city          ├─ hub_country         ├─ hub_language
     ├─ hub_category
     ├─ lnk_rental_customer    ├─ lnk_rental_inventory   ├─ lnk_rental_staff
     ├─ lnk_payment_rental     ├─ lnk_payment_customer   ├─ lnk_payment_staff
     ├─ lnk_inventory_film     ├─ lnk_inventory_store    ├─ lnk_film_actor
     ├─ lnk_film_category      ├─ lnk_film_language      ├─ lnk_customer_address
     ├─ lnk_customer_store     ├─ lnk_staff_address      ├─ lnk_staff_store
     ├─ lnk_address_city       ├─ lnk_city_country
     ├─ sat_film_core          ├─ sat_film_pricing        ├─ sat_rental_status
     ├─ sat_payment_detail     ├─ sat_customer_profile    ├─ sat_actor_name
     ├─ sat_staff_profile      ├─ sat_store_info          ├─ sat_address_detail
     ├─ sat_city_name          ├─ sat_country_name        ├─ sat_language_name
     ├─ sat_category_name      ├─ sat_inventory
     ├─ pit_film    ├─ pit_rental    ├─ pit_customer    ├─ pit_payment
     └─ brg_rental_film         └─ brg_film_cast
```

---

## Orchestration (Orders-ingest-job.yaml additions)

```
Bronze
  └─► Silver (film, rental, payment, + 12 ref tables in parallel)
        └─► NB_2: Hubs (all 13 in parallel)
              └─► NB_3: Links (all 17 in parallel)
              └─► NB_4: Satellites (all 14 in parallel)
                    └─► NB_5: PIT + Bridge
```

---

## Critical Files

- **New notebooks**: `notebooks/vault/NB_dv_metadata.ipynb`, `NB_ingest_to_hubs.ipynb`, `NB_ingest_to_links.ipynb`, `NB_ingest_to_satellites.ipynb`, `NB_dv_business_vault.ipynb`
- **New config**: `pipeline_configs/datavault/dv_model.json`
- **Extend**: `Orders-ingest-job.yaml` — add vault task group after silver tasks
- **Reuse**: `notebooks/helpers/NB_catalog_helpers.ipynb` patterns for DDL/MERGE helpers

---

## Verification

1. Load Silver → Hubs: distinct business key count must match Silver row count (no duplication)
2. Load same Silver twice → Hub/Link row counts unchanged (idempotency)
3. Update `rental_rate` on a film → `SAT_FILM_PRICING` gains exactly 1 new row; old row untouched
4. `LNK_RENTAL_CUSTOMER` row count = number of distinct `(rental_id, customer_id)` pairs in Silver
5. PIT_FILM `snapshot_date=T` → joined `SAT_FILM_PRICING.rental_rate` must equal the film's price as-of T
