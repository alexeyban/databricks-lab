# DV 2.0 → Semantic Layer

## Automatic Fact / Dimension Detection Rules

---

## 1. Purpose

This document defines **deterministic rules** for classifying Data Vault 2.0 entities into semantic layer components:

* **DIMENSION**
* **FACT**
* **BRIDGE_FACT**
* **SNAPSHOT_FACT**
* **HELPER / TECHNICAL**

These rules are used in the **DV → dbt Gold auto-generation pipeline**.

---

## 2. Core Principle

Classification is driven by:

> **Grain + behavior over time**

| Type          | Grain         | Behavior                    |
| ------------- | ------------- | --------------------------- |
| DIMENSION     | Entity        | Slowly changing             |
| FACT          | Event         | Insert-heavy, transactional |
| SNAPSHOT_FACT | Entity + Time | Periodic state              |
| BRIDGE        | Relationship  | Many-to-many                |

---

## 3. Input Signals

The classifier uses:

* DV metadata (`dv_model.json`)
* Hubs / Links / Satellites / PIT / Bridge tables
* Column names and types
* AI-generated descriptions
* Relationship topology

---

## 4. Level 1 — Hub Classification

### Rule H1 — Transactional Hub → FACT

**Condition:**

* Business key represents an event
* High insert rate
* Contains time attributes

**Examples:**

* `rental_id`
* `payment_id`
* `order_id`
* `transaction_id`

**Result:**

```
FACT
```

---

### Rule H2 — Master Data Hub → DIMENSION

**Condition:**

* Stable business entity
* Mostly descriptive attributes
* Low update frequency

**Examples:**

* `customer_id`
* `film_id`
* `store_id`
* `actor_id`

**Result:**

```
DIMENSION
```

---

### Rule H3 — Reference Hub → DIMENSION (Low Cardinality)

**Condition:**

* Lookup/reference data
* Small number of rows

**Examples:**

* `country`
* `category`
* `language`

**Result:**

```
DIMENSION (reference)
```

---

## 5. Level 2 — Satellite Analysis

### Rule S1 — Numeric-heavy Satellite → FACT Signal

**Condition:**

* Contains numeric measures:

  * `amount`
  * `price`
  * `cost`
  * `quantity`
  * `duration`

**Effect:**

```
Promote parent hub → FACT candidate
```

---

### Rule S2 — Descriptive Satellite → DIMENSION

**Condition:**

* Mostly text / descriptive fields:

  * names
  * emails
  * attributes

**Effect:**

```
DIMENSION
```

---

### Rule S3 — High-frequency Satellite → FACT Reinforcement

**Condition:**

* Many rows per hub key over time
* Frequent changes

**Effect:**

```
FACT (event-driven)
```

---

## 6. Level 3 — Link Analysis

### Rule L1 — Dimension-to-Dimension Link → BRIDGE

**Examples:**

* `film_actor`
* `film_category`

**Result:**

```
BRIDGE
```

---

### Rule L2 — Transactional Link → FACT Join

**Examples:**

* `rental ↔ customer`
* `payment ↔ rental`

**Effect:**

```
Contributes to FACT model
```

---

### Rule L3 — Multi-hop Chain → FACT Root

**Pattern:**

```
customer → rental → payment
```

**Result:**

```
FACT = terminal entity (payment)
```

---

## 7. Level 4 — PIT Tables

### Rule P1 — PIT over Dimension → DIMENSION (Current State)

**Example:**

```
PIT_CUSTOMER → dim_customer
```

---

### Rule P2 — PIT over Transaction → SNAPSHOT_FACT

**Condition:**

* Snapshot over event entity

**Result:**

```
SNAPSHOT_FACT
```

---

## 8. Level 5 — Bridge Tables

### Rule B1 — Bridge with Measures → FACT

**Condition:**

* Contains numeric aggregations

**Result:**

```
FACT
```

---

### Rule B2 — Bridge without Measures → FACT Base

**Example:**

```
BRG_RENTAL_FILM
```

**Effect:**

```
FACT (requires enrichment)
```

---

## 9. Level 6 — Column Heuristics

### Rule C1 — Measure Columns → FACT

**Columns:**

* `amount`
* `price`
* `cost`
* `revenue`

---

### Rule C2 — Time Columns → FACT

**Columns:**

* `created_at`
* `payment_date`
* `rental_date`

---

### Rule C3 — Descriptive Only → DIMENSION

---

## 10. Level 7 — AI Description Rules

### Rule A1 — Action Semantics → FACT

**Keywords:**

* "transaction"
* "payment"
* "event"
* "record of"

---

### Rule A2 — Entity Semantics → DIMENSION

**Keywords:**

* "represents"
* "entity"
* "person"
* "object"

---

### Rule A3 — Relationship Semantics → BRIDGE

**Keywords:**

* "relationship between"
* "mapping"
* "association"

---

## 11. Conflict Resolution

### Priority Order

```
1. Bridge rules
2. Transactional hub rules
3. Satellite signals
4. Column heuristics
5. AI descriptions
```

---

### Example

**HUB_PAYMENT:**

* Has attributes → DIMENSION signal
* Has amount + timestamp → FACT signal

**Resolution:**

```
FACT (stronger signal)
```

---

## 12. Classification Algorithm (Pseudo Code)

```python
def classify_entity(hub, satellites, links, description):
    if is_bridge(links):
        return "BRIDGE_FACT"

    if is_transactional_hub(hub):
        return "FACT"

    if has_measure_columns(satellites):
        return "FACT"

    if is_reference_data(hub):
        return "DIMENSION"

    if is_descriptive(satellites):
        return "DIMENSION"

    if ai_suggests_fact(description):
        return "FACT"

    return "DIMENSION"
```

---

## 13. Expected Output Structure

```json
{
  "entities": [
    {
      "name": "customer",
      "type": "DIMENSION",
      "source": "HUB_CUSTOMER"
    },
    {
      "name": "payment",
      "type": "FACT",
      "source": "HUB_PAYMENT"
    }
  ]
}
```

---

## 14. dvdrental Reference Classification

| Entity        | Type      |
| ------------- | --------- |
| customer      | DIMENSION |
| film          | DIMENSION |
| actor         | DIMENSION |
| store         | DIMENSION |
| rental        | FACT      |
| payment       | FACT      |
| film_actor    | BRIDGE    |
| film_category | BRIDGE    |

---

## 15. Implementation Notes

* Start with deterministic rules only
* Validate on dvdrental dataset
* Add AI refinement after baseline is stable
* Store classification results in generator session output

---

## 16. Next Steps

* Implement `step8_semantic_classifier.py`
* Generate dbt models based on classification
* Add metric inference layer
* Integrate with Genie testing workflow

---

## 17. Summary

This rule system enables:

* Automated semantic modeling from DV 2.0
* Deterministic fact/dimension detection
* Scalable dbt Gold generation
* AI-assisted semantic enrichment

---

**Status:** Ready for implementation


