# DV 2.0 Model Code Review — Validation Failed

**Session:** 20260405_200923
**Generated:** 2026-04-06 07:25:58 UTC
**Errors:** 1  **Warnings:** 8

---

## Error V6 — HUB_ACTOR

**Message:** Duplicate hub name 'HUB_ACTOR'

**Current value in 05_human_review.json:**
```json
{
  "name": "HUB_ACTOR",
  "target_table": "vault.hub_actor",
  "source_table": "silver.silver_actor",
  "business_key_columns": [
    "actor_id"
  ],
  "load_date_column": "last_update",
  "record_source": "cdc.dvdrental.actor",
  "enabled": true,
  "confidence": "HIGH",
  "rules_fired": [
    "R4"
  ]
}
```

**Fix:** Rename the duplicate entity — each hub/link/satellite/pit/bridge must have a unique name.

**To fix:** Edit `05_review_notebook.ipynb`, correct the issue, re-run the save cell, then resume:
```
python -m generators.dv_generator.main --resume 20260405_200923 --from-step step6_validator
```

---

## Warnings (non-blocking)

- **W2** `SAT_CATEGORY_CORE`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_CITY_CORE`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_COUNTRY_CORE`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_INVENTORY`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_LANGUAGE_CORE`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_PAYMENT_CORE`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_PAYMENT_PRICING`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_STORE`: Satellite has only 1 tracked column — consider merging with another satellite
