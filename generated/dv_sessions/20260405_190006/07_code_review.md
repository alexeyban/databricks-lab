# DV 2.0 Model Code Review — Validation Failed

**Session:** 20260405_190006
**Generated:** 2026-04-05 19:01:00 UTC
**Errors:** 3  **Warnings:** 8

---

## Error V7 — PIT_RENTAL

**Message:** PIT references satellite 'SAT_RENTAL_STATUS' which does not exist

**Current value in 05_human_review.json:**
```json
{
  "name": "PIT_RENTAL",
  "target_table": "vault.pit_rental",
  "hub": "HUB_RENTAL",
  "satellites": [
    "SAT_RENTAL_STATUS"
  ],
  "snapshot_grain": "daily",
  "enabled": true
}
```

**Fix:** Ensure the PIT's `hub` and all `satellites` values reference valid entity names.

**To fix:** Edit `05_review_notebook.ipynb`, correct the issue, re-run the save cell, then resume:
```
python -m generators.dv_generator.main --resume 20260405_190006 --from-step step6_validator
```

---

## Error V7 — PIT_CUSTOMER

**Message:** PIT references satellite 'SAT_CUSTOMER_PROFILE' which does not exist

**Current value in 05_human_review.json:**
```json
{
  "name": "PIT_CUSTOMER",
  "target_table": "vault.pit_customer",
  "hub": "HUB_CUSTOMER",
  "satellites": [
    "SAT_CUSTOMER_PROFILE"
  ],
  "snapshot_grain": "daily",
  "enabled": true
}
```

**Fix:** Ensure the PIT's `hub` and all `satellites` values reference valid entity names.

**To fix:** Edit `05_review_notebook.ipynb`, correct the issue, re-run the save cell, then resume:
```
python -m generators.dv_generator.main --resume 20260405_190006 --from-step step6_validator
```

---

## Error V7 — PIT_PAYMENT

**Message:** PIT references satellite 'SAT_PAYMENT_DETAIL' which does not exist

**Current value in 05_human_review.json:**
```json
{
  "name": "PIT_PAYMENT",
  "target_table": "vault.pit_payment",
  "hub": "HUB_PAYMENT",
  "satellites": [
    "SAT_PAYMENT_DETAIL"
  ],
  "snapshot_grain": "daily",
  "enabled": true
}
```

**Fix:** Ensure the PIT's `hub` and all `satellites` values reference valid entity names.

**To fix:** Edit `05_review_notebook.ipynb`, correct the issue, re-run the save cell, then resume:
```
python -m generators.dv_generator.main --resume 20260405_190006 --from-step step6_validator
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
