# DV 2.0 Model Code Review — Validation Failed

**Session:** 20260411_120242
**Generated:** 2026-04-11 12:02:56 UTC
**Errors:** 1  **Warnings:** 10

---

## Error V3 — LNK_ORDERS_PRODUCT

**Message:** hub_reference 'HUB_PRODUCT' does not exist in hubs list

**Current value in 05_human_review.json:**
```json
{
  "name": "LNK_ORDERS_PRODUCT",
  "target_table": "vault.lnk_orders_product",
  "source_table": "silver.silver_orders",
  "hub_references": [
    {
      "hub": "HUB_ORDERS",
      "source_column": "id"
    },
    {
      "hub": "HUB_PRODUCT",
      "source_column": "product_id"
    }
  ],
  "load_date_column": "inserted_at",
  "record_source": "cdc.dvdrental.orders",
  "enabled": true,
  "confidence": "HIGH",
  "rules_fired": [
    "L1"
  ]
}
```

**Fix:** Change the `hub` value to one of the valid hub names: `HUB_ACTOR`, `HUB_ADDRESS`, `HUB_CATEGORY`, `HUB_CITY`, `HUB_COUNTRY`, `HUB_CUSTOMER`, `HUB_FILM`, `HUB_INVENTORY`.

**To fix:** Edit `05_review_notebook.ipynb`, correct the issue, re-run the save cell, then resume:
```
python -m generators.dv_generator.main --resume 20260411_120242 --from-step step6_validator
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
- **W2** `SAT_ORDERS_CORE`: Satellite has only 1 tracked column — consider merging with another satellite
- **W2** `SAT_ORDERS_PRICING`: Satellite has only 1 tracked column — consider merging with another satellite
