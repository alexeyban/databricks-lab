# pump.fun Risk-Scoring Layer — Design

**Status (2026-09-14): Design proposal, not implemented. Bronze/Silver for
`pump_events`/`pump_tokens`/`pump_transfers` exist per
`PUMPFUN_PIPELINE.md`; this document proposes the additional Silver sources
and a new Gold layer needed to score newly-launched tokens for rug-pull /
scam risk.**

---

## Why this exists

`PUMPFUN_PIPELINE.md` lands raw pump.fun events into Bronze/Silver but does
no risk interpretation — `mint_authority`/`freeze_authority` are captured
in `silver.pump_tokens` as raw values, not evaluated. `ROADMAP.md` §9 lists
a migration funnel (`created → migrated → rugged`) as a planned Gold mart.
This document designs the risk-scoring layer that fills that gap, using the
signal set documented by the API provider itself in
`ingestion/pumpfun/docs/glossary_of_event_properties.txt`.

## Gaps in current Silver

Today's Silver only covers `create` (`silver_pump_tokens`), `transfer`
(`silver_pump_transfers`), and the generic event stream
(`silver_pump_events`). Several fields that the glossary explicitly calls
out as risk signals arrive only in event types that are currently left in
Bronze only (per `PUMPFUN_PIPELINE.md` design decision 4 / `ROADMAP.md`
§9): `createPool`, `migrate`, `add`, `remove`, `buy`, `sell`. Two new
Silver tables are needed before scoring is possible:

| New Silver table | Source `action` | Fields needed for risk |
|---|---|---|
| `silver_pump_pools` | `createPool`, `migrate`, `add`, `remove` | `burnedLiquidity`, `lockedLiquidityAfterMigration`, `poolCreatedBy`, `poolFeeRateAfterMigration`, `migrationThresholds`, `mayhemMode` |
| `silver_pump_trades` | `buy`, `sell` | `price`, `tokenAmount`/`quoteAmount`, `tradersInvolved`, `breakdown` (bundling detection), `postBalances` |

Both follow the same pattern as the existing three Silver transformations:
read `spark.readStream.table(BRONZE_TABLE)`, filter by `action`, extract
typed columns with `get_json_object`.

## Gold: `gold_pump_token_risk`

One row per `mint`, current-state snapshot (same spirit as the MERGE-based
current-state Silver tables in the dvdrental pipeline), recomputed
incrementally on every pipeline trigger. Given `pumpapi-lakehouse` is
already a single Lakeflow Declarative Pipeline (`pyspark.pipelines`), the
risk table is proposed as one more `@dp.table` in that same pipeline,
joining `pump_tokens` + `pump_pools` (new) + `pump_trades` (new) +
`pump_transfers` per `mint` — keeping risk scoring on the same ~2-minute
cadence as ingestion, with no separate job.

### Category A — hard blockers (boolean, not weighted)

The glossary itself states these must always be a specific value; treating
them as boolean overrides (rather than diluting them into the weighted
score) keeps them visible regardless of the total score.

| Flag | Condition | Source |
|---|---|---|
| `mint_authority_active` | `mintAuthority IS NOT NULL` | `silver_pump_tokens` |
| `freeze_authority_active` | `freezeAuthority IS NOT NULL` (possible honeypot) | `silver_pump_tokens` |
| `unsafe_token_extension` | `tokenProgram = 'spl-token-2022'` and `tokenExtensions` contains a value outside an allow-list of known-safe extensions | `silver_pump_tokens` / Bronze |
| `mayhem_mode_on` | `mayhemMode = true` | `silver_pump_pools` (new) |

Any active Category A flag forces the token's risk tier to at least
`high`, independent of the weighted score below.

### Category B — weighted score (0–100)

| Signal | Rule | Weight |
|---|---|---|
| Burned liquidity | parse `burnedLiquidity` (`"NN%"` → double); 0% → max penalty, 20–30%+ → 0 | up to 35 |
| Locked liquidity after migration (Meteora) | `lockedLiquidityAfterMigration < 100%` | up to 25 |
| Pool trust | `poolCreatedBy = 'custom'` or `'meteora-launchpad'` without 100% lock → penalty; migrated from `pump`/`raydium-launchpad` → trusted | up to 15 |
| Bundling / sniping at launch | share of distinct `trader` entries inside `breakdown` of `buy` events in the same block as `create` | up to 15 |
| Creator dump | `creator_wallet` (`txSigner` from the `create` event) appears in `tradersInvolved` as a seller within N minutes of `created_at` | up to 30 |
| Holder concentration | top-10 address share of supply, from `postBalances` / aggregated `silver_pump_transfers` | up to 20 |
| Post-migration fee | `poolFeeRateAfterMigration` close to the 10% Meteora ceiling | up to 10 |

Score → tier: `low` (0–20) / `medium` (21–50) / `high` (51–80) / `critical`
(81+, or any active Category A flag).

### Category C — migration funnel status

Directly implements the `created → migrated → rugged` mart from
`ROADMAP.md` §9, derived from the event sequence per `mint`:

- `created` — a `create` event exists
- `active` — `buy`/`sell` events exist after creation
- `migrated` — a `migrate` event exists
- `rugged` — heuristic: sharp drop in `tokensInPool`/`quoteInPool` toward
  zero in a short window, no subsequent `buy` activity, typically
  co-occurring with previously-low `burnedLiquidity` and a large `remove`
  event

## Monitoring and alerting

Reuse the existing `monitoring.dq_results` table and the Slack/Teams/email
alerting already built in `NB_schema_drift_helpers.ipynb` (from the DQ/GDPR
workstream — see `design/dq_gdpr/IMPLEMENTATION_PLAN.md`) rather than
building a second alerting path. When a token younger than a configurable
threshold crosses into `critical` tier, or a Category A flag fires, write a
result through the same helper pattern and alert the same channel.

## Open questions

- **Thresholds** (burned-liquidity %, creator-dump window in minutes,
  top-10 concentration %) are expert guesses here and should be calibrated
  against historically confirmed `rugged` tokens once enough are observed.
- **`tokenExtensions` allow-list**: the glossary references "our guide" for
  which spl-token-2022 extensions are safe, but that guide isn't in this
  repo. Start conservative (any extension present → flag) until a source
  list is found or built.
- **History vs. snapshot**: `gold_pump_token_risk` as designed is a
  snapshot (current state only). Seeing how a token's score evolved before
  it was rugged is valuable and argues for an append-only
  `gold_pump_token_risk_history` table alongside the snapshot — full Data
  Vault historization is deliberately out of scope per
  `PUMPFUN_PIPELINE.md` design decision 6.

## Next steps

1. Add `silver_pump_pools.py` and `silver_pump_trades.py` to
   `pumpapi-lakehouse/transformations/`.
2. Add `gold_pump_token_risk.py` to the same pipeline, joining the four
   Silver sources per `mint`.
3. Calibrate weights/thresholds against observed data.
4. Wire alerting through the existing DQ/GDPR monitoring helpers.
