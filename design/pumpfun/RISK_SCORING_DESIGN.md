# pump.fun Risk-Scoring Layer — Design

**Status (2026-09-14): Deployed and calibrated once against a real
confirmed rug (mint `12DqvhKnLFV9uiGiQYJeAkSLS7eF5FiE5UE61vA1pump`), which
the first-pass model scored `risk_score=20`/`low` despite an obvious
bot-sniped launch + delayed creator dump. Added sniping (distinct buyers
in a short post-launch window, not just same-tx bundling),
`creator_dumped_ever` (unbounded lookback — the original launch-window
cap missed a dump ~1h46m after creation) + `creator_dumped_recent`
(rolling window on scoring time, escalates to `critical`), and an
`extreme_concentration` tier-floor. Re-scored the same mint at
`risk_score=70`/`high` after the fix. Scoring now runs as
`processing/gold/NB_process_pump_token_risk.ipynb` — moved out of the
`pumpapi-lakehouse` Lakeflow pipeline so per-mint recompute can be
incremental (only mints touched since the last run); see that notebook's
first cell for why this doesn't fit the declarative `@dp.table`
framework. A second real case (mint
`12NvSK9hEZsaFzjyNGCAK3mk1UywrL7ENTKJmYKDpump`: pumped to ~$21.7K market
cap then collapsed ~1223x over the next day) had near-zero holder
concentration and no creator dump, so those signals stayed quiet;
`sniping_ratio` alone (20/100 points) wasn't enough to reach `medium`.
Added `sniper_flip_ratio` — the share of launch-window snipers who sell
again within 15 minutes (this mint: 258 snipers, 251/97% flipped) — a
deliberately *earlier* signal than price-drawdown-from-peak, which is
only observable after a crash has already happened.

A follow-up audit (`design/pumpfun/RISK_SCORING_GAP_AUDIT.md`, 15,912
low-risk tokens over 30 days) then found the second case's "near-zero
holder concentration" wasn't actually low — it was the *same underlying
bug* surfacing again: `_holder_concentration_signal` only read
`silver_pump_transfers`, but pump.fun holders overwhelmingly trade
against the bonding curve/pool rather than transferring peer-to-peer.
Across 30 sampled rugged tokens, transfers-only concentration read
0.5–16% when the true trade-based concentration was 99.98–99.996%.
Fixed by unioning net position from both `pump_trades` and
`pump_transfers`. Also added `scoring_model_version` (an incrementally
scored table can silently freeze a dead mint's row at an older model
version's output — 28 of the 30 audited tokens hadn't been re-touched
since `sniper_flip_ratio` shipped). Weights/thresholds are still
first-pass, now calibrated against three real cases plus a 30-day gap
audit — see Open Questions.

A fourth case (mint `C4oBvs4xg31Nr7FpW2ePb1UxE2zgL9ueCoAuwnvtpump`: 1 buy
+ 1 sell, ever, market cap flat around $28) surfaced a distinct concern:
`risk_score=0` there is mathematically correct — no signal fired because
no manipulation pattern occurred — but "no evidence of manipulation" and
"confirmed safe" are different claims for a token with essentially no
trading history to evaluate. Rather than changing risk_score/risk_tier's
meaning, added `total_trade_count`/`distinct_trader_count` (no baked-in
threshold, for any consumer including the planned Success Score to apply
its own bar) and a convenience `has_meaningful_activity` boolean
(`distinct_trader_count >= MIN_DISTINCT_TRADERS_FOR_MEANINGFUL_ACTIVITY`,
currently 3) as a separate data-sufficiency axis alongside the score.**

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
| `unsafe_token_extension` | `tokenExtensions` matches a deny-list of extensions that let the issuer/delegate move, block, tax, or freeze a holder's tokens without consent (see below) | `silver_pump_tokens` |
| `mayhem_mode_on` | `mayhemMode = true` | `silver_pump_pools` (new) |

Any active Category A flag forces the token's risk tier to at least
`high`, independent of the weighted score below.

**`unsafe_token_extension` deny-list.** Sourced from
[solana.com/docs/tokens/extensions](https://solana.com/docs/tokens/extensions)
(full spl-token-2022 extension catalog) and
[offside.io's Token-2022 security write-up](https://blog.offside.io/p/token-2022-security-best-practices-part-2)
(concrete attack patterns). Flagged as unsafe: `PermanentDelegate` (the
offside.io article's top example — a delegate can "directly transfer or
burn any amount of mint from any token account" bypassing owner
signatures), `NonTransferable`/`NonTransferableAccount`,
`Pausable`/`PausableAccount`, `DefaultAccountState` (can default new
accounts to frozen — a freeze-authority equivalent), `TransferHook`/
`TransferHookAccount` (arbitrary external program runs on every transfer —
a known honeypot vector), `TransferFeeConfig`/`TransferFeeAmount` (flagged
by presence, not fee magnitude — PumpAPI's `tokenExtensions` field doesn't
expose the actual fee rate, and offside.io notes a 0-max-fee config is
technically harmless, but the two can't be told apart from this field
alone), and `ConfidentialTransfer*`/`ConfidentialMintBurn` (own judgment,
not sourced from either article — flagged for opacity: they hide amounts
that the rest of this scoring model needs to observe, not for a documented
exploit).

Deliberately treated as benign: `MemoTransfer` and `InterestBearingConfig`
are integrator footguns per offside.io (broken transfers, interest-calc
mismatches), not tools for extracting value from holders.
`MetadataPointer`/`TokenMetadata`/`GroupPointer`/`TokenGroup`/
`GroupMemberPointer`/`TokenGroupMember` are safe as a property of *this*
mint — offside.io's actual finding is that a *third party* can create
spoofed Metadata/Group accounts pointing at *someone else's* legitimate
mint ("Everyone can create Token-2022 accounts of type Metadata, Group and
Group Member, fill these accounts with deliberately crafted data, and
point them to a legitimate mint"). That's a risk for whoever reads
metadata content, not for a mint that happens to use these extensions —
relevant if this pipeline ever surfaces token name/symbol/image, at which
point it must verify the reference is bidirectional
(`account.metadata_pointer.metadata_address == mint` AND
`mint.metadata == account`) before trusting it. Not currently applicable:
`gold_pump_token_risk` doesn't read metadata content today.

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
- **`tokenExtensions` deny-list — resolved**, sourced externally (see
  Category A above) rather than from the glossary's own unpublished
  "guide". Two residual gaps: (1) PumpAPI's exact `tokenExtensions` JSON
  shape (bare strings vs. objects) is still unconfirmed against real data
  — the implementation matches the extension name as a JSON token via
  regex rather than parsing a typed array, to be robust to either shape;
  (2) `TransferFeeConfig`/`TransferFeeAmount` are flagged by presence only
  since the field doesn't expose the fee rate — worth tightening to
  fee-magnitude scoring (like `poolFeeRateAfterMigration`) if PumpAPI ever
  exposes it.
- **Metadata/Group spoofing (bidirectional verification)**: not an issue
  for the current implementation, which never reads metadata content, but
  a real future footgun per offside.io — see the `unsafe_token_extension`
  note above. If/when this pipeline surfaces token name/symbol/image
  (e.g. for a dashboard or Slack alert), it must verify
  `account.metadata_pointer.metadata_address == mint` AND
  `mint.metadata == account` before trusting the displayed data, or it can
  be tricked into showing an attacker's spoofed name/image for a
  legitimate mint.
- **History vs. snapshot**: `gold_pump_token_risk` as designed is a
  snapshot (current state only). Seeing how a token's score evolved before
  it was rugged is valuable and argues for an append-only
  `gold_pump_token_risk_history` table alongside the snapshot — full Data
  Vault historization is deliberately out of scope per
  `PUMPFUN_PIPELINE.md` design decision 6.

## Next steps

1. ~~Add `silver_pump_pools.py` and `silver_pump_trades.py`~~ — done, #13.
2. ~~Add `gold_pump_token_risk.py`~~ — done, #14 (Categories A/B/C +
   `unsafe_token_extension` deny-list).
3. Merge and deploy #12/#13/#14, then verify `gold.gold_pump_token_risk`
   against live PumpAPI events — nothing in this design has run against
   real data yet.
4. Calibrate weights/thresholds against observed data (see residual gaps
   under Open questions).
5. Wire alerting through the existing DQ/GDPR monitoring helpers.
