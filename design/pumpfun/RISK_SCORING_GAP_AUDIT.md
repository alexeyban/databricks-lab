# pump.fun Risk-Scoring — Gap Audit (low-risk tokens, last 30 days)

**Status (2026-09-14): Gap #1 (holder-concentration data source) and gap #3
(model-version staleness → `scoring_model_version` column) implemented in
`processing/gold/NB_process_pump_token_risk.ipynb`. Gap #2 (sniper-flip
window) intentionally not built yet — per this doc's own assessment it's
likely subsumed by #1; re-audit after #1 ships before deciding.**

Audit of `workspace.gold.gold_pump_token_risk` rows
with `risk_tier = 'low'` and `created_at` in the last 30 days, looking for
confirmed scams the current model missed. Builds on the two prior
ground-truth cases already fixed in the model (mint
`12DqvhKnLFV9uiGiQYJeAkSLS7eF5FiE5UE61vA1pump` → added sniping/creator-dump
signals; mint `12NvSK9hEZsaFzjyNGCAK3mk1UywrL7ENTKJmYKDpump` → added
`sniper_flip_ratio`). See `RISK_SCORING_DESIGN.md` for the full signal
catalog and weights this audit assumes as context.

## Methodology

1. Pulled all `risk_tier = 'low'` rows from `gold_pump_token_risk` with
   `created_at >= now() - 30 days` — **15,912 tokens**.
2. Joined against `silver.pump_trades` to compute, per mint: trade count,
   distinct traders, peak `market_cap_quote`, and the most recent
   `market_cap_quote` by `event_time`. This drawdown ratio
   (`1 - last_mc/peak_mc`) is used here purely as an **audit/labeling
   tool** to surface likely-missed scams in a low-risk bucket — it is
   *not* being proposed as a scoring signal (see "Not proposed" below;
   this repeats the reasoning already used to reject it as a signal when
   `sniper_flip_ratio` was added).
3. Filtered to tokens with real activity (`trade_count >= 200`,
   `distinct_traders >= 50`) to exclude tokens that simply never took off,
   and to a drawdown ratio `>= 0.95` (peak-to-latest collapse of 95%+).
4. Took the top 30 by drawdown ratio for deep-dive review against the raw
   Silver tables, the same way the two prior ground-truth cases were
   investigated.

All 30 reviewed tokens: `funnel_status` mostly `migrated` (real trading
momentum, not stillborn launches), `mint_authority_active` /
`freeze_authority_active` / `unsafe_token_extension` / `mayhem_mode_on` all
`false`, `bundling_ratio` ~0, `creator_dumped_ever = false` for **all 30**.
`risk_score` ranged 6–20 (bucket `low`). Peak market caps ranged from
~$4.2K to an extreme **$17.37 billion** (mint `3nqHijNUExsnjNBb15WJsJ2xisyMVGN6FK4aUgZk1Rwj`),
with drawdowns of 99.96–99.9999%+ from peak.

## Найденные пробелы

### 1. Holder-concentration signal is built on the wrong data source (dominant finding)

`top_holder_share` is reconstructed entirely from `silver.pump_transfers`
(`RISK_SCORING_DESIGN.md` already flags this as an approximation). Across
all 30 sampled tokens it read suspiciously low — 0.5% to 16%, nowhere near
the `EXTREME_CONCENTRATION_THRESHOLD = 0.95` escalator — **despite
catastrophic, obviously-coordinated collapses**.

Recomputing concentration directly from `silver.pump_trades` (net position
per trader = `SUM(buy token_amount) - SUM(sell token_amount)`, top-10
share of positive net positions) tells a completely different story:

| mint | recorded `top_holder_share` (transfers-based) | actual top-10 share (trade-based) |
|---|---|---|
| `CHbodGeNZW5Fh12XLvobHAYzxwQjCWKk55kstzdYpump` | 2.4% | **99.996%** |
| `tD3pH7KapyBb66YWoZDM5BL6PuvCBcAVEvm3qFupump` | 0.76% | **99.98%** |

This isn't noise — it's a ~40x-to-4000x discrepancy in the *opposite*
direction of the truth. Root cause: on pump.fun, holders overwhelmingly
acquire and dispose of tokens by trading against the bonding
curve/pool (`buy`/`sell` in `pump_trades`), not by peer-to-peer wallet
transfers. `pump_transfers` only captures a minority side-channel
(withdrawals, wallet consolidation, etc.), so a signal built solely on it
is structurally blind to the primary concentration mechanism.

**Proposed fix — early signal.** Replace (or add alongside)
`_holder_concentration_signal`'s source table: compute net token position
per trader from `silver.pump_trades` instead of (or in addition to)
`silver.pump_transfers`, then rank/sum the same way. This is *not* a late
signal — net trading position is available continuously as trades stream
in, typically well within the same window the concentration itself builds
up (per this sample, most peaks occurred within the first day, often
within hours of creation). This single fix would very likely have caught
most or all of the 30 sampled tokens, since a `top_holder_share >= 0.95`
read on the *correct* data would trigger the existing `extreme_concentration`
escalator to `high` with no new weight/threshold tuning needed.

### 2. Sniper-flip's 15-minute window misses slower, "patient" cohort dumps

Not every coordinated bot cohort flips within `FLIP_WINDOW_MINUTES = 15`.
Example: `CHbodGeNZW5Fh12XLvobHAYzxwQjCWKk55kstzdYpump` — 26 launch-window
snipers, but only **1 of 26 (4%)** sold within 15 minutes; 7 of 26
eventually sold over a longer horizon, and the token still collapsed
99.9995% peak-to-latest. `sniper_flip_ratio` (once recomputed — see gap 3)
would read ~0.04 here, nowhere near the `EXTREME_SNIPER_FLIP_THRESHOLD =
0.8` escalator, even though this is clearly the same style of bot-driven
launch as the case that motivated adding the signal.

**Assessment: likely subsumed by fixing gap 1, not a separate fix to
build.** A cohort that accumulates a large position and only sells later
would already be caught by trade-based holder concentration the moment
they're holding a large share — which is, if anything, an *earlier*
warning than waiting for them to sell at all. Recommend re-running this
same audit after gap 1 ships before deciding whether a widened flip-window
variant is still needed on top.

### 3. Model-version staleness — not a scoring gap, an operational one

28 of the 30 sampled tokens still show `sniper_flip_ratio = NULL` /
`snipers_flipped_count = NULL`, even though that signal has been live in
`gold_pump_token_risk` since PR #19. Reason: `NB_process_pump_token_risk.ipynb`
is deliberately incremental — it only recomputes mints with *new* activity
since the last run (that's the point of the design). These 30 tokens are
dead (no new trades/pools/transfers), so they were never re-touched after
the sniper-flip logic shipped; their gold row is frozen at whatever the
model looked like the last time they *did* have activity.

This means `risk_tier = 'low'` on an old, inactive token is not
necessarily what the *current* model would say — it may just be what an
*older version* of the model said, silently. Two of the tokens most
directly analyzed for this audit (`FFGJYmwHPiwdoavyAv8eB46vECUNSEXmZkbSmsqEsHfX`,
`tD3pH7KapyBb...`) would score meaningfully differently if recomputed
fresh — hand-computing `sniper_flip_ratio` gave 0.70 (33 snipers, escalates
to `high`) and 0.63 (19 snipers — just under `MIN_SNIPERS_FOR_FLIP_ESCALATION
= 20`, so weighted-only, pushing the score to `medium`) respectively,
versus their stale `low` on record.

**Proposed fix — process, not formula.** After any change to the scoring
logic, force a one-time full re-score of existing rows (as was done
manually for the PR #18 rollout) rather than relying solely on organic new
activity to eventually touch every mint. Longer-term: add a
`scoring_model_version` column and a scheduled maintenance pass that
re-scores rows behind the current version even with no new source data,
so `gold_pump_token_risk` never silently drifts from what the deployed
model would currently compute.

## Не предлагается

- **Price-drawdown-from-peak as a scoring signal.** Used here only as the
  audit's labeling heuristic to find candidates, for the same reason it
  was rejected when `sniper_flip_ratio` was designed: it's only observable
  *after* the crash has already happened, so it can't function as an
  early-warning signal — by the time it fires, the damage to whoever
  bought the peak is already done.
- **Cross-mint wallet-cluster / "serial rugger" detection.** ~25 of the 30
  sampled tokens have a near-identical statistical profile (trader counts
  in the 1,800–3,300 range, similar sniping ratios, similar suspiciously
  low recorded concentration) that looks like it could be the same bot
  operation running near-identical launches. Checked for the obvious
  version of this — shared `creator_wallet` across those 25 mints — and
  found none (25 distinct creator wallets, one mint each), consistent with
  disposable deployer wallets per launch. A real test would need
  cross-mint sniper/wallet-overlap analysis, which is a structurally
  different (graph-shaped, cross-token) signal than anything the current
  per-mint scoring model computes. Worth a future look, not proposed as a
  fix here.
