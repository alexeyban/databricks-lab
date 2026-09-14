"""Rug-pull / scam risk score per pump.fun token, one row per mint.

Design: design/pumpfun/RISK_SCORING_DESIGN.md. Implemented as a
materialized view (plain `spark.read.table`, not `readStream`) inside the
same Lakeflow Declarative Pipeline as Bronze/Silver -- current-state
scoring is a better fit for a batch-recomputed materialized view than for
hand-rolled streaming stateful joins, and the engine still recomputes it
incrementally on every triggered pipeline update (same ~2-minute cadence
as ingestion).

Thresholds and weights below are first-pass values, not yet calibrated
against observed rugged tokens -- see RISK_SCORING_DESIGN.md "Open
questions". Revised once against a real confirmed rug (mint
12DqvhKnLFV9uiGiQYJeAkSLS7eF5FiE5UE61vA1pump: bot-sniped in the first
minute via ~3,362 separate single-trader buy transactions -- not a
same-tx bundle -- organic growth, then the creator sold ~1h46m after
creation, well outside the original 15-minute launch window) -- see the
sniping and creator-dump-ever signals below, added specifically because
the first-pass design missed that token entirely (scored low/20).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TOKENS_TABLE = "workspace.silver.pump_tokens"
POOLS_TABLE = "workspace.silver.pump_pools"
TRADES_TABLE = "workspace.silver.pump_trades"
TRANSFERS_TABLE = "workspace.silver.pump_transfers"

# How soon after `create` a buy/sell counts as "launch window" activity for
# the bundling and sniping signals. NOT used for creator-dump any more --
# see CREATOR_DUMP_RECENT_WINDOW_MINUTES and _creator_dump_ever_signal.
LAUNCH_WINDOW_MINUTES = 15

# How soon after `create` a burst of distinct buyers counts as sniping.
# Real rugged-token evidence: 3,362 distinct buyers total, essentially
# 1:1 trades-to-signatures (i.e. ~zero same-tx bundling) -- the bot
# signature here is *many separate single-trader transactions in rapid
# succession*, which _bundling_signal structurally cannot see (it only
# counts >1 distinct trader inside one transaction's breakdown[]).
SNIPE_WINDOW_SECONDS = 60

# Distinct buyers within SNIPE_WINDOW_SECONDS at/above this count is
# treated as maximum sniping risk; below it, risk scales linearly down to
# 0. Organic launches don't see dozens of distinct buyers in the first
# minute; bot-sniped ones (this real example included) do.
SNIPE_COUNT_FOR_MAX_RISK = 50

# Rolling window (relative to *now*, not to token creation) for detecting
# an actively-in-progress creator dump -- distinct from
# _creator_dump_ever_signal's unbounded historical check. A dump inside
# this window escalates risk_tier straight to "critical" (see below):
# it's the most actionable signal this model has, since it means the rug
# is happening right now, not that it happened at some point in the past.
CREATOR_DUMP_RECENT_WINDOW_MINUTES = 15

# top_holder_share at/above this floors risk_tier at "high" regardless of
# the weighted score -- ~100% concentration (this real example: 99.996%)
# is itself a critical signal that a linear-weighted contribution capped
# at WEIGHT_HOLDER_CONCENTRATION can't reliably surface on its own if
# every other signal happens to read 0 (exactly what happened here).
EXTREME_CONCENTRATION_THRESHOLD = 0.95

# How many top wallets to sum for the holder-concentration signal.
TOP_HOLDER_COUNT = 10

# burnedLiquidity% at/above this is treated as a fully safe pool (0 risk
# contribution); below it, risk scales linearly down to 0%. Note: pump.fun
# always burns 100% after migration by protocol design (per the glossary),
# so this signal reads "safe" for *every* pump.fun-native migration
# regardless of legitimacy -- it only actually discriminates for
# Meteora/custom pools. Not a bug, just non-informative for the common case.
BURNED_LIQUIDITY_SAFE_PCT = 25.0

# A pool `remove` event is treated as a drain (feeds the `rugged` funnel
# status) when reserves fall below this fraction of their observed peak.
RUG_DRAIN_RATIO = 0.05

# spl-token-2022 extensions that grant the issuer/delegate a way to move,
# block, tax, or freeze a holder's tokens without their consent, per
# https://solana.com/docs/tokens/extensions and
# https://blog.offside.io/p/token-2022-security-best-practices-part-2
# (PermanentDelegate is that article's top example: it can "directly
# transfer or burn any amount of mint from any token account" bypassing
# owner signatures). TransferFeeConfig/-Amount are flagged by presence, not
# fee magnitude -- PumpAPI's tokenExtensions field doesn't expose the fee
# rate, and a 0-max-fee TransferFeeConfig is technically harmless, but we
# can't tell the two apart from this field alone. Deliberately excluded as
# benign: MemoTransfer and InterestBearingConfig are operational/accounting
# footguns for integrators, not tools for extracting value from holders;
# MetadataPointer/TokenMetadata/GroupPointer/TokenGroup/etc. are safe as a
# property of the mint itself -- the known attack there is a *third party*
# creating spoofed Metadata/Group accounts that point at someone else's
# mint, which is a concern for whoever reads metadata content (must verify
# the pointer is bidirectional), not a property of this mint having the
# extension. See design/pumpfun/RISK_SCORING_DESIGN.md.
UNSAFE_TOKEN_EXTENSIONS = [
    "PermanentDelegate",
    "NonTransferable",
    "NonTransferableAccount",
    "Pausable",
    "PausableAccount",
    "DefaultAccountState",
    "TransferHook",
    "TransferHookAccount",
    "TransferFeeConfig",
    "TransferFeeAmount",
    "ConfidentialTransferMint",
    "ConfidentialTransferAccount",
    "ConfidentialTransferFeeConfig",
    "ConfidentialTransferFeeAmount",
    "ConfidentialMintBurn",
]
_UNSAFE_TOKEN_EXTENSION_PATTERN = r"\b(?:" + "|".join(UNSAFE_TOKEN_EXTENSIONS) + r")\b"

# Category B weights (max points each signal can contribute to risk_score).
WEIGHT_BURNED_LIQUIDITY = 35
WEIGHT_LOCKED_LIQUIDITY = 25
WEIGHT_POOL_TRUST = 15
WEIGHT_BUNDLING = 15
WEIGHT_SNIPING = 20
WEIGHT_CREATOR_DUMP_EVER = 30
WEIGHT_HOLDER_CONCENTRATION = 20
WEIGHT_POST_MIGRATION_FEE = 10


def _clamp01(col):
    return F.greatest(F.lit(0.0), F.least(col, F.lit(1.0)))


def _percent_string_to_double(col):
    """Parse a "NN%"-style string (burnedLiquidity's own glossary example)
    into a 0-100 double. NULL if no numeric prefix is found."""
    digits = F.regexp_extract(col, r"([0-9]+(\.[0-9]+)?)", 1)
    return F.when(digits != "", digits.cast("double"))


def _tokens_per_mint():
    """One row per mint: creation identity + the two hard-blocker fields
    that live on the `create` event (mint/freeze authority, extensions)."""
    window = Window.partitionBy("mint").orderBy(F.col("created_at").asc())
    return (
        spark.read.table(TOKENS_TABLE)
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "mint",
            "creator_wallet",
            "created_at",
            "mint_authority",
            "freeze_authority",
            "token_program",
            "token_extensions",
        )
    )


def _pool_state_per_mint():
    """Latest known pool state per mint, plus two lifecycle flags derived
    by looking across all of that mint's pool events: whether it ever
    migrated, and whether a `remove` event ever drained reserves close to
    their observed peak (candidate rug signal)."""
    pools = spark.read.table(POOLS_TABLE)
    mint_window = Window.partitionBy("mint")
    latest_window = Window.partitionBy("mint").orderBy(F.col("event_time").desc())

    enriched = (
        pools
        .withColumn("peak_quote_in_pool", F.max("quote_in_pool").over(mint_window))
        .withColumn(
            "has_migrate_event",
            F.max(F.when(F.col("action") == "migrate", 1).otherwise(0)).over(mint_window) == 1,
        )
        .withColumn(
            "is_drain_remove",
            (F.col("action") == "remove")
            & F.col("quote_in_pool").isNotNull()
            & (F.col("peak_quote_in_pool") > 0)
            & (F.col("quote_in_pool") / F.col("peak_quote_in_pool") < F.lit(RUG_DRAIN_RATIO)),
        )
        .withColumn(
            "has_large_remove_event",
            F.max(F.when(F.col("is_drain_remove"), 1).otherwise(0)).over(mint_window) == 1,
        )
    )

    return (
        enriched
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .withColumn(
            "burned_liquidity_pct",
            _percent_string_to_double(F.col("burned_liquidity")),
        )
        .withColumn(
            "locked_liquidity_after_migration_pct",
            _percent_string_to_double(F.col("locked_liquidity_after_migration")),
        )
        .select(
            "mint",
            "pool_created_by",
            "burned_liquidity_pct",
            "locked_liquidity_after_migration_pct",
            "pool_fee_rate_after_migration",
            F.coalesce(F.col("mayhem_mode"), F.lit(False)).alias("mayhem_mode_on"),
            F.col("has_migrate_event"),
            F.col("has_large_remove_event"),
        )
    )


def _launch_window_trades(tokens):
    """buy/sell trades for each mint within LAUNCH_WINDOW_MINUTES of its
    creation -- the shared input for the bundling and sniping signals."""
    trades = spark.read.table(TRADES_TABLE)
    launch = tokens.select("mint", "created_at")
    return (
        trades.alias("t")
        .join(launch.alias("k"), on="mint", how="inner")
        .filter(
            (F.col("t.event_time") >= F.col("k.created_at"))
            & (
                F.col("t.event_time")
                <= F.col("k.created_at") + F.expr(f"INTERVAL {LAUNCH_WINDOW_MINUTES} MINUTES")
            )
        )
    )


def _bundling_signal(tokens):
    """Share of launch-window buy transactions where breakdown[] shows more
    than one distinct trader in the same signature -- i.e. one transaction
    buying from several wallets at once (atomic bundle). Catches bundled
    sniping specifically; see _sniping_signal for the more common
    many-separate-transactions pattern this one structurally can't see."""
    launch_trades = _launch_window_trades(tokens)
    tx_trader_counts = (
        launch_trades
        .filter(F.col("t.trade_action") == "buy")
        .groupBy("mint", "signature")
        .agg(F.countDistinct("trader").alias("distinct_traders"))
    )
    return (
        tx_trader_counts
        .groupBy("mint")
        .agg(
            F.count("signature").alias("launch_buy_tx_count"),
            F.sum(F.when(F.col("distinct_traders") > 1, 1).otherwise(0)).alias("bundled_tx_count"),
        )
        .withColumn(
            "bundling_ratio",
            F.when(
                F.col("launch_buy_tx_count") > 0,
                F.col("bundled_tx_count") / F.col("launch_buy_tx_count"),
            ).otherwise(F.lit(0.0)),
        )
        .select("mint", "bundling_ratio")
    )


def _sniping_signal(tokens):
    """Distinct buy-traders within SNIPE_WINDOW_SECONDS of creation.
    A bot-rush doesn't need to be a same-transaction bundle -- confirmed
    against a real rugged token where ~3,362 distinct wallets bought via
    ~3,362 *separate* transactions (essentially 1 trader per signature,
    i.e. zero same-tx bundling) yet was obviously bot-driven by sheer
    velocity/volume in the first minute alone."""
    trades = spark.read.table(TRADES_TABLE)
    launch = tokens.select("mint", "created_at")
    snipe_window_trades = (
        trades.alias("t")
        .join(launch.alias("k"), on="mint", how="inner")
        .filter(
            (F.col("t.trade_action") == "buy")
            & (F.col("t.event_time") >= F.col("k.created_at"))
            & (
                F.col("t.event_time")
                <= F.col("k.created_at") + F.expr(f"INTERVAL {SNIPE_WINDOW_SECONDS} SECONDS")
            )
        )
    )
    return (
        snipe_window_trades
        .groupBy("mint")
        .agg(F.countDistinct("t.trader").alias("snipe_window_distinct_buyers"))
        .withColumn(
            "sniping_ratio",
            _clamp01(F.col("snipe_window_distinct_buyers") / F.lit(float(SNIPE_COUNT_FOR_MAX_RISK))),
        )
        .select("mint", "snipe_window_distinct_buyers", "sniping_ratio")
    )


def _creator_dump_ever_signal(tokens):
    """Whether the token's creator wallet ever shows up selling, anywhere
    in the observed trade history -- unbounded, not scoped to a launch
    window. The original design only checked LAUNCH_WINDOW_MINUTES after
    creation and missed a real rug where the creator sold ~1h46m after
    creation (well past initial organic growth) -- a "buy, wait for
    hype, dump" pattern needs an unbounded lookback to catch."""
    trades = spark.read.table(TRADES_TABLE)
    creators = tokens.select("mint", "creator_wallet")
    return (
        trades.alias("t")
        .join(creators.alias("k"), on="mint", how="inner")
        .filter(
            (F.col("t.trade_action") == "sell")
            & (F.col("t.trader") == F.col("k.creator_wallet"))
        )
        .select("mint")
        .distinct()
        .withColumn("creator_dumped_ever", F.lit(True))
    )


def _creator_dump_recent_signal(tokens):
    """Whether the creator wallet sold within the last
    CREATOR_DUMP_RECENT_WINDOW_MINUTES, relative to *now* (a rolling
    window on scoring time) rather than to token creation time. Flags an
    actively-in-progress dump for immediate tier escalation -- distinct
    from creator_dumped_ever's unbounded historical/reputational signal,
    which doesn't tell you whether the rug is happening right now."""
    trades = spark.read.table(TRADES_TABLE)
    creators = tokens.select("mint", "creator_wallet")
    cutoff = F.current_timestamp() - F.expr(f"INTERVAL {CREATOR_DUMP_RECENT_WINDOW_MINUTES} MINUTES")
    return (
        trades.alias("t")
        .join(creators.alias("k"), on="mint", how="inner")
        .filter(
            (F.col("t.trade_action") == "sell")
            & (F.col("t.trader") == F.col("k.creator_wallet"))
            & (F.col("t.event_time") >= cutoff)
        )
        .select("mint")
        .distinct()
        .withColumn("creator_dumped_recent", F.lit(True))
    )


def _trade_activity_flag():
    return (
        spark.read.table(TRADES_TABLE)
        .select("mint")
        .distinct()
        .withColumn("has_trade_activity", F.lit(True))
    )


def _holder_concentration_signal():
    """Share of net-positive token balance held by the top N wallets,
    reconstructed from silver_pump_transfers (in = to_wallet, out =
    from_wallet). Doesn't see initial mint allocation if it never moved as
    a transfer -- a known approximation, see RISK_SCORING_DESIGN.md."""
    transfers = spark.read.table(TRANSFERS_TABLE)

    inflow = transfers.select("mint", F.col("to_wallet").alias("wallet"), F.col("amount"))
    outflow = transfers.select(
        "mint", F.col("from_wallet").alias("wallet"), (-F.col("amount")).alias("amount")
    )

    net_balances = (
        inflow.unionByName(outflow)
        .groupBy("mint", "wallet")
        .agg(F.sum("amount").alias("net_amount"))
        .filter(F.col("net_amount") > 0)
    )

    rank_window = Window.partitionBy("mint").orderBy(F.col("net_amount").desc())
    ranked = net_balances.withColumn("_rank", F.row_number().over(rank_window))

    totals = net_balances.groupBy("mint").agg(F.sum("net_amount").alias("total_positive_balance"))
    top_holders = (
        ranked
        .filter(F.col("_rank") <= TOP_HOLDER_COUNT)
        .groupBy("mint")
        .agg(F.sum("net_amount").alias("top_holder_balance"))
    )

    return (
        totals
        .join(top_holders, on="mint", how="left")
        .withColumn(
            "top_holder_share",
            F.when(
                F.col("total_positive_balance") > 0,
                F.coalesce(F.col("top_holder_balance"), F.lit(0.0)) / F.col("total_positive_balance"),
            ),
        )
        .select("mint", "top_holder_share")
    )


@dp.table(
    name="workspace.gold.gold_pump_token_risk",
    comment="Rug-pull / scam risk score per pump.fun token, one row per mint. See design/pumpfun/RISK_SCORING_DESIGN.md.",
)
@dp.expect("valid_mint", "mint IS NOT NULL")
@dp.expect("valid_risk_score", "risk_score BETWEEN 0 AND 100")
def gold_pump_token_risk():
    tokens = _tokens_per_mint()
    pools = _pool_state_per_mint()
    bundling = _bundling_signal(tokens)
    sniping = _sniping_signal(tokens)
    creator_dump_ever = _creator_dump_ever_signal(tokens)
    creator_dump_recent = _creator_dump_recent_signal(tokens)
    trade_activity = _trade_activity_flag()
    holders = _holder_concentration_signal()

    joined = (
        tokens
        .join(pools, on="mint", how="left")
        .join(bundling, on="mint", how="left")
        .join(sniping, on="mint", how="left")
        .join(creator_dump_ever, on="mint", how="left")
        .join(creator_dump_recent, on="mint", how="left")
        .join(trade_activity, on="mint", how="left")
        .join(holders, on="mint", how="left")
        .withColumn("mayhem_mode_on", F.coalesce(F.col("mayhem_mode_on"), F.lit(False)))
        .withColumn("has_migrate_event", F.coalesce(F.col("has_migrate_event"), F.lit(False)))
        .withColumn("has_large_remove_event", F.coalesce(F.col("has_large_remove_event"), F.lit(False)))
        .withColumn("bundling_ratio", F.coalesce(F.col("bundling_ratio"), F.lit(0.0)))
        .withColumn("sniping_ratio", F.coalesce(F.col("sniping_ratio"), F.lit(0.0)))
        .withColumn("snipe_window_distinct_buyers", F.coalesce(F.col("snipe_window_distinct_buyers"), F.lit(0)))
        .withColumn("creator_dumped_ever", F.coalesce(F.col("creator_dumped_ever"), F.lit(False)))
        .withColumn("creator_dumped_recent", F.coalesce(F.col("creator_dumped_recent"), F.lit(False)))
        .withColumn("has_trade_activity", F.coalesce(F.col("has_trade_activity"), F.lit(False)))
    )

    # --- Category A: hard blockers (boolean, not weighted) ---
    # The glossary states these must always be a specific value; kept as
    # overrides rather than diluted into the weighted score.
    with_flags = (
        joined
        .withColumn("mint_authority_active", F.col("mint_authority").isNotNull())
        .withColumn("freeze_authority_active", F.col("freeze_authority").isNotNull())
        # Deny-list match against UNSAFE_TOKEN_EXTENSIONS -- a substring/
        # word-boundary check on the raw JSON rather than a parsed array,
        # since tokenExtensions' exact shape (bare strings vs. objects) is
        # unconfirmed; matching the extension name as a JSON token works
        # either way.
        .withColumn(
            "unsafe_token_extension",
            F.col("token_extensions").isNotNull()
            & F.col("token_extensions").rlike(_UNSAFE_TOKEN_EXTENSION_PATTERN),
        )
        .withColumn(
            "extreme_concentration",
            F.col("top_holder_share").isNotNull()
            & (F.col("top_holder_share") >= F.lit(EXTREME_CONCENTRATION_THRESHOLD)),
        )
    )

    # --- Category B: weighted score (0-100) ---
    pool_trust_risk = (
        F.when(F.col("pool_created_by") == "custom", F.lit(1.0))
        .when(
            (F.col("pool_created_by") == "meteora-launchpad")
            & (
                F.col("locked_liquidity_after_migration_pct").isNull()
                | (F.col("locked_liquidity_after_migration_pct") < 100)
            ),
            F.lit(1.0),
        )
        .otherwise(F.lit(0.0))
    )

    with_score = (
        with_flags
        .withColumn(
            "score_burned_liquidity",
            F.when(
                F.col("burned_liquidity_pct").isNotNull(),
                F.lit(WEIGHT_BURNED_LIQUIDITY)
                * _clamp01(F.lit(1.0) - F.col("burned_liquidity_pct") / F.lit(BURNED_LIQUIDITY_SAFE_PCT)),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "score_locked_liquidity",
            F.when(
                F.col("locked_liquidity_after_migration_pct").isNotNull(),
                F.lit(WEIGHT_LOCKED_LIQUIDITY)
                * _clamp01(F.lit(1.0) - F.col("locked_liquidity_after_migration_pct") / F.lit(100.0)),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("score_pool_trust", F.lit(WEIGHT_POOL_TRUST) * pool_trust_risk)
        .withColumn("score_bundling", F.lit(WEIGHT_BUNDLING) * _clamp01(F.col("bundling_ratio")))
        .withColumn("score_sniping", F.lit(WEIGHT_SNIPING) * _clamp01(F.col("sniping_ratio")))
        .withColumn(
            "score_creator_dump",
            F.when(F.col("creator_dumped_ever"), F.lit(float(WEIGHT_CREATOR_DUMP_EVER))).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "score_holder_concentration",
            F.when(
                F.col("top_holder_share").isNotNull(),
                F.lit(WEIGHT_HOLDER_CONCENTRATION) * _clamp01(F.col("top_holder_share")),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "score_post_migration_fee",
            F.when(
                F.col("pool_fee_rate_after_migration").isNotNull(),
                F.lit(WEIGHT_POST_MIGRATION_FEE) * _clamp01(F.col("pool_fee_rate_after_migration") / F.lit(0.1)),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "risk_score",
            F.least(
                F.round(
                    F.col("score_burned_liquidity")
                    + F.col("score_locked_liquidity")
                    + F.col("score_pool_trust")
                    + F.col("score_bundling")
                    + F.col("score_sniping")
                    + F.col("score_creator_dump")
                    + F.col("score_holder_concentration")
                    + F.col("score_post_migration_fee")
                ),
                F.lit(100.0),
            ).cast("int"),
        )
    )

    # --- Tier: score buckets, floored by escalators ---
    # any_hard_blocker/extreme_concentration floor the tier at "high";
    # creator_dumped_recent (an *active* dump, not a historical one)
    # floors it straight at "critical" -- the most urgent, actionable
    # signal this model has.
    with_tier = (
        with_score
        .withColumn(
            "score_tier",
            F.when(F.col("risk_score") >= 81, F.lit("critical"))
            .when(F.col("risk_score") >= 51, F.lit("high"))
            .when(F.col("risk_score") >= 21, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
        .withColumn(
            "any_hard_blocker",
            F.col("mint_authority_active")
            | F.col("freeze_authority_active")
            | F.col("unsafe_token_extension")
            | F.col("mayhem_mode_on"),
        )
        .withColumn(
            "risk_tier",
            F.when(F.col("creator_dumped_recent"), F.lit("critical"))
            .when(
                (F.col("any_hard_blocker") | F.col("extreme_concentration"))
                & F.col("score_tier").isin("low", "medium"),
                F.lit("high"),
            )
            .otherwise(F.col("score_tier")),
        )
    )

    # --- Category C: migration funnel status ---
    with_funnel = with_tier.withColumn(
        "funnel_status",
        F.when(
            F.col("has_large_remove_event")
            & (F.coalesce(F.col("burned_liquidity_pct"), F.lit(0.0)) < F.lit(BURNED_LIQUIDITY_SAFE_PCT)),
            F.lit("rugged"),
        )
        .when(F.col("has_migrate_event"), F.lit("migrated"))
        .when(F.col("has_trade_activity"), F.lit("active"))
        .otherwise(F.lit("created")),
    )

    return with_funnel.select(
        "mint",
        "creator_wallet",
        "created_at",

        # Category A
        "mint_authority_active",
        "freeze_authority_active",
        "unsafe_token_extension",
        "mayhem_mode_on",
        "extreme_concentration",
        "any_hard_blocker",

        # Category B contributing signals (kept for explainability)
        "burned_liquidity_pct",
        "locked_liquidity_after_migration_pct",
        "pool_created_by",
        "bundling_ratio",
        "snipe_window_distinct_buyers",
        "sniping_ratio",
        "creator_dumped_ever",
        "creator_dumped_recent",
        "top_holder_share",
        "pool_fee_rate_after_migration",

        # Score / tier / funnel
        "risk_score",
        "risk_tier",
        "funnel_status",

        F.current_timestamp().alias("scored_at"),
    )
