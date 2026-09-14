from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.bronze.pump_events_raw"

@dp.table(
    name="pump_pools",
    comment="Normalized PumpAPI pool lifecycle events (createPool/migrate/add/remove). Liquidity and pool-trust fields used by the Gold risk-scoring layer.",
)
@dp.expect("valid_signature", "signature IS NOT NULL")
@dp.expect("valid_action", "action IS NOT NULL")
@dp.expect("valid_pool_id", "pool_id IS NOT NULL")
def pump_pools():

    # Read only pool lifecycle events from Bronze.
    bronze = (
        spark.readStream
        .table(BRONZE_TABLE)
        .filter(
            F.get_json_object("event", "$.action").isin(
                "createPool", "migrate", "add", "remove"
            )
        )
    )

    return (
        bronze
        .select(
            # Event identity
            F.get_json_object("event", "$.signature").alias("signature"),
            F.get_json_object("event", "$.action").alias("action"),
            F.get_json_object("event", "$.block").cast("long").alias("block"),

            F.to_timestamp(
                F.from_unixtime(
                    F.get_json_object("event", "$.timestamp").cast("long") / F.lit(1000)
                )
            ).alias("event_time"),

            # Pool / token identity
            F.get_json_object("event", "$.mint").alias("mint"),
            F.get_json_object("event", "$.quoteMint").alias("quote_mint"),
            F.get_json_object("event", "$.poolId").alias("pool_id"),
            F.get_json_object("event", "$.pool").alias("pool"),
            F.get_json_object("event", "$.txSigner").alias("tx_signer"),

            # Pool reserves / liquidity state
            F.get_json_object("event", "$.tokensInPool").cast("double").alias("tokens_in_pool"),
            F.get_json_object("event", "$.quoteInPool").cast("double").alias("quote_in_pool"),
            F.get_json_object("event", "$.vTokensInBondingCurve").cast("double").alias("v_tokens_in_bonding_curve"),
            F.get_json_object("event", "$.vQuoteInBondingCurve").cast("double").alias("v_quote_in_bonding_curve"),
            F.get_json_object("event", "$.virtualQuoteInPool").cast("double").alias("virtual_quote_in_pool"),
            F.get_json_object("event", "$.virtualTokensInPool").cast("double").alias("virtual_tokens_in_pool"),

            F.get_json_object("event", "$.price").cast("double").alias("price"),
            F.get_json_object("event", "$.marketCapQuote").cast("double").alias("market_cap_quote"),
            F.get_json_object("event", "$.minPrice").cast("double").alias("min_price"),
            F.get_json_object("event", "$.maxPrice").cast("double").alias("max_price"),
            F.get_json_object("event", "$.curveType").alias("curve_type"),
            F.get_json_object("event", "$.binStep").cast("long").alias("bin_step"),
            F.get_json_object("event", "$.poolFeeRate").cast("double").alias("pool_fee_rate"),

            # Rug-pull / pool-trust risk signals (see
            # design/pumpfun/RISK_SCORING_DESIGN.md). burnedLiquidity and
            # lockedLiquidityAfterMigration are kept as raw strings (the
            # glossary's own example is "99%", not a bare number) -- parsing
            # them into a comparable double is a Gold-layer concern.
            F.get_json_object("event", "$.poolCreatedBy").alias("pool_created_by"),
            F.get_json_object("event", "$.lockedLiquidityAfterMigration").alias("locked_liquidity_after_migration"),
            F.get_json_object("event", "$.poolFeeRateAfterMigration").cast("double").alias("pool_fee_rate_after_migration"),
            F.get_json_object("event", "$.migrationThresholds").alias("migration_thresholds"),
            F.get_json_object("event", "$.burnedLiquidity").alias("burned_liquidity"),
            F.get_json_object("event", "$.mayhemMode").cast("boolean").alias("mayhem_mode"),
            F.get_json_object("event", "$.launchpadConfig").alias("launchpad_config"),
            F.get_json_object("event", "$.cashbackEnabled").cast("boolean").alias("cashback_enabled"),

            # Ingestion metadata
            F.col("_ingested_at"),
            F.col("_ingested_date"),
            "_source",
        )
    )
