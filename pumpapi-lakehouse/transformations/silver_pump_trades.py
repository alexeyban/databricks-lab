from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# Schema of one element inside PumpAPI "breakdown" -- the same buy/sell
# transaction split into its individual, non-aggregated trades. Multiple
# distinct traders under one signature/block is the bundling/sniping signal
# consumed by the Gold risk-scoring layer
# (design/pumpfun/RISK_SCORING_DESIGN.md).
breakdown_schema = ArrayType(
    StructType([
        StructField("action", StringType(), True),
        StructField("trader", StringType(), True),
        StructField("tokenAmount", DoubleType(), True),
        StructField("quoteAmount", DoubleType(), True),
    ])
)

BRONZE_TABLE = "workspace.bronze.pump_events_raw"

@dp.table(
    name="pump_trades",
    comment="Normalized PumpAPI buy/sell events. One row per individual trade inside a transaction's breakdown.",
)
@dp.expect("valid_signature", "signature IS NOT NULL")
@dp.expect("valid_mint", "mint IS NOT NULL")
@dp.expect("valid_trader", "trader IS NOT NULL")
def pump_trades():

    # Read only buy/sell events from Bronze.
    bronze = (
        spark.readStream
        .table(BRONZE_TABLE)
        .filter(
            F.get_json_object("event", "$.action").isin("buy", "sell")
        )
    )

    # Parse transaction-level fields and the breakdown array.
    parsed = (
        bronze
        .withColumn("signature", F.get_json_object("event", "$.signature"))
        .withColumn("action", F.get_json_object("event", "$.action"))
        .withColumn("block", F.get_json_object("event", "$.block").cast("long"))
        .withColumn(
            "event_timestamp_ms",
            F.get_json_object("event", "$.timestamp").cast("long"),
        )
        .withColumn("mint", F.get_json_object("event", "$.mint"))
        .withColumn("quote_mint", F.get_json_object("event", "$.quoteMint"))
        .withColumn("pool_id", F.get_json_object("event", "$.poolId"))
        .withColumn("pool", F.get_json_object("event", "$.pool"))
        .withColumn("tx_signer", F.get_json_object("event", "$.txSigner"))
        .withColumn(
            "total_token_amount",
            F.get_json_object("event", "$.tokenAmount").cast("double"),
        )
        .withColumn(
            "total_quote_amount",
            F.get_json_object("event", "$.quoteAmount").cast("double"),
        )
        .withColumn(
            "tokens_in_pool",
            F.get_json_object("event", "$.tokensInPool").cast("double"),
        )
        .withColumn(
            "quote_in_pool",
            F.get_json_object("event", "$.quoteInPool").cast("double"),
        )
        .withColumn("price", F.get_json_object("event", "$.price").cast("double"))
        .withColumn(
            "market_cap_quote",
            F.get_json_object("event", "$.marketCapQuote").cast("double"),
        )
        .withColumn(
            "priority_fee",
            F.get_json_object("event", "$.priorityFee").cast("double"),
        )
        # Kept as raw JSON text -- shape depends on the pool/aggregator and
        # is consumed directly by Gold (holder concentration / copy-trading
        # signals), same treatment as silver_pump_events.py.
        .withColumn(
            "traders_involved",
            F.get_json_object("event", "$.tradersInvolved"),
        )
        .withColumn(
            "post_balances",
            F.get_json_object("event", "$.postBalances"),
        )
        .withColumn(
            "breakdown",
            F.from_json(
                F.get_json_object("event", "$.breakdown"),
                breakdown_schema,
            ),
        )
    )

    # One output row per individual trade in breakdown[] -- this is what
    # lets the Gold layer count distinct traders per signature/block to
    # detect bundled/sniped buys right after token launch.
    exploded = (
        parsed
        .select(
            "signature",
            "action",
            "block",
            "event_timestamp_ms",
            "mint",
            "quote_mint",
            "pool_id",
            "pool",
            "tx_signer",
            "total_token_amount",
            "total_quote_amount",
            "tokens_in_pool",
            "quote_in_pool",
            "price",
            "market_cap_quote",
            "priority_fee",
            "traders_involved",
            "post_balances",
            "_ingested_at",
            "_ingested_date",
            "_source",
            F.posexplode("breakdown").alias("breakdown_index", "trade"),
        )
    )

    return (
        exploded
        .select(
            # Transaction identity
            "signature",

            # Position of this trade inside the transaction's breakdown.
            # Together with signature this forms a natural technical key.
            "breakdown_index",

            # Blockchain event time.
            F.to_timestamp(
                F.from_unixtime(F.col("event_timestamp_ms") / F.lit(1000))
            ).alias("event_time"),

            "block",
            "mint",
            "quote_mint",
            "pool_id",
            "pool",

            # Per-trade breakdown fields (individual trader, not the
            # transaction-level total).
            F.col("trade.action").alias("trade_action"),
            F.col("trade.trader").alias("trader"),
            F.col("trade.tokenAmount").alias("token_amount"),
            F.col("trade.quoteAmount").alias("quote_amount"),

            # Transaction-level context
            "tx_signer",
            "total_token_amount",
            "total_quote_amount",
            "tokens_in_pool",
            "quote_in_pool",
            "price",
            "market_cap_quote",
            "priority_fee",
            "traders_involved",
            "post_balances",

            # Ingestion metadata
            F.col("_ingested_at"),
            F.col("_ingested_date"),
            "_source",
        )
    )
