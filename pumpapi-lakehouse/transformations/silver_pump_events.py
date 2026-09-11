from pyspark import pipelines as dp
from pyspark.sql import functions as F


BRONZE_TABLE = "workspace.bronze.pump_events_raw"

event_schema = """
    timestamp BIGINT,
    block BIGINT,
    signature STRING,
    action STRING,
    mint STRING,
    quoteMint STRING,
    poolId STRING,
    pool STRING,
    txSigner STRING,
    tokenAmount DOUBLE,
    quoteAmount DOUBLE,
    tokensInPool DOUBLE,
    quoteInPool DOUBLE,
    price DOUBLE,
    marketCapQuote DOUBLE,
    virtualTokenReserves DOUBLE,
    virtualSolReserves DOUBLE,
    realTokenReserves DOUBLE,
    realSolReserves DOUBLE,
    mintAuthority STRING,
    freezeAuthority STRING,
    tokenProgram STRING,
    priorityFee DOUBLE,
    isSolana BOOLEAN,
    tradersInvolved STRING,
    programsUsed STRING,
    breakdown STRING
"""

@dp.table(
    name="pump_events",
    comment="Silver layer for PumpAPI blockchain events",
)
@dp.expect(
    "valid_signature",
    "signature IS NOT NULL",
)
@dp.expect(
    "valid_action",
    "action IS NOT NULL",
)
def pump_events():
    source = (
        spark.readStream
        .table(BRONZE_TABLE)
    )

    parsed = (
        source
        .withColumn(
            "event_json",
            F.from_json(
                F.col("event"),
                event_schema,
            ),
        )
    )

    return parsed.select(
        F.to_timestamp(
            F.from_unixtime(
                F.col("event_json.timestamp") / 1000
            )
        ).alias("event_time"),

        F.col("_ingested_at"),

        F.col("event_json.block").alias("block"),
        F.col("event_json.signature").alias("signature"),
        F.col("event_json.action").alias("action"),
        F.col("event_json.mint").alias("mint"),
        F.col("event_json.quoteMint").alias("quote_mint"),
        F.col("event_json.poolId").alias("pool_id"),
        F.col("event_json.pool").alias("pool"),
        F.col("event_json.txSigner").alias("tx_signer"),

        F.col("event_json.tokenAmount").alias("token_amount"),
        F.col("event_json.quoteAmount").alias("quote_amount"),

        F.col("event_json.tokensInPool").alias("tokens_in_pool"),
        F.col("event_json.quoteInPool").alias("quote_in_pool"),

        F.col("event_json.price").alias("price"),
        F.col("event_json.marketCapQuote").alias("market_cap_quote"),

        F.col("event_json.mintAuthority").alias("mint_authority"),
        F.col("event_json.freezeAuthority").alias("freeze_authority"),
        F.col("event_json.tokenProgram").alias("token_program"),

        F.col("event_json.priorityFee").alias("priority_fee"),
        F.col("event_json.isSolana").alias("is_solana"),

        F.col("event_json.tradersInvolved").alias("traders_involved"),
        F.col("event_json.programsUsed").alias("programs_used"),
        F.col("event_json.breakdown").alias("breakdown"),

        F.col("event").alias("_raw_event"),
    )
