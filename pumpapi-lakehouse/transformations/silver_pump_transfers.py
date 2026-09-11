from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# Schema of one element inside PumpAPI "transfers"
transfer_schema = ArrayType(
    StructType([
        StructField("from", StringType(), True),
        StructField("to", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("isSolana", BooleanType(), True),
        StructField("mint", StringType(), True),
        StructField(
            "programsUsed",
            ArrayType(StringType()),
            True,
        ),
    ])
)

BRONZE_TABLE = "workspace.bronze.pump_events_raw"

@dp.table(
    name="pump_transfers",
    comment="Normalized PumpAPI transfer events. One row per transfer.",
)
@dp.expect("valid_signature", "signature IS NOT NULL")
@dp.expect("valid_from_wallet", "from_wallet IS NOT NULL")
@dp.expect("valid_to_wallet", "to_wallet IS NOT NULL")
def pump_transfers():

    # Read only transfer events from Bronze.
    bronze = (
        spark.readStream
        .table(BRONZE_TABLE)
        .filter(
            F.get_json_object("event", "$.action") == "transfer"
        )
    )

    # Parse transaction-level fields and transfers array.
    parsed = (
        bronze
        .withColumn(
            "signature",
            F.get_json_object("event", "$.signature"),
        )
        .withColumn(
            "action",
            F.get_json_object("event", "$.action"),
        )
        .withColumn(
            "tx_signer",
            F.get_json_object("event", "$.txSigner"),
        )
        .withColumn(
            "block",
            F.get_json_object("event", "$.block").cast("long"),
        )
        .withColumn(
            "event_timestamp_ms",
            F.get_json_object("event", "$.timestamp").cast("long"),
        )
        .withColumn(
            "priority_fee",
            F.get_json_object("event", "$.priorityFee").cast("double"),
        )
        .withColumn(
            "transfers",
            F.from_json(
                F.get_json_object("event", "$.transfers"),
                transfer_schema,
            ),
        )
    )

    # One output row per item in transfers[].
    exploded = (
        parsed
        .select(
            "signature",
            "action",
            "tx_signer",
            "block",
            "event_timestamp_ms",
            "priority_fee",
            "_ingested_at",
            "_source",
            "_ingested_date",
            F.posexplode("transfers").alias(
                "transfer_index",
                "transfer",
            ),
        )
    )

    return (
        exploded
        .select(
            # Transaction identity
            "signature",

            # Position of this transfer inside the transaction.
            # Together with signature this forms a natural technical key.
            "transfer_index",

            # Blockchain event time.
            F.to_timestamp(
                F.from_unixtime(
                    F.col("event_timestamp_ms") / F.lit(1000)
                )
            ).alias("event_time"),

            "block",

            # Transfer details
            F.col("transfer.from").alias("from_wallet"),
            F.col("transfer.to").alias("to_wallet"),
            F.col("transfer.amount").alias("amount"),
            F.col("transfer.isSolana").alias("is_solana"),
            F.col("transfer.mint").alias("mint"),
            F.col("transfer.programsUsed").alias("programs_used"),

            # Transaction-level metadata
            "tx_signer",
            "priority_fee",

            # Ingestion metadata
            F.col("_ingested_at"),
            F.col("_ingested_date"),
            "_source",
        )
    )
