from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.bronze.pump_events_raw"

@dp.table(
    name="pump_tokens",
    comment="Normalized PumpAPI token creation events. One row per token creation event.",
)
@dp.expect("valid_mint", "mint IS NOT NULL")
@dp.expect("valid_signature", "creation_signature IS NOT NULL")
@dp.expect("valid_created_at", "created_at IS NOT NULL")
def pump_tokens():

    bronze = (
        spark.readStream
        .table(BRONZE_TABLE)
        .filter(
            F.get_json_object("event", "$.action") == "create"
        )
    )

    return (
        bronze
        .select(
            # Token identity
            F.get_json_object(
                "event", "$.mint"
            ).alias("mint"),

            # Creation event identity
            F.get_json_object(
                "event", "$.signature"
            ).alias("creation_signature"),

            F.get_json_object(
                "event", "$.block"
            ).cast("long").alias("block"),

            # Blockchain event timestamp
            F.to_timestamp(
                F.from_unixtime(
                    F.get_json_object(
                        "event", "$.timestamp"
                    ).cast("long") / F.lit(1000)
                )
            ).alias("created_at"),

            # Pool information
            F.get_json_object(
                "event", "$.poolId"
            ).alias("pool_id"),

            F.get_json_object(
                "event", "$.pool"
            ).alias("pool"),

            F.get_json_object(
                "event", "$.quoteMint"
            ).alias("quote_mint"),

            # Transaction signer / creator candidate
            F.get_json_object(
                "event", "$.txSigner"
            ).alias("creator_wallet"),

            # Initial token state
            F.get_json_object(
                "event", "$.tokenAmount"
            ).cast("double").alias("initial_token_amount"),

            F.get_json_object(
                "event", "$.quoteAmount"
            ).cast("double").alias("initial_quote_amount"),

            F.get_json_object(
                "event", "$.price"
            ).cast("double").alias("initial_price"),

            F.get_json_object(
                "event", "$.marketCapQuote"
            ).cast("double").alias("initial_market_cap_quote"),

            # Token authorities
            F.get_json_object(
                "event", "$.mintAuthority"
            ).alias("mint_authority"),

            F.get_json_object(
                "event", "$.freezeAuthority"
            ).alias("freeze_authority"),

            F.get_json_object(
                "event", "$.tokenProgram"
            ).alias("token_program"),

            # spl-token-2022 extensions -- kept as raw JSON text, no
            # allow-list of "safe" extensions exists yet (see
            # design/pumpfun/RISK_SCORING_DESIGN.md open questions)
            F.get_json_object(
                "event", "$.tokenExtensions"
            ).alias("token_extensions"),

            # Ingestion metadata
            F.col("_ingested_at"),

            F.col("_ingested_date"),

            "_source",
        )
    )
