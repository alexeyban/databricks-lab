import base64

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

import zstandard as zstd

SOURCE_PATH = "/Volumes/workspace/default/mnt/pumpapi"

# Fixed schema for the batch envelopes written by
# ingestion/pumpfun/app/writer.py — no schema inference/evolution needed,
# this shape is controlled by us. Each envelope's `data_b64` is a whole
# batch: JSONL text, zstd-compressed, then base64-encoded.
_ENVELOPE_SCHEMA = StructType([
    StructField("_source", StringType(), True),
    StructField("_ingested_at", StringType(), True),
    StructField("event_count", LongType(), True),
    StructField("codec", StringType(), True),
    StructField("uncompressed_bytes", LongType(), True),
    StructField("data_b64", StringType(), True),
])

_SUPPORTED_CODECS = {"zstd"}


def _decode_envelope(data_b64: str, codec: str) -> str | None:
    """base64-decode + decompress one envelope's payload back into JSONL text."""
    if data_b64 is None or codec is None:
        return None

    if codec not in _SUPPORTED_CODECS:
        # Unsupported codec: surface as a null jsonl_text rather than failing
        # the pipeline -- these rows are easy to spot and backfill later.
        return None

    # Instantiated per call, not module-level: a ZstdDecompressor holds a C
    # extension context that Spark can't pickle when shipping this UDF's
    # closure to executors ("TypeError: cannot pickle 'zstd.ZstdDecompressor'
    # object"), which fails the whole flow at analysis time.
    decompressor = zstd.ZstdDecompressor()
    compressed = base64.b64decode(data_b64)
    return decompressor.decompress(compressed).decode("utf-8")


decode_envelope = F.udf(_decode_envelope, StringType())


@dp.table(
    name="workspace.bronze.pump_events_raw",
    comment="Raw PumpAPI events, decoded from the zstd+base64 batch envelopes landed by ingestion/pumpfun",
    partition_cols=["_ingested_date"],
)
def pump_events():
    envelopes = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(_ENVELOPE_SCHEMA)
        .load(SOURCE_PATH)
    )

    decoded = (
        envelopes
        .withColumn("jsonl_text", decode_envelope(F.col("data_b64"), F.col("codec")))
        .filter(F.col("jsonl_text").isNotNull())
    )

    lines = (
        decoded
        .withColumn("line", F.explode(F.split(F.col("jsonl_text"), "\n")))
        .filter(F.col("line") != "")
    )

    return (
        lines
        .withColumn("_source", F.get_json_object(F.col("line"), "$._source"))
        .withColumn(
            "_ingested_at",
            F.to_timestamp(F.get_json_object(F.col("line"), "$._ingested_at")),
        )
        .withColumn("_ingested_date", F.to_date(F.col("_ingested_at")))
        .withColumn("event", F.get_json_object(F.col("line"), "$.event"))
        .select("_source", "_ingested_at", "_ingested_date", "event")
    )
