import base64
import gc

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

import zstandard as zstd

SOURCE_PATH = "/Volumes/workspace/default/mnt/pumpapi"

# Bounds how many envelope files Auto Loader reads per micro-batch. Each
# file can hold up to 5000 events (producer default). Kept as a
# complementary safeguard alongside the mapInPandas rewrite below -- on
# its own (tried at 100, then 20) it wasn't sufficient, see that
# function's docstring for the actual OOM root cause and fix.
MAX_FILES_PER_TRIGGER = 20

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

# One row per decompressed JSONL line, still as raw unparsed text -- the
# same shape "line" used to have after F.explode(F.split(jsonl_text, "\n")).
# All downstream field extraction (_source/_ingested_at/event) stays on
# this raw text via get_json_object, unchanged from before this rewrite.
_LINES_SCHEMA = StructType([
    StructField("line", StringType(), True),
])


def _decode_batch(pdf_iter):
    """mapInPandas worker: decompresses envelopes and emits one row per
    JSONL line, batch by batch (batch size bounded by the pipeline's
    spark.sql.execution.arrow.maxRecordsPerBatch config), instead of a
    scalar UDF that returned one big decompressed-text column per
    envelope row for Spark to then split()+explode() downstream.

    Root cause this works around: three different batch-size knobs tried
    in isolation (Auto Loader maxFilesPerTrigger at 100 then 20, then
    Arrow maxRecordsPerBatch=5 on top) all OOM'd at ~the same wall-clock
    mark regardless of size -- "Executor got terminated abnormally due to
    OUT_OF_MEMORY" / "[UDF_PYSPARK_ERROR.OOM] Python worker exited
    unexpectedly". That pattern points at the *reused* Python worker
    process's memory never being returned to the OS across many
    decompress calls (a known CPython/zstd-C-extension allocator
    behavior), not any single batch being too large. serverless
    pipelines reject spark.python.worker.reuse=false (which would force
    a fresh worker per task and sidestep this directly: "not allowed for
    serverless pipelines"), so this explicitly drops references to the
    decompressed buffers and calls gc.collect() after each batch as the
    next-best mitigation.
    """
    import pandas as pd

    for pdf in pdf_iter:
        lines = []

        for data_b64, codec in zip(pdf["data_b64"], pdf["codec"]):
            if data_b64 is None or codec is None or codec not in _SUPPORTED_CODECS:
                # Unsupported/missing codec: skip rather than failing the
                # pipeline -- these rows are easy to spot and backfill later.
                continue

            decompressor = zstd.ZstdDecompressor()
            compressed = base64.b64decode(data_b64)
            text = decompressor.decompress(compressed).decode("utf-8")
            del compressed, decompressor

            lines.extend(line for line in text.split("\n") if line)
            del text

        yield pd.DataFrame({"line": lines})

        del lines, pdf
        gc.collect()


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
        .option("cloudFiles.maxFilesPerTrigger", str(MAX_FILES_PER_TRIGGER))
        .schema(_ENVELOPE_SCHEMA)
        .load(SOURCE_PATH)
    )

    lines = (
        envelopes
        .select("data_b64", "codec")
        .mapInPandas(_decode_batch, schema=_LINES_SCHEMA)
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
