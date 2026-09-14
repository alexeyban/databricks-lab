import asyncio
import base64
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import zstandard as zstd

from app.config import Config


logger = logging.getLogger(__name__)

COMPRESSION_CODEC = "zstd"


class PumpAPIWriter:
    """
    Buffer PumpAPI events and flush each batch as one compressed envelope file.

    Each flush serializes the buffered records as JSONL, compresses that
    with zstd, and base64-encodes the compressed bytes into a single JSON
    envelope: {"_source", "_ingested_at", "event_count", "codec",
    "uncompressed_bytes", "data_b64"}. The Databricks side (a Delta Live
    Tables pipeline, see ingestion/consumers/NB_dlt_pumpfun_bronze.ipynb)
    reverses this: base64-decode -> zstd-decompress -> split JSONL -> parse
    into bronze.pumpfun_events.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.buffer: list[dict] = []
        self.lock = asyncio.Lock()
        self._compressor = zstd.ZstdCompressor(level=config.zstd_level)

        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    async def write(self, event: dict) -> None:
        """
        Add one event to the buffer.

        The event is not written immediately. It is flushed
        when the batch size is reached or by the periodic flush.
        """

        record = {
            "_source": self.config.source_name,
            "_ingested_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "event": event,
        }

        async with self.lock:
            self.buffer.append(record)

            if len(self.buffer) >= self.config.batch_size:
                await self._flush_locked()

    async def flush_loop(self) -> None:
        """Periodically flush buffered events to disk."""

        while True:
            await asyncio.sleep(
                self.config.flush_interval_seconds
            )

            async with self.lock:
                if self.buffer:
                    await self._flush_locked()

    async def flush(self) -> None:
        """Flush all currently buffered events."""

        async with self.lock:
            if self.buffer:
                await self._flush_locked()

    def _build_envelope(self, records: list[dict]) -> dict:
        """Serialize records as JSONL, compress with zstd, and base64-encode."""

        jsonl_bytes = "\n".join(
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for record in records
        ).encode("utf-8")

        compressed = self._compressor.compress(jsonl_bytes)
        data_b64 = base64.b64encode(compressed).decode("ascii")

        return {
            "_source": self.config.source_name,
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
            "event_count": len(records),
            "codec": COMPRESSION_CODEC,
            "uncompressed_bytes": len(jsonl_bytes),
            "data_b64": data_b64,
        }

    async def _flush_locked(self) -> None:
        """
        Compress the current buffer into one envelope file.

        The lock must already be held when calling this method.
        """

        if not self.buffer:
            return

        records = self.buffer
        self.buffer = []

        now = datetime.now(timezone.utc)

        date_dir = self.output_dir / now.strftime("%Y-%m-%d")
        date_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        filename = (
            f"events-{now.strftime('%Y%m%d-%H%M%S-%f')}.json"
        )

        filepath = date_dir / filename
        temp_filepath = filepath.with_suffix(".tmp")

        try:
            envelope = self._build_envelope(records)

            with temp_filepath.open(
                "w",
                encoding="utf-8",
            ) as file:
                file.write(
                    json.dumps(
                        envelope,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
                file.write("\n")

            # Atomic rename: the final .json file only appears
            # after the complete envelope has been written.
            temp_filepath.rename(filepath)

            compression_ratio = (
                envelope["uncompressed_bytes"] / len(envelope["data_b64"])
                if envelope["data_b64"]
                else 0
            )
            logger.info(
                "Flushed %d events to %s (%d -> %d bytes b64, %.1fx, %s)",
                len(records),
                filepath,
                envelope["uncompressed_bytes"],
                len(envelope["data_b64"]),
                compression_ratio,
                COMPRESSION_CODEC,
            )

        except Exception:
            # Put records back into the buffer if writing failed.
            self.buffer = records + self.buffer

            logger.exception(
                "Failed to write %d events to %s",
                len(records),
                filepath,
            )

            if temp_filepath.exists():
                temp_filepath.unlink()

            raise
