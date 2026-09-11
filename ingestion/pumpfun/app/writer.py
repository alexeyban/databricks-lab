import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.config import Config


logger = logging.getLogger(__name__)


class PumpAPIWriter:
    """Buffer PumpAPI events and write them to JSONL files."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.buffer: list[dict] = []
        self.lock = asyncio.Lock()

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

    async def _flush_locked(self) -> None:
        """
        Write the current buffer to a new JSONL file.

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
            f"events-{now.strftime('%Y%m%d-%H%M%S-%f')}.jsonl"
        )

        filepath = date_dir / filename
        temp_filepath = filepath.with_suffix(".tmp")

        try:
            with temp_filepath.open(
                "w",
                encoding="utf-8",
            ) as file:
                for record in records:
                    file.write(
                        json.dumps(
                            record,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    )
                    file.write("\n")

            # Atomic rename: the final .jsonl file only appears
            # after the complete batch has been written.
            temp_filepath.rename(filepath)

            logger.info(
                "Flushed %d events to %s",
                len(records),
                filepath,
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

