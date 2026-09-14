import asyncio
import json
import logging
import shutil
from pathlib import Path

import aiohttp

from app.config import Config


logger = logging.getLogger(__name__)


class DatabricksUploader:
    """Upload completed PumpAPI JSONL files to a Databricks UC Volume."""

    MAX_WORKERS = 8

    def __init__(self, config: Config) -> None:
        self.config = config
        self.output_dir = Path(config.output_dir)
        self.uploaded_dir_name = "uploaded"

    def find_pending_files(self) -> list[Path]:
        """Return JSONL files that have not been uploaded yet."""
        return sorted(
            filepath
            for filepath in self.output_dir.rglob("*.json")
            if self.uploaded_dir_name not in filepath.parts
        )

    def _remote_path(self, filepath: Path) -> str:
        """Build the corresponding Databricks Volume path."""
        relative_path = filepath.relative_to(self.output_dir)

        return (
            f"{self.config.databricks_volume_path}/"
            f"{relative_path.as_posix()}"
        )

    def _mark_uploaded(self, filepath: Path) -> None:
        """Move a successfully uploaded file to the uploaded directory."""
        relative_path = filepath.relative_to(self.output_dir)
        uploaded_path = (
            self.output_dir
            / relative_path.parent
            / self.uploaded_dir_name
            / relative_path.name
        )

        uploaded_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.move(str(filepath), str(uploaded_path))

        logger.info(
            "Marked uploaded: %s -> %s",
            filepath,
            uploaded_path,
        )

    async def upload_file(
        self,
        session: aiohttp.ClientSession,
        filepath: Path,
    ) -> bool:
        """Upload one file to Databricks."""
        remote_path = self._remote_path(filepath)

        url = (
            f"{self.config.databricks_host}"
            f"/api/2.0/fs/files{remote_path}"
        )

        try:
            data = await asyncio.to_thread(filepath.read_bytes)

            headers = {
                "Authorization": (
                    f"Bearer {self.config.databricks_token}"
                ),
                "Content-Type": "application/octet-stream",
            }

            async with session.put(
                url,
                headers=headers,
                data=data,
            ) as response:

                if response.status in (200, 201, 204):
                    logger.info(
                        "Uploaded %s (%d bytes), HTTP %s",
                        filepath,
                        len(data),
                        response.status,
                    )
                    return True

                if response.status == 409:
                    try:
                        error_data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError):
                        error_data = {}

                    if (
                        error_data.get("error_code")
                        == "ALREADY_EXISTS"
                    ):
                        logger.info(
                            "File already exists in Databricks: %s",
                            remote_path,
                        )
                        return True

                error_text = await response.text()

                logger.error(
                    "Upload failed for %s: HTTP %s: %s",
                    filepath,
                    response.status,
                    error_text,
                )

                return False

        except (aiohttp.ClientError, OSError) as exc:
            logger.error(
                "Upload failed for %s: %s",
                filepath,
                exc,
            )
            return False

    async def _worker(
        self,
        worker_id: int,
        queue: asyncio.Queue[Path],
        session: aiohttp.ClientSession,
    ) -> None:
        """Process pending files from the queue."""
        logger.info("Uploader worker %d started", worker_id)

        while True:
            filepath = await queue.get()

            try:
                success = await self.upload_file(
                    session=session,
                    filepath=filepath,
                )

                if success:
                    self._mark_uploaded(filepath)

            except Exception:
                logger.exception(
                    "Unexpected error processing %s",
                    filepath,
                )

            finally:
                queue.task_done()

    async def run_once(self) -> None:
        """Upload all currently pending files using a worker pool."""
        files = self.find_pending_files()

        if not files:
            return

        logger.info(
            "Found %d pending file(s)",
            len(files),
        )

        queue: asyncio.Queue[Path] = asyncio.Queue()

        for filepath in files:
            await queue.put(filepath)

        timeout = aiohttp.ClientTimeout(
            total=120,
            connect=30,
        )

        async with aiohttp.ClientSession(
            timeout=timeout,
        ) as session:

            workers = [
                asyncio.create_task(
                    self._worker(
                        worker_id=i + 1,
                        queue=queue,
                        session=session,
                    )
                )
                for i in range(self.MAX_WORKERS)
            ]

            await queue.join()

            for worker in workers:
                worker.cancel()

            await asyncio.gather(
                *workers,
                return_exceptions=True,
            )

        remaining = len(self.find_pending_files())

        logger.info(
            "Upload cycle completed: pending files=%d",
            remaining,
        )

    async def run(self) -> None:
        """Continuously upload pending files."""
        while True:
            try:
                await self.run_once()

            except Exception:
                logger.exception(
                    "Unexpected uploader error",
                )

            await asyncio.sleep(
                self.config.upload_interval_seconds
            )