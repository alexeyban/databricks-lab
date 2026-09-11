import asyncio
import logging

from app.config import Config
from app.pumpapi import PumpAPIClient
from app.writer import PumpAPIWriter
from app.uploader import DatabricksUploader

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


async def main() -> None:
    """Application entry point."""

    config = Config.from_env()

    logger.info("Starting PumpAPI ingestor")
    logger.info("Source: %s", config.source_name)
    logger.info("WebSocket: %s", config.pumpapi_ws_url)

    writer = PumpAPIWriter(config)
    uploader = DatabricksUploader(config)

    pumpapi_client = PumpAPIClient(
        config=config,
        event_handler=writer.write,
    )

    flush_task = asyncio.create_task(writer.flush_loop())
    uploader_task = asyncio.create_task(uploader.run())

    try:
        await pumpapi_client.run()
    finally:
        flush_task.cancel()
        uploader_task.cancel()

        await asyncio.gather(
            flush_task,
            uploader_task,
            return_exceptions=True,
        )

        await writer.flush()
        pumpapi_client.stop()
        logger.info("PumpAPI ingestor stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped")
