import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed

from app.config import Config


logger = logging.getLogger(__name__)

EventHandler = Callable[[dict], Awaitable[None]]


class PumpAPIClient:
    """
    Single WebSocket client for the PumpAPI data stream.

    The client maintains one WebSocket connection at a time.
    If the connection is lost, it automatically reconnects.
    """

    def __init__(
        self,
        config: Config,
        event_handler: EventHandler,
    ) -> None:
        self.config = config
        self.event_handler = event_handler
        self._running = True

    async def run(self) -> None:
        """
        Keep the PumpAPI stream running.

        Only one WebSocket connection is active at any time.
        Reconnect automatically after a connection failure.
        """

        reconnect_delay = self.config.reconnect_delay_seconds

        while self._running:
            try:
                await self._connect_and_consume()

                # If _connect_and_consume() returns normally,
                # reset the reconnect delay.
                reconnect_delay = (
                    self.config.reconnect_delay_seconds
                )

            except ConnectionClosed as exc:
                logger.warning(
                    "PumpAPI WebSocket connection closed: %s",
                    exc,
                )

            except asyncio.CancelledError:
                logger.info("PumpAPI client cancelled")
                raise

            except Exception:
                logger.exception(
                    "Unexpected PumpAPI WebSocket error"
                )

            if not self._running:
                break

            logger.info(
                "Reconnecting in %s seconds...",
                reconnect_delay,
            )

            await asyncio.sleep(reconnect_delay)

            # Exponential backoff.
            reconnect_delay = min(
                reconnect_delay * 2,
                60,
            )

    async def _connect_and_consume(self) -> None:
        """
        Open one WebSocket connection and consume events.

        The connection remains open for the lifetime of this method.
        """

        logger.info(
            "Connecting to PumpAPI: %s",
            self.config.pumpapi_ws_url,
        )

        async with websockets.connect(
            self.config.pumpapi_ws_url,
            ping_interval=self.config.ping_interval_seconds,
            ping_timeout=self.config.ping_timeout_seconds,
        ) as websocket:

            logger.info("PumpAPI WebSocket connected")

            # This is the single event consumption loop.
            async for message in websocket:
                await self._process_message(message)

        logger.info("PumpAPI WebSocket disconnected")

    async def _process_message(
        self,
        message: str | bytes,
    ) -> None:
        """Parse one PumpAPI message and pass it to the writer."""

        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")

            event = json.loads(message)

        except UnicodeDecodeError:
            logger.exception(
                "Failed to decode PumpAPI message"
            )
            return

        except json.JSONDecodeError:
            logger.exception(
                "Failed to parse PumpAPI JSON message"
            )
            return

        if not isinstance(event, dict):
            logger.warning(
                "Ignoring non-object PumpAPI message: %r",
                event,
            )
            return

        await self.event_handler(event)

    def stop(self) -> None:
        """Request graceful shutdown."""

        logger.info("Stopping PumpAPI client")
        self._running = False
