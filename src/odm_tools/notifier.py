import asyncio
import os
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Literal

import aio_pika
import structlog
from aio_pika import Channel, ExchangeType, Message
from aio_pika.abc import AbstractRobustConnection

from odm_tools.config import settings
from odm_tools.models import StatusUpdate

log = structlog.get_logger()


class AsyncRabbitMQNotifier:
    def __init__(self):
        self.cfg = settings.rmq
        self.connection: AbstractRobustConnection | None = None
        self.channel: Channel | None = None
        self._connection_lock = asyncio.Lock()
        self._is_connected = False
        self._notify = os.getenv("SUPPRESS_NOTIFICATIONS", True)
        if not self._notify:
            log.warning("Suppressing status message notifications")

    async def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        async with self._connection_lock:
            if self._is_connected:
                return

            try:
                # Build connection URL
                if self.cfg.ssl:
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    protocol = "amqps"
                else:
                    ssl_context = None
                    protocol = "amqp"

                connection_url = (
                    f"{protocol}://{self.cfg.username}:{self.cfg.password}"
                    f"@{self.cfg.host}:{self.cfg.port}/{self.cfg.vhost}"
                )

                self.connection = await aio_pika.connect_robust(
                    connection_url,
                    ssl_context=ssl_context,
                    client_properties={
                        "connection_name": "odm-requests",
                        "application": "odm-tools",
                    },
                )
                self.channel = await self.connection.channel()  # type: ignore
                await self.channel.set_qos(prefetch_count=10)  # type: ignore
                self._is_connected = True
                log.info("Connected to RabbitMQ", host=self.cfg.host, vhost=self.cfg.vhost)
            except Exception as e:
                log.error("Failed to connect to RabbitMQ", error=str(e))
                self._is_connected = False
                raise

    async def disconnect(self) -> None:
        async with self._connection_lock:
            if not self._is_connected:
                return

            try:
                if self.channel and not self.channel.is_closed:
                    await self.channel.close()

                if self.connection and not self.connection.is_closed:
                    await self.connection.close()

                self._is_connected = False
                log.info("Disconnected from RabbitMQ")

            except Exception as e:
                log.warning("Error during RabbitMQ disconnect", error=str(e))
            finally:
                self.connection = None
                self.channel = None
                self._is_connected = False

    async def _ensure_connected(self) -> None:
        """Ensure we have a valid connection."""
        if not self._is_connected or not self.connection or self.connection.is_closed:
            await self.connect()

    async def publish_status_update(self, status_update: StatusUpdate) -> bool:
        """
        Publish a StatusUpdate message to RabbitMQ exchange.

        Returns True if successful, False otherwise.
        """
        routing_key = f"{self.cfg.routing_key_prefix}.{status_update.status}"

        for attempt in range(self.cfg.retry_count):
            try:
                await self._ensure_connected()

                if not self.channel:
                    raise RuntimeError("Channel not available")

                message_json = status_update.model_dump_json(by_alias=True)
                message = Message(
                    message_json.encode(),
                    message_id=f"status-{status_update.request_id}-{status_update.datatype_id}-{status_update.status}",
                    app_id="odm-processor",
                    headers={
                        "dbCollection": "requestStatus",
                        "request_id": status_update.request_id,
                        "datatype_id": str(status_update.datatype_id),
                        "status": status_update.status,
                    },
                    content_type="application/json",
                    timestamp=status_update.timestamp.timestamp(),
                )

                exchange = await self.channel.declare_exchange(
                    name=self.cfg.exchange,
                    type=ExchangeType.TOPIC,
                    durable=True,
                    passive=True,
                )
                if self._notify:
                    await exchange.publish(message, routing_key=routing_key)
                else:
                    log.debug("Mocking notification")

                log.debug(
                    "StatusUpdate published",
                    exchange=self.cfg.exchange,
                    routing_key=routing_key,
                    request_id=status_update.request_id,
                    datatype_id=status_update.datatype_id,
                    status=status_update.status,
                    attempt=attempt + 1,
                )
                return True

            except Exception as e:
                log.warning(
                    "Failed to publish StatusUpdate",
                    exchange=self.cfg.exchange,
                    routing_key=routing_key,
                    request_id=status_update.request_id,
                    attempt=attempt + 1,
                    error=str(e),
                )

                if attempt < self.cfg.retry_count - 1:
                    await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                    self._is_connected = False
                else:
                    log.error(
                        "Failed to publish StatusUpdate after all retries",
                        exchange=self.cfg.exchange,
                        routing_key=routing_key,
                        request_id=status_update.request_id,
                        error=str(e),
                    )
                    return False

        return False

    async def _send_task_update(
        self,
        request_id: str,
        datatype_ids: list[int],
        status: Literal["start", "end", "update", "error"],
        message: str,
    ) -> bool:
        updates = [
            StatusUpdate(
                request_id=request_id,
                datatype_id=dtype,
                status=status,
                timestamp=datetime.now(tz=UTC),
                message=message,
            )
            for dtype in datatype_ids
        ]
        return all(await asyncio.gather(*(self.publish_status_update(u) for u in updates)))

    async def send_task_start(
        self,
        request_id: str,
        datatype_ids: list[int],
        message: str = "ODM Task started",
    ) -> bool:
        return await self._send_task_update(
            request_id=request_id,
            datatype_ids=datatype_ids,
            status="start",
            message=message,
        )

    async def send_task_update(
        self,
        request_id: str,
        datatype_ids: list[int],
        message: str,
    ) -> bool:
        return await self._send_task_update(
            request_id=request_id,
            datatype_ids=datatype_ids,
            status="update",
            message=message,
        )

    async def send_task_end(
        self,
        request_id: str,
        datatype_ids: list[int],
        message: str = "ODM Task completed",
    ) -> bool:
        return await self._send_task_update(
            request_id=request_id,
            datatype_ids=datatype_ids,
            status="end",
            message=message,
        )

    async def send_task_error(
        self,
        request_id: str,
        datatype_ids: list[int],
        message: str,
    ) -> bool:
        return await self._send_task_update(
            request_id=request_id,
            datatype_ids=datatype_ids,
            status="error",
            message=message,
        )

    @asynccontextmanager
    async def connection_context(self):
        """Context manager for managing connection lifecycle."""
        try:
            await self.connect()
            yield self
        finally:
            await self.disconnect()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
