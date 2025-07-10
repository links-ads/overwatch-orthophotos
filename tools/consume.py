import argparse
import asyncio
import json
import signal
import ssl
import sys
from datetime import datetime

import aio_pika
import structlog
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractRobustConnection

from odm_tools.config import settings

log = structlog.get_logger()


class AsyncRabbitMQConsumer:
    def __init__(self, queue_name: str):
        self.cfg = settings.rmq
        self.queue_name = queue_name
        self.connection: AbstractRobustConnection | None = None
        self.channel = None
        self.queue = None
        self._shutdown = False

    async def connect(self) -> None:
        """Establish connection to RabbitMQ."""
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
                    "connection_name": "odm-consumer",
                    "application": "odm-tools-consumer",
                },
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=10)

            # declare queue (passive to avoid creating wrong things)
            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                passive=True,
            )

            log.info("Connected to RabbitMQ", host=self.cfg.host, vhost=self.cfg.vhost, queue=self.queue_name)
        except Exception as e:
            log.error("Failed to connect to RabbitMQ", error=str(e))
            raise

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ."""
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()

            if self.connection and not self.connection.is_closed:
                await self.connection.close()

            log.info("Disconnected from RabbitMQ")
        except Exception as e:
            log.warning("Error during RabbitMQ disconnect", error=str(e))

    async def process_message(self, message: IncomingMessage) -> None:
        """Process incoming message."""
        try:
            timestamp = datetime.now().isoformat()

            # Try to parse as JSON for pretty printing
            try:
                body = message.body.decode("utf-8")
                parsed_body = json.loads(body)
                body_str = json.dumps(parsed_body, indent=2)
            except (json.JSONDecodeError, UnicodeDecodeError):
                body_str = str(message.body)

            print(f"\n[{timestamp}] Received message:")
            print(f"  Queue: {self.queue_name}")
            print(f"  Message ID: {message.message_id}")
            print(f"  Routing Key: {message.routing_key}")
            print(f"  Headers: {dict(message.headers) if message.headers else 'None'}")
            print(f"  Content Type: {message.content_type}")
            print("  Body:")
            print(f"    {body_str}")
            print("-" * 50)

            # Auto-acknowledge the message
            message.ack()

        except Exception as e:
            log.error("Error processing message", error=str(e))
            message.nack(requeue=False)

    async def start_consuming(self) -> None:
        """Start consuming messages."""
        await self.connect()

        log.info("Starting consumer", queue=self.queue_name)
        print(f"Listening for messages on queue '{self.queue_name}'...")
        print("Press Ctrl+C to exit")

        # Start consuming
        await self.queue.consume(self.process_message, no_ack=False)

        # Keep running until shutdown
        while not self._shutdown:
            await asyncio.sleep(1)

    def shutdown(self) -> None:
        """Signal shutdown."""
        self._shutdown = True
        log.info("Shutdown requested")


async def main():
    parser = argparse.ArgumentParser(description="RabbitMQ Queue Consumer")
    parser.add_argument("queue", help="Queue name to consume from")
    args = parser.parse_args()

    consumer = AsyncRabbitMQConsumer(args.queue)

    # Setup graceful shutdown
    def signal_handler():
        print("\nShutdown signal received...")
        consumer.shutdown()

    # Register signal handlers
    if sys.platform != "win32":
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)
    else:
        signal.signal(signal.SIGINT, lambda s, f: signal_handler())

    try:
        await consumer.start_consuming()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received")
    except Exception as e:
        log.error("Consumer error", error=str(e))
    finally:
        await consumer.disconnect()
        print("Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
