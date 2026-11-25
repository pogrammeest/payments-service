import asyncio
import contextlib
import logging
import uuid

from faststream import FastStream
from pydantic import BaseModel

from app.broker.topology import broker, declare_topology, new_queue, payments_exchange
from app.clients.gateway import GatewayClient
from app.clients.webhook import WebhookSender
from app.consumer.handler import PaymentProcessor
from app.consumer.relay import OutboxRelay
from app.core.config import get_settings
from app.db.session import session_factory

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

settings = get_settings()
gateway = GatewayClient(settings.gateway_url, settings.gateway_timeout)
webhook = WebhookSender(settings.webhook_timeout)
processor = PaymentProcessor(session_factory, gateway, webhook)
relay = OutboxRelay(broker, session_factory)

app = FastStream(broker)
_relay_task: asyncio.Task | None = None


class PaymentCreatedEvent(BaseModel):
    payment_id: uuid.UUID


@broker.subscriber(new_queue, payments_exchange)
async def handle_payment(event: PaymentCreatedEvent) -> None:
    await processor.process(event.payment_id)


@app.after_startup
async def start_relay() -> None:
    global _relay_task
    await declare_topology(broker)
    _relay_task = asyncio.create_task(relay.run(), name="outbox-relay")


@app.on_shutdown
async def shutdown() -> None:
    if _relay_task is not None:
        _relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _relay_task
    await gateway.close()
    await webhook.close()
