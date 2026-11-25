import asyncio
import contextlib
import logging
import uuid

from faststream import FastStream
from faststream.exceptions import RejectMessage
from faststream.rabbit.annotations import RabbitMessage
from pydantic import BaseModel

from app.broker.topology import (
    ATTEMPT_HEADER,
    broker,
    declare_topology,
    new_queue,
    payments_exchange,
    retry_delay,
    retry_routing_key,
)
from app.clients import TransientError
from app.clients.gateway import GatewayClient
from app.clients.webhook import WebhookSender
from app.consumer.handler import PaymentProcessor
from app.consumer.relay import OutboxRelay
from app.core.config import get_settings
from app.db.session import session_factory

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

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
async def handle_payment(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
    """Номер попытки берётся из заголовка x-attempt.

    На TransientError копия сообщения уходит в очередь задержки, после последней
    попытки сообщение отклоняется в DLQ. Остальные исключения FastStream тоже
    отклоняет без requeue, такие сообщения сразу попадают в DLQ.
    """
    attempt = int(message.headers.get(ATTEMPT_HEADER, 1))
    try:
        await processor.process(event.payment_id)
    except TransientError as exc:
        if attempt < settings.max_attempts:
            logger.warning(
                "Payment %s attempt %d failed (%s), retry in %d s",
                event.payment_id, attempt, exc, retry_delay(attempt),
            )
            await broker.publish(
                event.model_dump(mode="json"),
                exchange=payments_exchange,
                routing_key=retry_routing_key(attempt),
                headers={ATTEMPT_HEADER: attempt + 1},
                message_id=message.message_id,
                persist=True,
            )
            return  # оригинал ack'аем, копия ждёт в очереди задержки
        logger.error("Payment %s failed after %d attempts (%s), moving to DLQ", event.payment_id, attempt, exc)
        raise RejectMessage() from exc


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
