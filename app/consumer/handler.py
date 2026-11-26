import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.gateway import GatewayClient
from app.clients.webhook import WebhookSender
from app.models import Payment, PaymentStatus

logger = logging.getLogger(__name__)


def webhook_payload(payment: Payment) -> dict[str, Any]:
    return {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "metadata": payment.meta,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class PaymentProcessor:
    """Обработка события payment.created.

    Для одного платежа можно вызывать сколько угодно раз: деньги второй раз
    не спишутся, повторно уйдёт только webhook.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: GatewayClient,
        webhook: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session, session.begin():
            # FOR UPDATE: дубль того же события ждёт здесь и не идёт в шлюз параллельно.
            # Лок держим и во время запроса к шлюзу. Шлюз к тому же идемпотентен по payment_id.
            payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
            if payment is None:
                logger.warning("Payment %s not found, dropping message", payment_id)
                return

            if payment.status is PaymentStatus.PENDING:
                payment.status = await self._gateway.charge(payment)
                payment.processed_at = datetime.now(UTC)
                logger.info("Payment %s processed: %s", payment.id, payment.status.value)
            else:
                # статус уже сохранён, в прошлый раз упал webhook, повторяем только его
                logger.info("Payment %s already %s, sending webhook only", payment.id, payment.status.value)

            url, payload = payment.webhook_url, webhook_payload(payment)

        # webhook после коммита: если он упадёт, статус всё равно останется сохранённым
        await self._webhook.send(url, payload)
        logger.info("Webhook for payment %s delivered", payment_id)
