import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment
from app.schemas.payment import PaymentCreate

PAYMENT_CREATED = "payment.created"


class IdempotencyConflict(Exception):
    """Idempotency-Key уже использовался с другим телом запроса."""


def _same_request(payment: Payment, data: PaymentCreate) -> bool:
    # Decimal сравнивается по значению: 100 == 100.00
    return (
        payment.amount == data.amount
        and payment.currency == data.currency
        and payment.description == data.description
        and payment.meta == data.metadata
        and payment.webhook_url == str(data.webhook_url)
    )


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def _get_by_key(session: AsyncSession, key: str) -> Payment | None:
    result = await session.execute(select(Payment).where(Payment.idempotency_key == key))
    return result.scalar_one_or_none()


async def create_payment(session: AsyncSession, data: PaymentCreate, idempotency_key: str) -> Payment:
    """Создаёт платёж и событие в outbox в одной транзакции.

    Повтор с тем же Idempotency-Key возвращает существующий платёж,
    тот же ключ с другим телом даёт IdempotencyConflict.
    """
    existing = await _get_by_key(session, idempotency_key)
    if existing is not None:
        return _check_replay(existing, data)

    payment = Payment(
        id=uuid.uuid4(),
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        meta=data.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(data.webhook_url),
    )
    session.add(payment)
    session.add(
        OutboxEvent(
            aggregate_id=payment.id,
            event_type=PAYMENT_CREATED,
            payload={"payment_id": str(payment.id)},
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        # параллельный запрос с тем же ключом вставил строку раньше нас
        await session.rollback()
        existing = await _get_by_key(session, idempotency_key)
        if existing is None:
            raise
        return _check_replay(existing, data)

    await session.refresh(payment)
    return payment


def _check_replay(payment: Payment, data: PaymentCreate) -> Payment:
    if not _same_request(payment, data):
        raise IdempotencyConflict
    return payment
