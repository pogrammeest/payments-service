import uuid

import pytest
from faststream.rabbit import TestRabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError
from sqlalchemy.exc import InterfaceError

from app.broker.topology import ATTEMPT_HEADER, payments_exchange, retry_queue
from app.clients import TransientError
from app.consumer import main as consumer

retried: list[tuple[dict, int]] = []


@consumer.broker.subscriber(retry_queue(1), payments_exchange)
async def retry_spy(body: dict, message: RabbitMessage) -> None:
    retried.append((body, message.headers.get(ATTEMPT_HEADER)))


def failing_processor(monkeypatch, error: Exception) -> list:
    calls = []

    async def process(payment_id):
        calls.append(payment_id)
        raise error

    monkeypatch.setattr(consumer.processor, "process", process)
    return calls


@pytest.mark.parametrize(
    "error",
    [
        TransientError("gateway 503"),
        InterfaceError("UPDATE payments", {}, Exception("connection is closed")),
        RuntimeError("unexpected"),
    ],
    ids=["transient", "db-connection", "any-error"],
)
async def test_error_goes_to_retry_queue_with_next_attempt(monkeypatch, error):
    retried.clear()
    calls = failing_processor(monkeypatch, error)
    payment_id = str(uuid.uuid4())

    async with TestRabbitBroker(consumer.broker) as br:
        await br.publish({"payment_id": payment_id}, exchange=payments_exchange, routing_key="payments.new")

    assert calls == [uuid.UUID(payment_id)]
    assert retried == [({"payment_id": payment_id}, 2)]


async def test_last_attempt_is_rejected_to_dlq(monkeypatch):
    retried.clear()
    calls = failing_processor(monkeypatch, RuntimeError("still failing"))

    async with TestRabbitBroker(consumer.broker) as br:
        await br.publish(
            {"payment_id": str(uuid.uuid4())},
            exchange=payments_exchange,
            routing_key="payments.new",
            headers={ATTEMPT_HEADER: 3},
        )
        consumer.handle_payment.mock.assert_called_once()

    assert len(calls) == 1
    assert retried == []


async def test_invalid_json_fails_validation_like_any_bad_message(monkeypatch):
    # Без safe_decoder здесь был бы JSONDecodeError из decoder, до ack-контекста:
    # сообщение висело бы unacked. С ним битое тело падает на валидации, как {"foo": 1},
    # и FastStream отклоняет его в DLQ.
    calls = failing_processor(monkeypatch, RuntimeError("must not be called"))

    async with TestRabbitBroker(consumer.broker) as br:
        with pytest.raises(ValidationError):
            await br.publish(
                b"not json",
                exchange=payments_exchange,
                routing_key="payments.new",
                content_type="application/json",
            )

    assert calls == []
