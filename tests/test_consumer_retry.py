import uuid

import pytest
from faststream.rabbit import TestRabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.broker.topology import ATTEMPT_HEADER, payments_exchange, retry_queue
from app.clients import TransientError
from app.consumer import main as consumer


@pytest.fixture
def failing_processor(monkeypatch):
    calls = []

    async def process(payment_id):
        calls.append(payment_id)
        raise TransientError("gateway 503")

    monkeypatch.setattr(consumer.processor, "process", process)
    return calls


async def test_transient_error_goes_to_retry_queue_with_next_attempt(failing_processor):
    retried = []

    @consumer.broker.subscriber(retry_queue(1), payments_exchange)
    async def retry_spy(body: dict, message: RabbitMessage) -> None:
        retried.append((body, message.headers.get(ATTEMPT_HEADER)))

    payment_id = str(uuid.uuid4())
    async with TestRabbitBroker(consumer.broker) as br:
        await br.publish({"payment_id": payment_id}, exchange=payments_exchange, routing_key="payments.new")

    assert failing_processor == [uuid.UUID(payment_id)]
    assert retried == [({"payment_id": payment_id}, 2)]


async def test_last_attempt_is_rejected_to_dlq(failing_processor):
    async with TestRabbitBroker(consumer.broker) as br:
        await br.publish(
            {"payment_id": str(uuid.uuid4())},
            exchange=payments_exchange,
            routing_key="payments.new",
            headers={ATTEMPT_HEADER: 3},
        )
        consumer.handle_payment.mock.assert_called_once()

    assert len(failing_processor) == 1
