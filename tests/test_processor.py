import contextlib
import uuid
from decimal import Decimal

import pytest

from app.clients import TransientError
from app.consumer.handler import PaymentProcessor
from app.models import Currency, Payment, PaymentStatus


class FakeSession:
    def __init__(self, payment: Payment | None) -> None:
        self.payment = payment

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    @contextlib.asynccontextmanager
    async def begin(self):
        yield

    async def scalar(self, statement):
        return self.payment


class FakeGateway:
    def __init__(self, result: PaymentStatus | Exception) -> None:
        self.result = result
        self.calls = 0

    async def charge(self, payment):
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class FakeWebhook:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[str, dict]] = []

    async def send(self, url, payload):
        if self.error:
            raise self.error
        self.sent.append((url, payload))


def make_payment(status=PaymentStatus.PENDING) -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="test",
        meta={},
        status=status,
        idempotency_key="k",
        webhook_url="https://example.com/hook",
    )


def make_processor(payment, gateway, webhook) -> PaymentProcessor:
    return PaymentProcessor(lambda: FakeSession(payment), gateway, webhook)


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_pending_payment_is_charged_and_notified(result):
    payment, gateway, webhook = make_payment(), FakeGateway(result), FakeWebhook()

    await make_processor(payment, gateway, webhook).process(payment.id)

    assert payment.status is result
    assert payment.processed_at is not None
    assert gateway.calls == 1
    assert webhook.sent[0][0] == "https://example.com/hook"
    assert webhook.sent[0][1]["status"] == result.value


async def test_redelivery_of_processed_payment_only_sends_webhook():
    payment = make_payment(status=PaymentStatus.SUCCEEDED)
    gateway, webhook = FakeGateway(PaymentStatus.FAILED), FakeWebhook()

    await make_processor(payment, gateway, webhook).process(payment.id)

    assert gateway.calls == 0
    assert payment.status is PaymentStatus.SUCCEEDED
    assert len(webhook.sent) == 1


async def test_gateway_error_keeps_payment_pending():
    payment = make_payment()
    gateway, webhook = FakeGateway(TransientError("503")), FakeWebhook()

    with pytest.raises(TransientError):
        await make_processor(payment, gateway, webhook).process(payment.id)

    assert payment.status is PaymentStatus.PENDING
    assert webhook.sent == []


async def test_webhook_error_is_retryable():
    payment = make_payment()
    processor = make_processor(payment, FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhook(TransientError("500")))

    with pytest.raises(TransientError):
        await processor.process(payment.id)


async def test_missing_payment_is_dropped():
    gateway, webhook = FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhook()
    await make_processor(None, gateway, webhook).process(uuid.uuid4())
    assert gateway.calls == 0 and webhook.sent == []
