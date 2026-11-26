import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from app.db.session import get_session
from app.main import app
from app.models import Currency, Payment, PaymentStatus
from app.services import payments as service

HEADERS = {"X-API-Key": "dev-api-key", "Idempotency-Key": "key-1"}
BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "https://example.com/hook",
}


def make_payment(**overrides) -> Payment:
    fields = dict(
        id=uuid.uuid4(),
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Order #1",
        meta={"order_id": 1},
        status=PaymentStatus.PENDING,
        idempotency_key="key-1",
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
        processed_at=None,
    )
    fields.update(overrides)
    return Payment(**fields)


@pytest.fixture
def client():
    async def fake_session():
        yield None

    app.dependency_overrides[get_session] = fake_session
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_missing_api_key_returns_401(client):
    response = client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})
    assert response.status_code == 401


def test_wrong_api_key_returns_401(client):
    response = client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


def test_missing_idempotency_key_returns_422(client):
    response = client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": "dev-api-key"})
    assert response.status_code == 422


@pytest.mark.parametrize(
    "field,value",
    [("amount", "0"), ("amount", "1.001"), ("currency", "GBP"), ("metadata", {"blob": "x" * 20_000})],
    ids=["zero-amount", "three-decimals", "unknown-currency", "oversized-metadata"],
)
def test_invalid_body_returns_422(client, field, value):
    response = client.post("/api/v1/payments", json={**BODY, field: value}, headers=HEADERS)
    assert response.status_code == 422


def test_create_payment_returns_202(client, monkeypatch):
    payment = make_payment()

    async def fake_create(session, data, key):
        assert key == "key-1"
        assert data.amount == Decimal("100.50")
        return payment

    monkeypatch.setattr(service, "create_payment", fake_create)
    response = client.post("/api/v1/payments", json=BODY, headers=HEADERS)

    assert response.status_code == 202
    assert response.json()["payment_id"] == str(payment.id)
    assert response.json()["status"] == "pending"


def test_idempotency_conflict_returns_409(client, monkeypatch):
    async def fake_create(session, data, key):
        raise service.IdempotencyConflict

    monkeypatch.setattr(service, "create_payment", fake_create)
    response = client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert response.status_code == 409


def test_get_payment(client, monkeypatch):
    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=datetime.now(UTC))

    async def fake_get(session, payment_id):
        return payment if payment_id == payment.id else None

    monkeypatch.setattr(service, "get_payment", fake_get)

    response = client.get(f"/api/v1/payments/{payment.id}", headers={"X-API-Key": "dev-api-key"})
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "succeeded"
    assert data["metadata"] == {"order_id": 1}
    assert data["amount"] == "100.50"

    response = client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "dev-api-key"})
    assert response.status_code == 404
