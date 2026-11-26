import pytest
from fastapi.testclient import TestClient

from gateway_mock import main as mock

BODY = {"payment_id": "00000000-0000-0000-0000-000000000001", "amount": "10.00", "currency": "RUB"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(mock.settings, "delay_min", 0)
    monkeypatch.setattr(mock.settings, "delay_max", 0)
    mock._charges.clear()
    return TestClient(mock.app)


@pytest.mark.parametrize("forced", ["succeeded", "declined"])
def test_forced_result(client, forced):
    response = client.post(
        "/v1/charges", json=BODY, headers={"Idempotency-Key": forced, "X-Mock-Result": forced}
    )
    assert response.status_code == 200
    assert response.json()["status"] == forced


def test_error_returns_503_and_is_not_cached(client):
    headers = {"Idempotency-Key": "k"}
    response = client.post("/v1/charges", json=BODY, headers={**headers, "X-Mock-Result": "error"})
    assert response.status_code == 503

    response = client.post("/v1/charges", json=BODY, headers={**headers, "X-Mock-Result": "succeeded"})
    assert response.status_code == 200


def test_same_key_returns_same_result(client):
    first = client.post(
        "/v1/charges", json=BODY, headers={"Idempotency-Key": "k", "X-Mock-Result": "declined"}
    )
    second = client.post(
        "/v1/charges", json=BODY, headers={"Idempotency-Key": "k", "X-Mock-Result": "succeeded"}
    )
    assert first.json() == second.json()


def test_success_rate_is_applied(client, monkeypatch):
    monkeypatch.setattr(mock.settings, "success_rate", 1.0)
    assert (
        client.post("/v1/charges", json=BODY, headers={"Idempotency-Key": "a"}).json()["status"]
        == "succeeded"
    )
    monkeypatch.setattr(mock.settings, "success_rate", 0.0)
    assert (
        client.post("/v1/charges", json=BODY, headers={"Idempotency-Key": "b"}).json()["status"] == "declined"
    )
