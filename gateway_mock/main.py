"""Мок платёжного шлюза.

Отвечает через 2-5 секунд, 90% платежей проходят, остальные получают declined.
Ответ кэшируется по Idempotency-Key. Заголовком X-Mock-Result можно задать
результат руками (для тестов).
"""

import asyncio
import random
import uuid
from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class MockSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_", extra="ignore")

    success_rate: float = 0.9
    error_rate: float = 0.0
    delay_min: float = 2.0
    delay_max: float = 5.0


class ChargeStatus(StrEnum):
    SUCCEEDED = "succeeded"
    DECLINED = "declined"


class MockResult(StrEnum):
    SUCCEEDED = "succeeded"
    DECLINED = "declined"
    ERROR = "error"


class ChargeRequest(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: str


class ChargeResponse(BaseModel):
    payment_id: uuid.UUID
    status: ChargeStatus
    gateway_tx_id: uuid.UUID


settings = MockSettings()
app = FastAPI(title="Payment Gateway Mock")

# хранить в памяти достаточно: инстанс один, ответы нужны только на время ретраев
_charges: dict[str, ChargeResponse] = {}


def _decide(forced: MockResult | None) -> MockResult:
    if forced is not None:
        return forced
    if random.random() < settings.error_rate:
        return MockResult.ERROR
    if random.random() < settings.success_rate:
        return MockResult.SUCCEEDED
    return MockResult.DECLINED


@app.post("/v1/charges", response_model=ChargeResponse)
async def charge(
    request: ChargeRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    mock_result: Annotated[MockResult | None, Header(alias="X-Mock-Result")] = None,
) -> ChargeResponse:
    if idempotency_key in _charges:
        return _charges[idempotency_key]

    await asyncio.sleep(random.uniform(settings.delay_min, settings.delay_max))

    outcome = _decide(mock_result)
    if outcome is MockResult.ERROR:
        # 503 не кэшируем, иначе ретрай всегда будет получать ошибку
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Gateway unavailable")

    response = ChargeResponse(
        payment_id=request.payment_id,
        status=ChargeStatus(outcome.value),
        gateway_tx_id=uuid.uuid4(),
    )
    _charges[idempotency_key] = response
    return response


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
