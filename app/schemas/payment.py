import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.models import Currency, PaymentStatus

Amount = Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2)]
METADATA_MAX_BYTES = 16 * 1024


class PaymentCreate(BaseModel):
    amount: Amount
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @field_validator("metadata")
    @classmethod
    def metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        # metadata хранится в JSONB и уходит в каждом webhook, без лимита это способ раздуть БД
        if len(json.dumps(value, ensure_ascii=False).encode()) > METADATA_MAX_BYTES:
            raise ValueError(f"metadata must not exceed {METADATA_MAX_BYTES} bytes")
        return value


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="meta")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
