import httpx

from app.clients import TransientError
from app.models import Payment, PaymentStatus


class GatewayClient:
    def __init__(self, base_url: str, timeout: float) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def charge(self, payment: Payment) -> PaymentStatus:
        """Списывает платёж и возвращает финальный статус.

        На сетевые ошибки и 5xx бросает TransientError. payment_id уходит в шлюз
        как Idempotency-Key, так что повтор не спишет деньги дважды.
        """
        try:
            response = await self._client.post(
                "/v1/charges",
                json={
                    "payment_id": str(payment.id),
                    "amount": str(payment.amount),
                    "currency": payment.currency.value,
                },
                headers={"Idempotency-Key": str(payment.id)},
            )
        except httpx.HTTPError as exc:
            raise TransientError(f"gateway request failed: {exc!r}") from exc

        if response.status_code >= 500:
            raise TransientError(f"gateway returned {response.status_code}")
        if response.status_code != 200:
            # 4xx: шлюз отклонил сам запрос, повторять смысла нет
            return PaymentStatus.FAILED

        return PaymentStatus.SUCCEEDED if response.json()["status"] == "succeeded" else PaymentStatus.FAILED

    async def close(self) -> None:
        await self._client.aclose()
