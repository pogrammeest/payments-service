from typing import Any

import httpx

from app.clients import TransientError


class WebhookSender:
    def __init__(self, timeout: float) -> None:
        self._client = httpx.AsyncClient(timeout=timeout)

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        """Отправляет результат платежа. Всё, кроме 2xx, считаем временной ошибкой."""
        try:
            response = await self._client.post(url, json=payload)
        except httpx.HTTPError as exc:
            raise TransientError(f"webhook request failed: {exc!r}") from exc
        if not response.is_success:
            raise TransientError(f"webhook returned {response.status_code}")

    async def close(self) -> None:
        await self._client.aclose()
