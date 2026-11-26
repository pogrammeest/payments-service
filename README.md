# Payments Service

Сервис асинхронной обработки платежей. Принимает платёж, проводит его через платёжный шлюз (мок) и отправляет результат на webhook клиента.

![Схема сервиса](docs/architecture.png)

## Архитектура

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic.

- `api`: REST API. Проверяет `X-API-Key`, пишет платёж и событие в таблицу `outbox` в одной транзакции и отвечает 202. Повторный запрос с тем же `Idempotency-Key` возвращает уже созданный платёж.
- `consumer`: FastStream-приложение. Outbox relay раз в секунду публикует новые события в очередь `payments.new`. Обработчик забирает их, вызывает шлюз, сохраняет статус и шлёт webhook. При любой ошибке (шлюз, webhook, БД) делает ещё две попытки через 2 и 4 секунды, потом сообщение уходит в `payments.new.dlq`.
- `gateway-mock`: мок шлюза. Отвечает через 2-5 секунд, 90% платежей проходят.
- `postgres`, `rabbitmq`.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

API и Swagger: http://localhost:8000/docs, RabbitMQ: http://localhost:15672 (guest / guest). Миграции применяются при старте `api`.

Чтобы видеть входящие webhook, можно поднять тестовый приёмник и смотреть его логи:

```bash
docker compose --profile demo up -d webhook-receiver
docker compose logs -f webhook-receiver
```

## Создание платежа

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:8080/payments"
      }'
```

Ответ `202 Accepted`:

```json
{"payment_id": "8f1c...", "status": "pending", "created_at": "2026-11-25T12:00:00Z"}
```

Статус платежа:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-api-key"
```

## Тесты

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest
```

Postgres и RabbitMQ для тестов не нужны.

## Ограничения

- Если после трёх попыток сообщение ушло в DLQ, платёж остаётся `pending`. В `failed` его не переводим: при таймауте шлюза списание могло пройти. Такие сообщения разбираются вручную из `payments.new.dlq`.
- Webhook повторяется на любой ответ, кроме 2xx.
- `webhook_url` не проверяется на внутренние адреса, иначе не работал бы демо-приёмник `http://webhook-receiver:8080`. В проде нужен allowlist или проверка IP после резолва.
