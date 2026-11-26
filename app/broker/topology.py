"""Топология RabbitMQ.

payments (direct) --payments.new--> payments.new --(reject)--> payments.dlx --> payments.new.dlq
payments (direct) --payments.retry.Ns--> payments.retry.Ns --(TTL expired)--> payments --> payments.new
"""

from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import get_settings

NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.new.dlq"
ATTEMPT_HEADER = "x-attempt"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

new_queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

dead_letter_queue = RabbitQueue("payments.new.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_delay(attempt: int) -> int:
    """Пауза перед следующей попыткой, сек: 2, 4, 8..."""
    return 2**attempt


def retry_routing_key(attempt: int) -> str:
    return f"payments.retry.{retry_delay(attempt)}s"


def retry_queue(attempt: int) -> RabbitQueue:
    """Очередь задержки без консьюмеров: сообщение ждёт TTL и через DLX возвращается в payments.new."""
    name = retry_routing_key(attempt)
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": retry_delay(attempt) * 1000,
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )


# prefetch ограничивает число платежей в обработке: каждый держит соединение из пула БД
broker = RabbitBroker(
    get_settings().rabbit_url,
    default_channel=Channel(prefetch_count=get_settings().consumer_prefetch, publisher_confirms=True),
)


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(dead_letter_exchange)

    queue = await broker.declare_queue(new_queue)
    await queue.bind(exchange, routing_key=NEW_ROUTING_KEY)

    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dlx, routing_key=DLQ_ROUTING_KEY)

    # попытки 1..N-1 идут на повтор, после попытки N сообщение уходит в DLQ
    for attempt in range(1, get_settings().max_attempts):
        delay_queue = await broker.declare_queue(retry_queue(attempt))
        await delay_queue.bind(exchange, routing_key=retry_routing_key(attempt))
