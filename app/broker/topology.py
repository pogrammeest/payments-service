from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import get_settings

NEW_ROUTING_KEY = "payments.new"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)

new_queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
)

broker = RabbitBroker(get_settings().rabbit_url, publisher_confirms=True)


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(payments_exchange)
    queue = await broker.declare_queue(new_queue)
    await queue.bind(exchange, routing_key=NEW_ROUTING_KEY)
