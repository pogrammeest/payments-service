import asyncio
import contextlib
import logging

from faststream import FastStream

from app.broker.topology import broker, declare_topology
from app.consumer.relay import OutboxRelay
from app.db.session import session_factory

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastStream(broker)
relay = OutboxRelay(broker, session_factory)
_relay_task: asyncio.Task | None = None


@app.after_startup
async def start_relay() -> None:
    global _relay_task
    await declare_topology(broker)
    _relay_task = asyncio.create_task(relay.run(), name="outbox-relay")


@app.on_shutdown
async def stop_relay() -> None:
    if _relay_task is not None:
        _relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _relay_task
