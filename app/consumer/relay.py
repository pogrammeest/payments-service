"""Outbox relay: публикует события из outbox в RabbitMQ.

published_at ставится только после подтверждения от брокера. Если упасть между
publish и UPDATE, событие уйдёт повторно, consumer к этому готов.
"""

import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.topology import NEW_ROUTING_KEY, payments_exchange
from app.core.config import get_settings
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(self, broker: RabbitBroker, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._broker = broker
        self._session_factory = session_factory
        settings = get_settings()
        self._interval = settings.outbox_poll_interval
        self._batch_size = settings.outbox_batch_size

    async def run(self) -> None:
        while True:
            try:
                published = await self.publish_batch()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0
            # полная пачка значит, что есть ещё события: тогда берём следующую без паузы
            if published < self._batch_size:
                await asyncio.sleep(self._interval)

    async def publish_batch(self) -> int:
        async with self._session_factory() as session, session.begin():
            # SKIP LOCKED: несколько relay не возьмут одни и те же строки
            result = await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = result.scalars().all()

            published = 0
            for event in events:
                try:
                    await self._broker.publish(
                        event.payload,
                        exchange=payments_exchange,
                        routing_key=NEW_ROUTING_KEY,
                        message_id=str(event.id),
                        message_type=event.event_type,
                        persist=True,
                    )
                except Exception:
                    # что успели опубликовать, сохраняем, остальное уйдёт в следующий раз
                    logger.exception("Failed to publish outbox event %s", event.id)
                    break
                event.published_at = datetime.now(UTC)
                published += 1

            if published:
                logger.info("Published %d outbox event(s)", published)
            return published
