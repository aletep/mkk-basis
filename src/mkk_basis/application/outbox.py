from datetime import timedelta
from uuid import uuid4

from mkk_basis.application.errors import PublicationFailed
from mkk_basis.application.ports import Clock, EventPublisher, UowFactory


class PublishOutbox:
    def __init__(self, uow: UowFactory, clock: Clock, publisher: EventPublisher) -> None:
        self.uow, self.clock, self.publisher = uow, clock, publisher

    async def execute(self) -> bool:
        token = uuid4()
        async with self.uow() as tx:
            event = await tx.outbox.claim(
                self.clock.now(), self.clock.now() + timedelta(seconds=30), token
            )
            await tx.commit()
        if event is None:
            return False
        try:
            await self.publisher.publish(event)
        except PublicationFailed as exc:
            async with self.uow() as tx:
                await tx.outbox.failed(
                    event.id,
                    token,
                    self.clock.now() + timedelta(seconds=min(2 ** min(event.attempts - 1, 5), 30)),
                    str(exc)[:200],
                )
                await tx.commit()
        else:
            async with self.uow() as tx:
                await tx.outbox.published(event.id, token, self.clock.now())
                await tx.commit()
        return True
