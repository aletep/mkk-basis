"""Own the single processing task independently of an AMQP delivery callback."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from uuid import UUID

from mkk_basis.application.payments import ProcessingResult, ProcessPayment
from mkk_basis.application.ports import Clock

logger = logging.getLogger(__name__)


class ProcessingSupervisor:
    """A reconnect rejoins the live attempt; process shutdown cancels it explicitly.

    Only one task may execute the use case. The task and its result remain owned
    here if the delivery awaiting it disappears. Durable stages remain the source
    of truth after a process restart or a lost acknowledgement.
    """

    def __init__(
        self,
        process: ProcessPayment,
        clock: Clock,
        available: Callable[[], Awaitable[bool]],
    ) -> None:
        self.process, self.clock, self.available = process, clock, available
        self._lock = asyncio.Lock()
        self._payment_id: UUID | None = None
        self._task: asyncio.Task[ProcessingResult] | None = None
        self._closed = False

    async def _run(self, payment_id: UUID) -> ProcessingResult:
        while not await self.available():
            await self.clock.sleep(1)
        return await self.process.execute(payment_id)

    @staticmethod
    def _observe(task: asyncio.Task[ProcessingResult]) -> None:
        # A cancelled delivery might never retrieve an exception from its task.
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("processing_task_failed", exc_info=error)

    async def execute(self, payment_id: UUID) -> ProcessingResult:
        async with self._lock:
            if self._closed:
                raise asyncio.CancelledError
            if self._task is not None and self._payment_id != payment_id:
                # Another delivery may arrive first after reconnect. Finish the
                # owned task before allowing a different payment to start.
                with suppress(Exception):
                    await asyncio.shield(self._task)
                self._task = None
            if self._closed:
                raise asyncio.CancelledError
            if self._task is None:
                self._payment_id = payment_id
                self._task = asyncio.create_task(self._run(payment_id), name="payment-processing")
                self._task.add_done_callback(self._observe)
            task = self._task
        try:
            return await asyncio.shield(task)
        finally:
            # Keep an unfinished task when only the AMQP callback was cancelled.
            if task.done():
                async with self._lock:
                    if self._task is task:
                        self._task = None

    async def close(self) -> None:
        """Stop the owned task without refunding a begun durable attempt."""
        self._closed = True
        if self._task is not None:
            task = self._task
            try:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            finally:
                self._task = None
