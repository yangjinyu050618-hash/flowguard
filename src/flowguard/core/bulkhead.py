"""Bulkhead pattern for concurrency isolation and resource partition protection."""

import asyncio
from typing import Any, Optional
from flowguard.exceptions import BulkheadFullError


class Bulkhead:
    """
    Asynchronous Bulkhead isolating concurrent task capacity.

    Parameters
    ----------
    max_concurrent : int
        Maximum number of concurrent executions permitted.
    max_queued : int
        Maximum number of callers allowed to queue waiting for execution slots.
    queue_timeout : Optional[float]
        Maximum seconds a task can wait in queue before timing out (default: None).
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        max_queued: int = 20,
        queue_timeout: Optional[float] = None,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if max_queued < 0:
            raise ValueError("max_queued must be >= 0")

        self.max_concurrent = max_concurrent
        self.max_queued = max_queued
        self.queue_timeout = queue_timeout

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active_count = 0
        self._queued_count = 0
        self._lock = asyncio.Lock()

    @property
    def active_count(self) -> int:
        return self._active_count

    @property
    def queued_count(self) -> int:
        return self._queued_count

    async def __aenter__(self) -> "Bulkhead":
        async with self._lock:
            if self._active_count >= self.max_concurrent and self._queued_count >= self.max_queued:
                raise BulkheadFullError(
                    f"Bulkhead queue limit reached ({self.max_queued} queued, {self.max_concurrent} active)"
                )
            self._queued_count += 1

        acquired = False
        try:
            if self.queue_timeout is not None:
                try:
                    await asyncio.wait_for(self._semaphore.acquire(), timeout=self.queue_timeout)
                    acquired = True
                except asyncio.TimeoutError:
                    raise BulkheadFullError(
                        f"Bulkhead acquisition timed out after {self.queue_timeout}s waiting in queue"
                    )
            else:
                await self._semaphore.acquire()
                acquired = True
        finally:
            async with self._lock:
                self._queued_count = max(0, self._queued_count - 1)
                if acquired:
                    self._active_count += 1

        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        async with self._lock:
            self._active_count = max(0, self._active_count - 1)
        self._semaphore.release()
