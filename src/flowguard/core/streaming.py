"""Resilient Async Stream Wrapper protecting streaming LLM calls against concurrency blowout and mid-stream failure."""

import asyncio
import inspect
import logging
import time
from typing import Any, AsyncIterator, Generic, Optional, TypeVar
from flowguard.core.bulkhead import Bulkhead
from flowguard.core.circuit_breaker import CircuitBreaker
from flowguard.metrics.collector import MetricsCollector
from flowguard.exceptions import MidStreamInterruptedError

logger = logging.getLogger("flowguard.streaming")

T = TypeVar("T")


class ResilientStream(Generic[T], AsyncIterator[T]):
    """
    Protective wrapper around an AsyncIterator for streaming LLM calls.

    Key guarantees:
    1. Holds Bulkhead concurrency slot across the entire streaming lifespan.
    2. Deterministically releases Bulkhead on StopAsyncIteration, error, cancellation, or aclose().
    3. Traps mid-stream exceptions: prevents illegal duplicate retries and raises MidStreamInterruptedError.
    4. Records TTFT (Time To First Token) and total stream latency in MetricsCollector.
    5. Safe against early `break` when used with `async with stream:` or by calling `await stream.aclose()`.
    """

    def __init__(
        self,
        iterator: AsyncIterator[T],
        bulkhead: Optional[Bulkhead] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        metrics: Optional[MetricsCollector] = None,
        start_time: Optional[float] = None,
        first_chunk: Optional[T] = None,
        has_first_chunk: bool = False,
        is_empty: bool = False,
        ttft: Optional[float] = None,
    ) -> None:
        self._iterator = iterator
        self._bulkhead = bulkhead
        self._circuit_breaker = circuit_breaker
        self._metrics = metrics
        self._start_time = start_time or time.monotonic()
        self._first_chunk = first_chunk
        self._has_first_chunk = has_first_chunk
        self._first_chunk_yielded = False
        self._is_empty = is_empty
        self._ttft = ttft

        self._closed = False
        self._chunks_delivered = 0
        self._lock = asyncio.Lock()

    @property
    def chunks_delivered(self) -> int:
        """Total chunks yielded to consumer so far."""
        return self._chunks_delivered

    @property
    def is_closed(self) -> bool:
        """Whether the stream has finished or been closed."""
        return self._closed

    @property
    def ttft(self) -> Optional[float]:
        """Time to first token (seconds) if recorded, else None."""
        return self._ttft

    def __aiter__(self) -> "ResilientStream[T]":
        return self

    async def __anext__(self) -> T:
        if self._closed:
            raise StopAsyncIteration

        if self._is_empty:
            await self._finalize_success()
            raise StopAsyncIteration

        # Yield pre-fetched first chunk if available
        if self._has_first_chunk and not self._first_chunk_yielded:
            self._first_chunk_yielded = True
            self._chunks_delivered += 1
            return self._first_chunk  # type: ignore[return-value]

        try:
            call_chunk_start = time.monotonic()
            chunk = await self._iterator.__anext__()
            self._chunks_delivered += 1
            if self._ttft is None:
                self._ttft = call_chunk_start - self._start_time
                if self._metrics:
                    self._metrics.record_ttft(self._ttft)
            return chunk
        except StopAsyncIteration:
            await self._finalize_success()
            raise
        except asyncio.CancelledError:
            await self._finalize_cancelled()
            raise
        except Exception as exc:
            await self._finalize_error(exc)
            raise MidStreamInterruptedError(
                message=f"Stream failed after yielding {self._chunks_delivered} chunk(s): {exc}",
                chunks_delivered=self._chunks_delivered,
                cause=exc,
            ) from exc

    async def _finalize_success(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True

        total_latency = time.monotonic() - self._start_time
        if self._circuit_breaker:
            await self._circuit_breaker.record_success()
        if self._metrics:
            self._metrics.record_success(total_latency)
        if self._bulkhead:
            await self._bulkhead.release()

        # Close underlying iterator if supported
        if hasattr(self._iterator, "aclose") and inspect.iscoroutinefunction(self._iterator.aclose):
            try:
                await self._iterator.aclose()
            except Exception:
                logger.debug("Exception closing underlying iterator", exc_info=True)

    async def _finalize_error(self, exc: BaseException) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True

        total_latency = time.monotonic() - self._start_time
        if self._circuit_breaker:
            await self._circuit_breaker.record_failure(exc)
        if self._metrics:
            self._metrics.record_failure(total_latency, type(exc).__name__)
        if self._bulkhead:
            await self._bulkhead.release()

        if hasattr(self._iterator, "aclose") and inspect.iscoroutinefunction(self._iterator.aclose):
            try:
                await self._iterator.aclose()
            except Exception:
                pass

    async def _finalize_cancelled(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True

        if self._circuit_breaker:
            await self._circuit_breaker.record_cancelled()
        if self._bulkhead:
            await self._bulkhead.release()

        if hasattr(self._iterator, "aclose") and inspect.iscoroutinefunction(self._iterator.aclose):
            try:
                await self._iterator.aclose()
            except Exception:
                pass

    async def aclose(self) -> None:
        """Explicitly close the stream, releasing bulkhead slots and underlying resources."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True

        if self._bulkhead:
            await self._bulkhead.release()

        if hasattr(self._iterator, "aclose") and inspect.iscoroutinefunction(self._iterator.aclose):
            try:
                await self._iterator.aclose()
            except Exception:
                pass

    async def __aenter__(self) -> "ResilientStream[T]":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if not self._closed:
            if (
                exc_type is not None
                and issubclass(exc_type, Exception)
                and not issubclass(exc_type, (StopAsyncIteration, asyncio.CancelledError))
            ):
                await self._finalize_error(exc_val)
            elif exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
                await self._finalize_cancelled()
            else:
                await self.aclose()

    def __del__(self) -> None:
        if not self._closed and self._bulkhead is not None:
            logger.warning(
                "ResilientStream was garbage-collected without being cleanly closed! "
                "Use 'async with stream:' or ensure 'await stream.aclose()' is called."
            )
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.aclose())
            except RuntimeError:
                pass
