"""FlowGuard: Composable Resilience Pipeline (Retry -> Limiter -> Bulkhead -> Breaker -> Call -> Fallback)."""

import asyncio
import functools
import inspect
import logging
import time
from typing import Any, AsyncIterator, Callable, Coroutine, Optional, TypeVar, cast
from flowguard.core.limiter import BaseRateLimiter, TokenBucketLimiter
from flowguard.core.circuit_breaker import CircuitBreaker
from flowguard.core.retry import RetryPolicy
from flowguard.core.bulkhead import Bulkhead
from flowguard.core.streaming import ResilientStream
from flowguard.core.fallback import FallbackContext, is_context_handler
from flowguard.exceptions import CircuitBreakerOpenError, BulkheadFullError
from flowguard.metrics.collector import MetricsCollector

logger = logging.getLogger("flowguard.pipeline")

F = TypeVar("F", bound=Callable[..., Any])


class FlowGuard:
    """
    Unified Orchestrator combining Retry, Rate Limiting, Bulkhead Concurrency, Circuit Breaking, and Fallback.

    Architecture Hierarchy:
    Retry (outer) -> RateLimiter -> Bulkhead -> CircuitBreaker -> Target Downstream -> Fallback
    """

    def __init__(
        self,
        name: str = "default",
        limiter: Optional[BaseRateLimiter] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry: Optional[RetryPolicy] = None,
        bulkhead: Optional[Bulkhead] = None,
        fallback: Optional[Callable[..., Any]] = None,
        metrics: Optional[MetricsCollector] = None,
    ) -> None:
        self.name = name
        self.limiter = limiter
        self.circuit_breaker = circuit_breaker
        self.retry = retry
        self.bulkhead = bulkhead
        self.fallback = fallback
        self.metrics = metrics or MetricsCollector(name=name)

    async def _handle_fallback(
        self, __flowguard_exc__: BaseException, *args: Any, **kwargs: Any
    ) -> Any:
        if self.fallback is None:
            raise __flowguard_exc__

        ctx = FallbackContext(
            exception=__flowguard_exc__,
            args=args,
            kwargs=dict(kwargs),
            pipeline_name=self.name,
        )

        try:
            # P1: Inspect signature statically outside invocation
            if is_context_handler(self.fallback):
                res = self.fallback(ctx)
            else:
                has_exc_param = False
                try:
                    sig = inspect.signature(self.fallback)
                    has_exc_param = "exc" in sig.parameters and "exc" not in kwargs
                except (ValueError, TypeError):
                    has_exc_param = False

                if has_exc_param:
                    res = self.fallback(*args, exc=__flowguard_exc__, **kwargs)
                else:
                    res = self.fallback(*args, **kwargs)

            if inspect.isawaitable(res):
                return await res
            return res
        except Exception as fb_exc:
            logger.exception("[%s] Fallback execution failed: %s", self.name, fb_exc)
            raise fb_exc from __flowguard_exc__

    async def execute(
        self, func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any
    ) -> Any:
        tokens = kwargs.pop("__flowguard_tokens__", 1.0)

        async def _attempt() -> Any:
            # 1. Rate Limiting per physical attempt
            if self.limiter:
                try:
                    await self.limiter.acquire(tokens=tokens)
                except Exception as e:
                    logger.debug("[%s] Rate limit quota exhausted", self.name)
                    self.metrics.record_rejected("rate_limit")
                    raise e

            # 2. Bulkhead concurrency isolation
            if self.bulkhead:
                try:
                    async with self.bulkhead:
                        return await _circuit_and_call()
                except BulkheadFullError as e:
                    logger.debug("[%s] Bulkhead concurrency saturated", self.name)
                    self.metrics.record_rejected("bulkhead")
                    raise e
            else:
                return await _circuit_and_call()

        async def _circuit_and_call() -> Any:
            # 3. Circuit breaker check
            if self.circuit_breaker:
                try:
                    await self.circuit_breaker.before_call()
                except CircuitBreakerOpenError as e:
                    logger.debug("[%s] Call rejected by CircuitBreaker", self.name)
                    self.metrics.record_rejected("circuit_breaker")
                    raise e

            call_start = time.monotonic()
            try:
                res = await func(*args, **kwargs)
                if self.circuit_breaker:
                    await self.circuit_breaker.record_success()
                latency = time.monotonic() - call_start
                self.metrics.record_success(latency)
                return res
            except asyncio.CancelledError:
                if self.circuit_breaker:
                    await self.circuit_breaker.record_cancelled()
                raise
            except Exception as exc:
                if self.circuit_breaker:
                    await self.circuit_breaker.record_failure(exc)
                latency = time.monotonic() - call_start
                self.metrics.record_failure(latency, type(exc).__name__)
                raise exc

        try:
            # 4. Outer retry loop
            if self.retry:
                return await self.retry.execute(_attempt)
            return await _attempt()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return await self._handle_fallback(exc, *args, **kwargs)

    async def execute_stream(
        self,
        func: Callable[..., Any],
        *args: Any,
        prefetch_first_chunk: bool = True,
        **kwargs: Any,
    ) -> ResilientStream[Any]:
        """
        Execute an async streaming function (or async generator) within the resilience pipeline.

        Guarantees:
        1. Bulkhead concurrency slot is held across the entire stream duration.
        2. Pre-first-token failures can be retried or diverted to fallback.
        3. Mid-stream failures (after first token delivered) fail-fast with MidStreamInterruptedError and trip circuit.
        4. TTFT and end-to-end latencies are recorded in MetricsCollector.
        """
        tokens = kwargs.pop("__flowguard_tokens__", 1.0)

        async def _attempt_stream() -> ResilientStream[Any]:
            # 1. Rate Limiting per physical attempt
            if self.limiter:
                try:
                    await self.limiter.acquire(tokens=tokens)
                except Exception as e:
                    logger.debug("[%s] Rate limit quota exhausted for stream", self.name)
                    self.metrics.record_rejected("rate_limit")
                    raise e

            # 2. Bulkhead concurrency slot (held across entire stream lifespan)
            bulkhead_acquired = False
            if self.bulkhead:
                try:
                    await self.bulkhead.acquire()
                    bulkhead_acquired = True
                except BulkheadFullError as e:
                    logger.debug("[%s] Bulkhead concurrency saturated for stream", self.name)
                    self.metrics.record_rejected("bulkhead")
                    raise e

            call_start = time.monotonic()
            try:
                # 3. Circuit breaker check
                if self.circuit_breaker:
                    try:
                        await self.circuit_breaker.before_call()
                    except CircuitBreakerOpenError as e:
                        logger.debug("[%s] Stream call rejected by CircuitBreaker", self.name)
                        self.metrics.record_rejected("circuit_breaker")
                        raise e

                # 4. Invoke target function or accept pre-instantiated stream
                if callable(func):
                    raw_res = func(*args, **kwargs)
                else:
                    raw_res = func
                if inspect.isawaitable(raw_res):
                    stream_obj = await raw_res
                else:
                    stream_obj = raw_res

                # 5. Extract async iterator
                iterator: AsyncIterator[Any]
                if hasattr(stream_obj, "__aiter__"):
                    iterator = stream_obj.__aiter__()
                elif hasattr(stream_obj, "__anext__"):
                    iterator = stream_obj
                else:
                    raise TypeError(
                        f"Expected async iterable/iterator from stream function, got {type(stream_obj)}"
                    )

                first_chunk = None
                has_first_chunk = False
                is_empty = False
                ttft: Optional[float] = None

                if prefetch_first_chunk:
                    try:
                        first_chunk = await iterator.__anext__()
                        has_first_chunk = True
                        ttft = time.monotonic() - call_start
                        if self.metrics:
                            self.metrics.record_ttft(ttft)
                    except StopAsyncIteration:
                        is_empty = True

                # ResilientStream takes over ownership of the bulkhead slot
                return ResilientStream(
                    iterator=iterator,
                    bulkhead=self.bulkhead,
                    circuit_breaker=self.circuit_breaker,
                    metrics=self.metrics,
                    start_time=call_start,
                    first_chunk=first_chunk,
                    has_first_chunk=has_first_chunk,
                    is_empty=is_empty,
                    ttft=ttft,
                )
            except Exception as exc:
                if bulkhead_acquired and self.bulkhead:
                    await self.bulkhead.release()
                if self.circuit_breaker:
                    await self.circuit_breaker.record_failure(exc)
                latency = time.monotonic() - call_start
                self.metrics.record_failure(latency, type(exc).__name__)
                raise exc

        try:
            if self.retry:
                return await self.retry.execute(_attempt_stream)
            return await _attempt_stream()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            fallback_res = await self._handle_fallback(exc, *args, **kwargs)
            return cast(ResilientStream[Any], fallback_res)

    def __call__(self, func: F) -> F:
        """Decorator interface for protecting async functions and async generator functions."""
        if inspect.isasyncgenfunction(func):

            @functools.wraps(func)
            async def stream_gen_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self.execute_stream(func, *args, **kwargs)

            return cast(F, stream_gen_wrapper)

        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"FlowGuard decorator currently supports async functions and async generators only, got {func}"
            )

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.execute(func, *args, **kwargs)

        return cast(F, wrapper)


def guard(
    name: str = "default",
    rate_per_sec: Optional[float] = None,
    burst_capacity: Optional[float] = None,
    max_retries: int = 0,
    max_attempts: Optional[int] = None,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    max_concurrent: Optional[int] = None,
    fallback: Optional[Callable[..., Any]] = None,
    stream: bool = False,
) -> Callable[[F], F]:
    """
    Convenient all-in-one decorator factory with fallback degradation support.
    """
    limiter = None
    if rate_per_sec is not None:
        limiter = TokenBucketLimiter(rate=rate_per_sec, capacity=burst_capacity or rate_per_sec)

    circuit_breaker = None
    if failure_threshold > 0:
        circuit_breaker = CircuitBreaker(
            failure_threshold=failure_threshold, recovery_timeout=recovery_timeout
        )

    attempts = (
        max_attempts if max_attempts is not None else (max_retries + 1 if max_retries > 0 else 1)
    )
    retry_policy = None
    if attempts > 1:
        retry_policy = RetryPolicy(max_attempts=attempts)

    bulkhead = None
    if max_concurrent is not None and max_concurrent > 0:
        bulkhead = Bulkhead(max_concurrent=max_concurrent)

    pipeline = FlowGuard(
        name=name,
        limiter=limiter,
        circuit_breaker=circuit_breaker,
        retry=retry_policy,
        bulkhead=bulkhead,
        fallback=fallback,
    )

    if stream:

        def stream_decorator(fn: F) -> F:
            @functools.wraps(fn)
            async def stream_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await pipeline.execute_stream(fn, *args, **kwargs)

            return cast(F, stream_wrapper)

        return stream_decorator

    return pipeline


def guard_stream(
    name: str = "default",
    rate_per_sec: Optional[float] = None,
    burst_capacity: Optional[float] = None,
    max_retries: int = 0,
    max_attempts: Optional[int] = None,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    max_concurrent: Optional[int] = None,
    fallback: Optional[Callable[..., Any]] = None,
    prefetch_first_chunk: bool = True,
) -> Callable[[F], F]:
    """
    Convenient decorator factory dedicated to protecting async streaming functions and generators.
    """
    limiter = None
    if rate_per_sec is not None:
        limiter = TokenBucketLimiter(rate=rate_per_sec, capacity=burst_capacity or rate_per_sec)

    circuit_breaker = None
    if failure_threshold > 0:
        circuit_breaker = CircuitBreaker(
            failure_threshold=failure_threshold, recovery_timeout=recovery_timeout
        )

    attempts = (
        max_attempts if max_attempts is not None else (max_retries + 1 if max_retries > 0 else 1)
    )
    retry_policy = None
    if attempts > 1:
        retry_policy = RetryPolicy(max_attempts=attempts)

    bulkhead = None
    if max_concurrent is not None and max_concurrent > 0:
        bulkhead = Bulkhead(max_concurrent=max_concurrent)

    pipeline = FlowGuard(
        name=name,
        limiter=limiter,
        circuit_breaker=circuit_breaker,
        retry=retry_policy,
        bulkhead=bulkhead,
        fallback=fallback,
    )

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await pipeline.execute_stream(
                fn, *args, prefetch_first_chunk=prefetch_first_chunk, **kwargs
            )

        return cast(F, wrapper)

    return decorator
