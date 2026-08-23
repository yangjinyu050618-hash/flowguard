"""FlowGuard: Composable Resilience Pipeline (Limiter + Breaker + Retry + Bulkhead)."""

import functools
import inspect
import time
from typing import Any, Callable, Coroutine, Optional, TypeVar, cast
from flowguard.core.limiter import BaseRateLimiter, TokenBucketLimiter
from flowguard.core.circuit_breaker import CircuitBreaker
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.core.bulkhead import Bulkhead
from flowguard.metrics.collector import MetricsCollector

F = TypeVar("F", bound=Callable[..., Any])


class FlowGuard:
    """
    Unified Orchestrator combining Rate Limiting, Circuit Breaking, Retry, and Concurrency Bulkhead.

    Parameters
    ----------
    name : str
        Identifier for this pipeline.
    limiter : Optional[BaseRateLimiter]
        Rate limiter instance.
    circuit_breaker : Optional[CircuitBreaker]
        Circuit breaker instance.
    retry : Optional[RetryPolicy]
        Retry policy instance.
    bulkhead : Optional[Bulkhead]
        Bulkhead concurrency barrier instance.
    metrics : Optional[MetricsCollector]
        Metrics collector for telemetry.
    """

    def __init__(
        self,
        name: str = "default",
        limiter: Optional[BaseRateLimiter] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        retry: Optional[RetryPolicy] = None,
        bulkhead: Optional[Bulkhead] = None,
        metrics: Optional[MetricsCollector] = None,
    ) -> None:
        self.name = name
        self.limiter = limiter
        self.circuit_breaker = circuit_breaker
        self.retry = retry
        self.bulkhead = bulkhead
        self.metrics = metrics or MetricsCollector(name=name)

    async def execute(self, func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any) -> Any:
        start_time = time.monotonic()
        tokens = kwargs.pop("__flowguard_tokens__", 1.0)

        # 1. Rate Limiting
        if self.limiter:
            try:
                await self.limiter.acquire(tokens=tokens)
            except Exception as e:
                self.metrics.record_rejected("rate_limit")
                raise e

        # 2. Bulkhead execution wrapper
        async def _core_call() -> Any:
            # 3. Circuit breaker check
            if self.circuit_breaker:
                await self.circuit_breaker.before_call()

            call_start = time.monotonic()
            try:
                res = await func(*args, **kwargs)
                if self.circuit_breaker:
                    await self.circuit_breaker.record_success()
                latency = time.monotonic() - call_start
                self.metrics.record_success(latency)
                return res
            except Exception as exc:
                if self.circuit_breaker:
                    await self.circuit_breaker.record_failure(exc)
                latency = time.monotonic() - call_start
                self.metrics.record_failure(latency, type(exc).__name__)
                raise exc

        async def _bulkhead_wrapped() -> Any:
            if self.bulkhead:
                async with self.bulkhead:
                    return await _core_call()
            return await _core_call()

        # 4. Retry layer
        if self.retry:
            return await self.retry.execute(_bulkhead_wrapped)
        return await _bulkhead_wrapped()

    def __call__(self, func: F) -> F:
        """Decorator interface for protecting async functions."""
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"FlowGuard decorator currently supports async functions only, got {func}")

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.execute(func, *args, **kwargs)

        return cast(F, wrapper)


def guard(
    name: str = "default",
    rate_per_sec: Optional[float] = None,
    burst_capacity: Optional[float] = None,
    max_retries: int = 1,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    max_concurrent: Optional[int] = None,
) -> Callable[[F], F]:
    """
    Convenient all-in-one decorator factory.
    """
    limiter = None
    if rate_per_sec is not None:
        limiter = TokenBucketLimiter(rate=rate_per_sec, capacity=burst_capacity or rate_per_sec)

    circuit_breaker = None
    if failure_threshold > 0:
        circuit_breaker = CircuitBreaker(failure_threshold=failure_threshold, recovery_timeout=recovery_timeout)

    retry_policy = None
    if max_retries > 1:
        retry_policy = RetryPolicy(max_attempts=max_retries)

    bulkhead = None
    if max_concurrent is not None and max_concurrent > 0:
        bulkhead = Bulkhead(max_concurrent=max_concurrent)

    pipeline = FlowGuard(
        name=name,
        limiter=limiter,
        circuit_breaker=circuit_breaker,
        retry=retry_policy,
        bulkhead=bulkhead,
    )
    return pipeline
