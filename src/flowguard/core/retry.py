"""Smart retry algorithms with Exponential Backoff, Full Jitter, and Decorrelated Jitter."""

import asyncio
import inspect
import random
from typing import Any, Callable, Coroutine, Optional, Tuple, Type, TypeVar
from flowguard.exceptions import FlowGuardError, MaxRetriesExceededError

T = TypeVar("T")


class BackoffStrategy:
    """Base class for backoff calculation strategies."""
    def compute_delay(self, attempt: int, previous_delay: float) -> float:
        raise NotImplementedError


class ExponentialBackoff(BackoffStrategy):
    """
    Exponential backoff with full, equal, or decorrelated jitter.

    Parameters
    ----------
    base_delay : float
        Initial retry backoff delay in seconds (default: 0.5s).
    max_delay : float
        Maximum upper-bound delay cap in seconds (default: 60.0s).
    multiplier : float
        Exponential growth multiplier (default: 2.0).
    jitter : str
        Jitter mode: 'full', 'equal', 'decorrelated', or 'none' (default: 'full').
    """

    def __init__(
        self,
        base_delay: float = 0.5,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: str = "full",
    ) -> None:
        self.base_delay = max(0.001, base_delay)
        self.max_delay = max(self.base_delay, max_delay)
        self.multiplier = multiplier
        if jitter not in ("full", "equal", "decorrelated", "none"):
            raise ValueError(f"Invalid jitter type '{jitter}'. Must be 'full', 'equal', 'decorrelated', or 'none'.")
        self.jitter = jitter

    def compute_delay(self, attempt: int, previous_delay: float = 0.0) -> float:
        if self.jitter == "decorrelated":
            # Decorrelated Jitter: Sleep = min(max_delay, uniform(base_delay, prev_delay * 3))
            upper = max(self.base_delay, previous_delay * 3.0)
            return min(self.max_delay, random.uniform(self.base_delay, upper))

        temp = min(self.max_delay, self.base_delay * (self.multiplier ** max(0, attempt - 1)))
        if self.jitter == "full":
            return random.uniform(0, temp)
        elif self.jitter == "equal":
            half = temp / 2.0
            return half + random.uniform(0, half)
        return temp


class RetryPolicy:
    """
    Configurable asynchronous retry policy.

    Parameters
    ----------
    max_attempts : int
        Maximum number of execution attempts (including initial call). Default: 3.
    backoff : Optional[BackoffStrategy]
        Backoff algorithm instance. Defaults to ExponentialBackoff().
    retryable_exceptions : Tuple[Type[Exception], ...]
        Exceptions eligible for retry. Defaults to (Exception,).
    fatal_exceptions : Tuple[Type[Exception], ...]
        Exceptions that immediately abort retry loop. Defaults to (FlowGuardError,).
    reraise : bool
        If True, re-raises the original last exception instead of wrapping in MaxRetriesExceededError.
    on_retry : Optional[Callable[[int, BaseException, float], Any]]
        Callback executed before each sleep retry (attempt, exception, delay).
    """

    def __init__(
        self,
        max_attempts: int = 3,
        backoff: Optional[BackoffStrategy] = None,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        fatal_exceptions: Tuple[Type[Exception], ...] = (FlowGuardError,),
        reraise: bool = False,
        on_retry: Optional[Callable[[int, BaseException, float], Any]] = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.max_attempts = max_attempts
        self.backoff = backoff or ExponentialBackoff()
        self.retryable_exceptions = retryable_exceptions
        self.fatal_exceptions = fatal_exceptions
        self.reraise = reraise
        self.on_retry = on_retry

    async def execute(self, func: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any) -> T:
        last_exc: Optional[BaseException] = None
        prev_delay = 0.0

        for attempt in range(1, self.max_attempts + 1):
            try:
                return await func(*args, **kwargs)
            except self.fatal_exceptions:
                raise
            except self.retryable_exceptions as exc:
                last_exc = exc
                if attempt == self.max_attempts:
                    break

                delay = self.backoff.compute_delay(attempt, prev_delay)
                prev_delay = delay

                if self.on_retry:
                    res = self.on_retry(attempt, exc, delay)
                    if inspect.isawaitable(res):
                        await res

                await asyncio.sleep(delay)

        if self.reraise and last_exc is not None:
            raise last_exc

        raise MaxRetriesExceededError(
            f"Operation failed after {self.max_attempts} attempts: {last_exc}",
            attempts=self.max_attempts,
            last_exception=last_exc,
        ) from last_exc
