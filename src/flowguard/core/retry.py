"""Smart retry algorithms with Exponential Backoff, Full Jitter, and Decorrelated Jitter."""

import asyncio
import inspect
import random
from typing import Any, Callable, Coroutine, Optional, Tuple, Type, TypeVar
from flowguard.exceptions import FlowGuardError, MaxRetriesExceededError, PermanentHTTPError

T = TypeVar("T")


def is_permanent_client_error(exc: BaseException) -> bool:
    """Detect if an exception represents an unretryable client/auth HTTP error."""
    # 1. FlowGuard internal errors or permanent HTTP errors
    if isinstance(exc, (FlowGuardError, PermanentHTTPError)):
        return True

    # 2. HTTP status code inspection (supports .status_code and Google GenAI .code)
    code = getattr(exc, "status_code", None)
    if code is None:
        code = getattr(exc, "code", None)

    if isinstance(code, int):
        # 4xx client errors (excluding 408 Request Timeout, 409 Conflict, 429 Rate Limit)
        if 400 <= code < 500 and code not in (408, 409, 429):
            return True
        if code in (408, 409, 429) or 500 <= code < 600:
            return False

    # 3. Class name patterns for authentication / bad request in API SDKs (OpenAI, Anthropic, Google)
    name = type(exc).__name__
    if name in {
        "AuthenticationError",
        "PermissionDeniedError",
        "NotFoundError",
        "BadRequestError",
        "UnprocessableEntityError",
        "InvalidRequestError",
        "InvalidArgument",
        "PermissionDenied",
        "Unauthenticated",
        "NotFound",
    }:
        return True

    return False


class BackoffStrategy:
    """Base class for backoff calculation strategies."""

    def compute_delay(self, attempt: int, previous_delay: float) -> float:
        raise NotImplementedError


class ExponentialBackoff(BackoffStrategy):
    """
    Exponential backoff with full, equal, or decorrelated jitter.
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
            raise ValueError(
                f"Invalid jitter type '{jitter}'. Must be 'full', 'equal', 'decorrelated', or 'none'."
            )
        self.jitter = jitter

    def compute_delay(self, attempt: int, previous_delay: float = 0.0) -> float:
        if self.jitter == "decorrelated":
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
    Configurable asynchronous retry policy with automatic fatal error fail-fast.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        backoff: Optional[BackoffStrategy] = None,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        fatal_exceptions: Tuple[Type[Exception], ...] = (FlowGuardError, PermanentHTTPError),
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

    async def execute(
        self, func: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any
    ) -> T:
        last_exc: Optional[BaseException] = None
        prev_delay = 0.0

        for attempt in range(1, self.max_attempts + 1):
            try:
                return await func(*args, **kwargs)
            except self.fatal_exceptions:
                raise
            except self.retryable_exceptions as exc:
                if is_permanent_client_error(exc):
                    raise exc

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
