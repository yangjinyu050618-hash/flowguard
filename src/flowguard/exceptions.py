"""FlowGuard core exceptions hierarchy."""

from typing import Optional


class FlowGuardError(Exception):
    """Base exception class for all FlowGuard errors."""
    pass


class RateLimitExceededError(FlowGuardError):
    """Raised when rate limit quota is exhausted and non-blocking or timeout exceeded."""

    def __init__(self, message: str = "Rate limit quota exceeded", retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class CircuitBreakerOpenError(FlowGuardError):
    """Raised when a request is attempted while the circuit breaker is in OPEN or saturated HALF_OPEN state."""

    def __init__(self, message: str = "Circuit breaker is OPEN; requests are rejected", reset_timeout: Optional[float] = None) -> None:
        super().__init__(message)
        self.reset_timeout = reset_timeout


class BulkheadFullError(FlowGuardError):
    """Raised when concurrent execution slots and queue capacity are exhausted."""

    def __init__(self, message: str = "Bulkhead execution slots and queue capacity saturated") -> None:
        super().__init__(message)


class MaxRetriesExceededError(FlowGuardError):
    """Raised when an operation has failed and exceeded configured max retry attempts."""

    def __init__(self, message: str = "Maximum retry attempts exhausted", attempts: int = 0, last_exception: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception
