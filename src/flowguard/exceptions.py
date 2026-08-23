"""FlowGuard core exceptions hierarchy."""

from typing import Optional


class FlowGuardError(Exception):
    """Base exception for all FlowGuard internal framework errors."""

    def __init__(self, message: str = "FlowGuard error") -> None:
        super().__init__(message)
        self.message = message


class RateLimitExceededError(FlowGuardError):
    """Raised when token quota cannot be acquired within the timeout window."""

    def __init__(
        self, message: str = "Rate limit exceeded", retry_after: Optional[float] = None
    ) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class CircuitBreakerOpenError(FlowGuardError):
    """Raised when an operation is blocked because the circuit breaker is OPEN or probe limits reached."""

    def __init__(
        self, message: str = "Circuit breaker is OPEN", reset_timeout: Optional[float] = None
    ) -> None:
        super().__init__(message)
        self.reset_timeout = reset_timeout


class BulkheadFullError(FlowGuardError):
    """Raised when concurrent executions or queued capacity exceed configured bulkhead limits."""

    def __init__(self, message: str = "Bulkhead concurrency limit reached") -> None:
        super().__init__(message)


class MaxRetriesExceededError(FlowGuardError):
    """Raised when retry attempts are exhausted without successful resolution."""

    def __init__(
        self,
        message: str = "Maximum retries exceeded",
        attempts: int = 0,
        last_exception: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


class HTTPStatusError(Exception):
    """Base exception for downstream HTTP response errors."""

    def __init__(self, status_code: int = 500, message: str = "") -> None:
        msg = f"HTTP {status_code}: {message}" if message else f"HTTP {status_code}"
        super().__init__(msg)
        self.status_code = status_code


class TransientHTTPError(HTTPStatusError):
    """Raised on transient HTTP errors (e.g. 429 Too Many Requests, 5xx) suitable for retrying."""

    pass


class PermanentHTTPError(HTTPStatusError):
    """Raised on permanent client/auth HTTP errors (e.g. 400, 401, 403, 404, 422) that should not be retried."""

    pass
