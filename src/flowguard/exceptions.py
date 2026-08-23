"""FlowGuard core exceptions hierarchy."""

from typing import Optional


class FlowGuardError(Exception):
    """Base exception for all FlowGuard internal framework errors."""

    pass


class RateLimitExceededError(FlowGuardError):
    """Raised when token quota cannot be acquired within the timeout window."""

    def __init__(self, message: str, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class CircuitBreakerOpenError(FlowGuardError):
    """Raised when an operation is blocked because the circuit breaker is OPEN or probe limits reached."""

    def __init__(self, message: str, reset_timeout: Optional[float] = None) -> None:
        super().__init__(message)
        self.reset_timeout = reset_timeout


class BulkheadFullError(FlowGuardError):
    """Raised when concurrent executions or queued capacity exceed configured bulkhead limits."""

    pass


class MaxRetriesExceededError(FlowGuardError):
    """Raised when retry attempts are exhausted without successful resolution."""

    def __init__(
        self, message: str, attempts: int, last_exception: Optional[BaseException] = None
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


class HTTPStatusError(Exception):
    """Base exception for downstream HTTP response errors."""

    def __init__(self, status_code: int, message: str = "") -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code


class TransientHTTPError(HTTPStatusError):
    """Raised on transient HTTP errors (e.g. 429 Too Many Requests, 502, 503, 504) suitable for retrying."""

    pass


class PermanentHTTPError(HTTPStatusError):
    """Raised on permanent client/auth HTTP errors (e.g. 400, 401, 403, 404, 422) that should not be retried."""

    pass
