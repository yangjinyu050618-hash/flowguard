"""
FlowGuard: High-Performance Async Rate Limiting, Circuit Breaking & Resilience Orchestration for LLM & API Pipelines.
"""

from flowguard.exceptions import (
    FlowGuardError,
    RateLimitExceededError,
    CircuitBreakerOpenError,
    BulkheadFullError,
    MaxRetriesExceededError,
    HTTPStatusError,
    TransientHTTPError,
    PermanentHTTPError,
)
from flowguard.core.limiter import (
    BaseRateLimiter,
    TokenBucketLimiter,
    SlidingWindowLimiter,
)
from flowguard.core.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
)
from flowguard.core.retry import (
    RetryPolicy,
    ExponentialBackoff,
    BackoffStrategy,
)
from flowguard.core.bulkhead import (
    Bulkhead,
)
from flowguard.core.pipeline import (
    FlowGuard,
    guard,
)
from flowguard.metrics.collector import (
    MetricsCollector,
)

__version__ = "0.2.2"
__all__ = [
    "__version__",
    "FlowGuardError",
    "RateLimitExceededError",
    "CircuitBreakerOpenError",
    "BulkheadFullError",
    "MaxRetriesExceededError",
    "HTTPStatusError",
    "TransientHTTPError",
    "PermanentHTTPError",
    "BaseRateLimiter",
    "TokenBucketLimiter",
    "SlidingWindowLimiter",
    "CircuitBreaker",
    "CircuitState",
    "RetryPolicy",
    "ExponentialBackoff",
    "BackoffStrategy",
    "Bulkhead",
    "FlowGuard",
    "guard",
    "MetricsCollector",
]
