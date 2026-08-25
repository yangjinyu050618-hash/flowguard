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
from flowguard.core.fallback import (
    FallbackContext,
    ChoiceFallback,
    with_fallback_context,
)
from flowguard.metrics.collector import (
    MetricsCollector,
)

__version__ = "0.3.0"
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
    "FallbackContext",
    "ChoiceFallback",
    "with_fallback_context",
    "MetricsCollector",
]
