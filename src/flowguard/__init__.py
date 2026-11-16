"""
FlowGuard: High-Performance Async Rate Limiting, Circuit Breaking & Resilience Orchestration for LLM & API Pipelines.
"""

from flowguard.exceptions import (
    FlowGuardError,
    RateLimitExceededError,
    CircuitBreakerOpenError,
    BulkheadFullError,
    MaxRetriesExceededError,
    MidStreamInterruptedError,
    HTTPStatusError,
    TransientHTTPError,
    PermanentHTTPError,
)
from flowguard.core.limiter import (
    BaseRateLimiter,
    TokenBucketLimiter,
    SlidingWindowLimiter,
)
from flowguard.backends.redis import (
    RedisTokenBucketLimiter,
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
from flowguard.core.streaming import (
    ResilientStream,
)
from flowguard.core.pipeline import (
    FlowGuard,
    guard,
    guard_stream,
)
from flowguard.core.fallback import (
    FallbackContext,
    ChoiceFallback,
    with_fallback_context,
)
from flowguard.metrics.collector import (
    MetricsCollector,
)
from flowguard.metrics.otel import (
    OpenTelemetryExporter,
    GenAIMetrics,
    export_opentelemetry,
)
from flowguard.adapters.openai_compatible import (
    ResilientOpenAICompatible,
    create_deepseek_adapter,
    create_vllm_adapter,
    create_ollama_adapter,
)

__version__ = "0.3.0"
__all__ = [
    "__version__",
    "FlowGuardError",
    "RateLimitExceededError",
    "CircuitBreakerOpenError",
    "BulkheadFullError",
    "MaxRetriesExceededError",
    "MidStreamInterruptedError",
    "HTTPStatusError",
    "TransientHTTPError",
    "PermanentHTTPError",
    "BaseRateLimiter",
    "TokenBucketLimiter",
    "SlidingWindowLimiter",
    "RedisTokenBucketLimiter",
    "CircuitBreaker",
    "CircuitState",
    "RetryPolicy",
    "ExponentialBackoff",
    "BackoffStrategy",
    "Bulkhead",
    "ResilientStream",
    "FlowGuard",
    "guard",
    "guard_stream",
    "FallbackContext",
    "ChoiceFallback",
    "with_fallback_context",
    "MetricsCollector",
    "OpenTelemetryExporter",
    "GenAIMetrics",
    "export_opentelemetry",
    "ResilientOpenAICompatible",
    "create_deepseek_adapter",
    "create_vllm_adapter",
    "create_ollama_adapter",
]
