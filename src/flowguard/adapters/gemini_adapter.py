"""Drop-in FlowGuard resilience wrapper for Google Gemini & GenAI clients."""

from typing import Any, Callable, Optional, Tuple, Type
from flowguard.core.limiter import TokenBucketLimiter
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.core.pipeline import FlowGuard
from flowguard.exceptions import FlowGuardError, PermanentHTTPError

DEFAULT_FATAL_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    FlowGuardError,
    PermanentHTTPError,
    ValueError,
    TypeError,
    KeyError,
)


class ResilientGemini:
    """
    Transparent rate-limited, retry-protected and fallback-enabled wrapper around Google GenAI client.

    Requires an asynchronous Google GenAI client (e.g. client.aio.models.generate_content or client.generate_content_async).
    """

    def __init__(
        self,
        client: Any,
        rpm_limit: float = 300.0,
        rpm_burst_capacity: Optional[float] = None,
        tpm_limit: Optional[float] = 60_000.0,
        tpm_burst_capacity: Optional[float] = None,
        max_retries: int = 4,
        acquire_timeout: Optional[float] = 60.0,
        fallback: Optional[Callable[..., Any]] = None,
        fatal_exceptions: Tuple[Type[Exception], ...] = DEFAULT_FATAL_EXCEPTIONS,
    ) -> None:
        # P2-1: Fail-fast verification of async client interface
        has_aio = hasattr(client, "aio") and hasattr(client.aio, "models")
        has_async_gen = hasattr(client, "generate_content_async")
        if not (has_aio or has_async_gen):
            raise TypeError(
                "ResilientGemini requires an asynchronous client (e.g. client.aio.models.generate_content or client.generate_content_async). For synchronous models, use client.aio."
            )

        self._client = client
        self.acquire_timeout = acquire_timeout

        rpm_burst = rpm_burst_capacity or max(1.0, min(float(rpm_limit), float(rpm_limit) / 5.0))
        self.rpm_limiter = TokenBucketLimiter(
            rate=rpm_limit / 60.0,
            capacity=rpm_burst,
        )

        if tpm_limit:
            burst = tpm_burst_capacity or max(100.0, float(tpm_limit) / 5.0)
            self.tpm_limiter: Optional[TokenBucketLimiter] = TokenBucketLimiter(
                rate=float(tpm_limit) / 60.0,
                capacity=burst,
            )
        else:
            self.tpm_limiter = None

        if max_retries > 0:
            self.retry_policy: Optional[RetryPolicy] = RetryPolicy(
                max_attempts=max_retries + 1,
                backoff=ExponentialBackoff(base_delay=1.0, max_delay=30.0, multiplier=2.0),
                fatal_exceptions=fatal_exceptions,
            )
        else:
            self.retry_policy = None

        self.pipeline = FlowGuard(
            name="gemini-resilient-pipeline",
            limiter=self.rpm_limiter,
            retry=self.retry_policy,
            fallback=fallback,
        )

    async def generate_content(
        self, model: str, contents: Any, estimated_tokens: int = 500, **kwargs: Any
    ) -> Any:
        """Call client.aio.models.generate_content with per-attempt TPM and RPM throttling."""

        async def _call(*_args: Any, **call_kw: Any) -> Any:
            if self.tpm_limiter:
                await self.tpm_limiter.acquire(
                    tokens=float(estimated_tokens), timeout=self.acquire_timeout
                )
            kw = dict(call_kw)
            m = kw.pop("model", model)
            c = kw.pop("contents", contents)
            kw.pop("estimated_tokens", None)

            if hasattr(self._client, "aio") and hasattr(self._client.aio, "models"):
                return await self._client.aio.models.generate_content(model=m, contents=c, **kw)
            else:
                return await self._client.generate_content_async(c, **kw)

        return await self.pipeline.execute(
            _call, model=model, contents=contents, estimated_tokens=estimated_tokens, **kwargs
        )
