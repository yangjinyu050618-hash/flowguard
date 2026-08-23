"""Drop-in FlowGuard resilience wrapper for OpenAI & LLM API clients."""

from typing import Any, Optional, Tuple, Type
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


class ResilientOpenAI:
    """
    Transparent rate-limited and retry-protected wrapper around OpenAI AsyncClient.

    Parameters
    ----------
    client : Any
        AsyncOpenAI client instance.
    rpm_limit : float
        Requests Per Minute limit (e.g. 500 RPM).
    rpm_burst_capacity : Optional[float]
        Token bucket burst capacity for requests (defaults to max(1.0, min(rpm_limit, rpm_limit / 5.0))).
    tpm_limit : Optional[float]
        Tokens Per Minute limit (e.g. 100,000 TPM).
    tpm_burst_capacity : Optional[float]
        Token bucket burst capacity for TPM tokens (defaults to max(100.0, tpm_limit / 5.0)).
    max_retries : int
        Max auto-retries on transient errors (0 disables retry, N retries = N+1 total attempts).
    acquire_timeout : Optional[float]
        Timeout in seconds when acquiring RPM/TPM quota (default: 60.0s).
    fatal_exceptions : Tuple[Type[Exception], ...]
        Exceptions that fail fast and are never retried (defaults to non-transient exceptions).
    """

    def __init__(
        self,
        client: Any,
        rpm_limit: float = 500.0,
        rpm_burst_capacity: Optional[float] = None,
        tpm_limit: Optional[float] = None,
        tpm_burst_capacity: Optional[float] = None,
        max_retries: int = 4,
        acquire_timeout: Optional[float] = 60.0,
        fatal_exceptions: Tuple[Type[Exception], ...] = DEFAULT_FATAL_EXCEPTIONS,
    ) -> None:
        self._client = client
        self.acquire_timeout = acquire_timeout

        # Item 5: Sane burst capacity for low RPM limits (e.g. rpm_limit=1 -> capacity=1.0)
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

        # Item 4: Correct max_retries semantics (N retries -> N+1 attempts; 0 -> no retry policy)
        if max_retries > 0:
            self.retry_policy: Optional[RetryPolicy] = RetryPolicy(
                max_attempts=max_retries + 1,
                backoff=ExponentialBackoff(base_delay=1.0, max_delay=30.0, multiplier=2.0),
                fatal_exceptions=fatal_exceptions,
            )
        else:
            self.retry_policy = None

        self.pipeline = FlowGuard(
            name="openai-resilient-pipeline",
            limiter=self.rpm_limiter,
            retry=self.retry_policy,
        )

    async def create_chat_completion(self, estimated_tokens: int = 500, **kwargs: Any) -> Any:
        """Call chat.completions.create with per-attempt TPM and RPM throttling."""

        async def _call() -> Any:
            # Item 2: Deduct TPM quota on every physical attempt
            if self.tpm_limiter:
                await self.tpm_limiter.acquire(
                    tokens=float(estimated_tokens), timeout=self.acquire_timeout
                )
            return await self._client.chat.completions.create(**kwargs)

        return await self.pipeline.execute(_call)
