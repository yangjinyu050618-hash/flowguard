"""Drop-in FlowGuard resilience wrapper for OpenAI & LLM API clients."""

from typing import Any, Optional
from flowguard.core.limiter import TokenBucketLimiter
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.core.pipeline import FlowGuard


class ResilientOpenAI:
    """
    Transparent rate-limited and retry-protected wrapper around OpenAI AsyncClient.

    Parameters
    ----------
    client : Any
        AsyncOpenAI client instance.
    rpm_limit : float
        Requests Per Minute limit (e.g. 500 RPM).
    tpm_limit : Optional[float]
        Tokens Per Minute limit (e.g. 100,000 TPM).
    tpm_burst_capacity : Optional[float]
        Explicit token bucket burst capacity (default: max(tpm_limit / 5.0, 2000.0)).
    max_retries : int
        Max auto-retries on transient errors.
    acquire_timeout : Optional[float]
        Timeout in seconds when acquiring RPM/TPM quota (default: 60.0s).
    """

    def __init__(
        self,
        client: Any,
        rpm_limit: float = 500.0,
        tpm_limit: Optional[float] = None,
        tpm_burst_capacity: Optional[float] = None,
        max_retries: int = 4,
        acquire_timeout: Optional[float] = 60.0,
    ) -> None:
        self._client = client
        self.acquire_timeout = acquire_timeout
        self.rpm_limiter = TokenBucketLimiter(
            rate=rpm_limit / 60.0,
            capacity=max(10.0, rpm_limit / 10.0),
        )
        if tpm_limit:
            # Proportional burst capacity tied to user's quota (12s of quota or user specified)
            burst = tpm_burst_capacity or max(float(tpm_limit) / 5.0, 2000.0)
            self.tpm_limiter: Optional[TokenBucketLimiter] = TokenBucketLimiter(
                rate=float(tpm_limit) / 60.0,
                capacity=burst,
            )
        else:
            self.tpm_limiter = None

        self.retry_policy = RetryPolicy(
            max_attempts=max_retries,
            backoff=ExponentialBackoff(base_delay=1.0, max_delay=30.0, multiplier=2.0),
        )
        self.pipeline = FlowGuard(
            name="openai-resilient-pipeline",
            limiter=self.rpm_limiter,
            retry=self.retry_policy,
        )

    async def create_chat_completion(self, estimated_tokens: int = 500, **kwargs: Any) -> Any:
        """Call chat.completions.create with adaptive token & request throttling."""
        if self.tpm_limiter:
            await self.tpm_limiter.acquire(tokens=float(estimated_tokens), timeout=self.acquire_timeout)

        async def _call() -> Any:
            return await self._client.chat.completions.create(**kwargs)

        return await self.pipeline.execute(_call)
