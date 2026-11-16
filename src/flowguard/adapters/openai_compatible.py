"""Universal OpenAI-compatible Resilience Adapter for DeepSeek, vLLM, Ollama, OpenRouter, and custom LLM endpoints."""

import logging
from typing import Any, Callable, Dict, Optional, Tuple, Type
from flowguard.core.limiter import TokenBucketLimiter
from flowguard.core.retry import RetryPolicy, ExponentialBackoff
from flowguard.core.pipeline import FlowGuard
from flowguard.core.streaming import ResilientStream
from flowguard.exceptions import FlowGuardError, PermanentHTTPError

logger = logging.getLogger("flowguard.adapters.openai_compatible")

DEFAULT_FATAL_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    FlowGuardError,
    PermanentHTTPError,
    ValueError,
    TypeError,
    KeyError,
)


class ResilientOpenAICompatible:
    """
    Production-grade resilience adapter for any OpenAI-compatible endpoint.

    Supported engines:
    - DeepSeek (deepseek-chat, deepseek-reasoner / R1)
    - vLLM & SGLang self-hosted clusters
    - Ollama local inference
    - OpenRouter & SiliconFlow gateways

    Key capabilities:
    1. Sole Retry Ownership: Strips client-level SDK retries via with_options(max_retries=0).
    2. Prompt Caching Reconciliation: Automatically refunds over-reserved TPM tokens based on cache hits.
    3. Reasoning Tokens Extraction: Captures reasoning_tokens for reasoning models (DeepSeek-R1 / o1).
    4. Streaming Resilience: execute_stream() holds Bulkhead and tracks TTFT.
    """

    def __init__(
        self,
        client: Any,
        rpm_limit: float = 300.0,
        rpm_burst_capacity: Optional[float] = None,
        tpm_limit: Optional[float] = None,
        tpm_burst_capacity: Optional[float] = None,
        max_retries: int = 4,
        acquire_timeout: Optional[float] = 60.0,
        fallback: Optional[Callable[..., Any]] = None,
        fatal_exceptions: Tuple[Type[Exception], ...] = DEFAULT_FATAL_EXCEPTIONS,
        name: str = "openai-compatible-pipeline",
        service_name: Optional[str] = None,
    ) -> None:
        # P1-1: Establish FlowGuard as sole retry owner at client level using with_options without mutating input client
        if hasattr(client, "with_options"):
            self._client = client.with_options(max_retries=0)
        else:
            self._client = client

        self.service_name = service_name or name
        self.name = self.service_name
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
            name=name,
            limiter=self.rpm_limiter,
            retry=self.retry_policy,
            fallback=fallback,
        )

    async def create_chat_completion(
        self,
        estimated_tokens: int = 500,
        cache_discount: float = 0.5,
        **kwargs: Any,
    ) -> Any:
        """
        Execute chat.completions.create with RPM/TPM throttling, retry protection,
        and automatic token bucket refund upon prompt cache hits.
        """

        async def _call(*_args: Any, **call_kw: Any) -> Any:
            if self.tpm_limiter:
                await self.tpm_limiter.acquire(
                    tokens=float(estimated_tokens), timeout=self.acquire_timeout
                )
            req_kw = dict(call_kw)
            req_kw.pop("estimated_tokens", None)
            req_kw.pop("cache_discount", None)

            response = await self._client.chat.completions.create(**req_kw)

            # Reconcile token bucket with actual usage
            await self._reconcile_usage(
                response=response,
                estimated_tokens=estimated_tokens,
                cache_discount=cache_discount,
            )
            return response

        return await self.pipeline.execute(_call, estimated_tokens=estimated_tokens, **kwargs)

    async def create_chat_completion_stream(
        self,
        estimated_tokens: int = 500,
        prefetch_first_chunk: bool = True,
        **kwargs: Any,
    ) -> ResilientStream[Any]:
        """
        Execute chat.completions.create with stream=True within the FlowGuard streaming pipeline.
        Holds Bulkhead concurrency slot and provides mid-stream fault protection.
        """

        async def _stream_call(*_args: Any, **call_kw: Any) -> Any:
            if self.tpm_limiter:
                await self.tpm_limiter.acquire(
                    tokens=float(estimated_tokens), timeout=self.acquire_timeout
                )
            req_kw = dict(call_kw)
            req_kw.pop("estimated_tokens", None)
            req_kw["stream"] = True
            return await self._client.chat.completions.create(**req_kw)

        return await self.pipeline.execute_stream(
            _stream_call,
            prefetch_first_chunk=prefetch_first_chunk,
            estimated_tokens=estimated_tokens,
            **kwargs,
        )

    async def _reconcile_usage(
        self,
        response: Any,
        estimated_tokens: int,
        cache_discount: float,
    ) -> None:
        """Reconcile estimated token quota with actual returned usage."""
        if not self.tpm_limiter or not hasattr(response, "usage") or response.usage is None:
            return

        try:
            usage = response.usage
            prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
            completion_tokens = getattr(usage, "completion_tokens", 0) or 0

            # Extract cached tokens (OpenAI cached_tokens or DeepSeek prompt_cache_hit_tokens)
            cached_tokens = 0
            prompt_details = getattr(usage, "prompt_tokens_details", None)
            if prompt_details is not None:
                cached_tokens = getattr(prompt_details, "cached_tokens", 0) or 0
            elif hasattr(usage, "prompt_cache_hit_tokens"):
                cached_tokens = getattr(usage, "prompt_cache_hit_tokens", 0) or 0

            # Bill cached tokens at discount
            uncached_prompt = max(0, prompt_tokens - cached_tokens)
            effective_prompt = uncached_prompt + (cached_tokens * cache_discount)
            actual_billed_tokens = effective_prompt + completion_tokens

            # Refund over-estimated tokens
            diff = float(estimated_tokens) - float(actual_billed_tokens)
            if diff > 0:
                await self.tpm_limiter.refund(diff)
                logger.debug(
                    "Reconciled token bucket: estimated=%d, actual=%.1f, refunded=%.1f",
                    estimated_tokens,
                    actual_billed_tokens,
                    diff,
                )
        except Exception:
            logger.debug("Exception during token usage reconciliation", exc_info=True)

    async def reconcile_usage(
        self,
        estimated_tokens: int,
        response: Any,
        cache_discount: float = 0.5,
    ) -> None:
        """Public method for reconciling token usage and refunding over-estimated tokens."""
        await self._reconcile_usage(
            response=response,
            estimated_tokens=estimated_tokens,
            cache_discount=cache_discount,
        )

    @staticmethod
    def extract_usage_metrics(response: Any) -> Dict[str, int]:
        """
        Extract detailed token metrics including prompt caching and reasoning tokens.

        Supports OpenAI o1/o3-mini, DeepSeek-R1 (thinking tokens), and standard chat models.
        """
        metrics = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cached_tokens": 0,
            "reasoning_tokens": 0,
        }

        usage = getattr(response, "usage", None)
        if usage is None and isinstance(response, dict):
            usage = response.get("usage")

        if usage is None:
            return metrics

        def get_val(obj: Any, key: str, default: int = 0) -> int:
            if hasattr(obj, key):
                val = getattr(obj, key)
                return int(val) if val is not None else default
            if isinstance(obj, dict) and key in obj:
                val = obj[key]
                return int(val) if val is not None else default
            return default

        metrics["prompt_tokens"] = get_val(usage, "prompt_tokens")
        metrics["completion_tokens"] = get_val(usage, "completion_tokens")
        metrics["total_tokens"] = get_val(usage, "total_tokens")

        prompt_details = getattr(usage, "prompt_tokens_details", None)
        if prompt_details is None and isinstance(usage, dict):
            prompt_details = usage.get("prompt_tokens_details")
        if prompt_details is not None:
            metrics["cached_tokens"] = get_val(prompt_details, "cached_tokens")
        else:
            metrics["cached_tokens"] = get_val(usage, "prompt_cache_hit_tokens")

        comp_details = getattr(usage, "completion_tokens_details", None)
        if comp_details is None and isinstance(usage, dict):
            comp_details = usage.get("completion_tokens_details")
        if comp_details is not None:
            metrics["reasoning_tokens"] = get_val(comp_details, "reasoning_tokens")

        return metrics


def create_deepseek_adapter(
    client: Optional[Any] = None,
    api_key: Optional[str] = None,
    base_url: str = "https://api.deepseek.com",
    rpm_limit: float = 300.0,
    tpm_limit: Optional[float] = 60_000.0,
    max_retries: int = 4,
    fallback: Optional[Callable[..., Any]] = None,
) -> ResilientOpenAICompatible:
    """Create a tuned FlowGuard adapter for DeepSeek APIs."""
    if client is None:
        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=api_key or "EMPTY", base_url=base_url)
        except ImportError:
            raise ImportError(
                "openai package is required when client is None. Install it via: pip install openai"
            )

    return ResilientOpenAICompatible(
        client=client,
        rpm_limit=rpm_limit,
        tpm_limit=tpm_limit,
        max_retries=max_retries,
        fallback=fallback,
        service_name="deepseek",
        name="deepseek-resilient-pipeline",
    )


def create_vllm_adapter(
    client: Optional[Any] = None,
    base_url: str = "http://localhost:8000/v1",
    api_key: str = "EMPTY",
    rpm_limit: float = 600.0,
    tpm_limit: Optional[float] = 120_000.0,
    max_retries: int = 3,
    fallback: Optional[Callable[..., Any]] = None,
) -> ResilientOpenAICompatible:
    """Create a tuned FlowGuard adapter for self-hosted vLLM / SGLang clusters."""
    if client is None:
        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        except ImportError:
            raise ImportError(
                "openai package is required when client is None. Install it via: pip install openai"
            )

    return ResilientOpenAICompatible(
        client=client,
        rpm_limit=rpm_limit,
        tpm_limit=tpm_limit,
        max_retries=max_retries,
        fallback=fallback,
        service_name="vllm",
        name="vllm-resilient-pipeline",
    )


def create_ollama_adapter(
    client: Optional[Any] = None,
    base_url: str = "http://localhost:11434/v1",
    api_key: str = "ollama",
    rpm_limit: float = 120.0,
    tpm_limit: Optional[float] = 40_000.0,
    max_retries: int = 2,
    fallback: Optional[Callable[..., Any]] = None,
) -> ResilientOpenAICompatible:
    """Create a tuned FlowGuard adapter for local Ollama endpoints."""
    if client is None:
        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        except ImportError:
            raise ImportError(
                "openai package is required when client is None. Install it via: pip install openai"
            )

    return ResilientOpenAICompatible(
        client=client,
        rpm_limit=rpm_limit,
        tpm_limit=tpm_limit,
        max_retries=max_retries,
        fallback=fallback,
        service_name="ollama",
        name="ollama-resilient-pipeline",
    )
