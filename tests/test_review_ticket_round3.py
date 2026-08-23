"""Comprehensive tests covering all 7 items from Review Ticket Round 3."""

import asyncio
import pytest
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState
from flowguard.core.pipeline import FlowGuard
from flowguard.core.retry import ExponentialBackoff
from flowguard.adapters.openai_adapter import ResilientOpenAI
from flowguard.adapters.httpx_adapter import (
    ResilientHTTPClient,
    TransientHTTPError,
    PermanentHTTPError,
)
from flowguard.metrics.collector import MetricsCollector
from flowguard.metrics.exporter import export_prometheus
from flowguard import __version__


# -------------------------------------------------------------------------
# Item 1: HALF_OPEN probe cancellation releases slot
# -------------------------------------------------------------------------
async def test_half_open_probe_cancellation_releases_slot():
    cb = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=0.05,
        half_open_success_threshold=2,
        half_open_max_probes=1,
    )
    await cb.record_failure(RuntimeError("trip"))
    assert cb.state == CircuitState.OPEN

    await asyncio.sleep(0.06)
    pipeline = FlowGuard(name="cb-cancel-pipe", circuit_breaker=cb)

    probe_started = asyncio.Event()

    async def hanging_probe():
        probe_started.set()
        await asyncio.sleep(10.0)

    t = asyncio.create_task(pipeline.execute(hanging_probe))
    await probe_started.wait()
    assert cb._half_open_inflight == 1

    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert cb._half_open_inflight == 0
    assert cb.state == CircuitState.HALF_OPEN

    async def fast_probe():
        return "probe_ok"

    res = await pipeline.execute(fast_probe)
    assert res == "probe_ok"


# -------------------------------------------------------------------------
# Item 2: OpenAI TPM per-attempt consumption on retry
# -------------------------------------------------------------------------
async def test_openai_tpm_per_attempt_accounting_on_retry():
    class FlakyOpenAI:
        def __init__(self):
            self.attempts = 0
            self.chat = self

        class completions:
            @staticmethod
            async def create(**kwargs):
                pass

    flaky = FlakyOpenAI()

    async def mock_create(**kwargs):
        flaky.attempts += 1
        if flaky.attempts < 3:
            raise TransientHTTPError(503, "Service Unavailable")
        return {"choices": [{"message": {"content": "ok"}}]}

    flaky.completions.create = mock_create

    adapter = ResilientOpenAI(
        client=flaky, rpm_limit=600, tpm_limit=60_000, tpm_burst_capacity=10_000, max_retries=4
    )
    # Fast backoff for testing
    adapter.retry_policy.backoff = ExponentialBackoff(base_delay=0.001, jitter="none")
    adapter.tpm_limiter.rate = 0.001
    adapter.tpm_limiter.tokens = 10_000.0

    res = await adapter.create_chat_completion(estimated_tokens=100, model="gpt-4o")
    assert res["choices"][0]["message"]["content"] == "ok"
    assert flaky.attempts == 3

    consumed = 10_000.0 - adapter.tpm_limiter.current_tokens
    assert consumed == pytest.approx(300.0, abs=1.0)


# -------------------------------------------------------------------------
# Item 3: Error classification (Transient vs Permanent)
# -------------------------------------------------------------------------
async def test_httpx_adapter_error_classification():
    class MockResponse:
        def __init__(self, status_code):
            self.status_code = status_code
            self.text = f"status-{status_code}"

    class MockHTTPX:
        def __init__(self):
            self.calls = 0

        async def get(self, url, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return MockResponse(503)
            return MockResponse(200)

    mock_client = MockHTTPX()
    pipe = FlowGuard(name="httpx-pipe")
    resilient_http = ResilientHTTPClient(mock_client, pipe)

    with pytest.raises(TransientHTTPError):
        await resilient_http.get("https://api.test/retryable")

    class AuthFailHTTPX:
        async def get(self, url, **kwargs):
            return MockResponse(401)

    auth_client = ResilientHTTPClient(AuthFailHTTPX(), pipe)
    with pytest.raises(PermanentHTTPError):
        await auth_client.get("https://api.test/unauthorized")


# -------------------------------------------------------------------------
# Item 4: max_retries semantics (0, 1, 4)
# -------------------------------------------------------------------------
async def test_openai_max_retries_semantics():
    class AlwaysFailing:
        def __init__(self):
            self.calls = 0
            self.chat = self

        class completions:
            pass

    async def fail_call(**kw):
        failing.calls += 1
        raise TransientHTTPError(503, "Service Unavailable")

    # 1. max_retries = 0 -> exactly 1 attempt
    failing = AlwaysFailing()
    failing.completions.create = fail_call
    adapter0 = ResilientOpenAI(client=failing, max_retries=0)
    with pytest.raises(Exception):
        await adapter0.create_chat_completion(estimated_tokens=10)
    assert failing.calls == 1

    # 2. max_retries = 1 -> exactly 2 attempts (1 initial + 1 retry)
    failing = AlwaysFailing()
    failing.completions.create = fail_call
    adapter1 = ResilientOpenAI(client=failing, max_retries=1)
    adapter1.retry_policy.backoff = ExponentialBackoff(base_delay=0.001, jitter="none")
    with pytest.raises(Exception):
        await adapter1.create_chat_completion(estimated_tokens=10)
    assert failing.calls == 2

    # 3. max_retries = 4 -> exactly 5 attempts (1 initial + 4 retries)
    failing = AlwaysFailing()
    failing.completions.create = fail_call
    adapter4 = ResilientOpenAI(client=failing, max_retries=4)
    adapter4.retry_policy.backoff = ExponentialBackoff(base_delay=0.001, jitter="none")
    with pytest.raises(Exception):
        await adapter4.create_chat_completion(estimated_tokens=10)
    assert failing.calls == 5


# -------------------------------------------------------------------------
# Item 5: Low RPM quota burst capacity
# -------------------------------------------------------------------------
def test_low_rpm_burst_capacity():
    class Dummy:
        pass

    adapter = ResilientOpenAI(client=Dummy(), rpm_limit=1.0)
    assert adapter.rpm_limiter.capacity == 1.0


# -------------------------------------------------------------------------
# Item 6: Prometheus label escaping
# -------------------------------------------------------------------------
def test_prometheus_label_escaping():
    collector = MetricsCollector(name='bad"pipeline\nname\\test')
    collector.record_failure(0.05, 'Bad"Error\nType\\')
    collector.record_rejected("rate_limit")

    prom_text = export_prometheus(collector)
    assert r"\nname" in prom_text
    assert r"bad\"pipeline" in prom_text
    assert r"Bad\"Error\nType\\" in prom_text


# -------------------------------------------------------------------------
# Item 7: Version synchronization
# -------------------------------------------------------------------------
def test_version_sync():
    import tomllib

    toml_path = r"C:\Users\24389\.gemini\antigravity\scratch\flowguard\pyproject.toml"
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)
    assert __version__ == data["project"]["version"]
    assert __version__ == "0.2.2"
