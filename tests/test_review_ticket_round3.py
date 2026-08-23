"""Comprehensive tests covering all items from Review Ticket Round 3."""

import asyncio
import os
import re
import pytest
from flowguard.core.circuit_breaker import CircuitBreaker, CircuitState
from flowguard.core.pipeline import FlowGuard
from flowguard.core.retry import ExponentialBackoff
from flowguard.adapters.openai_adapter import ResilientOpenAI
from flowguard.adapters.httpx_adapter import ResilientHTTPClient
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
    adapter.retry_policy.backoff = ExponentialBackoff(base_delay=0.001, jitter="none")
    adapter.tpm_limiter.rate = 0.001
    adapter.tpm_limiter.tokens = 10_000.0

    res = await adapter.create_chat_completion(estimated_tokens=100, model="gpt-4o")
    assert res["choices"][0]["message"]["content"] == "ok"
    assert flaky.attempts == 3

    consumed = 10_000.0 - adapter.tpm_limiter.current_tokens
    assert consumed == pytest.approx(300.0, abs=1.0)


# -------------------------------------------------------------------------
# Item 3: Error classification (401 fatal, 5xx retryable, 400 fatal)
# -------------------------------------------------------------------------
async def test_httpx_adapter_all_status_codes():
    class MockResponse:
        def __init__(self, status_code):
            self.status_code = status_code
            self.text = f"code-{status_code}"

    class MockHTTPX:
        async def get(self, url, **kwargs):
            code = int(url.split("/")[-1])
            return MockResponse(code)

    pipe = FlowGuard(name="httpx-test-pipe")
    client = ResilientHTTPClient(MockHTTPX(), pipe)

    # 1. 401 must raise PermanentHTTPError (fatal)
    with pytest.raises(PermanentHTTPError) as exc:
        await client.get("https://api.test/401")
    assert exc.value.status_code == 401

    # 2. 501 / unlisted 5xx must raise TransientHTTPError (retryable)
    with pytest.raises(TransientHTTPError) as exc:
        await client.get("https://api.test/501")
    assert exc.value.status_code == 501

    # 3. 503 must raise TransientHTTPError
    with pytest.raises(TransientHTTPError) as exc:
        await client.get("https://api.test/503")
    assert exc.value.status_code == 503

    # 4. 200 must succeed
    resp = await client.get("https://api.test/200")
    assert resp.status_code == 200


async def test_openai_permanent_errors_not_retried():
    class AuthError(Exception):
        status_code = 401

    class OpenAIAuthFail:
        def __init__(self):
            self.calls = 0
            self.chat = self

        class completions:
            pass

    async def fail_auth(**kw):
        auth_mock.calls += 1
        raise AuthError("Invalid API key")

    auth_mock = OpenAIAuthFail()
    auth_mock.completions.create = fail_auth

    adapter = ResilientOpenAI(client=auth_mock, max_retries=4)
    if adapter.retry_policy:
        adapter.retry_policy.backoff = ExponentialBackoff(base_delay=0.001, jitter="none")

    with pytest.raises(AuthError):
        await adapter.create_chat_completion(estimated_tokens=50)

    # Must fail immediately without retrying
    assert auth_mock.calls == 1


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
    if adapter1.retry_policy:
        adapter1.retry_policy.backoff = ExponentialBackoff(base_delay=0.001, jitter="none")
    with pytest.raises(Exception):
        await adapter1.create_chat_completion(estimated_tokens=10)
    assert failing.calls == 2

    # 3. max_retries = 4 -> exactly 5 attempts (1 initial + 4 retries)
    failing = AlwaysFailing()
    failing.completions.create = fail_call
    adapter4 = ResilientOpenAI(client=failing, max_retries=4)
    if adapter4.retry_policy:
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
    assert "\\nname" in prom_text
    assert 'bad\\"pipeline' in prom_text
    assert 'Bad\\"Error\\nType\\\\' in prom_text


# -------------------------------------------------------------------------
# Item 7: Version synchronization (Python 3.9-3.13 compatible, no hardcoded path)
# -------------------------------------------------------------------------
def test_version_sync():
    toml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "pyproject.toml"))
    with open(toml_path, "r", encoding="utf-8") as f:
        content = f.read()
    match = re.search(r'version\s*=\s*"([^"]+)"', content)
    assert match is not None, "Could not find project.version in pyproject.toml"
    expected_version = match.group(1)
    assert __version__ == expected_version
    assert __version__ == "0.2.2"


# -------------------------------------------------------------------------
# Item 8: Public exception constructors backward compatibility (default args)
# -------------------------------------------------------------------------
def test_exception_constructors_default_args():
    e1 = RateLimitExceededError()
    assert e1.retry_after is None
    assert "Rate limit" in str(e1)

    e2 = CircuitBreakerOpenError()
    assert e2.reset_timeout is None
    assert "OPEN" in str(e2)

    e3 = BulkheadFullError()
    assert "Bulkhead" in str(e3)

    e4 = MaxRetriesExceededError()
    assert e4.attempts == 0
    assert e4.last_exception is None

    e5 = FlowGuardError()
    assert "FlowGuard" in str(e5)

    e6 = HTTPStatusError()
    assert e6.status_code == 500

    e7 = TransientHTTPError()
    assert e7.status_code == 500

    e8 = PermanentHTTPError()
    assert e8.status_code == 500
