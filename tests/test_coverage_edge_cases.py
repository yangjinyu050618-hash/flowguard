"""Edge-case tests pushing line coverage above 95%."""

from flowguard.metrics.collector import MetricsCollector
from flowguard.adapters.httpx_adapter import ResilientHTTPClient
from flowguard.adapters.openai_adapter import ResilientOpenAI
from flowguard.core.pipeline import FlowGuard, guard


def test_metrics_collector_reset_and_truncation():
    collector = MetricsCollector(name="truncation-test", max_failure_types=2)
    # Exceed 10000 latencies to test slice truncation
    for _ in range(10005):
        collector.record_success(0.001)

    assert len(collector.latencies) <= 5005

    # Exceed max failure types to test 'other' bin
    collector.record_failure(0.01, "ErrorA")
    collector.record_failure(0.01, "ErrorB")
    collector.record_failure(0.01, "ErrorC")

    summary = collector.get_summary()
    assert summary["failure_by_type"]["other"] == 1

    collector.reset()
    assert collector.total_requests == 0
    assert len(collector.latencies) == 0


async def test_httpx_adapter_post_and_request():
    class MockHTTPX:
        async def post(self, url, **kw):
            return "post-ok"

        async def request(self, method, url, **kw):
            return f"{method}-ok"

    guard_pipe = FlowGuard(name="httpx-full")
    client = ResilientHTTPClient(MockHTTPX(), guard_pipe)

    assert await client.post("https://api.test/post") == "post-ok"
    assert await client.request("PUT", "https://api.test/put") == "PUT-ok"


async def test_openai_adapter_without_tpm():
    class MockChat:
        async def create(self, **kw):
            return "chat-ok"

    class MockOpenAI:
        def __init__(self):
            self.chat = type("Chat", (), {"completions": MockChat()})()

    # Create without TPM limit
    client = ResilientOpenAI(client=MockOpenAI(), rpm_limit=300, tpm_limit=None)
    res = await client.create_chat_completion(messages=[])
    assert res == "chat-ok"


async def test_guard_factory_with_bulkhead():
    @guard(name="full-guard", rate_per_sec=100.0, max_retries=1, max_concurrent=5)
    async def worker_fn(val: int):
        return val * 2

    assert await worker_fn(21) == 42
