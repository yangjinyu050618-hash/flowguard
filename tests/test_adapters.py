import asyncio
import pytest
from flowguard.adapters.openai_adapter import ResilientOpenAI
from flowguard.adapters.httpx_adapter import ResilientHTTPClient
from flowguard.core.pipeline import FlowGuard


class DummyCompletionChoice:
    def __init__(self, content: str):
        self.message = type("Message", (), {"content": content})


class DummyCompletionResponse:
    def __init__(self, content: str):
        self.choices = [DummyCompletionChoice(content)]


class DummyChatCompletions:
    async def create(self, **kwargs):
        await asyncio.sleep(0.01)
        return DummyCompletionResponse("OpenAI mock response")


class DummyAsyncOpenAI:
    def __init__(self):
        self.chat = type("Chat", (), {"completions": DummyChatCompletions()})()


@pytest.mark.asyncio
async def test_openai_adapter():
    mock_client = DummyAsyncOpenAI()
    resilient_client = ResilientOpenAI(
        client=mock_client, rpm_limit=600, tpm_limit=50000, max_retries=2
    )

    resp = await resilient_client.create_chat_completion(
        estimated_tokens=100, model="gpt-4o", messages=[{"role": "user", "content": "hi"}]
    )
    assert resp.choices[0].message.content == "OpenAI mock response"


@pytest.mark.asyncio
async def test_httpx_adapter():
    class DummyHTTPX:
        async def get(self, url, **kwargs):
            return type("Response", (), {"status_code": 200, "text": "OK"})()

    guard = FlowGuard(name="httpx-test")
    client = ResilientHTTPClient(DummyHTTPX(), guard)
    resp = await client.get("https://api.example.com/health")
    assert resp.status_code == 200
