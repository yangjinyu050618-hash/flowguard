"""
Official SDK Contract Verification Tests.

Verifies that FlowGuard adapters strictly comply with official OpenAI, Anthropic,
and Google GenAI SDK signatures, execute public adapter methods end-to-end,
and enforce sole retry ownership WITHOUT making real network requests.
"""

from unittest.mock import AsyncMock
import pytest
from flowguard.adapters import ResilientAnthropic, ResilientGemini, ResilientOpenAI


# -------------------------------------------------------------------------
# 1. Official OpenAI SDK Contract Test (End-to-End Execution via MockTransport)
# -------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_official_openai_sdk_contract():
    try:
        import httpx
        from openai import AsyncOpenAI
        from openai.types.chat import ChatCompletion
    except ImportError:
        pytest.skip("openai or httpx not installed; skipping official SDK contract test")

    call_count = 0

    def openai_mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1

        # Simulate 503 transient error on first attempt, 200 on retry
        if call_count == 1:
            return httpx.Response(
                503, json={"error": {"message": "Service Unavailable", "type": "server_error"}}
            )

        body = {
            "id": "chatcmpl-mock-test-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4o",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "Mocked response from OpenAI!"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 15, "total_tokens": 25},
        }
        return httpx.Response(200, json=body)

    raw_client = AsyncOpenAI(
        api_key="sk-mock-key-contract-test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(openai_mock_handler)),
        max_retries=2,
    )
    adapter = ResilientOpenAI(client=raw_client, max_retries=3)

    # 1. Verify parent client is not mutated & derived client has max_retries=0
    assert raw_client.max_retries == 2
    assert adapter._client.max_retries == 0

    # 2. Actually invoke public adapter method end-to-end
    res = await adapter.create_chat_completion(
        model="gpt-4o",
        messages=[{"role": "user", "content": "Say hello!"}],
        estimated_tokens=50,
    )

    # 3. Verify response deserialization into official OpenAI model
    assert isinstance(res, ChatCompletion)
    assert res.choices[0].message.content == "Mocked response from OpenAI!"
    assert res.usage.total_tokens == 25
    # Verified FlowGuard retried the 503 without SDK internal retry amplification
    assert call_count == 2


# -------------------------------------------------------------------------
# 2. Official Anthropic SDK Contract Test (End-to-End Execution via Strict Mock)
# -------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_official_anthropic_sdk_contract():
    try:
        from anthropic import AsyncAnthropic
        from anthropic.types import Message, TextBlock, Usage
    except ImportError:
        pytest.skip("anthropic SDK not installed; skipping official SDK contract test")

    raw_client = AsyncAnthropic(api_key="mock-anthropic-key", max_retries=2)
    adapter = ResilientAnthropic(client=raw_client, max_retries=3)

    # 1. Verify parent client is not mutated & derived client has max_retries=0
    assert raw_client.max_retries == 2
    assert adapter._client.max_retries == 0

    mock_msg = Message(
        id="msg_mock_001",
        type="message",
        role="assistant",
        content=[TextBlock(type="text", text="Mocked response from Claude!")],
        model="claude-3-5-sonnet-20241022",
        stop_reason="end_turn",
        usage=Usage(input_tokens=12, output_tokens=18),
    )

    # Use strict spec mirroring official messages.create signature
    adapter._client.messages.create = AsyncMock(
        spec=raw_client.messages.create,
        return_value=mock_msg,
    )

    # 2. Actually invoke public adapter method
    res = await adapter.create_message(
        model="claude-3-5-sonnet-20241022",
        max_tokens=100,
        messages=[{"role": "user", "content": "Hello Claude!"}],
        estimated_tokens=30,
    )

    # 3. Verify official response structure
    assert isinstance(res, Message)
    assert res.content[0].text == "Mocked response from Claude!"
    assert res.usage.input_tokens == 12
    # Verify no illegal max_retries argument was forwarded to messages.create
    _, kwargs = adapter._client.messages.create.call_args
    assert "max_retries" not in kwargs


# -------------------------------------------------------------------------
# 3. Official Google GenAI SDK Contract Test (End-to-End Execution via Strict Mock)
# -------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_official_gemini_sdk_contract():
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        pytest.skip("google-genai SDK not installed; skipping official SDK contract test")

    raw_client = genai.Client(api_key="mock-gemini-key")
    adapter = ResilientGemini(client=raw_client, max_retries=3)

    mock_resp = types.GenerateContentResponse(
        candidates=[
            types.Candidate(
                content=types.Content(
                    parts=[types.Part.from_text(text="Mocked response from Gemini!")],
                    role="model",
                )
            )
        ]
    )

    adapter._client.aio.models.generate_content = AsyncMock(
        spec=raw_client.aio.models.generate_content,
        return_value=mock_resp,
    )

    # Actually invoke public adapter method
    res = await adapter.generate_content(
        model="gemini-2.5-flash",
        contents="Hello Gemini!",
        estimated_tokens=40,
    )

    assert isinstance(res, types.GenerateContentResponse)
    assert res.candidates[0].content.parts[0].text == "Mocked response from Gemini!"
    # Verify model and contents passed correctly
    _, kwargs = adapter._client.aio.models.generate_content.call_args
    assert kwargs["model"] == "gemini-2.5-flash"
    assert kwargs["contents"] == "Hello Gemini!"
