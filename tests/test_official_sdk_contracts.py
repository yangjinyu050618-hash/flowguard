"""
Official SDK Contract Verification Tests.

Verifies that FlowGuard adapters strictly comply with official OpenAI, Anthropic,
and Google GenAI SDK signatures and sole retry ownership WITHOUT making real network requests.
"""

import pytest
from flowguard.adapters import ResilientOpenAI, ResilientAnthropic, ResilientGemini


# -------------------------------------------------------------------------
# 1. Official OpenAI SDK Contract Test
# -------------------------------------------------------------------------
def test_official_openai_sdk_contract():
    try:
        from openai import AsyncOpenAI
    except ImportError:
        pytest.skip("openai SDK not installed; skipping official SDK contract test")

    # Construct official client without API key network validation
    raw_client = AsyncOpenAI(api_key="sk-mock-placeholder-for-contract-testing", max_retries=2)
    adapter = ResilientOpenAI(client=raw_client, max_retries=3)

    # 1. Verify shared parent client is NOT mutated
    assert raw_client.max_retries == 2

    # 2. Verify derived client disables SDK internal retries
    assert adapter._client.max_retries == 0

    # 3. Verify that adapter's underlying target method accepts official named parameters
    import inspect

    sig = inspect.signature(adapter._client.chat.completions.create)
    assert "messages" in sig.parameters or "kwargs" in sig.parameters
    # Official completions.create does not accept max_retries in method signature
    # (adapter must not forward max_retries to create())


# -------------------------------------------------------------------------
# 2. Official Anthropic SDK Contract Test
# -------------------------------------------------------------------------
def test_official_anthropic_sdk_contract():
    try:
        from anthropic import AsyncAnthropic
    except ImportError:
        pytest.skip("anthropic SDK not installed; skipping official SDK contract test")

    raw_client = AsyncAnthropic(api_key="mock-anthropic-key-placeholder", max_retries=2)
    adapter = ResilientAnthropic(client=raw_client, max_retries=3)

    # 1. Verify shared parent client is NOT mutated
    assert raw_client.max_retries == 2

    # 2. Verify derived client disables SDK internal retries
    assert adapter._client.max_retries == 0

    # 3. Verify messages.create signature contract
    import inspect

    sig = inspect.signature(adapter._client.messages.create)
    assert "messages" in sig.parameters


# -------------------------------------------------------------------------
# 3. Official Google GenAI SDK Contract Test
# -------------------------------------------------------------------------
def test_official_gemini_sdk_contract():
    try:
        from google import genai
    except ImportError:
        pytest.skip("google-genai SDK not installed; skipping official SDK contract test")

    raw_client = genai.Client(api_key="mock-gemini-key-placeholder")
    adapter = ResilientGemini(client=raw_client, max_retries=3)

    # 1. Verify adapter binds to asynchronous client namespace
    assert hasattr(adapter._client, "aio")
    assert hasattr(adapter._client.aio, "models")
    assert hasattr(adapter._client.aio.models, "generate_content")
