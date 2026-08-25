"""
Example 05: Multi-LLM Resilience (OpenAI, Anthropic Claude & Google Gemini)
==========================================================================
Demonstrates unified rate limiting, retry backoff, and cross-model failover.
"""

import asyncio
import os
import sys

# Allow running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from flowguard.adapters import ResilientOpenAI, ResilientAnthropic, ResilientGemini


# Mock AI clients
class MockOpenAI:
    class chat:
        class completions:
            @staticmethod
            async def create(**kwargs):
                return {"choices": [{"message": {"content": "OpenAI: 42"}}]}


class MockAnthropic:
    class messages:
        @staticmethod
        async def create(**kwargs):
            return {"content": [{"text": "Claude: The meaning of life is 42."}]}


class MockGemini:
    class aio:
        class models:
            @staticmethod
            async def generate_content(model: str, contents: str, **kwargs):
                return type("Resp", (), {"text": "Gemini: 42 is the answer."})()


async def main() -> None:
    openai_client = ResilientOpenAI(MockOpenAI(), rpm_limit=500.0, tpm_limit=60_000.0)
    claude_client = ResilientAnthropic(MockAnthropic(), rpm_limit=300.0, tpm_limit=40_000.0)
    gemini_client = ResilientGemini(MockGemini(), rpm_limit=300.0, tpm_limit=60_000.0)

    print("--- Querying OpenAI GPT-4o ---")
    res1 = await openai_client.create_chat_completion(
        estimated_tokens=100, model="gpt-4o", messages=[{"role": "user", "content": "Query"}]
    )
    print(res1["choices"][0]["message"]["content"])

    print("\n--- Querying Anthropic Claude 3.5 Sonnet ---")
    res2 = await claude_client.create_message(
        estimated_tokens=150,
        model="claude-3-5-sonnet",
        messages=[{"role": "user", "content": "Query"}],
    )
    print(res2["content"][0]["text"])

    print("\n--- Querying Google Gemini 2.5 Flash ---")
    res3 = await gemini_client.generate_content(
        model="gemini-2.5-flash", contents="Query", estimated_tokens=120
    )
    print(res3.text)


if __name__ == "__main__":
    asyncio.run(main())
