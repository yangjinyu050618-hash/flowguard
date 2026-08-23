"""
Example 02: Resilient OpenAI Client Throttling
==============================================
Demonstrates using ResilientOpenAI to throttle OpenAI chat completions
with dual-axis RPM (Requests Per Minute) and TPM (Tokens Per Minute) limiters.
"""

import asyncio
import os
import sys

# Allow running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from flowguard.adapters import ResilientOpenAI


class MockAsyncOpenAI:
    """Mock OpenAI AsyncClient for demo purposes without requiring an actual API key."""

    class chat:
        class completions:
            @staticmethod
            async def create(**kwargs):
                await asyncio.sleep(0.05)
                model = kwargs.get("model", "gpt-4o")
                prompt = kwargs.get("messages", [{}])[-1].get("content", "")
                return {
                    "id": "chatcmpl-mock",
                    "model": model,
                    "choices": [{"message": {"content": f"Resilient AI response to: {prompt}"}}],
                }


async def main() -> None:
    raw_client = MockAsyncOpenAI()

    # Wrap client with 500 RPM and 60,000 TPM limit with auto-retries
    client = ResilientOpenAI(
        client=raw_client,
        rpm_limit=500.0,  # 500 requests per minute
        tpm_limit=60_000.0,  # 60,000 tokens per minute
        max_retries=4,  # Retry on 429/503
    )

    print("--- Sending Chat Requests with TPM Throttling ---")
    prompts = [
        "Explain quantum computing in one sentence.",
        "What is asyncio in Python?",
        "How does token bucket rate limiting work?",
    ]

    for prompt in prompts:
        response = await client.create_chat_completion(
            estimated_tokens=500,
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
        )
        content = response["choices"][0]["message"]["content"]
        print(f"Prompt: {prompt}")
        print(f"Response: {content}\n")


if __name__ == "__main__":
    asyncio.run(main())
