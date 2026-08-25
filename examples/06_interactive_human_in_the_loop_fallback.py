"""
Example 06: Human-in-the-Loop Interactive Fallback Model Selection
==================================================================
Demonstrates how FlowGuard prompts the user or decision-engine
when a primary LLM (e.g. GPT-4o / GPT-5) trips a circuit breaker,
allowing the user to choose an alternative model dynamically!
"""

import asyncio
import os
import sys

# Allow running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from flowguard import guard, ChoiceFallback, FallbackContext


# Define Candidate Alternate Models
async def call_claude_35(prompt: str) -> str:
    print("  [Executing Route 1] Calling Claude 3.5 Sonnet...")
    return f"[Claude 3.5 Sonnet Output]: Solved '{prompt}' with high precision."


async def call_deepseek_r1(prompt: str) -> str:
    print("  [Executing Route 2] Calling DeepSeek R1...")
    return f"[DeepSeek R1 Output]: Deep reasoned response to '{prompt}'."


async def call_gemini_flash(prompt: str) -> str:
    print("  [Executing Route 3] Calling Gemini 2.5 Flash...")
    return f"[Gemini 2.5 Flash Output]: Lightning fast response to '{prompt}'."


# Interactive Selector Callback (queries user / UI / agent)
async def ask_user_for_model_choice(ctx: FallbackContext, available_options: list[str]) -> str:
    exc = ctx.exception
    print(f"\n[FlowGuard Decision Trigger] Primary model failed with: {type(exc).__name__} ({exc})")
    print(f"Request prompt: '{ctx.args[0]}'")
    print(f"Available fallback options: {available_options} (or return None to abort)")

    # In a real CLI: choice = input("Please select a candidate or press Enter to abort: ") or None
    # In a Web App: await websocket_user_selection_modal(...)
    # For automated demo: select 'deepseek-r1'
    simulated_choice = "deepseek-r1"
    print(f"User selected: '{simulated_choice}'\n")
    return simulated_choice


# Setup ChoiceFallback router
interactive_fallback = ChoiceFallback(
    candidates={
        "claude-3.5-sonnet": call_claude_35,
        "deepseek-r1": call_deepseek_r1,
        "gemini-2.5-flash": call_gemini_flash,
    },
    selector=ask_user_for_model_choice,
)


@guard(
    name="gpt-primary-pipeline",
    failure_threshold=1,
    recovery_timeout=30.0,
    fallback=interactive_fallback,
)
async def ask_primary_gpt(prompt: str) -> str:
    print(f"Calling primary model GPT-4o with prompt: '{prompt}'...")
    # Simulate a circuit-tripping outage on the primary model
    raise ConnectionResetError("GPT-4o API 503 Service Unavailable")


async def main() -> None:
    print("--- Starting Interactive Human-in-the-Loop Fallback Demo ---")
    final_output = await ask_primary_gpt("Explain quantum entanglement in 2 sentences")
    print(f"Final System Output:\n{final_output}")


if __name__ == "__main__":
    asyncio.run(main())
