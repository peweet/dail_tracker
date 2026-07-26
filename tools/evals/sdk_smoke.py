"""Smoke test: can claude-agent-sdk drive a session on this machine?"""

import anyio
from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, ResultMessage, TextBlock, query


async def main():
    opts = ClaudeAgentOptions(
        model="claude-haiku-4-5-20251001",
        max_turns=1,
        allowed_tools=[],
        system_prompt="You are a smoke test. Obey exactly.",
    )
    async for msg in query(prompt="Reply with exactly: SMOKE-OK", options=opts):
        if isinstance(msg, AssistantMessage):
            for b in msg.content:
                if isinstance(b, TextBlock):
                    print("assistant:", b.text.strip())
        if isinstance(msg, ResultMessage):
            print("cost_usd:", msg.total_cost_usd, "turns:", msg.num_turns, "is_error:", msg.is_error)


anyio.run(main)
