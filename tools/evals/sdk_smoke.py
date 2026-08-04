"""Smoke test: can the selected coding-agent provider drive a session?"""

import anyio
from pathlib import Path

try:
    from .provider_adapter import EvalRequest, run_eval
except ImportError:  # direct ``python tools/evals/sdk_smoke.py`` execution
    from provider_adapter import EvalRequest, run_eval

REPO = Path(__file__).resolve().parents[2]


async def main():
    result = await run_eval(
        EvalRequest(
            prompt="Reply with exactly: SMOKE-OK",
            cwd=REPO,
            claude_model="claude-haiku-4-5-20251001",
            project_settings=False,
            sandbox="read-only",
            system_prompt="You are a smoke test. Obey exactly.",
            allowed_tools=[],
            max_turns=1,
        )
    )
    print("assistant:", result.final_text.strip())
    print(
        "cost_usd:",
        result.cost_usd,
        "turns:",
        result.num_turns,
        "is_error:",
        result.is_error,
    )
    if result.error:
        print("error:", result.error)


if __name__ == "__main__":
    anyio.run(main)
