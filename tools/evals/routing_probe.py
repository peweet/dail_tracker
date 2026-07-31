"""First real CLAUDE.md-steering benchmark via claude-agent-sdk.

Two probes, each a question the CLAUDE.md routing table explicitly maps to a
cheap MCP navigation tool. PASS = a navigation tool fires before any raw
Read/Grep/Glob. This measures what the config CAUSES, not what past sessions did.
"""

import json

import anyio
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ResultMessage,
    ToolUseBlock,
    query,
)

NAV = {
    "mcp__dail-tracker__describe_dataset",
    "mcp__dail-tracker__search_project",
    "mcp__dail-tracker__list_datasets",
    "mcp__dail-tracker__outline",
    "mcp__dail-tracker__view_deps",
}
RAW = {"Read", "Grep", "Glob"}

PROBES = [
    (
        "data-shape",
        "What columns and grain does the procurement awarded dataset have? Just tell me, don't change anything.",
    ),
    ("where-lives", "Which dataset or view covers ministerial diaries? Just point me at it."),
    # Capability control: not a steering test — it ORDERS the tool call. If this
    # fails, the MCP tools are unavailable in headless runs and the probes above
    # measured environment, not steering.
    (
        "capability",
        "Call the dail-tracker MCP tool describe_dataset for any procurement dataset and report one line of its output. If you cannot find or call that tool, reply exactly TOOL-UNAVAILABLE.",
    ),
]


PROJ = r"C:\Users\pglyn\PycharmProjects\dail_extractor"


async def run_probe(name: str, prompt: str) -> dict:
    opts = ClaudeAgentOptions(
        model="claude-sonnet-5",
        max_turns=8,
        cwd=PROJ,
        setting_sources=["project"],
        permission_mode="bypassPermissions",
        # The SDK does not load the project's .mcp.json (and its
        # ${workspaceFolder} placeholders), so the server must be wired
        # explicitly or every probe measures tool-absence, not steering.
        mcp_servers={
            "dail-tracker": {
                "command": PROJ + r"\.venv\Scripts\python.exe",
                "args": [PROJ + r"\mcp_server\server.py"],
                "env": {"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
            }
        },
    )
    calls = []
    cost = None
    err = None
    try:
        async for msg in query(prompt=prompt, options=opts):
            if isinstance(msg, AssistantMessage):
                for b in msg.content:
                    if isinstance(b, ToolUseBlock):
                        calls.append(b.name)
            if isinstance(msg, ResultMessage):
                cost = msg.total_cost_usd
    except Exception as e:  # keep the partial tool sequence — it's the finding
        err = f"{type(e).__name__}: {e}"
    first_nav = next((i for i, c in enumerate(calls) if c in NAV), None)
    first_raw = next((i for i, c in enumerate(calls) if c in RAW), None)
    ok = first_nav is not None and (first_raw is None or first_nav < first_raw)
    out = {"probe": name, "pass": ok, "tool_sequence": calls, "cost_usd": cost}
    if err:
        out["error"] = err
    return out


async def main():
    import sys

    # e.g. `routing_probe.py capability` runs one probe. Args that match no probe
    # name are IGNORED (all probes run): promptfoo's exec provider always appends
    # the prompt as a trailing argv, which filtered everything to 0/0 on
    # 2026-07-31 — an unknown arg must not silently select nothing.
    known = {n for n, _ in PROBES}
    wanted = [a for a in sys.argv[1:] if a in known]
    ignored = [a for a in sys.argv[1:] if a not in known]
    if ignored:
        print(f"NOTE: ignoring non-probe args {ignored!r}; probes: {sorted(known)}")
    probes = [(n, p) for n, p in PROBES if not wanted or n in wanted]
    results = []
    for name, prompt in probes:
        try:
            results.append(await run_probe(name, prompt))
        except Exception as e:
            results.append({"probe": name, "error": f"{type(e).__name__}: {e}"})
        print(json.dumps(results[-1]))
    passed = sum(1 for r in results if r.get("pass"))
    print(f"SUMMARY: {passed}/{len(results)} probes chose navigation-first")


anyio.run(main)
