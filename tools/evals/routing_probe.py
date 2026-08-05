"""Project-instruction steering benchmark via the selected agent provider.

Two probes ask questions the repository routing table maps to cheap MCP
navigation tools. PASS means a navigation tool fires before raw Read/Grep/Glob.
The capability control distinguishes steering failure from missing MCP wiring.
"""

import json
import sys
from pathlib import Path

import anyio

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from tools.evals.provider_adapter import EvalRequest, dail_tracker_mcp, run_eval  # noqa: E402

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
    (
        "capability",
        "Call the dail-tracker MCP tool describe_dataset for any procurement dataset and report one line of its "
        "output. If you cannot find or call that tool, reply exactly TOOL-UNAVAILABLE.",
    ),
]

PROJ = str(Path(__file__).resolve().parents[2])


async def run_probe(name: str, prompt: str) -> dict:
    calls: list[str] = []
    cost = None
    err = None
    provider = None
    model = None
    try:
        result = await run_eval(
            EvalRequest(
                prompt=prompt,
                cwd=PROJ,
                claude_model="claude-sonnet-5",
                max_turns=8,
                sandbox="read-only",
                project_settings=True,
                # Explicit for both backends: neither should rely on accidental
                # user-level MCP configuration in a benchmark.
                mcp_servers=dail_tracker_mcp(PROJ),
            )
        )
        calls = result.tool_names
        cost = result.cost_usd
        provider = result.provider
        model = result.model
        if result.is_error:
            err = result.error
    except Exception as exc:  # keep partial output: provider failure is the finding
        err = f"{type(exc).__name__}: {exc}"

    first_nav = next((i for i, call in enumerate(calls) if call in NAV), None)
    first_raw = next((i for i, call in enumerate(calls) if call in RAW), None)
    ok = first_nav is not None and (first_raw is None or first_nav < first_raw)
    out = {
        "probe": name,
        "pass": ok,
        "tool_sequence": calls,
        "cost_usd": cost,
        "provider": provider,
        "model": model,
    }
    if err:
        out["error"] = err
    return out


async def main():
    import sys

    # Unknown arguments are ignored because promptfoo's exec provider appends
    # the prompt as a trailing argv; they must not silently select zero probes.
    known = {name for name, _ in PROBES}
    wanted = [arg for arg in sys.argv[1:] if arg in known]
    ignored = [arg for arg in sys.argv[1:] if arg not in known]
    if ignored:
        print(f"NOTE: ignoring non-probe args {ignored!r}; probes: {sorted(known)}")
    probes = [(name, prompt) for name, prompt in PROBES if not wanted or name in wanted]
    results = []
    for name, prompt in probes:
        try:
            results.append(await run_probe(name, prompt))
        except Exception as exc:
            results.append({"probe": name, "error": f"{type(exc).__name__}: {exc}"})
        print(json.dumps(results[-1]))
    passed = sum(1 for result in results if result.get("pass"))
    print(f"SUMMARY: {passed}/{len(results)} probes chose navigation-first")


if __name__ == "__main__":
    anyio.run(main)
