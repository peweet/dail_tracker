"""A/B bench: do the 2026-07-25 tools (search_speeches/search_questions/column_deps)
beat the baseline toolset on the SAME tasks?

Design (PaddleOCR-style): identical prompts, identical model, two variants —
`baseline` blocks the new tools via disallowed_tools, `newtools` allows them.
Ground truth was established INDEPENDENTLY of the new tools (plain ILIKE
reference queries; manual read of the payments view chain) so the test is not
circular. Metrics per run: correctness score vs ground truth, tool-call count,
turns, cost. The final answer text is kept for qualitative comparison.

Run: .venv/Scripts/python tools/evals/package_bench.py [baseline|newtools] [task...]
Lesson from routing_probe baked in: the SDK loads no .mcp.json — mcp_servers is
wired explicitly or every probe measures tool-absence.
"""

import json
import re

import anyio
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ResultMessage,
    ToolUseBlock,
    query,
)

PROJ = r"C:\Users\pglyn\PycharmProjects\dail_extractor"

NEW_TOOLS = [
    "mcp__dail-tracker__search_speeches",
    "mcp__dail-tracker__search_questions",
    "mcp__dail-tracker__column_deps",
]

TASKS = {
    "column-lineage": {
        "prompt": (
            "In this repo's sql_views layer: if the total_paid column exposed by the view "
            "v_payments_yearly_evolution were renamed, which OTHER registered views would "
            "break, directly or transitively? Reply ONLY with a JSON array of view names."
        ),
        "gt": {"v_payments_alltime_ranking", "v_payments_alltime_summary"},
        "kind": "exact_set",
    },
    "topic-speeches": {
        "prompt": (
            "Using the dail-tracker data: which TDs have spoken most on the Dáil floor about "
            "defective concrete blocks / mica redress? Reply ONLY with a JSON array of up to "
            "5 speaker names, most prominent first."
        ),
        # ILIKE reference top-8 (bench_gt.py 2026-07-25) — generous superset for precision
        "gt": {
            "taoiseach",
            "thomas pringle",
            "pearse doherty",
            "richard boyd barrett",
            "charles ward",
            "peadar tóibín",
            "pádraig mac lochlainn",
            "eoin ó broin",
        },
        "kind": "name_overlap",
    },
    "topic-questions": {
        "prompt": (
            "Using the dail-tracker data: which TWO departments/ministries receive the most "
            "parliamentary questions about direct provision? Reply ONLY with a JSON array of "
            "the two ministry names."
        ),
        "gt": {"children", "justice"},
        "kind": "name_overlap",
    },
}


def _norm(s: str) -> str:
    s = s.lower().replace("deputy ", "").replace("the ", "").replace("minister for ", "")
    return re.sub(r"[^a-zá-ú ]", "", s).strip()


def score(kind: str, gt: set, answer: list) -> float:
    ans = {_norm(str(a)) for a in answer if str(a).strip()}
    if not ans:
        return 0.0
    gtn = {_norm(g) for g in gt}
    if kind == "exact_set":
        inter = len(ans & gtn)
        return round(2 * inter / (len(ans) + len(gtn)), 3)  # F1 on sets
    hits = sum(1 for a in ans if any(g in a or a in g for g in gtn))
    return round(hits / len(ans), 3)  # precision vs reference superset


def parse_answer(text: str) -> list:
    m = re.search(r"\[.*?\]", text or "", re.S)
    if not m:
        return []
    try:
        v = json.loads(m.group(0))
        return v if isinstance(v, list) else []
    except Exception:
        return []


async def run_task(task: str, variant: str) -> dict:
    spec = TASKS[task]
    opts = ClaudeAgentOptions(
        model="claude-sonnet-5",
        max_turns=12,
        cwd=PROJ,
        setting_sources=["project"],
        permission_mode="bypassPermissions",
        disallowed_tools=NEW_TOOLS if variant == "baseline" else [],
        mcp_servers={
            "dail-tracker": {
                "command": PROJ + r"\.venv\Scripts\python.exe",
                "args": [PROJ + r"\mcp_server\server.py"],
                "env": {"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
            }
        },
    )
    calls: list[str] = []
    cost = None
    text = ""
    err = None
    try:
        async for msg in query(prompt=spec["prompt"], options=opts):
            if isinstance(msg, AssistantMessage):
                for b in msg.content:
                    if isinstance(b, ToolUseBlock):
                        calls.append(b.name)
            if isinstance(msg, ResultMessage):
                cost = msg.total_cost_usd
                text = getattr(msg, "result", "") or ""
    except Exception as e:  # keep partials — they are the finding
        err = f"{type(e).__name__}: {e}"
    answer = parse_answer(text)
    out = {
        "task": task,
        "variant": variant,
        "score": score(spec["kind"], spec["gt"], answer),
        "tool_calls": len(calls),
        "mcp_calls": sum(c.startswith("mcp__") for c in calls),
        "new_tool_calls": sum(c in NEW_TOOLS for c in calls),
        "cost_usd": round(cost, 4) if cost else None,
        "answer": answer,
        "sequence": calls,
    }
    if err:
        out["error"] = err
    return out


async def main():
    import sys

    args = [a.lower() for a in sys.argv[1:]]
    variants = [v for v in ("baseline", "newtools") if not args or v in args] or ["baseline", "newtools"]
    tasks = [t for t in TASKS if t in args] or list(TASKS)
    for variant in variants:
        for task in tasks:
            r = await run_task(task, variant)
            print(json.dumps(r, ensure_ascii=False))


anyio.run(main)
