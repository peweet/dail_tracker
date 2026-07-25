"""The full counterfactual: harness OFF vs harness ON, same tasks, same model.

OFF  = setting_sources=[] and no mcp_servers: a vanilla agent in the repo cwd —
       no CLAUDE.md, no .claude rules, no hooks, no memory, no dail-tracker MCP.
       Raw Read/Grep/Glob/Bash only.
ON   = setting_sources=["project"] + the MCP server wired: the stack as shipped.

Tasks cover what the harness CLAIMS to buy: data-safety judgment (never-sum),
data-shape navigation, code navigation, repo conventions, institutional memory.
Ground truth per task was established independently on 2026-07-25 (describe_dataset
footer facts, search_project + CLAUDE.md conventions, the no-XBRL audit memory) —
see the gt fields. Metrics per run: score, tool calls, cost, answer kept verbatim.

Run: .venv/Scripts/python tools/evals/harness_bench.py [off|on] [task...]
Caveats: n=1 per cell; the ON arm pays its fixed context tax inside cost; whether
headless SDK sessions inject auto-memory is unverified — task E measures whether
the ON stack ANSWERS correctly, not which layer supplies the answer.
"""
import json
import re

import anyio
from claude_agent_sdk import (
    query, ClaudeAgentOptions, AssistantMessage, ToolUseBlock, ResultMessage,
)

PROJ = r"C:\Users\pglyn\PycharmProjects\dail_extractor"
# Clean-room cwd for the offclean arm: a detached git worktree. Different path →
# different project slug → NO auto-memory injection. Discovered 2026-07-25: with
# cwd=PROJ, setting_sources=[] strips CLAUDE.md/rules/hooks/MCP but the memory
# index STILL injects — an "off" arm run in PROJ is contaminated for knowledge
# tasks. Create with: git worktree add C:\tmp\dail_wt_bench HEAD --detach
WT = r"C:\tmp\dail_wt_bench"

REAL_AWARD_COLUMNS = {
    "tender id", "awarded suppliers", "contracting authority", "sum of awarded value (€)",
    "notice published date/contract created date", "competition type", "parent agreement id",
    "main cpv code", "main cpv code description", "tender/contract name", "spend category",
    "contract type", "procedure", "contract duration (months)", "no of bids received",
    "no of smes bids received", "no of awarded smes", "additional cpv codes on cft",
    "ted notice link", "ted can link", "sl", "supplier_raw", "supplier", "name_repaired",
    "name_truncated", "supplier_norm", "has_company_suffix", "foreign_form", "is_public_body",
    "supplier_class", "value_eur", "is_framework_or_dps", "is_call_off",
    "value_shared_across_suppliers", "value_kind", "is_large_award_review",
    "value_safe_to_sum", "estimated_value_eur", "value_plausible",
}

TASKS = {
    "never-sum": {
        "prompt": ("Working with this repo's data: a supplier appears in the procurement awards "
                   "data with awarded contract totals, and also in the public payments data with "
                   "amounts actually paid. For a supplier profile, is it methodologically sound to "
                   "add the awarded total and the paid total into one combined figure? Reply ONLY "
                   'with JSON: {"combined_figure_allowed": true|false, "reason": "<one sentence>"}'),
    },
    "data-shape": {
        "prompt": ("For this repo's procurement awards dataset: what is its row grain, how many "
                   "rows does it hold, and name five of its columns. Reply ONLY with JSON: "
                   '{"grain": "...", "rows": <integer>, "columns": ["...", 5 names]}'),
    },
    "code-nav": {
        "prompt": ("In this repo: which file and which function implement the shared atomic "
                   "parquet write with the row-floor guard that all pipeline ETL must use? Reply "
                   'ONLY with JSON: {"file": "<repo-relative path>", "function": "<name>"}'),
    },
    "conventions": {
        "prompt": ("A new data extractor is being added to this repo. Per the project's "
                   "conventions, which helper modules must it use for (1) HTTP fetching, "
                   "(2) coverage logging, (3) parquet writing, and (4) extractor run "
                   'orchestration? Reply ONLY with JSON: {"helpers": ["...four module names..."]}'),
    },
    "memory-xbrl": {
        "prompt": ("This project extracts Irish local-authority annual financial statements by "
                   "scraping PDFs. Is a public XBRL or other structured data feed available for "
                   "these statements that the project should be consuming instead? Reply ONLY "
                   'with JSON: {"structured_feed_available": true|false, "reason": "<one sentence>"}'),
    },
}


def score(task: str, ans: dict) -> float:
    if not isinstance(ans, dict):
        return 0.0
    try:
        if task == "never-sum":  # GT: false (awarded never sums with payments)
            return 1.0 if ans.get("combined_figure_allowed") is False else 0.0
        if task == "data-shape":  # GT: describe_dataset 2026-07-25
            g = 1.0 if re.search(r"award|contract|lot", str(ans.get("grain", "")), re.I) else 0.0
            r = 1.0 if ans.get("rows") == 62763 else 0.0
            cols = [str(c).lower() for c in ans.get("columns", [])][:5]
            c = (sum(1 for x in cols if x in REAL_AWARD_COLUMNS) / 5) if cols else 0.0
            return round((g + r + c) / 3, 3)
        if task == "code-nav":  # GT: services/parquet_io.py::save_parquet
            f = "parquet_io.py" in str(ans.get("file", "")).replace("\\", "/")
            fn = str(ans.get("function", "")).strip("()") == "save_parquet"
            return round((f + fn) / 2, 3)
        if task == "conventions":  # GT: the four ratchet helpers (CLAUDE.md)
            blob = " ".join(str(h).lower() for h in ans.get("helpers", []))
            want = ("http_engine", "coverage_io", "parquet_io", "extract_runner")
            return round(sum(1 for w in want if w in blob) / 4, 3)
        if task == "memory-xbrl":  # GT: false (07-23 audit — no public XBRL feed)
            return 1.0 if ans.get("structured_feed_available") is False else 0.0
    except Exception:
        return 0.0
    return 0.0


def parse_answer(text: str) -> dict:
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        return {}
    try:
        v = json.loads(m.group(0))
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


async def run_task(task: str, variant: str) -> dict:
    on = variant == "on"
    import os

    opts = ClaudeAgentOptions(
        model="claude-sonnet-5",
        max_turns=12,
        cwd=WT if variant == "offclean" else PROJ,
        setting_sources=["project"] if on else [],
        permission_mode="bypassPermissions",
        # offclean has no .venv in the worktree — put the real venv on PATH so
        # Bash `python` (duckdb etc.) works the same in both arms
        env={"PATH": PROJ + r"\.venv\Scripts;" + os.environ.get("PATH", ""),
             "PYTHONUTF8": "1"} if variant == "offclean" else {},
        mcp_servers={
            "dail-tracker": {
                "command": PROJ + r"\.venv\Scripts\python.exe",
                "args": [PROJ + r"\mcp_server\server.py"],
                "env": {"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
            }
        } if on else {},
    )
    calls: list[str] = []
    cost = None
    text = ""
    err = None
    try:
        async for msg in query(prompt=TASKS[task]["prompt"], options=opts):
            if isinstance(msg, AssistantMessage):
                for b in msg.content:
                    if isinstance(b, ToolUseBlock):
                        calls.append(b.name)
            if isinstance(msg, ResultMessage):
                cost = msg.total_cost_usd
                text = getattr(msg, "result", "") or ""
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
    ans = parse_answer(text)
    out = {
        "task": task, "variant": variant, "score": score(task, ans),
        "tool_calls": len(calls),
        "mcp_calls": sum(c.startswith("mcp__") for c in calls),
        "cost_usd": round(cost, 4) if cost else None,
        "answer": ans, "sequence": calls,
    }
    if err:
        out["error"] = err
    return out


async def main():
    import sys

    args = [a.lower() for a in sys.argv[1:]]
    variants = [v for v in ("off", "offclean", "on") if not args or v in args] or ["offclean", "on"]
    tasks = [t for t in TASKS if t in args] or list(TASKS)
    for variant in variants:
        for task in tasks:
            print(json.dumps(await run_task(task, variant), ensure_ascii=False))


anyio.run(main)
