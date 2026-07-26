"""Row-7 experiment: does trimming CLAUDE.md's prose hurt quality or steering?

The efficiency scorecard's one partial row — the always-loaded prompt has never had
the per-line "would removing this cause a mistake?" prune the Claude Code docs
prescribe. This isolates CLAUDE.md content as the ONLY variable: a git worktree holds
everything else constant (tracked code + data, copied .claude/rules, memory injected
via git identity, MCP wired explicitly); the two arms differ only in the CLAUDE.md file.

  full  — the current 67-line CLAUDE.md, copied in verbatim.
  trim  — an editorial prune: keeps the routing table + never-break data rules +
          firewall + convention-ratchet + test command; cuts the token-discipline
          prose, the meta-note, the environment prose, and "where to look first".

Same 5 read tasks + 2 steering probes as harness_bench, scored mechanically. Cost is
NOT the point (CLAUDE.md is ~1.4k of the ~8.4k fixed tax → trimming saves ~0.2% of
spend); this is purely a quality/steering safety check. Expected: parity = safe to
prune (low value); a drop = the cut content was load-bearing.

Run: .venv/Scripts/python tools/evals/claude_md_prune_bench.py [full|trim]
"""
import json
import os
import shutil
import subprocess
import sys

import anyio
from claude_agent_sdk import (
    query, ClaudeAgentOptions, AssistantMessage, ToolUseBlock, ResultMessage,
)

PROJ = r"C:\Users\pglyn\PycharmProjects\dail_extractor"
PY = PROJ + r"\.venv\Scripts\python.exe"
WT = r"C:\tmp\dail_prune_bench"

sys.path.insert(0, os.path.join(PROJ, "tools", "evals"))
from harness_bench import TASKS, score, parse_answer  # reuse tasks + scorers

NAV = {"mcp__dail-tracker__describe_dataset", "mcp__dail-tracker__search_project",
       "mcp__dail-tracker__list_datasets"}
RAW = {"Read", "Grep", "Glob"}

# The editorial trim: load-bearing content only. Kept deliberately faithful to the
# original's wording so the ONLY change is what's removed, not how it's phrased.
TRIMMED = """# CLAUDE.md — Dail Tracker

## First move — route the question (reflex, not a mandate)

| The question | First move | Instead of |
|---|---|---|
| columns / rows / grain of dataset X? | `describe_dataset("X")` | Reading a parquet |
| which dataset / view / doc covers T? | `search_project("T")` | repo-wide `Grep` |
| where is function/class F defined? | `outline(file)` -> ranged `Read` | Reading the whole file |
| will renaming/reordering a SQL view break things? | `view_deps` | grepping `sql_views/` |
| will renaming a column break views? | `column_deps(view, col)` | grepping `sql_views/` |
| who said / who's asking about topic T? | `search_speeches` / `search_questions` | ILIKE trawls |
| can I sum these money columns? | never-sum grain rule | assuming |
| a well-briefed build | `builder` subagent | building in a long main thread |

- Never `Read` data files (tens of MB; floods context) — query via the dail-tracker MCP.
- Delegate broad search to the `explore` subagent; scope every Grep/Glob.

## Data conventions (the never-break rules)

- Polars for ETL, pandas only in the UI layer.
- Three money grains never union/sum together (procurement awarded vs. payments vs. budget; never sum TED).
- Parquet writes are atomic, zstd + statistics, with a row-floor guard — don't bypass the helpers.
- Members join on the normalised TD name (NFKD accent-fold) — reuse the normaliser, don't invent matching.
- Provenance is the user's domain — don't invent figures, don't infer values in UI copy.

## Streamlit + commands

- Logic firewall: pages (`utility/pages_code/`) hold no business logic — queries go through `utility/data_access/`.
- Convention ratchet (`tools/check_conventions.py`): extractors use `services/http_engine`, `coverage_io.save_coverage`, `parquet_io.save_parquet`, `extract_runner.run_extractor`; pages import formatters from `ui/format.py` and use `@dt_page`.
- Tests: `.venv/Scripts/python -m pytest -q` (fast: `-m "not integration and not sql and not sources and not bronze"`).
"""


def sh(args, timeout=600):
    return subprocess.run(args, cwd=PROJ, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", timeout=timeout)


def make_wt(variant):
    if os.path.exists(WT):
        shutil.rmtree(WT, ignore_errors=True)
        sh(["git", "worktree", "prune"])
    r = sh(["git", "worktree", "add", WT, "HEAD", "--detach"])
    if r.returncode != 0:
        raise RuntimeError(f"worktree add failed: {r.stderr[:300]}")
    # rules identical in both arms
    shutil.copytree(os.path.join(PROJ, ".claude", "rules"),
                    os.path.join(WT, ".claude", "rules"), dirs_exist_ok=True)
    # the ONLY variable: CLAUDE.md content
    if variant == "full":
        shutil.copyfile(os.path.join(PROJ, "CLAUDE.md"), os.path.join(WT, "CLAUDE.md"))
    else:
        with open(os.path.join(WT, "CLAUDE.md"), "w", encoding="utf-8") as fh:
            fh.write(TRIMMED)
    return len(open(os.path.join(WT, "CLAUDE.md"), encoding="utf-8").read())


def drop_wt():
    shutil.rmtree(WT, ignore_errors=True)
    sh(["git", "worktree", "prune"])


STEER = [
    ("steer-datashape", "What columns and grain does the procurement awards dataset have? Just tell me."),
    ("steer-wherelives", "Which dataset or view covers ministerial diaries? Just point me at it."),
]


async def run_one(prompt, wt):
    opts = ClaudeAgentOptions(
        model="claude-sonnet-5", max_turns=12, cwd=wt,
        setting_sources=["project"], permission_mode="bypassPermissions",
        env={"PATH": PROJ + r"\.venv\Scripts;" + os.environ.get("PATH", ""), "PYTHONUTF8": "1"},
        mcp_servers={"dail-tracker": {"command": PY, "args": [PROJ + r"\mcp_server\server.py"],
                                      "env": {"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}}},
    )
    calls, cost, text = [], None, ""
    try:
        async for msg in query(prompt=prompt, options=opts):
            if isinstance(msg, AssistantMessage):
                for b in msg.content:
                    if isinstance(b, ToolUseBlock):
                        calls.append(b.name)
            if isinstance(msg, ResultMessage):
                cost = msg.total_cost_usd
                text = getattr(msg, "result", "") or ""
    except Exception as e:
        text = f"__ERR__ {type(e).__name__}"
    return calls, cost, text


async def main():
    args = [a.lower() for a in sys.argv[1:]]
    variants = [v for v in ("full", "trim") if v in args] or ["full", "trim"]
    for variant in variants:
        size = make_wt(variant)
        try:
            tot_score = tot_cost = 0.0
            rows = []
            for task, spec in TASKS.items():
                calls, cost, text = await run_one(spec["prompt"], WT)
                s = score(task, parse_answer(text))
                tot_score += s
                tot_cost += cost or 0
                rows.append(f"{task}={s}")
            steer_ok = 0
            for name, prompt in STEER:
                calls, cost, text = await run_one(prompt, WT)
                first_nav = next((i for i, c in enumerate(calls) if c in NAV), None)
                first_raw = next((i for i, c in enumerate(calls) if c in RAW), None)
                ok = first_nav is not None and (first_raw is None or first_nav < first_raw)
                steer_ok += int(ok)
                tot_cost += cost or 0
            print(json.dumps({
                "variant": variant, "claude_md_chars": size,
                "task_score": round(tot_score, 2), "task_detail": rows,
                "steering_nav_first": f"{steer_ok}/{len(STEER)}",
                "total_cost_usd": round(tot_cost, 4),
            }), flush=True)
        finally:
            drop_wt()


anyio.run(main)
