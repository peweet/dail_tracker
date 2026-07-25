"""Write-task counterfactual: can the agent BUILD as well without the harness?

The read-task benches (harness_bench.py) showed retrieval parity + efficiency;
this one scores DELIVERY: three write-shaped tasks, each run in a disposable
git worktree so no arm ever touches the real repo. Scoring is mechanical
(pytest green, feature runs, SQL parses + registration-order clean) — no
judge model.

Arms:
  on    — worktree KEEPS its .git pointer (memory injects via git identity —
          that's part of the harness) + setting_sources=["project"] + MCP wired.
  clean — .git pointer DELETED (verified clean room, see
          project_harness_ab_benchmark_2026_07_25) + no settings, no MCP.

Both arms get the main venv on PATH (the worktree has no .venv — tooling
parity, not a harness feature). Worktrees check out HEAD, so tasks target
files that exist at HEAD.

Run: .venv/Scripts/python tools/evals/build_bench.py [on|clean] [task...]
Cost: roughly $0.5-1.5 per task per arm at sonnet prices [Indicative — read-task
probes measured $0.13-0.44; build tasks run more turns].
"""
import json
import os
import re
import shutil
import subprocess

import anyio
from claude_agent_sdk import (
    query, ClaudeAgentOptions, AssistantMessage, ToolUseBlock, ResultMessage,
)

PROJ = r"C:\Users\pglyn\PycharmProjects\dail_extractor"
VENV = PROJ + r"\.venv\Scripts"
PY = VENV + r"\python.exe"
WT_BASE = r"C:\tmp\dail_build_bench"


def sh(args: list[str], cwd: str = PROJ, timeout: int = 300) -> subprocess.CompletedProcess:
    env = dict(os.environ, PATH=VENV + ";" + os.environ.get("PATH", ""),
               PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", timeout=timeout, env=env)


# ── tasks ────────────────────────────────────────────────────────────────────

def plant_style_lint_bug(wt: str) -> None:
    """Break the 'unverified' discharge in the hook — makes exactly the
    test_discharged_figure_passes[unverified...] cases fail."""
    p = os.path.join(wt, "tools", "hooks", "style_lint.py")
    src = open(p, encoding="utf-8").read()
    assert r"\bunverified\b" in src, "plant target missing at HEAD"
    open(p, "w", encoding="utf-8").write(src.replace(r"\bunverified\b", r"\bunverifiedzq\b", 1))


def score_bugfix(wt: str) -> tuple[float, str]:
    """Planted-bug fix: the style-lint test file must pass AND be untouched."""
    before = open(os.path.join(wt, "test", "tools", "test_style_lint.py"), encoding="utf-8").read()
    r = sh([PY, "-m", "pytest", "-q", "test/tools/test_style_lint.py", "--no-header"], cwd=wt)
    tests_green = r.returncode == 0
    after = open(os.path.join(wt, "test", "tools", "test_style_lint.py"), encoding="utf-8").read()
    untouched = before == after
    return (1.0 if tests_green and untouched else 0.5 if tests_green else 0.0,
            f"tests_green={tests_green} test_file_untouched={untouched}")


def score_feature(wt: str) -> tuple[float, str]:
    """--json flag on tools/discoveries.py: feature works, their test exists+passes,
    existing tools tests still green."""
    score, notes = 0.0, []
    r = sh([PY, "tools/discoveries.py", "--json", "planning"], cwd=wt)
    ok_json = r.returncode == 0
    if ok_json and r.stdout.strip():
        try:
            for ln in r.stdout.strip().splitlines():
                json.loads(ln)
        except Exception:
            ok_json = False
    score += 0.5 if ok_json else 0.0
    notes.append(f"json_flag_works={ok_json}")
    new_tests = [f for f in os.listdir(os.path.join(wt, "test", "tools"))
                 if "discover" in f and f.startswith("test_")]
    r2 = sh([PY, "-m", "pytest", "-q", "test/tools", "--no-header"], cwd=wt, timeout=600)
    suite_green = r2.returncode == 0
    score += 0.25 if (new_tests and suite_green) else 0.0
    score += 0.25 if suite_green else 0.0
    notes.append(f"own_test_added={bool(new_tests)} tools_suite_green={suite_green}")
    return score, " ".join(notes)


def score_sqlview(wt: str) -> tuple[float, str]:
    """New view parses via the repo's own AST layer, reads real views, and does
    not create a sorted-glob registration-order risk."""
    import sys
    sys.path.insert(0, PROJ)
    from pathlib import Path
    from mcp_server import sql_index
    views = sql_index.build_graph(Path(wt))  # fresh graph over the WORKTREE
    v = views.get("v_payments_party_year")
    if v is None:
        return 0.0, "view not found"
    parses = v["mode"] == "ast"
    deps_exist = all(d in views for d in v["reads"]) and bool(v["reads"] or v["reads_parquet"])
    risks = [x for x in sql_index.order_risks(views)
             if x["view"] == "v_payments_party_year" or x["needs"] == "v_payments_party_year"]
    score = (0.4 * parses) + (0.3 * deps_exist) + (0.3 * (not risks))
    return round(score, 2), f"parses_ast={parses} deps_exist={deps_exist} order_risks={len(risks)}"


TASKS = {
    "bugfix": {
        "prompt": ("The test file test/tools/test_style_lint.py has failing tests. Diagnose and "
                   "fix the BUG in the hook it tests. Do not modify the test file itself. Run the "
                   "tests to confirm they pass before finishing."),
        "plant": plant_style_lint_bug,
        "score": score_bugfix,
    },
    "feature": {
        "prompt": ("Add a --json flag to tools/discoveries.py: when passed, matching rows print "
                   "as one JSON object per line instead of the human format (no flag = unchanged "
                   "behaviour). Add a test for the new flag in test/tools/. Follow this project's "
                   "conventions. Run the tests before finishing."),
        "plant": None,
        "score": score_feature,
    },
    "sqlview": {
        "prompt": ("Create a new registered SQL view v_payments_party_year in sql_views/payments/ "
                   "giving total Travel & Accommodation payments per party per year (from the "
                   "existing payments views/base). Respect this repo's view-registration "
                   "conventions, including registration ORDER. Verify your SQL parses."),
        "plant": None,
        "score": score_sqlview,
    },
}


# ── worktree lifecycle ───────────────────────────────────────────────────────

def make_worktree(tag: str, clean: bool) -> str:
    wt = f"{WT_BASE}_{tag}"
    if os.path.exists(wt):
        shutil.rmtree(wt, ignore_errors=True)
        sh(["git", "worktree", "prune"])
    r = sh(["git", "worktree", "add", wt, "HEAD", "--detach"], timeout=600)
    if r.returncode != 0:
        raise RuntimeError(f"worktree add failed: {r.stderr[:300]}")
    if clean:
        os.remove(os.path.join(wt, ".git"))  # sever git identity -> no memory injection
    return wt


def drop_worktree(wt: str) -> None:
    shutil.rmtree(wt, ignore_errors=True)
    sh(["git", "worktree", "prune"])


# ── runner ───────────────────────────────────────────────────────────────────

async def run_task(task: str, variant: str) -> dict:
    spec = TASKS[task]
    on = variant == "on"
    wt = make_worktree(f"{task}_{variant}", clean=not on)
    try:
        if spec["plant"]:
            spec["plant"](wt)
        opts = ClaudeAgentOptions(
            model="claude-sonnet-5",
            max_turns=25,
            cwd=wt,
            setting_sources=["project"] if on else [],
            permission_mode="bypassPermissions",
            env={"PATH": VENV + ";" + os.environ.get("PATH", ""), "PYTHONUTF8": "1"},
            mcp_servers={
                "dail-tracker": {
                    "command": PY,
                    "args": [PROJ + r"\mcp_server\server.py"],
                    "env": {"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
                }
            } if on else {},
        )
        calls: list[str] = []
        cost = None
        err = None
        try:
            async for msg in query(prompt=spec["prompt"], options=opts):
                if isinstance(msg, AssistantMessage):
                    for b in msg.content:
                        if isinstance(b, ToolUseBlock):
                            calls.append(b.name)
                if isinstance(msg, ResultMessage):
                    cost = msg.total_cost_usd
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        score, detail = spec["score"](wt)
        out = {"task": task, "variant": variant, "score": score, "detail": detail,
               "tool_calls": len(calls),
               "mcp_calls": sum(c.startswith("mcp__") for c in calls),
               "cost_usd": round(cost, 4) if cost else None}
        if err:
            out["agent_error"] = err
        return out
    finally:
        drop_worktree(wt)


async def main():
    import sys

    args = [a.lower() for a in sys.argv[1:]]
    variants = [v for v in ("on", "clean") if v in args] or ["on", "clean"]
    tasks = [t for t in TASKS if t in args] or list(TASKS)
    for variant in variants:
        for task in tasks:
            print(json.dumps(await run_task(task, variant), ensure_ascii=False), flush=True)


anyio.run(main)
