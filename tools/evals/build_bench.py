"""Write-task counterfactual: can the agent BUILD as well without the harness?

The read-task benches (harness_bench.py) showed retrieval parity + efficiency;
this one scores DELIVERY: three write-shaped tasks, each run in a disposable
git worktree so no arm ever touches the real repo. Scoring is mechanical
(pytest green, feature runs, SQL parses + registration-order clean) — no
judge model.

Arms:
  on     — worktree KEEPS its .git pointer (memory injects via git identity —
           that's part of the harness) + setting_sources=["project"] + MCP wired.
  clean  — .git pointer DELETED (verified clean room, see
           project_harness_ab_benchmark_2026_07_25) + no settings, no MCP.
  hybrid — clean room + a NAVIGATOR-QUALITY BRIEF instead of the bare task:
           files, constraints, acceptance check baked into the prompt. Models
           the navigator/builder split with true lean execution (only possible
           on the SDK path — interactive subagents receive the FULL harness
           regardless, verified by introspection 2026-07-26).

Both arms get the main venv on PATH (the worktree has no .venv — tooling
parity, not a harness feature). Worktrees check out HEAD, so tasks target
files that exist at HEAD.

Run: .venv/Scripts/python tools/evals/build_bench.py [on|clean] [task...]
Cost: roughly $0.5-1.5 per task per arm at sonnet prices [Indicative — read-task
probes measured $0.13-0.44; build tasks run more turns].
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import anyio

PROJ_PATH = Path(__file__).resolve().parents[2]
if str(PROJ_PATH) not in sys.path:
    sys.path.insert(0, str(PROJ_PATH))

from tools.evals.provider_adapter import EvalRequest, dail_tracker_mcp, run_eval  # noqa: E402

PROJ = str(PROJ_PATH)
VENV = str(Path(PROJ) / ".venv" / ("Scripts" if os.name == "nt" else "bin"))
PY = str(Path(VENV) / ("python.exe" if os.name == "nt" else "python"))
WT_BASE = r"C:\tmp\dail_build_bench"


def sh(args: list[str], cwd: str = PROJ, timeout: int = 300) -> subprocess.CompletedProcess:
    env = dict(os.environ, PATH=VENV + ";" + os.environ.get("PATH", ""), PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    return subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=env
    )


# ── tasks ────────────────────────────────────────────────────────────────────


def plant_style_lint_bug(wt: str) -> None:
    """Break the 'unverified' discharge in the hook — makes exactly the
    test_discharged_figure_passes[unverified...] cases fail."""
    p = os.path.join(wt, "tools", "hooks", "style_lint.py")
    with open(p, encoding="utf-8") as fh:
        src = fh.read()
    assert r"\bunverified\b" in src, "plant target missing at HEAD"
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(src.replace(r"\bunverified\b", r"\bunverifiedzq\b", 1))


def score_bugfix(wt: str) -> tuple[float, str]:
    """Planted-bug fix: the style-lint test file must pass AND be untouched."""
    target = os.path.join(wt, "test", "tools", "test_style_lint.py")
    with open(target, encoding="utf-8") as fh:
        before = fh.read()
    r = sh([PY, "-m", "pytest", "-q", "test/tools/test_style_lint.py", "--no-header"], cwd=wt)
    tests_green = r.returncode == 0
    with open(target, encoding="utf-8") as fh:
        after = fh.read()
    untouched = before == after
    return (
        1.0 if tests_green and untouched else 0.5 if tests_green else 0.0,
        f"tests_green={tests_green} test_file_untouched={untouched}",
    )


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
    new_tests = [f for f in os.listdir(os.path.join(wt, "test", "tools")) if "discover" in f and f.startswith("test_")]
    # test_fact_cards / test_runtime_manifest fail AT HEAD independent of the agent
    # (their fixes are in the uncommitted 07-25 batch) — excluding them keeps the
    # suite-green component earnable and symmetric across arms
    r2 = sh(
        [
            PY,
            "-m",
            "pytest",
            "-q",
            "test/tools",
            "--no-header",
            "--ignore=test/tools/test_fact_cards.py",
            "--ignore=test/tools/test_runtime_manifest.py",
            # baseline JSON is NOT git-tracked (verified 2026-07-26) so this fails in
            # every HEAD worktree independent of the agent — a ship-gap, not a signal
            "--ignore=test/tools/test_source_fidelity_gate.py",
        ],
        cwd=wt,
        timeout=600,
    )
    suite_green = r2.returncode == 0
    score += 0.25 if (new_tests and suite_green) else 0.0
    score += 0.25 if suite_green else 0.0
    notes.append(f"own_test_added={bool(new_tests)} tools_suite_green={suite_green}")
    if not suite_green:  # name the failures so a red cell is diagnosable post-teardown
        fails = [ln for ln in (r2.stdout or "").splitlines() if ln.startswith("FAILED")][:4]
        notes.append("fails=" + ("; ".join(fails) if fails else (r2.stdout or "")[-200:]))
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
    risks = [
        x
        for x in sql_index.order_risks(views)
        if x["view"] == "v_payments_party_year" or x["needs"] == "v_payments_party_year"
    ]
    score = (0.4 * parses) + (0.3 * deps_exist) + (0.3 * (not risks))
    return round(score, 2), f"parses_ast={parses} deps_exist={deps_exist} order_risks={len(risks)}"


# Navigator-quality briefs: what the full-harness main thread would hand a lean
# builder — files, hard constraints, acceptance check. No solution content.
BRIEFS = {
    "bugfix": (
        "BRIEF — bug fix.\n"
        "Failing tests: test/tools/test_style_lint.py (the discharge/'unverified' cases).\n"
        "The code under test: tools/hooks/style_lint.py — the bug is in the hook, not the tests.\n"
        "Constraint: do not modify the test file.\n"
        "Acceptance: `python -m pytest -q test/tools/test_style_lint.py` fully green. Run it before finishing."
    ),
    "feature": (
        "BRIEF — small feature.\n"
        "File: tools/discoveries.py (a CLI over tools/discoveries.jsonl; existing flags --domain/--list/--add).\n"
        "Change: add a --json flag — matching rows print as one JSON object per line; no flag = behaviour unchanged.\n"
        "Add a test in test/tools/ (e.g. test_discoveries_json.py) exercising the flag via subprocess.\n"
        "Acceptance: `python tools/discoveries.py --json planning` emits valid JSON lines and exits 0; "
        "`python -m pytest -q test/tools --ignore=test/tools/test_fact_cards.py "
        "--ignore=test/tools/test_runtime_manifest.py --ignore=test/tools/test_source_fidelity_gate.py` green. Run both."
    ),
    "sqlview": (
        "BRIEF — new SQL view.\n"
        "Create sql_views/payments/payments_party_year.sql defining v_payments_party_year: total Travel & "
        "Accommodation payments per party per year, reading FROM v_payments_base (columns include party_name, "
        "payment_year, amount_num, house).\n"
        "Constraint: sql_views registration is sorted-glob per directory — a view's filename must sort AFTER its "
        "same-directory dependencies; payments_party_year.sql sorts after payments_base.sql, so reading "
        "v_payments_base is safe; do NOT read views whose filenames sort after yours.\n"
        "Acceptance: the body parses via DuckDB (`SELECT json_serialize_sql('<body>')` has no error). Verify it."
    ),
}

TASKS = {
    "bugfix": {
        "prompt": (
            "The test file test/tools/test_style_lint.py has failing tests. Diagnose and "
            "fix the BUG in the hook it tests. Do not modify the test file itself. Run the "
            "tests to confirm they pass before finishing."
        ),
        "plant": plant_style_lint_bug,
        "score": score_bugfix,
    },
    "feature": {
        "prompt": (
            "Add a --json flag to tools/discoveries.py: when passed, matching rows print "
            "as one JSON object per line instead of the human format (no flag = unchanged "
            "behaviour). Add a test for the new flag in test/tools/. Follow this project's "
            "conventions. Run the tests before finishing."
        ),
        "plant": None,
        "score": score_feature,
    },
    "sqlview": {
        "prompt": (
            "Create a new registered SQL view v_payments_party_year in sql_views/payments/ "
            "giving total Travel & Accommodation payments per party per year (from the "
            "existing payments views/base). Respect this repo's view-registration "
            "conventions, including registration ORDER. Verify your SQL parses."
        ),
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
    prompt = BRIEFS[task] if variant == "hybrid" else spec["prompt"]
    wt = make_worktree(f"{task}_{variant}", clean=not on)
    try:
        if spec["plant"]:
            spec["plant"](wt)
        calls: list[str] = []
        cost = None
        err = None
        provider = None
        model = None
        try:
            result = await run_eval(
                EvalRequest(
                    prompt=prompt,
                    cwd=wt,
                    claude_model="claude-sonnet-5",
                    max_turns=25,
                    sandbox="workspace-write",
                    project_settings=on,
                    env={
                        "PATH": VENV + os.pathsep + os.environ.get("PATH", ""),
                        "PYTHONUTF8": "1",
                    },
                    mcp_servers=dail_tracker_mcp(PROJ, PY) if on else {},
                )
            )
            calls = result.tool_names
            cost = result.cost_usd
            provider = result.provider
            model = result.model
            if result.is_error:
                err = result.error
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        score, detail = spec["score"](wt)
        out = {
            "task": task,
            "variant": variant,
            "score": score,
            "detail": detail,
            "tool_calls": len(calls),
            "mcp_calls": sum(c.startswith("mcp__") for c in calls),
            "cost_usd": round(cost, 4) if cost else None,
            "provider": provider,
            "model": model,
        }
        if err:
            out["agent_error"] = err
        return out
    finally:
        drop_worktree(wt)


async def main():
    import sys

    args = [a.lower() for a in sys.argv[1:]]
    variants = [v for v in ("on", "clean", "hybrid") if v in args] or ["on", "clean", "hybrid"]
    tasks = [t for t in TASKS if t in args] or list(TASKS)
    for variant in variants:
        for task in tasks:
            print(json.dumps(await run_task(task, variant), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    anyio.run(main)
