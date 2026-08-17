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
from pathlib import Path

import anyio

PROJ_PATH = Path(__file__).resolve().parents[2]
if str(PROJ_PATH) not in sys.path:
    sys.path.insert(0, str(PROJ_PATH))

from tools.evals.provider_adapter import EvalRequest, dail_tracker_mcp, run_eval  # noqa: E402

PROJ = str(PROJ_PATH)
PY = str(Path(PROJ) / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
WT = r"C:\tmp\dail_prune_bench"

sys.path.insert(0, os.path.join(PROJ, "tools", "evals"))
from harness_bench import PUBLIC_TASKS, parse_answer, score_answer  # noqa: E402 ? after sys.path insert

STEER_POLICIES = {
    "steer-datashape": [
        "mcp__dail-tracker__describe_dataset",
        "mcp__dail-tracker__list_datasets",
    ],
    "steer-wherelives": ["mcp__dail-tracker__search_project"],
}
NAV = set(STEER_POLICIES["steer-datashape"] + STEER_POLICIES["steer-wherelives"])
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
    return subprocess.run(
        args, cwd=PROJ, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout
    )


def make_wt(variant):
    if os.path.exists(WT):
        shutil.rmtree(WT, ignore_errors=True)
        sh(["git", "worktree", "prune"])
    r = sh(["git", "worktree", "add", WT, "HEAD", "--detach"])
    if r.returncode != 0:
        raise RuntimeError(f"worktree add failed: {r.stderr[:300]}")
    # rules identical in both arms
    shutil.copytree(os.path.join(PROJ, ".claude", "rules"), os.path.join(WT, ".claude", "rules"), dirs_exist_ok=True)
    # the ONLY variable: CLAUDE.md content
    if variant == "full":
        shutil.copyfile(os.path.join(PROJ, "CLAUDE.md"), os.path.join(WT, "CLAUDE.md"))
    else:
        with open(os.path.join(WT, "CLAUDE.md"), "w", encoding="utf-8") as fh:
            fh.write(TRIMMED)
    # Codex's portable project-instruction surface is AGENTS.md. Mirroring the
    # arm content keeps the experiment's independent variable identical while
    # each provider reads its native instruction filename.
    shutil.copyfile(os.path.join(WT, "CLAUDE.md"), os.path.join(WT, "AGENTS.md"))
    return len(open(os.path.join(WT, "CLAUDE.md"), encoding="utf-8").read())


def drop_wt():
    shutil.rmtree(WT, ignore_errors=True)
    sh(["git", "worktree", "prune"])


STEER = [
    ("steer-datashape", "What columns and grain does the procurement awards dataset have? Just tell me."),
    ("steer-wherelives", "Which dataset or view covers ministerial diaries? Just point me at it."),
]


async def run_one(prompt, wt, allowed_mcp_tools=()):
    calls, cost, text = [], None, ""
    provider, model = None, None
    try:
        result = await run_eval(
            EvalRequest(
                prompt=prompt,
                cwd=wt,
                claude_model="claude-sonnet-5",
                max_turns=12,
                sandbox="read-only",
                project_settings=True,
                env={
                    "PATH": str(Path(PROJ) / ".venv" / "Scripts") + os.pathsep + os.environ.get("PATH", ""),
                    "PYTHONUTF8": "1",
                },
                allowed_tools=list(allowed_mcp_tools) or None,
                mcp_servers=dail_tracker_mcp(PROJ, PY) if allowed_mcp_tools else {},
            )
        )
        calls = result.tool_names
        cost = result.cost_usd
        text = result.final_text
        provider = result.provider
        model = result.model
        if result.is_error:
            text = f"__ERR__ {result.error or 'provider error'}"
    except Exception as exc:
        text = f"__ERR__ {type(exc).__name__}: {exc}"
    return calls, cost, text, provider, model


async def main():
    args = [a.lower() for a in sys.argv[1:]]
    variants = [v for v in ("full", "trim") if v in args] or ["full", "trim"]
    for variant in variants:
        size = make_wt(variant)
        try:
            tot_score = tot_cost = 0.0
            rows = []
            for task, spec in PUBLIC_TASKS.items():
                calls, cost, text, provider, model = await run_one(
                    spec["prompt"], WT, spec.get("allowed_mcp_tools", [])
                )
                s = score_answer(spec, parse_answer(text))
                tot_score += s
                tot_cost += cost or 0
                rows.append(f"{task}={s}")
            steer_ok = 0
            for name, prompt in STEER:
                calls, cost, text, provider, model = await run_one(prompt, WT, STEER_POLICIES[name])
                first_nav = next((i for i, c in enumerate(calls) if c in NAV), None)
                first_raw = next((i for i, c in enumerate(calls) if c in RAW), None)
                ok = first_nav is not None and (first_raw is None or first_nav < first_raw)
                steer_ok += int(ok)
                tot_cost += cost or 0
            print(
                json.dumps(
                    {
                        "variant": variant,
                        "claude_md_chars": size,
                        "task_score": round(tot_score, 2),
                        "task_detail": rows,
                        "steering_nav_first": f"{steer_ok}/{len(STEER)}",
                        "total_cost_usd": round(tot_cost, 4),
                        "provider": provider,
                        "model": model,
                    }
                ),
                flush=True,
            )
        finally:
            drop_wt()


if __name__ == "__main__":
    anyio.run(main)
