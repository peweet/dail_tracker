"""Cost-of-change benchmark (pattern P1, doc/REFACTORING_TOKEN_ECONOMICS.md).

Replays a frozen representative-change prompt in a fresh agent against a throwaway
git worktree of HEAD, records what the agent read and what it cost, then discards
the worktree. Rows append to logs/cost_of_change.jsonl; comparisons are only valid
between rows with the same candidate, prompt_sha256, and model.

The agent runs with setting_sources=[] — no CLAUDE.md, no hooks, no MCP — so the
number measures pure code-orientation cost, matching the article's protocol
(martinfowler.com/articles/exploring-gen-ai/refactoring-economic-benefit.html).

Usage:
    .venv/Scripts/python tools/evals/cost_of_change_bench.py --smoke
    .venv/Scripts/python tools/evals/cost_of_change_bench.py --candidates C6
    .venv/Scripts/python tools/evals/cost_of_change_bench.py --candidates all

Interpretation guardrails (2026-07-31, caching + reproducibility literature:
arXiv:2601.06007, 2605.28840, 2506.09501):
  * total_input_tokens is ~90% prompt-cache reads (measured 88.5-96.3% across the
    C1-C7 rows) — a flat total across arms is expected, not a null result. Compare
    cost_usd and the fresh classes (usage.input_tokens + cache_creation_input_tokens).
  * One run per arm is Indicative only: output length has large run-to-run variance
    at fixed config. Run with --runs 5 per arm, then decide with
    tools/evals/bench_compare.py --candidate CN — it accepts a delta only when it
    exceeds the within-arm spread and reports tool_sequence agreement (the stabler
    comparator).
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

import services.runtime_env  # noqa: E402, F401  isort:skip  (BLAS cap; first project import at every entry point)

import argparse  # noqa: E402
import hashlib  # noqa: E402
import json  # noqa: E402
import subprocess  # noqa: E402
import tempfile  # noqa: E402
import time  # noqa: E402
from datetime import UTC, datetime  # noqa: E402

import anyio  # noqa: E402
from claude_agent_sdk import (  # noqa: E402
    AssistantMessage,
    ClaudeAgentOptions,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
    query,
)

PROMPT_DIR = REPO / "tools" / "evals" / "bench_prompts"
LOG_PATH = REPO / "logs" / "cost_of_change.jsonl"

# Candidate -> (prompt file, primary file whose LoC is the leading metric)
CANDIDATES = {
    "C1": ("C1_shared_css.md", "utility/shared_css.py"),
    "C2": ("C2_procurement_page.md", "utility/pages_code/procurement.py"),
    "C3": ("C3_member_overview.md", "utility/pages_code/member_overview.py"),
    "C4": ("C4_ui_components.md", "utility/ui/components.py"),
    "C5": ("C5_mcp_server.md", "mcp_server/server.py"),
    "C6": ("C6_sql_view_tests.md", "test/sql_views/test_sql_views.py"),
    "C7": ("C7_public_body_extractor.md", "extractors/procurement_public_body_extract.py"),
    # TALE budget A/B (arXiv:2412.18547): identical brief ± a numeric prose budget.
    # Compare with bench_compare.py — output_tokens is the metric under test.
    "TB_OFF": ("TB_budget_off.md", "test/sql_views/test_sql_views.py"),
    "TB_ON": ("TB_budget_on.md", "test/sql_views/test_sql_views.py"),
}

DISALLOWED_TOOLS = [
    "Bash",
    "PowerShell",
    "WebFetch",
    "WebSearch",
    "Task",
    "NotebookEdit",
    "TodoWrite",
]

SMOKE_PROMPT = (
    "Read the file README.md if it exists (otherwise any one file in the repo root), "
    "then reply with exactly: BENCH-SMOKE-OK followed by this JSON block:\n"
    '{"files_read": [{"path": "<the file>", "chars": 0}], "response_chars": 0}'
)


def _git(args, cwd=REPO):
    return subprocess.run(
        ["git", *args], cwd=str(cwd), capture_output=True, text=True, check=True
    ).stdout.strip()


def _add_worktree(tag: str, ref: str) -> Path:
    # Outside the repo tree so a live bench never shadows Grep/Glob results in it.
    base = Path(tempfile.gettempdir()) / "dail_bench_worktrees"
    base.mkdir(exist_ok=True)
    path = base / f"wt_{tag}_{int(time.time())}"
    _git(["worktree", "add", "--detach", str(path), ref])
    return path


def _remove_worktree(path: Path) -> None:
    try:
        _git(["worktree", "remove", "--force", str(path)])
    except subprocess.CalledProcessError as exc:
        print(f"WARN: worktree remove failed ({exc.stderr or exc}); pruning", file=sys.stderr)
        _git(["worktree", "prune"])


def _parse_selfreport(text: str):
    idx = text.rfind('"files_read"')
    if idx == -1:
        return None
    start = text.rfind("{", 0, idx)
    if start == -1:
        return None
    decoder = json.JSONDecoder()
    try:
        obj, _ = decoder.raw_decode(text[start:])
        return obj
    except ValueError:
        return None


async def run_one(cand: str, prompt: str, model: str, max_turns: int, keep_wt: bool, ref: str = "HEAD") -> dict:
    wt = _add_worktree(cand, ref)
    tool_calls: list[dict] = []
    final_text_parts: list[str] = []
    result: ResultMessage | None = None
    err = None
    t0 = time.monotonic()
    try:
        # allowed_tools does NOT restrict under bypassPermissions (C6 pilot ran
        # Bash despite it) — disallowed_tools is the restrictor.
        opts = ClaudeAgentOptions(
            model=model,
            max_turns=max_turns,
            cwd=str(wt),
            setting_sources=[],
            permission_mode="bypassPermissions",
            allowed_tools=["Read", "Grep", "Glob", "Edit", "Write"],
            disallowed_tools=DISALLOWED_TOOLS,
        )
        async for msg in query(prompt=prompt, options=opts):
            if isinstance(msg, AssistantMessage):
                final_text_parts = []  # keep only the last assistant message's text
                for b in msg.content:
                    if isinstance(b, ToolUseBlock):
                        inp = b.input or {}
                        tool_calls.append(
                            {
                                "tool": b.name,
                                "path": inp.get("file_path") or inp.get("path"),
                                "offset": inp.get("offset"),
                                "limit": inp.get("limit"),
                                "pattern": inp.get("pattern"),
                            }
                        )
                    elif isinstance(b, TextBlock):
                        final_text_parts.append(b.text)
            elif isinstance(msg, ResultMessage):
                result = msg
    except Exception as exc:  # keep the partial record — it's still a finding
        err = f"{type(exc).__name__}: {exc}"
    finally:
        if keep_wt:
            print(f"NOTE: keeping worktree {wt}", file=sys.stderr)
        else:
            _remove_worktree(wt)

    wall_s = round(time.monotonic() - t0, 1)
    final_text = "\n".join(final_text_parts)
    selfreport = _parse_selfreport(final_text)
    usage = dict(result.usage) if result and result.usage else {}
    total_input = sum(
        usage.get(k) or 0
        for k in ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    )
    selfreport_chars = (
        sum(f.get("chars") or 0 for f in selfreport.get("files_read", []))
        if isinstance(selfreport, dict)
        else None
    )
    row = {
        "ts": datetime.now(UTC).isoformat(timespec="seconds"),
        "candidate": cand,
        "model": model,
        "harness": {"setting_sources": [], "disallowed_tools": DISALLOWED_TOOLS},
        "git_head": _git(["rev-parse", "--short", ref]),
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16],
        "primary_file": CANDIDATES.get(cand, (None, None))[1],
        "primary_file_loc": None,
        "wall_s": wall_s,
        "num_turns": result.num_turns if result else None,
        "cost_usd": result.total_cost_usd if result else None,
        "usage": usage,
        "total_input_tokens": total_input or None,
        "output_tokens": usage.get("output_tokens"),
        "reads": [c for c in tool_calls if c["tool"] == "Read"],
        "tool_sequence": [c["tool"] for c in tool_calls],
        "selfreport": selfreport,
        "selfreport_read_chars": selfreport_chars,
        "selfreport_tokens_est": (selfreport_chars // 4) if selfreport_chars else None,
        "is_error": bool(result.is_error) if result else True,
        "error": err,
        "final_text_tail": final_text[-500:],
    }
    primary = CANDIDATES.get(cand, (None, None))[1]
    if primary:
        # LoC must reflect the ARM's tree (the ref), not the live working tree.
        try:
            row["primary_file_loc"] = len(_git(["show", f"{ref}:{primary}"]).splitlines())
        except subprocess.CalledProcessError:
            # Refactored into a package at this ref: the leading metric becomes the largest module.
            pkg = primary.rsplit(".", 1)[0]
            locs = [
                len(_git(["show", f"{ref}:{name}"]).splitlines())
                for name in _git(["ls-tree", "--name-only", ref, f"{pkg}/"]).splitlines()
                if name.endswith(".py")
            ]
            row["primary_file_loc"] = max(locs) if locs else None
    return row


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--candidates", default="", help="comma-separated C1..C7, or 'all'")
    ap.add_argument("--model", default="claude-sonnet-5")
    ap.add_argument("--max-turns", type=int, default=60)
    ap.add_argument("--smoke", action="store_true", help="run plumbing check only")
    ap.add_argument("--keep-worktree", action="store_true")
    ap.add_argument(
        "--runs", type=int, default=1,
        help="repeats per candidate (>=5 for a decidable A/B — see bench_compare.py)",
    )
    ap.add_argument(
        "--ref", default="HEAD",
        help="git ref the worktree is created from — lets an arm run at its exact "
        "before/after commit (rows record rev-parse of this ref as git_head)",
    )
    args = ap.parse_args()

    if args.smoke:
        jobs = [("SMOKE", SMOKE_PROMPT)]
    else:
        names = list(CANDIDATES) if args.candidates == "all" else [
            c.strip() for c in args.candidates.split(",") if c.strip()
        ]
        unknown = [c for c in names if c not in CANDIDATES]
        if unknown or not names:
            ap.error(f"unknown/empty candidates: {unknown or '(none given)'}")
        jobs = [
            (c, (PROMPT_DIR / CANDIDATES[c][0]).read_text(encoding="utf-8")) for c in names
        ]

    LOG_PATH.parent.mkdir(exist_ok=True)
    runs = max(1, args.runs)
    for cand, prompt in jobs:
        for run_idx in range(1, runs + 1):
            max_turns = 6 if cand == "SMOKE" else args.max_turns
            print(
                f"=== {cand} run {run_idx}/{runs} ({args.model}, ref={args.ref}, max_turns={max_turns}) ===",
                flush=True,
            )
            row = await run_one(cand, prompt, args.model, max_turns, args.keep_worktree, ref=args.ref)
            with LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            summary = {
                k: row[k]
                for k in (
                    "candidate",
                    "total_input_tokens",
                    "output_tokens",
                    "selfreport_tokens_est",
                    "num_turns",
                    "wall_s",
                    "cost_usd",
                    "is_error",
                    "error",
                )
            }
            u = row["usage"]
            summary["input_classes"] = {
                "fresh": u.get("input_tokens"),
                "cache_write": u.get("cache_creation_input_tokens"),
                "cache_read": u.get("cache_read_input_tokens"),
            }
            print(json.dumps(summary), flush=True)


anyio.run(main)
