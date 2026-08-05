"""Counterfactual harness benchmark with clean-room execution and JSONL evidence.

OFFCLEAN and ON run in the same ephemeral copy of the live working tree. The copy
contains project guidance for the ON arm, but no Git metadata, private product
overlay, eval prompts, scorer, or tests for the scorer. OFFCLEAN disables project
settings and MCP. The legacy OFF arm runs in the source checkout and is retained
only to reproduce the known auto-memory contamination.

The ON arm explicitly trusts the validated ephemeral project layer and bypasses
the interactive hook-hash prompt so project hooks are not silently omitted. The
paid path runs the same preflight checks before starting either provider.

This is cwd-level contamination resistance, not a host security boundary. For a
strict secret holdout, run the benchmark in a container/VM that mounts only the
cleanroom plus the provider runtime, with the holdout mounted evaluator-side.

Public tasks are smoke/regression checks. A real evaluation can supply a private
JSON task file outside this repository with ``--tasks-file``; the evaluator reads
its expected structured answers, but that file is never copied into agent cwd.

Run:
    python tools/evals/harness_bench.py --repeat 3 offclean on
    python tools/evals/harness_bench.py --tasks-file C:/private/holdout.json on

The script emits one metadata row, one row per task attempt, and aggregate rows.
It makes paid provider calls; tests import helpers but never call ``main``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anyio

PROJ_PATH = Path(__file__).resolve().parents[2]
if str(PROJ_PATH) not in sys.path:
    sys.path.insert(0, str(PROJ_PATH))

from tools.evals.cleanroom import prepare_cleanroom, validate_cleanroom  # noqa: E402
from tools.evals.provider_adapter import EvalRequest, dail_tracker_mcp, run_eval  # noqa: E402

PROJ = str(PROJ_PATH)
VARIANTS = ("off", "offclean", "on")
PREFLIGHT_REQUIRED_FILES = (
    "AGENTS.md",
    ".codex/config.toml",
    ".codex/agents/reviewer.toml",
    ".codex/agents/scout.toml",
    ".codex/agents/worker.toml",
    "mcp_server/server.py",
    "tools/check_agent_context.py",
    "tools/hooks/closeout_gate.py",
    "tools/hooks/discovery_hint.py",
    "tools/hooks/guard_subagent_spawn.py",
    "tools/hooks/session_context.py",
)

PUBLIC_TASKS: dict[str, dict[str, Any]] = {
    "never-sum": {
        "kind": "never-sum",
        "prompt": (
            "Working with this repo's data: a supplier appears in the procurement awards "
            "data with awarded contract totals, and also in the public payments data with "
            "amounts actually paid. For a supplier profile, is it methodologically sound to "
            "add the awarded total and the paid total into one combined figure? Reply ONLY "
            'with JSON: {"combined_figure_allowed": true|false, "reason": "<one sentence>"}'
        ),
    },
    "data-shape": {
        "kind": "data-shape",
        "prompt": (
            "For this repo's procurement awards dataset: what is its row grain, how many "
            "rows does it hold, and name five of its columns. Reply ONLY with JSON: "
            '{"grain": "...", "rows": <integer>, "columns": ["...", 5 names]}'
        ),
    },
    "code-nav": {
        "kind": "code-nav",
        "prompt": (
            "In this repo: which file and which function implement the shared atomic "
            "parquet write with the row-floor guard that all pipeline ETL must use? Reply "
            'ONLY with JSON: {"file": "<repo-relative path>", "function": "<name>"}'
        ),
    },
    "conventions": {
        "kind": "conventions",
        "prompt": (
            "A new data extractor is being added to this repo. Per the project's "
            "conventions, which helper modules must it use for (1) HTTP fetching, "
            "(2) coverage logging, (3) parquet writing, and (4) extractor run "
            'orchestration? Reply ONLY with JSON: {"helpers": ["...four module names..."]}'
        ),
    },
    "memory-xbrl": {
        "kind": "memory-xbrl",
        "prompt": (
            "This project extracts Irish local-authority annual financial statements by "
            "scraping PDFs. Is a public XBRL or other structured data feed available for "
            "these statements that the project should be consuming instead? Reply ONLY "
            'with JSON: {"structured_feed_available": true|false, "reason": "<one sentence>"}'
        ),
    },
}


def current_awards_ground_truth(repo: Path = PROJ_PATH) -> dict[str, Any]:
    """Read mutable shape facts at evaluation time, not from a frozen answer key."""
    payload = json.loads((repo / "data" / "_meta" / "fact_cards.json").read_text(encoding="utf-8"))
    card = payload["facts"]["procurement_awards"]
    return {
        "rows": int(card["rows"]),
        "grain": str(card["grain"]),
        "columns": {str(column).lower() for column in card["columns"]},
    }


def _leaf_scores(expected: Any, actual: Any) -> list[bool]:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return [False] * max(1, len(expected))
        scores: list[bool] = []
        for key, value in expected.items():
            scores.extend(_leaf_scores(value, actual.get(key)))
        return scores or [actual == expected]
    if isinstance(expected, list):
        return [actual == expected]
    return [actual == expected]


def score_answer(task: dict[str, Any], answer: dict[str, Any], *, repo: Path = PROJ_PATH) -> float:
    if not isinstance(answer, dict):
        return 0.0
    kind = task["kind"]
    try:
        if kind == "private-exact":
            leaves = _leaf_scores(task["expected"], answer)
            return round(sum(leaves) / len(leaves), 3)
        if kind == "never-sum":
            return 1.0 if answer.get("combined_figure_allowed") is False else 0.0
        if kind == "data-shape":
            truth = current_awards_ground_truth(repo)
            grain = 1.0 if str(answer.get("grain", "")).strip().lower() == truth["grain"].lower() else 0.0
            rows = 1.0 if answer.get("rows") == truth["rows"] else 0.0
            columns = [str(column).lower() for column in answer.get("columns", [])][:5]
            column_score = sum(column in truth["columns"] for column in columns) / 5 if columns else 0.0
            return round((grain + rows + column_score) / 3, 3)
        if kind == "code-nav":
            file_ok = "parquet_io.py" in str(answer.get("file", "")).replace("\\", "/")
            function_ok = str(answer.get("function", "")).strip("()") == "save_parquet"
            return round((file_ok + function_ok) / 2, 3)
        if kind == "conventions":
            blob = " ".join(str(helper).lower() for helper in answer.get("helpers", []))
            wanted = ("http_engine", "coverage_io", "parquet_io", "extract_runner")
            return round(sum(helper in blob for helper in wanted) / len(wanted), 3)
        if kind == "memory-xbrl":
            return 1.0 if answer.get("structured_feed_available") is False else 0.0
    except (KeyError, TypeError, ValueError):
        return 0.0
    return 0.0


def parse_answer(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text or "", re.DOTALL)
    if not match:
        return {}
    try:
        value = json.loads(match.group(0))
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        return {}


def load_private_tasks(path: Path) -> dict[str, dict[str, Any]]:
    resolved = path.resolve()
    try:
        resolved.relative_to(PROJ_PATH)
    except ValueError:
        pass
    else:
        raise ValueError("private eval task files must live outside the repository")
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    records = payload.get("tasks") if isinstance(payload, dict) else None
    if not isinstance(records, dict) or not records:
        raise ValueError("private task file must contain a non-empty 'tasks' object")
    tasks: dict[str, dict[str, Any]] = {}
    for task_id, record in records.items():
        if not isinstance(record, dict) or not isinstance(record.get("prompt"), str):
            raise ValueError(f"task {task_id!r} needs a string prompt")
        if not isinstance(record.get("expected"), dict):
            raise ValueError(f"task {task_id!r} needs an expected JSON object")
        tasks[str(task_id).lower()] = {
            "kind": "private-exact",
            "prompt": record["prompt"],
            "expected": record["expected"],
        }
    return tasks


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _git_value(*args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", PROJ, *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unknown"


def run_manifest(tasks: dict[str, dict[str, Any]], *, repeats: int, cleanroom: dict | None) -> dict[str, Any]:
    task_fingerprint = {
        task_id: {key: value for key, value in task.items() if key != "expected"} for task_id, task in tasks.items()
    }
    return {
        "type": "eval_run",
        "run_id": str(uuid.uuid4()),
        "started_utc": datetime.now(UTC).isoformat(),
        "source_revision": _git_value("rev-parse", "HEAD"),
        "source_dirty": bool(_git_value("status", "--porcelain", "--untracked-files=no")),
        "harness_sha256": _sha256(Path(__file__).read_bytes()),
        "task_suite_sha256": _sha256(json.dumps(task_fingerprint, sort_keys=True).encode("utf-8")),
        "task_ids": list(tasks),
        "repeats": repeats,
        "provider_override": os.environ.get("DAIL_EVAL_PROVIDER", "auto"),
        "model_override": os.environ.get("DAIL_EVAL_MODEL", "provider-default"),
        "reasoning_effort": os.environ.get("DAIL_EVAL_REASONING_EFFORT", "medium"),
        "infrastructure_label": os.environ.get("DAIL_EVAL_INFRA_LABEL", "unspecified"),
        "holdout_version": os.environ.get("DAIL_EVAL_HOLDOUT_VERSION", "public-smoke"),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "max_turns": 12,
        "timeout_seconds": os.environ.get("DAIL_EVAL_TIMEOUT_SECONDS", "provider-default"),
        "network_policy": "disabled for Codex; provider adapter default for Claude",
        "isolation_level": "cwd-cleanroom; host filesystem is not isolated" if cleanroom else "none",
        "cleanroom": cleanroom,
    }


async def run_task(
    task_id: str,
    task: dict[str, Any],
    variant: str,
    *,
    cwd: Path,
    repeat_index: int,
    run_id: str,
) -> dict[str, Any]:
    on = variant == "on"
    started_at = time.perf_counter()
    result = None
    error = None
    try:
        result = await run_eval(
            EvalRequest(
                prompt=task["prompt"],
                cwd=cwd,
                claude_model="claude-sonnet-5",
                max_turns=12,
                sandbox="read-only",
                project_settings=on,
                trusted_project_hooks=on,
                env={"PYTHONUTF8": "1"},
                mcp_servers=dail_tracker_mcp(PROJ) if on else {},
            )
        )
        if result.is_error:
            error = result.error
    except Exception as exc:  # preserve a scored attempt row for infrastructure failures
        error = f"{type(exc).__name__}: {exc}"

    calls = result.tool_names if result else []
    answer = parse_answer(result.final_text if result else "")
    row: dict[str, Any] = {
        "type": "attempt",
        "run_id": run_id,
        "repeat": repeat_index,
        "task": task_id,
        "variant": variant,
        "score": score_answer(task, answer),
        "tool_calls": len(calls),
        "mcp_calls": sum(call.startswith("mcp__") for call in calls),
        "cost_usd": round(result.cost_usd, 4) if result and result.cost_usd is not None else None,
        "answer": answer,
        "sequence": calls,
        "provider": result.provider if result else None,
        "model": result.model if result else None,
        "usage": result.usage if result else {},
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
    }
    if error:
        row["error"] = error
    return row


def summary_rows(attempts: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    keys = sorted({(row["variant"], row["task"]) for row in attempts})
    for variant, task_id in keys:
        selected = [row for row in attempts if row["variant"] == variant and row["task"] == task_id]
        scores = [float(row["score"]) for row in selected]
        latencies = [float(row.get("elapsed_seconds", 0.0)) for row in selected]
        costs = [float(row["cost_usd"]) for row in selected if row.get("cost_usd") is not None]
        usage_keys = (
            "input_tokens",
            "cache_read_input_tokens",
            "raw_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
        )
        rows.append(
            {
                "type": "summary",
                "run_id": run_id,
                "variant": variant,
                "task": task_id,
                "n": len(scores),
                "score_mean": round(statistics.fmean(scores), 3),
                "score_min": min(scores),
                "score_max": max(scores),
                "error_count": sum("error" in row for row in selected),
                "elapsed_seconds_mean": round(statistics.fmean(latencies), 3),
                "elapsed_seconds_min": min(latencies),
                "elapsed_seconds_max": max(latencies),
                "tool_calls_total": sum(int(row.get("tool_calls", 0)) for row in selected),
                "mcp_calls_total": sum(int(row.get("mcp_calls", 0)) for row in selected),
                "cost_usd_total": round(sum(costs), 4) if costs else None,
                **{
                    f"{key}_total": sum(int(row.get("usage", {}).get(key, 0)) for row in selected) for key in usage_keys
                },
            }
        )
    return rows


def preflight_report(cleanroom: Path) -> dict[str, Any]:
    """Validate the real agent cwd without starting a provider or spending tokens."""
    metadata = validate_cleanroom(cleanroom)
    missing = [relative for relative in PREFLIGHT_REQUIRED_FILES if not (cleanroom / relative).is_file()]
    if missing:
        raise ValueError(f"eval cleanroom is missing required files: {', '.join(missing)}")
    if (cleanroom / "planning" / "product").exists():
        raise ValueError("eval cleanroom exposes the private product overlay")
    return {
        "type": "preflight",
        "ok": True,
        "files_copied": metadata["files_copied"],
        "source_revision": metadata["source_revision"],
        "required_files": list(PREFLIGHT_REQUIRED_FILES),
        "scorer_excluded": not (cleanroom / "tools" / "evals").exists(),
        "git_metadata_excluded": not (cleanroom / ".git").exists(),
        "private_overlay_excluded": not (cleanroom / "planning" / "product").exists(),
        "provider_calls": 0,
    }


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("selectors", nargs="*", help="variants and/or task ids")
    parser.add_argument("--repeat", type=_positive_int, default=1)
    parser.add_argument("--tasks-file", type=Path, help="private holdout JSON outside the repository")
    parser.add_argument("--preflight", action="store_true", help="validate isolation and wiring without provider calls")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    tasks = load_private_tasks(args.tasks_file) if args.tasks_file else dict(PUBLIC_TASKS)
    selectors = [selector.lower() for selector in args.selectors]
    unknown = sorted(set(selectors) - set(VARIANTS) - set(tasks))
    if unknown:
        raise SystemExit(f"unknown selector(s): {', '.join(unknown)}")
    variants = [variant for variant in VARIANTS if variant in selectors] or ["offclean", "on"]
    selected_tasks = [task_id for task_id in tasks if task_id in selectors] or list(tasks)

    if args.preflight:
        with prepare_cleanroom(PROJ_PATH) as clean_path:
            print(json.dumps(preflight_report(clean_path), ensure_ascii=False))
        return

    attempts: list[dict[str, Any]] = []
    needs_cleanroom = any(variant != "off" for variant in variants)
    if needs_cleanroom:
        with prepare_cleanroom(PROJ_PATH) as clean_path:
            clean_meta = preflight_report(clean_path)
            manifest = run_manifest(tasks, repeats=args.repeat, cleanroom=clean_meta)
            print(json.dumps(manifest, ensure_ascii=False), flush=True)
            for repeat_index in range(1, args.repeat + 1):
                for variant in variants:
                    cwd = PROJ_PATH if variant == "off" else clean_path
                    for task_id in selected_tasks:
                        row = await run_task(
                            task_id,
                            tasks[task_id],
                            variant,
                            cwd=cwd,
                            repeat_index=repeat_index,
                            run_id=manifest["run_id"],
                        )
                        attempts.append(row)
                        print(json.dumps(row, ensure_ascii=False), flush=True)
    else:
        manifest = run_manifest(tasks, repeats=args.repeat, cleanroom=None)
        print(json.dumps(manifest, ensure_ascii=False), flush=True)
        for repeat_index in range(1, args.repeat + 1):
            for task_id in selected_tasks:
                row = await run_task(
                    task_id,
                    tasks[task_id],
                    "off",
                    cwd=PROJ_PATH,
                    repeat_index=repeat_index,
                    run_id=manifest["run_id"],
                )
                attempts.append(row)
                print(json.dumps(row, ensure_ascii=False), flush=True)

    for row in summary_rows(attempts, manifest["run_id"]):
        print(json.dumps(row, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    anyio.run(main, sys.argv[1:])
