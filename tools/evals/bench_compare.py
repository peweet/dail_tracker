"""Decide a cost-of-change A/B from logs/cost_of_change.jsonl — the n>=5 doctrine.

Companion to cost_of_change_bench.py (which appends the rows; its --runs flag
produces the repeats). Rows are comparable only within (candidate, prompt_sha256,
model); arms are git_head values inside such a group (before vs after a refactor).

Decision rule (doc/archive/TOKEN_OPTIMIZATION_LITERATURE_2026_07_31.md, E1/E7: arXiv
2605.28840 + 2506.09501): accept a metric delta only when the difference of arm
medians exceeds BOTH arms' within-arm spread (max - min); otherwise it is NOISE —
output length varies ~9k tokens run-to-run at fixed config, so n=1 deltas are
Indicative-grade. tool_sequence agreement (mean pairwise SequenceMatcher ratio,
plus the modal share of the first two calls — E3: steps 1-2 carry ~60% of the
divergence signal) is the stabler comparator: a low-agreement arm means the task
brief is ambiguous — rewrite the brief before trusting any token delta (E4).

Usage:
    .venv/Scripts/python tools/evals/bench_compare.py --candidate C2
    .venv/Scripts/python tools/evals/bench_compare.py --candidate C2 --min-runs 3
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from statistics import median

REPO = Path(__file__).resolve().parents[2]
LOG_PATH = REPO / "logs" / "cost_of_change.jsonl"

METRICS = ("cost_usd", "fresh_tokens", "output_tokens", "num_turns")


def fresh_tokens(row: dict) -> int | None:
    """Sum observed disjoint fresh-input classes; absent counters remain unknown."""
    u = row.get("usage") or {}
    if not isinstance(u, dict):
        return None
    reported = row.get("usage_reported_fields")
    if reported is not None and not {"input_tokens", "cache_creation_input_tokens"}.issubset(reported):
        return None
    a, b = u.get("input_tokens"), u.get("cache_creation_input_tokens")
    if type(a) is not int or type(b) is not int or a < 0 or b < 0:
        return None
    return a + b


def seq_agreement(seqs: list[list[str]]) -> float | None:
    """Mean pairwise SequenceMatcher ratio over tool-name sequences (1.0 = identical)."""
    if len(seqs) < 2:
        return None
    ratios = [SequenceMatcher(a=seqs[i], b=seqs[j]).ratio() for i in range(len(seqs)) for j in range(i + 1, len(seqs))]
    return round(sum(ratios) / len(ratios), 3)


def first2_modal_share(seqs: list[list[str]]) -> str | None:
    """'4/5' — how many runs open with the modal first-two tool calls."""
    if not seqs:
        return None
    counts = Counter(tuple(s[:2]) for s in seqs)
    return f"{counts.most_common(1)[0][1]}/{len(seqs)}"


def _completed(row: dict) -> bool:
    return not row.get("is_error") and not row.get("error")


def _metric_value(row: dict, metric: str):
    reported = row.get("usage_reported_fields")
    if metric == "output_tokens" and reported is not None and metric not in reported:
        return None
    value = fresh_tokens(row) if metric == "fresh_tokens" else row.get(metric)
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def _metric_stats(rows: list[dict], metric: str) -> dict | None:
    values = [_metric_value(row, metric) for row in rows]
    # Partial observations cannot support an arm-level metric.
    if not values or any(value is None for value in values):
        return None
    return {"median": round(median(values), 4), "min": min(values), "max": max(values), "n": len(values)}


def _per_success(rows: list[dict], metric: str, successful_n: int):
    values = [_metric_value(row, metric) for row in rows]
    if not successful_n or any(value is None for value in values):
        return None
    return sum(values) / successful_n


def arm_stats(rows: list[dict]) -> dict:
    completed = [r for r in rows if _completed(r)]
    successful = [r for r in completed if r.get("quality_passed") is True]
    costs = [_metric_value(row, "cost_usd") for row in rows]
    observed = [value for value in costs if value is not None]
    total = sum(observed) if len(observed) == len(rows) else None
    out: dict = {
        "n": len(rows),
        "successful_n": len(successful),
        "completed_n": len(completed),
        "failure_n": len(rows) - len(completed) + sum(r.get("quality_passed") is False for r in completed),
        "quality_unknown_n": sum(
            r.get("quality_passed") is not True and r.get("quality_passed") is not False for r in completed
        ),
        "cost_observed_n": len(observed),
        "cost_usd_observed_total": sum(observed) if observed else None,
        "cost_usd_spent": total,
        "cost_per_success": _per_success(rows, "cost_usd", len(successful)),
        "per_success": {metric: _per_success(rows, metric, len(successful)) for metric in METRICS},
        "head": rows[0].get("git_head") if rows else None,
    }
    out.update({metric: _metric_stats(successful, metric) for metric in METRICS})
    seqs = [r.get("tool_sequence") or [] for r in successful]
    out["tool_seq_agreement"] = seq_agreement(seqs)
    out["first2_modal"] = first2_modal_share(seqs)
    return out


def verdict(a: dict, b: dict, metric: str) -> str:
    """DECIDABLE only when |Δmedian| exceeds both arms' spread; else NOISE. A
    single-run arm has no measurable spread, so its delta can never be DECIDABLE."""
    if a.get("quality_unknown_n") or b.get("quality_unknown_n"):
        return f"{metric}: INCONCLUSIVE (quality acceptance unmeasured)"
    if a.get("failure_n") or b.get("failure_n"):
        return f"{metric}: INCONCLUSIVE (failure-affected arms; inspect total cost per success)"
    sa, sb = a.get(metric), b.get(metric)
    if not sa or not sb:
        return f"{metric}: no data in one arm"
    delta = sb["median"] - sa["median"]
    spread = max(sa["max"] - sa["min"], sb["max"] - sb["min"])
    pct = f" ({100 * delta / sa['median']:+.0f}%)" if sa["median"] else ""
    if sa["n"] < 2 or sb["n"] < 2:
        call = "INDICATIVE (single-run arm — spread unmeasured, add runs)"
    elif abs(delta) > spread:
        call = "DECIDABLE"
    else:
        call = "NOISE (delta within run-to-run spread)"
    return f"{metric}: Δmedian {delta:+g}{pct} vs within-arm spread {spread:g} → {call}"


def comparison_identity(row: dict) -> tuple | None:
    """Full identity required before two benchmark rows are comparable."""
    fields = (
        "provider",
        "model",
        "reasoning_effort",
        "harness_sha256",
        "tool_policy_identity",
        "input_sha256",
        "usage_schema_identity",
    )
    values = tuple(row.get(field) for field in fields)
    if not row.get("git_head") or any(not isinstance(value, str) or not value.strip() for value in values):
        return None
    return values


def compare(rows: list[dict], candidate: str, min_runs: int) -> list[str]:
    """Group by full experiment identity, then compare the last two git_head arms."""
    rows = [r for r in rows if r.get("candidate") == candidate]
    if not rows or (key := comparison_identity(rows[-1])) is None:
        return [f"{candidate}: INCOMPARABLE (no complete latest experiment identity in {LOG_PATH.name})"]
    grp = [r for r in rows if comparison_identity(r) == key]
    skipped = len(rows) - len(grp)
    arms: dict[str, list[dict]] = {}
    for r in grp:  # insertion order = ts order (append-only log)
        arms.setdefault(r.get("git_head"), []).append(r)
    lines = [
        f"{candidate} | identity {key!r}"
        + (f" | {skipped} row(s) under other experiment identity ignored" if skipped else "")
    ]
    for head, arm in arms.items():
        s = arm_stats(arm)
        lines.append(
            f"  arm {head} n={s['n']} successful_n={s['successful_n']} failure_n={s['failure_n']} "
            f"quality_unknown_n={s['quality_unknown_n']} spent={s['cost_usd_spent']} "
            f"cost_per_success={s['cost_per_success']} cost_observed={s['cost_observed_n']}/{s['n']}: "
            + "  ".join(f"{m} {s[m]['median']:g} [{s[m]['min']:g}-{s[m]['max']:g}]" for m in METRICS if s[m])
            + f"  | tool_seq agreement {s['tool_seq_agreement']} first2 {s['first2_modal']}"
        )
        if s["successful_n"] < min_runs:
            lines.append(f"    ⚠ n<{min_runs} — Indicative-grade; add runs before deciding")
        if s["tool_seq_agreement"] is not None and s["tool_seq_agreement"] < 0.6:
            lines.append("    ⚠ low within-arm agreement — brief is ambiguous; rewrite it first (E4)")
    heads = list(arms)
    if len(heads) < 2:
        lines.append("one arm only — run the bench again on the other git_head to compare")
        return lines
    a, b = arm_stats(arms[heads[-2]]), arm_stats(arms[heads[-1]])
    if a["failure_n"] or b["failure_n"]:
        lines.append("failure-affected arms: total cost and usage per accepted task include failed attempts")
    if min(a["successful_n"], b["successful_n"]) < min_runs:
        lines.append("INSUFFICIENT accepted repeats; no adoption verdict")
        return lines
    lines.append(f"VERDICTS ({heads[-2]} → {heads[-1]}):")
    lines.extend("  " + verdict(a, b, m) for m in METRICS)
    if len(heads) > 2:
        lines.append(f"({len(heads) - 2} older arm(s) not compared: {heads[:-2]})")
    return lines


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--candidate", required=True, help="C1..C7 (or SMOKE)")
    ap.add_argument("--min-runs", type=int, default=5)
    ap.add_argument("--log", default=str(LOG_PATH))
    args = ap.parse_args()
    rows = []
    with open(args.log, encoding="utf-8") as fh:
        for line in fh:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    print("\n".join(compare(rows, args.candidate, args.min_runs)))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
