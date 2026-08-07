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
import sys
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from statistics import median

REPO = Path(__file__).resolve().parents[2]
LOG_PATH = REPO / "logs" / "cost_of_change.jsonl"

METRICS = ("cost_usd", "fresh_tokens", "output_tokens", "num_turns")


def fresh_tokens(row: dict) -> int | None:
    """The two full-price input classes — cache reads are 0.1x and dominate the flat
    total_input_tokens, so 'fresh' is the class a refactor can actually move."""
    u = row.get("usage") or {}
    a, b = u.get("input_tokens"), u.get("cache_creation_input_tokens")
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)


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


def arm_stats(rows: list[dict]) -> dict:
    out: dict = {"n": len(rows), "head": rows[0].get("git_head")}
    for m in METRICS:
        vals = [v for r in rows if (v := (fresh_tokens(r) if m == "fresh_tokens" else r.get(m))) is not None]
        out[m] = (
            {"median": round(median(vals), 4), "min": min(vals), "max": max(vals), "n": len(vals)} if vals else None
        )
    seqs = [r.get("tool_sequence") or [] for r in rows]
    out["tool_seq_agreement"] = seq_agreement(seqs)
    out["first2_modal"] = first2_modal_share(seqs)
    return out


def verdict(a: dict, b: dict, metric: str) -> str:
    """DECIDABLE only when |Δmedian| exceeds both arms' spread; else NOISE. A
    single-run arm has no measurable spread, so its delta can never be DECIDABLE."""
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


def compare(rows: list[dict], candidate: str, min_runs: int) -> list[str]:
    """Group -> latest (prompt_sha256, model) group -> last two git_head arms -> verdicts.
    Returns printable lines (pure, for tests)."""
    rows = [r for r in rows if r.get("candidate") == candidate and not r.get("is_error")]
    if not rows:
        return [f"no non-error rows for {candidate} in {LOG_PATH.name}"]
    key = (rows[-1].get("prompt_sha256"), rows[-1].get("model"))
    grp = [r for r in rows if (r.get("prompt_sha256"), r.get("model")) == key]
    skipped = len(rows) - len(grp)
    arms: dict[str, list[dict]] = {}
    for r in grp:  # insertion order = ts order (append-only log)
        arms.setdefault(r.get("git_head"), []).append(r)
    lines = [
        f"{candidate} | prompt {key[0]} | model {key[1]}"
        + (f" | {skipped} row(s) under other prompt/model ignored" if skipped else "")
    ]
    for head, arm in arms.items():
        s = arm_stats(arm)
        lines.append(
            f"  arm {head} n={s['n']}: "
            + "  ".join(f"{m} {s[m]['median']:g} [{s[m]['min']:g}-{s[m]['max']:g}]" for m in METRICS if s[m])
            + f"  | tool_seq agreement {s['tool_seq_agreement']} first2 {s['first2_modal']}"
        )
        if s["n"] < min_runs:
            lines.append(f"    ⚠ n<{min_runs} — Indicative-grade; add runs before deciding")
        if s["tool_seq_agreement"] is not None and s["tool_seq_agreement"] < 0.6:
            lines.append("    ⚠ low within-arm agreement — brief is ambiguous; rewrite it first (E4)")
    heads = list(arms)
    if len(heads) < 2:
        lines.append("one arm only — run the bench again on the other git_head to compare")
        return lines
    a, b = arm_stats(arms[heads[-2]]), arm_stats(arms[heads[-1]])
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
