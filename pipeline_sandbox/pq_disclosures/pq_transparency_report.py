"""
pq_transparency_report.py — EXPERIMENTAL transparency index over mined PQ answers.

Reads a parquet produced by pq_answer_mine_experimental.py and ranks departments
by how often they REFUSE data-seeking written questions (least transparent first).

Conservative denominator: only data-seeking, answered questions count. A "refusal"
needs an explicit withholding basis (commercial sensitivity / does not publish /
not held centrally / privacy / disproportionate cost). "Deflections" (operational
matter for the agency) are reported separately, NOT as refusals.

This is a SIGNAL, not a verdict: small per-department samples are noisy, and the
classifier is ~80-85% precise (soft "not readily available" can over-count). Rates
are shown with n so they can be read with appropriate scepticism.

Run:
    python -m pipeline_sandbox.pq_disclosures.pq_transparency_report \
        --in data/_sandbox/pq_disclosures_broad.parquet --min-n 20
"""

from __future__ import annotations

import argparse
import sys

import polars as pl


def _distinct_replies(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse grouped questions to one row per distinct ministerial reply.

    Grouped questions (many TDs, one shared answer) otherwise inflate every rate
    — and refusals cluster on exactly the heavily-grouped controversial topics,
    so the row-based rate overstates. The honest unit is the distinct reply.
    """
    return df.filter(pl.col("has_reply")).unique(subset=["department", "answer_text"])


def report(df: pl.DataFrame, min_n: int) -> pl.DataFrame:
    """One row per department over data-seeking distinct replies."""
    base = _distinct_replies(df).filter(pl.col("data_sought"))
    g = (
        base.group_by("department")
        .agg(
            n=pl.len(),
            refusals=pl.col("is_refusal").sum(),
            deflections=pl.col("is_deflection").sum(),
            discloses=pl.col("discloses").sum(),
        )
        .with_columns(
            refusal_rate=(pl.col("refusals") / pl.col("n")).round(3),
            disclose_rate=(pl.col("discloses") / pl.col("n")).round(3),
        )
        # least transparent first = highest refusal rate
        .sort(["refusal_rate", "n"], descending=[True, True])
    )
    return g.filter(pl.col("n") >= min_n)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in", dest="inp", default="data/_sandbox/pq_disclosures_broad.parquet")
    ap.add_argument("--min-n", type=int, default=20, help="min data-seeking answers to rank a dept")
    args = ap.parse_args(argv)

    df = pl.read_parquet(args.inp)
    seeking = _distinct_replies(df).filter(pl.col("data_sought"))
    print(f"corpus: {df.height} Q&A rows | {_distinct_replies(df).height} distinct replies "
          f"| {seeking.height} data-seeking (the denominator)")
    print(f"overall refusal rate (data-seeking): {seeking['is_refusal'].mean():.1%}")
    print(f"overall disclosure rate            : {seeking['discloses'].mean():.1%}")
    print()

    ranked = report(df, args.min_n)
    print(f"=== DEPARTMENTS RANKED — LEAST TRANSPARENT FIRST (n>={args.min_n}) ===")
    print(f"{'refuse%':>7} {'disc%':>6} {'defl':>4} {'n':>4}  department")
    for r in ranked.to_dicts():
        print(
            f"{r['refusal_rate']*100:6.1f}% {r['disclose_rate']*100:5.1f}% "
            f"{r['deflections']:4d} {r['n']:4d}  {r['department']}"
        )

    print()
    print("=== refusal grounds invoked (distinct replies) ===")
    rt = (
        _distinct_replies(df).filter(pl.col("is_refusal"))
        .group_by("refusal_type")
        .agg(n=pl.len())
        .sort("n", descending=True)
    )
    for r in rt.to_dicts():
        print(f"  {r['n']:4d}  {r['refusal_type']}")

    low = seeking.group_by("department").agg(n=pl.len())
    low = low.filter(pl.col("n") < args.min_n).sort("n", descending=True)
    if low.height:
        print()
        print(f"(too few to rank, n<{args.min_n}): "
              + ", ".join(f"{r['department']}({r['n']})" for r in low.to_dicts()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
