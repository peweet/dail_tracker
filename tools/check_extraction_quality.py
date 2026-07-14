"""tools/check_extraction_quality.py — publish-time MATCH-RATE guard for extractors.

check_output_regressions answers "did the ROW COUNT collapse?" — but a source's PDF/HTML
layout can drift while the parser keeps running and keeps producing roughly the SAME row
count, with the extracted FIELDS silently degraded (blank, garbled, or unmatched against a
reference). Row count looks healthy; the data is garbage. This guard catches THAT class of
regression by watching the matched-vs-total ratios extractors already publish to
data/_meta/*_coverage.json (e.g. judiciary_diary_link's row-level judge-match rate,
entity_xref's CRO-match rate) — the same signal a human would eyeball, just automated and
run on every pipeline pass instead of only when someone happens to look.

PILOT (2026-07-14): 2 extractors with an existing, real matched/total ratio in their
coverage JSON. Add an ADAPTERS entry to extend to more — see the docstring on ADAPTERS.

Baseline: ``data/_meta/extraction_quality_baseline.json`` — ``{coverage_file: {metric: ratio}}``.
Regenerate deliberately after a legitimate change with ``--update-baseline`` (same convention
as check_output_regressions.py).

Pattern mirrors check_output_regressions.py / check_freshness / build_source_health:
    python tools/check_extraction_quality.py                    # report, write JSON, exit 0
    python tools/check_extraction_quality.py --strict            # exit 1 on any regression (CI gate)
    python tools/check_extraction_quality.py --update-baseline   # accept current ratios as baseline
    python tools/check_extraction_quality.py --tolerance 0.1     # stricter relative-drop sensitivity
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import PROJECT_ROOT  # noqa: E402

META_DIR = PROJECT_ROOT / "data" / "_meta"
BASELINE_PATH = META_DIR / "extraction_quality_baseline.json"
REPORT_PATH = META_DIR / "extraction_quality_regressions.json"
DEFAULT_TOLERANCE = 0.15  # a ratio falling >15% relative below its baseline is a regression

# coverage_file -> function(coverage_dict) -> {metric_name: (matched, total)}
# Each entry is a PILOT target: an extractor that already publishes a real matched/total
# count in its coverage JSON. Add more here as extractors are brought under the guard —
# no other file needs to change (the CLI/report/baseline machinery is generic).
ADAPTERS: dict[str, Callable[[dict], dict[str, tuple[int, int]]]] = {
    "judiciary_diary_link_coverage.json": lambda d: {
        "cases_row_level": (
            d["row_level"]["cases"]["rows_matched"],
            d["row_level"]["cases"]["rows_with_judge"],
        ),
        "schedule_row_level": (
            d["row_level"]["schedule"]["rows_matched"],
            d["row_level"]["schedule"]["rows_with_judge"],
        ),
    },
    "supplier_entity_xref_coverage.json": lambda d: {
        "cro_match": (d["with_cro"], d["supplier_entities"]),
    },
}


def emit_current(meta_dir: Path = META_DIR) -> dict[str, dict]:
    """{coverage_file: {metric: ratio}} for every ADAPTERS entry.

    A missing file, an adapter KeyError (coverage schema drifted), or a zero-total metric
    is recorded with an ``error`` marker rather than crashing the guard — mirrors
    check_output_regressions.emit_current's bad-file handling."""
    out: dict[str, dict] = {}
    for fname, adapt in ADAPTERS.items():
        path = meta_dir / fname
        if not path.exists():
            out[fname] = {"error": "missing"}
            continue
        try:
            data = orjson.loads(path.read_bytes())
            metrics = adapt(data)
            ratios: dict[str, float] = {}
            for metric, (matched, total) in metrics.items():
                if total <= 0:
                    ratios[metric] = {"error": "zero-total"}
                else:
                    ratios[metric] = round(matched / total, 4)
            out[fname] = ratios
        except Exception as e:  # noqa: BLE001 — a guard must report, not crash, on a bad file
            out[fname] = {"error": f"{type(e).__name__}: {e}"}
    return out


def find_regressions(
    current: dict[str, dict],
    baseline: dict[str, dict],
    tolerance: float = DEFAULT_TOLERANCE,
) -> list[dict]:
    """Pure comparison: every way the current match-rate is WORSE than the baseline.

    Only baselined (coverage_file, metric) pairs are checked (a brand-new metric has no
    baseline yet and is not a regression). Ratio improvement is never flagged."""
    regressions: list[dict] = []
    for fname, base_metrics in baseline.items():
        cur_metrics = current.get(fname)
        if cur_metrics is None or "error" in cur_metrics:
            regressions.append(
                {
                    "coverage_file": fname,
                    "kind": "MISSING",
                    "detail": (cur_metrics or {}).get("error", "absent"),
                }
            )
            continue
        for metric, b_ratio in base_metrics.items():
            c_ratio = cur_metrics.get(metric)
            if c_ratio is None:
                regressions.append({"coverage_file": fname, "metric": metric, "kind": "METRIC_MISSING"})
                continue
            if isinstance(c_ratio, dict):  # {"error": "zero-total"} etc.
                regressions.append(
                    {"coverage_file": fname, "metric": metric, "kind": "METRIC_ERROR", "detail": c_ratio}
                )
                continue
            if not isinstance(b_ratio, (int, float)):
                continue  # baseline itself was an error snapshot; nothing to compare
            floor = b_ratio * (1 - tolerance)
            if c_ratio < floor:
                regressions.append(
                    {
                        "coverage_file": fname,
                        "metric": metric,
                        "kind": "MATCH_RATE_DROP",
                        "baseline_ratio": b_ratio,
                        "ratio": c_ratio,
                        "pct_of_baseline": round(c_ratio / b_ratio * 100, 1) if b_ratio else None,
                    }
                )
    return regressions


def _load(path: Path) -> dict:
    if path.exists():
        try:
            return orjson.loads(path.read_bytes())
        except Exception:  # noqa: BLE001
            return {}
    return {}


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--update-baseline", action="store_true", help="accept current ratios as the new baseline")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any regression is found (CI gate)")
    ap.add_argument(
        "--tolerance", type=float, default=DEFAULT_TOLERANCE, help="relative ratio-drop fraction (default 0.15)"
    )
    ap.add_argument("--print", action="store_true", dest="echo", help="echo the report JSON to stdout")
    args = ap.parse_args(argv)

    current = emit_current()

    if args.update_baseline:
        _write(BASELINE_PATH, current)
        print(f"extraction-quality baseline updated: {len(current)} coverage file(s) -> {BASELINE_PATH}")
        return 0

    baseline = _load(BASELINE_PATH)
    if not baseline:
        print(
            f"extraction-quality guard: NO baseline at {BASELINE_PATH} - run --update-baseline once to "
            "capture current match-rates as the reference. Skipping (no gate)."
        )
        return 0

    regressions = find_regressions(current, baseline, tolerance=args.tolerance)
    payload = {"tolerance": args.tolerance, "n_baseline": len(baseline), "regressions": regressions}
    _write(REPORT_PATH, payload)

    if regressions:
        print(f"extraction-quality guard: {len(regressions)} REGRESSION(S) vs baseline:")
        for r in regressions:
            print(
                f"  [{r['kind']}] {r['coverage_file']}: { {k: v for k, v in r.items() if k not in ('coverage_file', 'kind')} }"
            )
        print("If this is an intended change, re-baseline: python tools/check_extraction_quality.py --update-baseline")
    else:
        print(
            f"extraction-quality guard: OK - {len(baseline)} coverage file(s) within {int(args.tolerance * 100)}% floor."
        )

    if args.echo:
        sys.stdout.buffer.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        print()

    return 1 if (args.strict and regressions) else 0


if __name__ == "__main__":
    sys.exit(main())
