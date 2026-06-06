"""tools/check_output_regressions.py — publish-time COMPLETENESS guard for gold.

Freshness answers "is the data RECENT?"; this answers "is it COMPLETE?". A PDF/layout
drift (or an API schema change) can parse WITHOUT error yet silently drop most rows or
rename/remove a column — shipping partial data behind a green pipeline. This guard
compares the current gold layer against a committed baseline and flags regressions.

Baseline: ``data/_meta/output_baseline.json`` — ``{parquet_name: {rows, columns}}`` for
every committed gold parquet. It travels with the repo, so CI can gate on it. Regenerate
deliberately after a legitimate change with ``--update-baseline``.

Regression kinds (row GROWTH and NEW columns are never regressions):
    MISSING      a baselined output is absent now
    EMPTIED      baseline had rows, output now has 0
    ROW_DROP     rows fell below baseline * (1 - tolerance)   [default tolerance 0.5]
    COL_REMOVED  a baselined column is no longer present (schema drift)

Pattern mirrors check_freshness / build_source_health:
    python tools/check_output_regressions.py                 # report, write JSON, exit 0
    python tools/check_output_regressions.py --strict        # exit 1 on any regression (CI gate)
    python tools/check_output_regressions.py --update-baseline  # accept current gold as the baseline
    python tools/check_output_regressions.py --tolerance 0.3 # stricter row-drop sensitivity
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import orjson
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import GOLD_PARQUET_DIR, PROJECT_ROOT  # noqa: E402

BASELINE_PATH = PROJECT_ROOT / "data" / "_meta" / "output_baseline.json"
REPORT_PATH = PROJECT_ROOT / "data" / "_meta" / "output_regressions.json"
DEFAULT_TOLERANCE = 0.5  # a >50% row loss is a regression; tune with --tolerance


def emit_current(gold_dir: Path = GOLD_PARQUET_DIR) -> dict[str, dict]:
    """{parquet_name: {rows, columns}} for every committed gold parquet.

    A bad file is recorded with an ``error`` marker rather than crashing the guard."""
    out: dict[str, dict] = {}
    for p in sorted(gold_dir.glob("*.parquet")):
        try:
            lf = pl.scan_parquet(p)
            rows = int(lf.select(pl.len()).collect().item())
            cols = list(lf.collect_schema().names())
            out[p.name] = {"rows": rows, "columns": cols}
        except Exception as e:  # noqa: BLE001 — a guard must report, not crash, on a bad file
            out[p.name] = {"error": f"{type(e).__name__}"}
    return out


def find_regressions(
    current: dict[str, dict],
    baseline: dict[str, dict],
    tolerance: float = DEFAULT_TOLERANCE,
) -> list[dict]:
    """Pure comparison: every way the current gold is WORSE than the baseline.

    Only the baselined outputs are checked (a brand-new output has no baseline yet and
    is not a regression). Row growth and added columns are never flagged."""
    regressions: list[dict] = []
    for name, base in baseline.items():
        cur = current.get(name)
        if cur is None or "error" in cur:
            regressions.append({"output": name, "kind": "MISSING", "detail": "absent or unreadable now"})
            continue
        b_rows, c_rows = base.get("rows"), cur.get("rows")
        if isinstance(b_rows, int) and isinstance(c_rows, int) and b_rows > 0:
            if c_rows == 0:
                regressions.append({"output": name, "kind": "EMPTIED", "baseline_rows": b_rows, "rows": 0})
            elif c_rows < int(b_rows * (1 - tolerance)):
                regressions.append(
                    {
                        "output": name,
                        "kind": "ROW_DROP",
                        "baseline_rows": b_rows,
                        "rows": c_rows,
                        "pct": round((c_rows - b_rows) / b_rows * 100, 1),
                    }
                )
        removed = sorted(set(base.get("columns") or []) - set(cur.get("columns") or []))
        if removed:
            regressions.append({"output": name, "kind": "COL_REMOVED", "columns": removed})
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
    ap.add_argument("--update-baseline", action="store_true", help="accept current gold as the new baseline")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any regression is found (CI gate)")
    ap.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE, help="row-drop fraction (default 0.5)")
    ap.add_argument("--print", action="store_true", dest="echo", help="echo the report JSON to stdout")
    args = ap.parse_args(argv)

    current = emit_current()

    if args.update_baseline:
        _write(BASELINE_PATH, {"outputs": current})
        print(f"output baseline updated: {len(current)} gold parquets -> {BASELINE_PATH}")
        return 0

    baseline = (_load(BASELINE_PATH) or {}).get("outputs", {})
    if not baseline:
        print(
            f"output regression guard: NO baseline at {BASELINE_PATH} - run --update-baseline once to "
            "capture the current gold as the reference. Skipping (no gate)."
        )
        return 0

    regressions = find_regressions(current, baseline, tolerance=args.tolerance)
    payload = {"tolerance": args.tolerance, "n_baseline": len(baseline), "regressions": regressions}
    _write(REPORT_PATH, payload)

    if regressions:
        print(f"output regression guard: {len(regressions)} REGRESSION(S) vs baseline:")
        for r in regressions:
            print(f"  [{r['kind']}] {r['output']}: { {k: v for k, v in r.items() if k not in ('output', 'kind')} }")
        print("If this is an intended change, re-baseline: python tools/check_output_regressions.py --update-baseline")
    else:
        print(f"output regression guard: OK - {len(baseline)} gold outputs within {int(args.tolerance * 100)}% floor.")

    if args.echo:
        sys.stdout.buffer.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        print()

    return 1 if (args.strict and regressions) else 0


if __name__ == "__main__":
    sys.exit(main())
