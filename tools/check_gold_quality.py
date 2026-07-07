"""tools/check_gold_quality.py — content-quality regression guard for the gold layer.

``check_output_regressions.py`` answers "did a table lose ROWS or COLUMNS?". This answers
the next question: "did the CONTENT rot?" — the silent failures that keep the row count
and schema intact while corrupting the data:

    * a column silently goes 100% NULL (a join broke, a source field was renamed) — the
      column is still there, so the output-baseline never notices;
    * duplicate rows multiply (a missing ``.unique()`` after a concat, a join fan-out) —
      totals and counts inflate without any error;
    * mojibake (the Unicode replacement char ``�``) appears in text (a source served a new
      encoding the parser mis-decoded);
    * null-sentinel strings ('null', 'n/a', '#N/A') leak in as real values.

These are table-specific by nature — a duplicated ``member_name='Vacancy'`` row on a state
board is LEGITIMATE; an identical row in a judge→bench lookup is a bug. So this guard does
NOT hard-code "duplicates are bad". Instead it follows the metric-repository pattern (à la
Deequ / Elementary): it MEASURES each metric, baselines the current reality, and flags only
a REGRESSION — a metric that got worse than the committed baseline. That accepts today's
known quirks while catching tomorrow's drift, with no false-positives on stable-but-odd data.

Usage mirrors check_output_regressions.py:
    python tools/check_gold_quality.py                  # measure, write report, exit 0
    python tools/check_gold_quality.py --strict         # exit 1 on any regression (CI gate)
    python tools/check_gold_quality.py --update-baseline # accept current gold as the baseline
    python tools/check_gold_quality.py --print          # echo the report
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import orjson
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import GOLD_PARQUET_DIR, PROJECT_ROOT  # noqa: E402

BASELINE_PATH = PROJECT_ROOT / "data" / "_meta" / "gold_quality_baseline.json"
REPORT_PATH = PROJECT_ROOT / "data" / "_meta" / "gold_quality.json"

REPLACEMENT_CHAR = "�"  # � — mojibake from a mis-decoded byte stream
# Whole-value null sentinels (case-insensitive, after trim). Deliberately conservative:
# excludes "-"/"na"/"" which are frequently legitimate as-filed values; the baseline absorbs
# any that are real, so only NEW/growing contamination is flagged.
SENTINELS = frozenset({"null", "none", "nan", "n/a", "#n/a", "undefined", "nil"})

# Regression sensitivity: a duplicate-row increase must exceed BOTH a fraction of the baseline
# AND an absolute floor, so a +1-row wobble on a big table is not flagged as drift.
DUP_GROWTH_FRAC = 0.10
DUP_GROWTH_FLOOR = 10


def measure_table(df: pl.DataFrame) -> dict:
    """Compute the content-quality metric record for one gold table."""
    rows = df.height
    all_null_cols: list[str] = []
    encoding: dict[str, int] = {}
    sentinels: dict[str, int] = {}
    for c, dt in df.schema.items():
        col = df[c]
        if col.null_count() == rows and rows > 0:
            all_null_cols.append(c)
            continue
        if dt == pl.Utf8:
            nn = col.drop_nulls()
            if len(nn) == 0:
                all_null_cols.append(c)
                continue
            n_enc = int(nn.str.contains(REPLACEMENT_CHAR, literal=True).sum())
            if n_enc:
                encoding[c] = n_enc
            n_sent = int(nn.str.strip_chars().str.to_lowercase().is_in(list(SENTINELS)).sum())
            if n_sent:
                sentinels[c] = n_sent
    dup_rows = int(df.is_duplicated().sum()) if rows else 0
    return {
        "rows": rows,
        "all_null_cols": sorted(all_null_cols),
        "dup_rows": dup_rows,
        "encoding": encoding,
        "sentinels": sentinels,
    }


def emit_current(gold_dir: Path = GOLD_PARQUET_DIR) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in sorted(gold_dir.glob("*.parquet")):
        try:
            out[p.name] = measure_table(pl.read_parquet(p))
        except Exception as e:  # noqa: BLE001 — a guard reports, never crashes, on a bad file
            out[p.name] = {"error": type(e).__name__}
    return out


def find_regressions(current: dict[str, dict], baseline: dict[str, dict]) -> list[dict]:
    """Pure comparison: every way a table's content got WORSE than the baseline."""
    regressions: list[dict] = []
    for name, base in baseline.items():
        cur = current.get(name)
        if cur is None or "error" in cur:
            regressions.append({"output": name, "kind": "MISSING", "detail": "absent or unreadable now"})
            continue
        # a column that newly went all-null (it carried data in the baseline)
        new_empty = sorted(set(cur.get("all_null_cols", [])) - set(base.get("all_null_cols", [])))
        if new_empty:
            regressions.append({"output": name, "kind": "COL_EMPTIED", "columns": new_empty})
        # duplicate rows grew materially
        b_dup, c_dup = base.get("dup_rows", 0), cur.get("dup_rows", 0)
        if c_dup > b_dup + DUP_GROWTH_FLOOR and c_dup > b_dup * (1 + DUP_GROWTH_FRAC):
            regressions.append({"output": name, "kind": "DUP_INCREASE", "baseline": b_dup, "now": c_dup})
        # encoding artifacts appeared in a new column or grew in an existing one
        enc = _grown(base.get("encoding", {}), cur.get("encoding", {}))
        if enc:
            regressions.append({"output": name, "kind": "ENCODING_INCREASE", "columns": enc})
        # null-sentinel contamination appeared/grew
        sent = _grown(base.get("sentinels", {}), cur.get("sentinels", {}))
        if sent:
            regressions.append({"output": name, "kind": "SENTINEL_INCREASE", "columns": sent})
    return regressions


def _grown(base: dict[str, int], cur: dict[str, int]) -> dict[str, dict]:
    """Columns whose per-column count is new or higher than baseline."""
    out: dict[str, dict] = {}
    for col, n in cur.items():
        b = base.get(col, 0)
        if n > b:
            out[col] = {"baseline": b, "now": n}
    return out


def summarise(current: dict[str, dict]) -> dict:
    """A flat triage view of the current findings (for the tracked report / investigation)."""
    return {
        "tables_with_all_null_cols": {k: v["all_null_cols"] for k, v in current.items() if v.get("all_null_cols")},
        "tables_with_dup_rows": {
            k: {"dup_rows": v["dup_rows"], "rows": v["rows"]} for k, v in current.items() if v.get("dup_rows")
        },
        "tables_with_encoding_artifacts": {k: v["encoding"] for k, v in current.items() if v.get("encoding")},
        "tables_with_sentinels": {k: v["sentinels"] for k, v in current.items() if v.get("sentinels")},
    }


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
    ap.add_argument("--update-baseline", action="store_true", help="accept current gold metrics as the baseline")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any content regression is found (CI gate)")
    ap.add_argument("--print", action="store_true", dest="echo", help="echo the report JSON to stdout")
    args = ap.parse_args(argv)

    current = emit_current()

    if args.update_baseline:
        _write(BASELINE_PATH, {"metrics": current})
        print(f"gold quality baseline updated: {len(current)} tables -> {BASELINE_PATH}")
        # also refresh the human-facing triage report
        _write(REPORT_PATH, {"summary": summarise(current)})
        return 0

    baseline = (_load(BASELINE_PATH) or {}).get("metrics", {})
    summary = summarise(current)
    if not baseline:
        _write(REPORT_PATH, {"summary": summary})
        print(
            f"gold quality guard: NO baseline at {BASELINE_PATH} — run --update-baseline once to capture "
            "the current gold as the reference. Wrote triage report only (no gate)."
        )
        return 0

    regressions = find_regressions(current, baseline)
    _write(REPORT_PATH, {"n_baseline": len(baseline), "regressions": regressions, "summary": summary})

    if regressions:
        print(f"gold quality guard: {len(regressions)} CONTENT REGRESSION(S) vs baseline:")
        for r in regressions:
            print(f"  [{r['kind']}] {r['output']}: { {k: v for k, v in r.items() if k not in ('output', 'kind')} }")
        print("If intended, re-baseline: python tools/check_gold_quality.py --update-baseline")
    else:
        print(f"gold quality guard: OK — {len(baseline)} gold tables, no content drift vs baseline.")

    if args.echo:
        sys.stdout.buffer.write(
            orjson.dumps({"regressions": regressions, "summary": summary}, option=orjson.OPT_INDENT_2)
        )
        print()

    return 1 if (args.strict and regressions) else 0


if __name__ == "__main__":
    sys.exit(main())
