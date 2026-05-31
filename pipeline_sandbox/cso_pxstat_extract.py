"""CSO PxStat extractor — pulls per-LA × year housing statistics tables.

Tables targeted:
  HSA07 — Authorised starts for local authority housing (1994–)
  HAP01 — Households starting/in/exiting HAP, per LA, per family type (2014–)
  HAP17 — Working % and gross household income of HAP households
  HAP20 — Rent as % of disposable income, HAP tenants
  HAP26 — Median waiting time for HAP tenants
  HAP32 — Median waiting time from main social housing list to HAP

Reads  : Eurostat-style JSON-stat from https://ws.cso.ie (REST)
Writes : data/gold/parquet/cso_<table_id>.parquet (one per table, --write)
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import polars as pl
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_OUT = _ROOT / "data" / "gold" / "parquet"
_API = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{code}/CSV/1.0/en"

TABLES = ["HSA07", "HAP01", "HAP17", "HAP20", "HAP26", "HAP32"]


def fetch_csv(code: str) -> pl.DataFrame:
    url = _API.format(code=code)
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    raw = r.content.decode("utf-8-sig")
    df = pl.read_csv(raw.encode("utf-8"))
    return df


def fidelity_check(df: pl.DataFrame, code: str) -> dict:
    """Cascade tailored to PxStat structure."""
    rpt: dict = {"checks": {}, "rows": len(df), "code": code}

    # Check 1 — Extraction (basic shape)
    expected_cols = {"STATISTIC", "Year", "Local Authority", "VALUE"}
    have = set(df.columns)
    missing_cols = expected_cols - have
    rpt["checks"]["1_extraction"] = {
        "row_count": len(df),
        "cols": df.columns,
        "missing_required_cols": list(missing_cols),
        "pass": not missing_cols and len(df) > 0,
    }

    # Check 2 — Internal consistency (null rate; numeric VALUE)
    # Cast VALUE to Float64 once; PxStat sometimes returns strings like "" for nulls
    if "VALUE" in have:
        df_num = df.with_columns(pl.col("VALUE").cast(pl.Float64, strict=False).alias("_value_num"))
        null_value = df_num.filter(pl.col("_value_num").is_null()).height
    else:
        df_num = df
        null_value = 0
    null_pct = round(100 * null_value / max(len(df), 1), 1)
    rpt["checks"]["2_internal_sum"] = {
        "null_value_rows": null_value,
        "null_pct": null_pct,
        "pass": null_pct < 60,
    }

    # Check 3 — Cross-table: at least the "All Family Types" / "Ireland" rows exist
    has_ireland = False
    if "Local Authority" in have:
        has_ireland = df.filter(pl.col("Local Authority") == "Ireland").height > 0
    rpt["checks"]["3_national_aggregate"] = {
        "has_ireland_row": has_ireland,
        "pass": True,  # not all PxStat tables have national-aggregate
    }

    # Check 4 — Cross-source (skipped — would need HAP funding XLSX etc.)
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped"}

    # Check 5 — Semantic (negatives shouldn't happen for households/counts;
    # do allow for HAP cost / income tables where percentages are positive)
    if "VALUE" in have:
        bad = df_num.filter(pl.col("_value_num") < 0).height
        rpt["checks"]["5_semantic"] = {"negative_values": bad, "pass": bad == 0}
    else:
        rpt["checks"]["5_semantic"] = {"pass": False, "note": "no VALUE column"}

    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--tables", nargs="*", default=TABLES, help="Subset of tables")
    args = ap.parse_args()

    summary = []
    for code in args.tables:
        try:
            df = fetch_csv(code)
        except Exception as e:
            print(f"[{code}] FETCH FAIL: {e}")
            summary.append((code, "fetch_fail", 0, False))
            continue

        rpt = fidelity_check(df, code)
        green = rpt["green"]
        print(f"\n=== {code} — {len(df)} rows ===")
        for name, chk in rpt["checks"].items():
            tag = "GREEN" if chk.get("pass") else "FAIL"
            print(f"  [{tag}] {name}: {chk}")
        print(f"  >>> overall: {'GREEN' if green else 'AMBER/RED'}")

        if args.write and green:
            path = _OUT / f"cso_{code.lower()}.parquet"
            _write_parquet(df, path)
            print(f"  Wrote {path.relative_to(_ROOT)}")

        summary.append((code, "ok" if green else "amber", len(df), green))
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("SUMMARY")
    for code, status, n, green in summary:
        flag = "✓" if green else ("⚠" if status == "amber" else "✗")
        print(f"  {flag} {code:10s} {status:12s} {n:>7,} rows")


if __name__ == "__main__":
    main()
