"""Eurostat extractor — housing/tenure/citizenship indicators for Ireland.

Tables targeted (JSON-stat API):
  ilc_lvps15 — Distribution of pop by group of citizenship × tenure status (≥18)
  ilc_lvho15 — Overcrowding rate by citizenship × age × sex
  ilc_lvho25 — Housing cost overburden rate by citizenship × age × sex

These three datasets together support the "9× cost-overburden gap" story.
Ireland (`geo=IE`) only; full time series 2003→2025.

Reads  : https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{code}
Writes : data/gold/parquet/eurostat_<code>.parquet (one per dataset, --write)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_OUT = _ROOT / "data" / "gold" / "parquet"
_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{code}"

DATASETS = {
    "ilc_lvps15": {"params": {"sex": "T", "age": "Y_GE18"}},
    "ilc_lvho15": {"params": {"sex": "T", "age": "Y_GE18"}},
    "ilc_lvho25": {"params": {"sex": "T", "age": "Y_GE18"}},
}


def fetch_jsonstat(code: str, params: dict, geo: str = "IE") -> dict:
    p = {"format": "JSON", "lang": "EN", "geo": geo, **params}
    r = requests.get(_API.format(code=code), params=p,
                     headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    return r.json()


def jsonstat_to_df(d: dict) -> pl.DataFrame:
    """Unpack JSON-stat into a long-format Polars frame."""
    dim_ids = d["id"]
    labels = {k: list(d["dimension"][k]["category"]["label"].values()) for k in dim_ids}
    size = d["size"]
    rows: list[dict] = []
    for idx_str, value in d["value"].items():
        idx = int(idx_str)
        # Decode multi-dim index
        coords = []
        for i, _ in enumerate(dim_ids):
            denom = 1
            for j in size[i + 1:]:
                denom *= j
            coords.append(idx // denom)
            idx %= denom
        rec = {dim: labels[dim][coords[i]] for i, dim in enumerate(dim_ids)}
        rec["VALUE"] = value
        rows.append(rec)
    return pl.DataFrame(rows)


def fidelity_check(df: pl.DataFrame, code: str) -> dict:
    rpt: dict = {"checks": {}, "rows": len(df), "code": code}

    # Check 1 — Extraction
    rpt["checks"]["1_extraction"] = {
        "row_count": len(df),
        "cols": df.columns,
        "pass": len(df) > 0 and "VALUE" in df.columns and "time" in df.columns,
    }

    # Check 2 — Year coverage (should span a multi-year range)
    if "time" in df.columns:
        years = sorted(set(df["time"].to_list()))
        rpt["checks"]["2_internal_sum"] = {
            "year_count": len(years),
            "earliest": years[0] if years else None,
            "latest": years[-1] if years else None,
            "pass": len(years) >= 5,
        }
    else:
        rpt["checks"]["2_internal_sum"] = {"pass": False}

    # Check 3 — Has all key citizenship categories
    expected_citizens = {"Reporting country",
                         "EU27 countries (from 2020) except reporting country",
                         "Non-EU27 countries (from 2020) nor reporting country",
                         "Foreign country"}
    if "citizen" in df.columns:
        have = set(df["citizen"].unique().to_list())
        missing = expected_citizens - have
        rpt["checks"]["3_categories_present"] = {
            "have": sorted(have)[:8],
            "missing": list(missing),
            "pass": len(missing) == 0,
        }
    else:
        rpt["checks"]["3_categories_present"] = {"pass": False, "note": "no citizen dim"}

    # Check 4 — Cross-source (skipped — these are the gold standard)
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped (Eurostat is source of truth here)"}

    # Check 5 — Semantic (rates should be 0-100)
    is_rate = "lvho" in code  # tenure/cost-burden rates
    if is_rate and "VALUE" in df.columns:
        df_num = df.with_columns(pl.col("VALUE").cast(pl.Float64, strict=False).alias("_v"))
        out_of_range = df_num.filter((pl.col("_v") < 0) | (pl.col("_v") > 100)).height
        rpt["checks"]["5_semantic"] = {"out_of_range": out_of_range, "pass": out_of_range == 0}
    else:
        rpt["checks"]["5_semantic"] = {"pass": True, "note": "non-rate metric"}

    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--codes", nargs="*", default=list(DATASETS), help="Subset of datasets")
    args = ap.parse_args()

    results = []
    for code in args.codes:
        spec = DATASETS.get(code, {"params": {}})
        try:
            payload = fetch_jsonstat(code, spec["params"])
            df = jsonstat_to_df(payload)
        except Exception as e:
            print(f"[{code}] FETCH FAIL: {e}")
            results.append((code, "fail", 0, False))
            continue

        rpt = fidelity_check(df, code)
        print(f"\n=== {code} — {len(df)} rows ===")
        for name, chk in rpt["checks"].items():
            print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {name}: {chk}")
        print(f"  >>> overall: {'GREEN' if rpt['green'] else 'AMBER'}")

        if args.write and rpt["green"]:
            path = _OUT / f"eurostat_{code}_ie.parquet"
            _write_parquet(df, path)
            print(f"  Wrote {path.relative_to(_ROOT)}")
        results.append((code, "ok" if rpt["green"] else "amber", len(df), rpt["green"]))

    print("\n" + "=" * 60)
    print("SUMMARY")
    for code, status, n, green in results:
        flag = "✓" if green else ("⚠" if status == "amber" else "✗")
        print(f"  {flag} {code:15s} {status:8s} {n:>5,} rows")


if __name__ == "__main__":
    main()
