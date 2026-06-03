"""INGEST (sandbox, BUDGET tier): local-government FINANCE from CSO PxStat.

Fills the one VALUE-TAXONOMY tier nothing else covers — BUDGET (`budget_allocated`).
Procurement micro-layers (eTenders awards, LA POs, TED, semi-state) are covered by other
context windows; the macro "how much each tier of government spends, by function" layer was
identified but never ingested. The amalgamated per-LA AFS is PDF on gov.ie (heavy, deferred);
CSO PxStat gives the same picture at national aggregate, CLEAN via API (no scraping, no OCR).

Pulls (JSON-stat 2.0):
  NAH27 — Expenditure of Local Government classified by Purpose/function
  NAH20 — Receipts and Expenditure of Local Government
Tidies to long rows, TAGS every row with the taxonomy (realisation_tier=BUDGET,
value_kind=budget_allocated), and reports coverage + latest-year function split.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_la_finance_budget.py
PRE-ETL: writes a tidy CSV to c:/tmp only; nothing wired to pipeline.py.
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

H = {"User-Agent": "dail-tracker research probe"}
OUT = Path("c:/tmp/la_finance_budget.csv")
PXSTAT = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{m}/JSON-stat/2.0/en"
TABLES = {
    "GFA04": "Govt expenditure by ESA economic category — CURRENT (2000–2025)",
    "GFA01": "Govt revenue & expenditure (ESA2010) — CURRENT (1995–2025)",
}


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def jsonstat_to_long(ds: dict, matrix: str) -> pl.DataFrame:
    """Unflatten a JSON-stat 2.0 dataset (row-major over `id` dims) to long rows."""
    ids = ds["id"]
    sizes = ds["size"]
    dims = ds["dimension"]
    # per-dim ordered category codes + labels
    cats = []
    for d in ids:
        idx = dims[d]["category"]["index"]
        lab = dims[d]["category"].get("label", {})
        order = sorted(idx, key=lambda k: idx[k]) if isinstance(idx, dict) else list(idx)
        cats.append([(c, lab.get(c, c)) for c in order])
    values = ds["value"]
    n = 1
    for s in sizes:
        n *= s
    rows = []
    for flat in range(n):
        # decode flat index into per-dim positions (row-major, last dim fastest)
        rem = flat
        pos = []
        for s in reversed(sizes):
            pos.append(rem % s)
            rem //= s
        pos = list(reversed(pos))
        v = values[flat] if isinstance(values, list) else values.get(str(flat))
        if v in (None, "", ":"):
            continue
        row = {"matrix": matrix}
        for di, dname in enumerate(ids):
            code, label = cats[di][pos[di]]
            row[dims[dname].get("label", dname)] = label
        with contextlib.suppress(Exception):
            row["value"] = float(v)
            rows.append(row)
    return pl.DataFrame(rows)


def pull(matrix: str) -> pl.DataFrame | None:
    try:
        r = requests.get(PXSTAT.format(m=matrix), headers=H, timeout=60)
        r.raise_for_status()
        ds = r.json()
        ds = ds.get("dataset", ds)
        return jsonstat_to_long(ds, matrix)
    except Exception as e:
        print(f"  {matrix} ERR {type(e).__name__}: {str(e)[:80]}")
        return None


def main() -> None:
    hr("INGEST — CSO local-government finance (BUDGET tier)")
    frames = []
    for m, desc in TABLES.items():
        df = pull(m)
        if df is None or df.is_empty():
            continue
        # find the time + statistic columns generically
        ycol = next((c for c in df.columns if df[c].cast(pl.Utf8).str.contains(r"^(19|20)\d\d$").any()), None)
        scol = next((c for c in df.columns if c not in ("matrix", "value", ycol)), None)
        yrs = sorted(df[ycol].unique().to_list()) if ycol else []
        print(f"  {m} ({desc}): {df.height:,} rows | years {yrs[0]}–{yrs[-1]} | "
              f"{df[scol].n_unique() if scol else '?'} statistics")
        frames.append(df)

    if not frames:
        print("nothing ingested.")
        return
    allcols = sorted({c for f in frames for c in f.columns})
    tidy = pl.concat([f.select([pl.col(c) if c in f.columns else pl.lit(None).alias(c) for c in allcols])
                      for f in frames], how="vertical_relaxed")
    # TAXONOMY TAGS
    tidy = tidy.with_columns(
        pl.lit("BUDGET").alias("realisation_tier"),
        pl.lit("budget_allocated").alias("value_kind"),
        pl.lit(True).alias("value_safe_to_sum_within_table"),  # summable only within a (matrix,year)
        pl.lit("CSO PxStat (GFA01/GFA04), CC-BY 4.0").alias("source"),
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tidy.write_csv(OUT)

    # latest-year breakdown for the expenditure table (pick the highest-cardinality dim)
    n27 = tidy.filter(pl.col("matrix") == "GFA04")
    if not n27.is_empty():
        meta = {"matrix", "value", "realisation_tier", "value_kind",
                "value_safe_to_sum_within_table", "source"}
        ycol = next((c for c in n27.columns if n27[c].cast(pl.Utf8).str.contains(r"^(19|20)\d\d$").any()), None)
        scol = max((c for c in n27.columns if c not in meta and c != ycol),
                   key=lambda c: n27[c].n_unique())
        latest = n27[ycol].cast(pl.Utf8).max()
        hr(f"GOVT EXPENDITURE BY ESA CATEGORY — {latest} (GFA04, general government, €m)")
        cur = (n27.filter(pl.col(ycol).cast(pl.Utf8) == latest)
               .filter(~pl.col(scol).str.contains("(?i)total"))
               .group_by(scol).agg(pl.col("value").sum().alias("v"))
               .sort("v", descending=True).head(10))
        for r in cur.iter_rows(named=True):
            print(f"  €{r['v']:>10,.0f}m  {r[scol][:60]}")

    hr("VERDICT")
    print(f"ingested {tidy.height:,} BUDGET-tier rows from {len(frames)} CSO tables -> {OUT}")
    print("realisation_tier=BUDGET, value_kind=budget_allocated (summable only within a")
    print("(matrix,year); NEVER add to AWARDED/COMMITTED/SPENT tiers). National aggregate —")
    print("the per-LA amalgamated AFS (gov.ie PDF, 2009–2023) is the richer deferred follow-up.")


if __name__ == "__main__":
    main()
