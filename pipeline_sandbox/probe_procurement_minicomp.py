"""PROBE (throwaway): OGP 'Contracts for Mini-Competitions and Standalone Awards'.
A SEPARATE dataset from the eTenders notices — the actual framework call-offs.
Tests: schema, supplier->CRO match (reusing the canonicalise+match from the
eTenders extractor), and whether these contracts are DISTINCT from eTenders awards.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_minicomp.py
Reads CRO silver; downloads CSVs to c:/tmp; writes nothing persistent.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from cro_normalise import name_norm_expr  # noqa: E402
from procurement_etenders_extract import build_canonical_map, tidy_name  # noqa: E402

CRO = ROOT / "data/silver/cro/companies.parquet"
H = {"User-Agent": "dail-tracker research"}
TMP = Path("c:/tmp")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def resource_urls() -> list[str]:
    r = requests.get("https://data.gov.ie/api/3/action/package_search",
                     params={"q": "Contracts for Mini-Competitions and Standalone Awards", "rows": 50},
                     headers=H, timeout=40)
    urls = []
    for d in r.json()["result"]["results"]:
        if "mini-compet" in d["title"].lower() or "standalone" in d["title"].lower():
            for x in d["resources"]:
                u = x.get("url", "")
                if u.lower().endswith(".csv"):
                    urls.append(u)
    return sorted(set(urls))


def load_csv(url: str) -> pl.DataFrame:
    fn = TMP / ("minicomp_" + url.rsplit("/", 1)[-1])
    if not fn.exists():
        fn.write_bytes(requests.get(url, headers=H, timeout=120).content)
    return pl.read_csv(fn, infer_schema_length=0, truncate_ragged_lines=True, ignore_errors=True, encoding="utf8-lossy")


def main() -> None:
    urls = resource_urls()
    hr("MINI-COMPETITIONS & STANDALONE AWARDS")
    print(f"CSV resources: {len(urls)}")
    frames = []
    for u in urls:
        try:
            df = load_csv(u)
            df = df.rename({c: c.replace("﻿", "").strip() for c in df.columns})
            # harmonise schema drift across quarters
            ren = {}
            for c in df.columns:
                if c == "Supplier Name":
                    ren[c] = "Suppliers"
                elif c.replace(" ", "") == "ContractAwardConfirmedDate":
                    ren[c] = "Contract Award Confirmed Date"
            if ren:
                df = df.rename(ren)
            frames.append(df)
            print(f"  {u.rsplit('/', 1)[-1]}: {df.height:,} rows, cols={df.columns}")
        except Exception as e:
            print("  ERR", u, repr(e))
    if not frames:
        return
    # align on common columns
    common = set(frames[0].columns)
    for f in frames[1:]:
        common &= set(f.columns)
    common = [c for c in frames[0].columns if c in common]
    allc = pl.concat([f.select(common) for f in frames], how="vertical_relaxed").unique()
    hr("UNIONED")
    print(f"rows: {allc.height:,}  | common cols: {common}")

    sup_col = next((c for c in allc.columns if c.lower() == "suppliers"), None)
    auth_col = next((c for c in allc.columns if "Contracting Authority" in c and "Client" not in c), None)
    print(f"supplier col={sup_col!r}  authority col={auth_col!r}")

    # explode suppliers (try | ; , as separators conservatively: only | and ;)
    aw = (
        allc.with_columns(pl.col(sup_col).str.replace_all(";", "|").str.split("|").alias("sl"))
        .explode("sl")
        .with_columns(pl.col("sl").map_elements(tidy_name, return_dtype=pl.Utf8).alias("supplier_raw"))
        .filter(pl.col("supplier_raw").str.len_chars() >= 3)
    )
    cmap = build_canonical_map(aw.select("supplier_raw").unique().to_series().to_list())
    aw = aw.with_columns(pl.col("supplier_raw").replace(cmap).alias("supplier"))
    aw = aw.with_columns(name_norm_expr("supplier").alias("supplier_norm"))

    hr("SUPPLIERS")
    distinct = aw.select(["supplier", "supplier_norm"]).unique(subset=["supplier_norm"]).filter(pl.col("supplier_norm").str.len_chars() >= 4)
    print(f"award-supplier rows: {aw.height:,} | distinct suppliers: {distinct.height:,} | repaired spellings: {len(cmap):,}")
    print(aw.group_by("supplier").len().sort("len", descending=True).head(10))

    # CRO match
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num"])
    per = distinct.join(cro, left_on="supplier_norm", right_on="name_norm", how="left").group_by("supplier_norm").agg(pl.col("company_num").drop_nulls().n_unique().alias("n"))
    one = per.filter(pl.col("n") == 1).height
    hr("SUPPLIER -> CRO")
    print(f"distinct suppliers: {per.height:,} | exact 1:1: {one:,} ({one / per.height:.1%})")

    # distinctness vs eTenders
    et = ROOT / "data/sandbox/parquet/procurement_awards.parquet"
    if et.exists():
        et_names = set(pl.read_parquet(et)["supplier_norm"].drop_nulls().to_list())
        mc_names = set(distinct["supplier_norm"].to_list())
        overlap = len(mc_names & et_names)
        hr("OVERLAP vs eTenders awards")
        print(f"mini-comp distinct suppliers: {len(mc_names):,}")
        print(f"  also in eTenders awards: {overlap:,} ({overlap / len(mc_names):.1%})")
        print(f"  ONLY in mini-comp (net-new suppliers): {len(mc_names) - overlap:,}")

    hr("DATE / VALUE CHECK")
    print("columns suggest:", [c for c in allc.columns if "Date" in c or "Value" in c or "CPV" in c])
    print("NOTE: mini-comp has supplier+dates+CPV but typically NO award value column.")


if __name__ == "__main__":
    main()
