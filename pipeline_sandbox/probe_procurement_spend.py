"""PROBE (throwaway): the ACTUAL-SPEND procurement layer — 'Procurement Related
Payments over 20,000' (18 datasets) + 'Purchase Orders over 20,000' (100+ datasets).
These give real money paid to suppliers (vs the eTenders framework CEILINGS).

Goal: measure format/schema heterogeneity (the normalisation cost), find which
carry supplier + amount, and test supplier->CRO + actual-spend on the clean CSVs.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_spend.py
"""

from __future__ import annotations

import io
import re
import sys
from collections import Counter
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from shared.name_norm import name_norm_expr  # noqa: E402

CRO = ROOT / "data/silver/cro/companies.parquet"
H = {"User-Agent": "dail-tracker research"}
TMP = Path("c:/tmp")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def search(q: str, rows: int = 200) -> list[dict]:
    r = requests.get("https://data.gov.ie/api/3/action/package_search",
                     params={"q": q, "rows": rows}, headers=H, timeout=40)
    return r.json()["result"]["results"]


def resources(pkgs: list[dict], want: str) -> list[tuple[str, str, str]]:
    out = []
    for d in pkgs:
        if want.lower() not in d["title"].lower():
            continue
        for x in d.get("resources", []):
            u = x.get("url", "")
            fmt = (x.get("format", "") or u.rsplit(".", 1)[-1]).lower()
            if u:
                out.append((d["title"][:55], fmt, u))
    return out


def fmt_spread(res: list[tuple[str, str, str]]) -> Counter:
    return Counter(f for _, f, _ in res)


def load_csv_bytes(u: str) -> pl.DataFrame:
    b = requests.get(u, headers=H, timeout=90).content
    return pl.read_csv(io.BytesIO(b), infer_schema_length=0, truncate_ragged_lines=True,
                       ignore_errors=True, encoding="utf8-lossy")


SUP_RE = re.compile(r"supplier|payee|vendor|beneficiar|name", re.I)
AMT_RE = re.compile(r"amount|value|total|paid|gross|€|net", re.I)


def main() -> None:
    pay = search("Procurement Related Payments over 20,000")
    po = search("Purchase Orders over 20")
    pay_res = resources(pay, "Procurement Related Payments")
    po_res = resources(po, "Purchase Orders over")

    hr("ACTUAL-SPEND DATASET LANDSCAPE")
    print(f"'Procurement Related Payments over 20,000' resources: {len(pay_res)}  formats={dict(fmt_spread(pay_res))}")
    print(f"'Purchase Orders over 20,000' resources           : {len(po_res)}  formats={dict(fmt_spread(po_res))}")
    publishers = {t.split(" Purchase")[0].split(" Procurement")[0] for t, _, _ in pay_res + po_res}
    print(f"distinct publishing bodies (approx): {len(publishers)}")

    cro = pl.read_parquet(CRO).select(["name_norm", "company_num"])

    # sample several CSV payments resources, inspect schema + match feasibility
    hr("SAMPLE CSV SCHEMAS (payments over 20k)")
    csvs = [r for r in pay_res if r[1] == "csv"][:6]
    samples = []
    for title, _, u in csvs:
        try:
            df = load_csv_bytes(u)
            df = df.rename({c: c.replace("﻿", "").strip() for c in df.columns})
            sup = next((c for c in df.columns if SUP_RE.search(c)), None)
            amt = next((c for c in df.columns if AMT_RE.search(c)), None)
            print(f"\n  {title}")
            print(f"    cols: {df.columns}")
            print(f"    -> supplier col={sup!r}  amount col={amt!r}  rows={df.height:,}")
            if sup and amt:
                samples.append((df, sup, amt, title))
        except Exception as e:
            print(f"  ERR {title}: {e!r}")

    if samples:
        hr("ACTUAL-SPEND CRO MATCH (on sampled payments CSVs)")
        for df, sup, amt, title in samples[:4]:
            d = df.select([pl.col(sup).alias("supplier"), pl.col(amt).alias("amt")]).drop_nulls("supplier")
            d = d.with_columns(
                name_norm_expr("supplier").alias("nn"),
                pl.col("amt").str.replace_all(r"[^0-9.]", "").cast(pl.Float64, strict=False).alias("eur"),
            )
            dist = d.select(["supplier", "nn"]).unique(subset=["nn"]).filter(pl.col("nn").str.len_chars() >= 4)
            m = dist.join(cro, left_on="nn", right_on="name_norm", how="left").group_by("nn").agg(pl.col("company_num").drop_nulls().n_unique().alias("n"))
            one = m.filter(pl.col("n") == 1).height
            tot = d["eur"].sum()
            print(f"  {title[:45]}: {d.height:,} payment rows, €{(tot or 0)/1e6:.1f}m, "
                  f"distinct suppliers {dist.height:,}, CRO 1:1 {one}/{m.height} ({one / max(1, m.height):.0%})")
            print("    top payees:", d.group_by("supplier").agg(pl.col("eur").sum().alias("e")).sort("e", descending=True).head(3).select("supplier").to_series().to_list())

    hr("VERDICT")
    print("actual-spend layer = real € paid (not ceilings), supplier-named, CRO-matchable,")
    print("BUT spread across 100+ publishers in mixed CSV/XLSX/PDF with NON-UNIFORM schemas")
    print("(supplier/amount columns named differently per body) -> a normalisation project.")


if __name__ == "__main__":
    main()
