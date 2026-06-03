"""PROBE (throwaway): PULL TED — Irish contract-AWARD notices with REAL award values.

TED (ted.europa.eu) is the EU procurement journal. Its v3 Search API is public + ZERO
AUTH. eForms-era notices (2024+) carry structured award VALUES + winners — the thing the
OGP eTenders CSV can't give (that's ceilings). This pulls IE `can-standard` award notices
since 2024 and uncovers: total award value, top buyers, top winning suppliers, CPV mix,
and CRO match (by name AND by the winner-identifier, which is often a CRO/VAT number).

Winner resolution learned from the JSON: `organisation-name-tenderer.eng[]` is the
reliable winner-name list (winner-partname is sparse); `winner-identifier[]` parallels it
and frequently IS the company reg number. tender-value[] is in EUR.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_ted_ireland.py
Reads CRO silver; hits the TED API; writes a small JSON summary to c:/tmp.
"""

from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from cro_normalise import name_norm_expr  # noqa: E402

CRO = ROOT / "data/silver/cro/companies.parquet"
OUT = Path("c:/tmp/ted_ireland_summary.json")
URL = "https://api.ted.europa.eu/v3/notices/search"
H = {"User-Agent": "dail-tracker research probe", "Accept": "application/json"}
FIELDS = ["publication-number", "buyer-name", "tender-value", "organisation-name-tenderer",
          "winner-identifier", "classification-cpv", "dispatch-date"]
QUERY = "buyer-country=IRL AND notice-type=can-standard AND publication-date>=20240101"
PAGE_CAP = 36  # 250/page -> up to 9,000 notices (covers the 2024+ eForms era)

CPV_DIV = {
    "45": "Construction", "71": "Architecture/Engineering", "79": "Business/Consulting",
    "72": "IT services", "85": "Health/Social", "80": "Education", "90": "Environment/Waste",
    "50": "Repair/Maintenance", "48": "Software", "33": "Medical equipment",
    "34": "Transport equipment", "09": "Energy/Fuel", "73": "R&D", "55": "Hotel/Catering",
    "60": "Transport services", "92": "Recreation/Culture", "30": "Office/IT equipment",
    "98": "Other services", "70": "Real estate", "66": "Financial/Insurance",
}


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def first_eng(v) -> str | None:
    """Notice text fields are multilingual dicts {lang: [..]} — pull any usable value."""
    if isinstance(v, dict):
        for key in ("eng", *v.keys()):
            if v.get(key):
                val = v[key]
                return (val[0] if isinstance(val, list) else val)
    elif isinstance(v, list) and v:
        return v[0]
    elif isinstance(v, str):
        return v
    return None


def names_list(v) -> list[str]:
    if isinstance(v, dict):
        for key in ("eng", *v.keys()):
            if isinstance(v.get(key), list):
                return [str(x) for x in v[key]]
    if isinstance(v, list):
        return [str(x) for x in v]
    return []


def to_eur(v) -> float:
    vals = v if isinstance(v, list) else [v]
    tot = 0.0
    for x in vals:
        with contextlib.suppress(Exception):
            tot += float(str(x).replace(",", ""))
    return tot


def pull() -> list[dict]:
    notices, page = [], 1
    while page <= PAGE_CAP:
        body = {"query": QUERY, "fields": FIELDS, "limit": 250, "page": page,
                "paginationMode": "PAGE_NUMBER"}
        r = requests.post(URL, json=body, headers=H, timeout=120)
        if r.status_code != 200:
            print(f"  page {page} -> {r.status_code} {r.text[:120]}")
            break
        batch = r.json().get("notices", [])
        if not batch:
            break
        notices += batch
        print(f"  page {page}: +{len(batch)}  (total {len(notices)})")
        if len(batch) < 250:
            break
        page += 1
    return notices


def main() -> None:
    hr("PULL TED — Irish contract-award notices (eForms era, 2024+)")
    raw = pull()
    print(f"\npulled {len(raw):,} award notices")

    rows = []
    for n in raw:
        winners = names_list(n.get("organisation-name-tenderer")) or names_list(n.get("tendering-party-name"))
        ids = n.get("winner-identifier") or []
        ids = ids if isinstance(ids, list) else [ids]
        cpv = n.get("classification-cpv") or []
        cpv = cpv if isinstance(cpv, list) else [cpv]
        div = CPV_DIV.get((str(cpv[0])[:2] if cpv else ""), "Other/Unknown")
        val = to_eur(n.get("tender-value"))
        rows.append({
            "pub": n.get("publication-number"),
            "buyer": first_eng(n.get("buyer-name")) or "?",
            "value": val,
            "n_winners": len(winners),
            "winners": winners,
            "ids": [str(i) for i in ids],
            "cpv_div": div,
            "date": (n.get("dispatch-date") or "")[:7],
        })
    df = pl.DataFrame(rows)
    with_val = df.filter(pl.col("value") > 0)
    multi = df.filter(pl.col("n_winners") > 1)

    hr("HEADLINE")
    print(f"notices: {df.height:,}  |  with a € value: {with_val.height:,} ({with_val.height / max(1, df.height):.0%})")
    print(f"total award value (sum tender-value): €{df['value'].sum() / 1e9:.2f}bn  "
          f"(CAVEAT: {multi.height:,} are MULTI-supplier frameworks → value not per-firm)")
    print(f"median single award: €{with_val['value'].median():,.0f}   "
          f"date span: {df['date'].min()} … {df['date'].max()}")

    hr("TOP BUYERS (contracting authorities) by notice count")
    for r in df.group_by("buyer").agg(pl.len().alias("n"), pl.col("value").sum().alias("eur")) \
            .sort("n", descending=True).head(12).iter_rows(named=True):
        print(f"  {r['n']:>4}  €{r['eur'] / 1e6:>8.1f}m  {r['buyer'][:54]}")

    hr("TOP WINNING SUPPLIERS by notice appearances (exploded)")
    exp = df.explode("winners").filter(pl.col("winners").is_not_null() & (pl.col("winners").str.strip_chars() != ""))
    win = exp.group_by("winners").agg(pl.len().alias("n")).sort("n", descending=True)
    for r in win.head(15).iter_rows(named=True):
        print(f"  {r['n']:>4}  {r['winners'][:60]}")

    hr("BIGGEST SINGLE AWARDS")
    for r in with_val.sort("value", descending=True).head(10).iter_rows(named=True):
        w = (r["winners"][0] if r["winners"] else "?")[:38]
        print(f"  €{r['value'] / 1e6:>8.1f}m  {r['buyer'][:34]:<34} -> {w}")

    hr("SECTOR (CPV division) MIX")
    for r in df.group_by("cpv_div").agg(pl.len().alias("n"), pl.col("value").sum().alias("eur")) \
            .sort("n", descending=True).head(12).iter_rows(named=True):
        print(f"  {r['n']:>4}  €{r['eur'] / 1e6:>8.1f}m  {r['cpv_div']}")

    hr("CRO MATCH (winners → company register)")
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num"])
    sup = (exp.select(pl.col("winners").alias("supplier")).unique()
           .with_columns(name_norm_expr("supplier").alias("nn"))
           .filter(pl.col("nn").str.len_chars() >= 4))
    m = sup.join(cro, left_on="nn", right_on="name_norm", how="left")
    hit = m.filter(pl.col("company_num").is_not_null()).select("nn").n_unique()
    print(f"distinct winners: {sup.height:,}   CRO 1:1 by NAME: {hit} ({hit / max(1, sup.height):.0%})")
    # winner-identifier sometimes IS a CRO number — measure direct id overlap
    cro_ids = set(pl.read_parquet(CRO).select("company_num").to_series().to_list())
    all_ids = {i for r in rows for i in r["ids"] if i}
    id_hit = sum(1 for i in all_ids if i in cro_ids)
    print(f"distinct winner-identifiers: {len(all_ids):,}   match CRO company_num directly: {id_hit} "
          f"({id_hit / max(1, len(all_ids)):.0%})  <- IE reg numbers; rest are VAT/foreign")

    OUT.write_text(json.dumps({
        "notices": df.height, "with_value": with_val.height,
        "total_value_eur": df["value"].sum(), "multi_supplier": multi.height,
        "distinct_winners": sup.height, "cro_name_rate": hit / max(1, sup.height),
        "top_buyers": df.group_by("buyer").len().sort("len", descending=True).head(20).to_dicts(),
    }, indent=2), encoding="utf-8")
    hr("VERDICT")
    print("TED = the REAL-VALUE award layer eTenders ceilings can't give: public, zero-auth,")
    print(f"~{df.height:,} IE awards since 2024, {with_val.height / max(1, df.height):.0%} with a value, winners CRO-matchable.")
    print(f"summary -> {OUT}")


if __name__ == "__main__":
    main()
