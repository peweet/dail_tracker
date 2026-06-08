"""TED (EU procurement journal) -> Irish contract-AWARD notices -> SILVER parquet.

Promoted from probe_ted_ireland.py. TED's v3 Search API is public + zero-auth; eForms-era
notices (2024+) carry structured award VALUES + winners — the real-value award layer the
OGP eTenders ceilings can't give.

MEDALLION FLOW (deliberately SILVER, not gold):
  bronze  raw API JSON, cached + re-downloadable (c:/tmp/ted_ie_awards_raw.json)
  silver  data/silver/parquet/ted_ie_awards.parquet   <- THIS script (cleaned, reconciled)
  gold    deferred — only when a sql_views/ted_*.sql view EXPOSES it to the frontend.
Gold = "exposed as-is via a SQL view"; TED still needs reconciliation first (winner->CRO,
pan-EU framework exclusion, multilingual resolution), so it lands in silver and a later
view + gold summary builds on top. Mirrors how procurement_awards sat in silver before it
was promoted on shipping.

Grain: ONE ROW PER (notice x winner). A TED notice can list several winners (multi-supplier
framework); tender-value is a NOTICE-level figure (the framework TOTAL), never per-winner —
so value is carried but value_safe_to_sum is FALSE for multi-winner / framework / pan-EU
rows (same discipline as procurement_etenders_extract.py). Winners are CRO-matched by name
AND by winner-identifier (often the IE company number). Bare-personal-name identifiers ->
sole-trader quarantine flag.

NOT wired into pipeline.py. Silver parquet is regenerable from the API (left untracked).

Run:
  ./.venv/Scripts/python.exe extractors/ted_ireland_extract.py
  ./.venv/Scripts/python.exe extractors/ted_ireland_extract.py --max-pages 4   # quick
  ./.venv/Scripts/python.exe extractors/ted_ireland_extract.py --refresh        # ignore cache
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from shared.name_norm import name_norm_expr  # noqa: E402

CRO = ROOT / "data/silver/cro/companies.parquet"
RAW_CACHE = ROOT / "data/bronze/ted/ted_ie_awards_raw.json"  # bronze: raw API capture (portable, headless-safe)
OUT_SILVER = ROOT / "data/silver/parquet/ted_ie_awards.parquet"
OUT_COV = ROOT / "data/_meta/ted_ie_awards_coverage.json"

URL = "https://api.ted.europa.eu/v3/notices/search"
H = {"User-Agent": "dail-tracker research probe", "Accept": "application/json"}
FIELDS = [
    "publication-number",
    "buyer-name",
    "tender-value",
    "tender-value-cur",
    "organisation-name-tenderer",
    "winner-identifier",
    "classification-cpv",
    "dispatch-date",
    "notice-type",
]
QUERY = "buyer-country=IRL AND notice-type=can-standard AND publication-date>=20240101"
PAGE_CAP = 40  # 250/page

SOURCE = {
    "dataset": "TED — Tenders Electronic Daily (contract award notices, Ireland)",
    "publisher": "Publications Office of the European Union",
    "api": URL,
    "query": QUERY,
    "landing_page": "https://ted.europa.eu/",
    "notice_url_template": "https://ted.europa.eu/en/notice/-/detail/{publication_number}",
    "license": "EU open data — reuse authorised under Commission Decision 2011/833/EU",
    "attribution": "Contains information from TED (© European Union), reused under Decision 2011/833/EU.",
}

CPV_DIV = {
    "45": "Construction",
    "71": "Architecture/Engineering",
    "79": "Business/Consulting",
    "72": "IT services",
    "85": "Health/Social",
    "80": "Education",
    "90": "Environment/Waste",
    "50": "Repair/Maintenance",
    "48": "Software",
    "33": "Medical equipment",
    "34": "Transport equipment",
    "09": "Energy/Fuel",
    "73": "R&D",
    "55": "Hotel/Catering",
    "60": "Transport services",
    "92": "Recreation/Culture",
    "30": "Office/IT equipment",
    "98": "Other services",
    "70": "Real estate",
    "66": "Financial/Insurance",
}
COMPANY_SUFFIX = re.compile(
    r"\b(limited|ltd|dac|plc|clg|uc|llp|teoranta|teo|unlimited company|t/a|group|company|holdings|services|solutions|consult|partners|associates|university|institute|board|council|&)\b",
    re.I,
)
# TED winners skew FOREIGN (Bechtle AG, Proact IT Sweden AB, CloudFerro S.A., Vaisala Oyj) —
# without this they fall through COMPANY_SUFFIX and get mislabelled sole_trader, inflating the
# privacy flag. Mirrors the FOREIGN_FORM regex in procurement_etenders_extract.py.
FOREIGN_FORM = re.compile(
    r"\b(gmbh|ag|s\.?a\.?|n\.?v\.?|s\.?a\.?s|s\.?p\.?a|spa|inc|llc|\bpty\b|\bab\b|\bas\b|a/s|\bbv\b|\boy\b|oyj|srl|sl|sarl|aps|kft|ltda|s\.?r\.?o)\b",
    re.I,
)
PAN_EU_HINT = re.compile(r"g[eé]ant|cloudferro|european dynamics|t-systems|softwareone|telecom italia", re.I)
PAN_EU_VALUE = 100_000_000  # multi-winner notices above this are framework ceilings, not IE spend


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def first_eng(v):
    if isinstance(v, dict):
        for key in ("eng", *v.keys()):
            if v.get(key):
                val = v[key]
                return val[0] if isinstance(val, list) else val
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


def pull(max_pages: int) -> list[dict]:
    notices, page = [], 1
    while page <= max_pages:
        body = {"query": QUERY, "fields": FIELDS, "limit": 250, "page": page, "paginationMode": "PAGE_NUMBER"}
        r = requests.post(URL, json=body, headers=H, timeout=120)
        if r.status_code != 200:
            print(f"  page {page} -> {r.status_code} {r.text[:140]}")
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


def load_raw(max_pages: int, refresh: bool) -> list[dict]:
    if RAW_CACHE.exists() and not refresh:
        try:
            data = json.loads(RAW_CACHE.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                print(f"using cached raw capture: {RAW_CACHE} ({len(data):,} notices)")
                return data
        except Exception:
            pass
    raw = pull(max_pages)
    RAW_CACHE.parent.mkdir(parents=True, exist_ok=True)
    RAW_CACHE.write_text(json.dumps(raw), encoding="utf-8")
    print(f"wrote raw capture (bronze) -> {RAW_CACHE}")
    return raw


def clean_identifier(s: str) -> str:
    """winner-identifier is mixed (CRO num / VAT / name). Reduce to digits, drop leading
    zeros so it can join CRO company_num (the investigation's ~84% id-match finding)."""
    digits = re.sub(r"\D", "", s or "")
    return digits.lstrip("0")


def build_rows(raw: list[dict]) -> list[dict]:
    rows = []
    for n in raw:
        winners = names_list(n.get("organisation-name-tenderer")) or names_list(n.get("tendering-party-name"))
        ids = n.get("winner-identifier") or []
        ids = ids if isinstance(ids, list) else [ids]
        cpv = n.get("classification-cpv") or []
        cpv = cpv if isinstance(cpv, list) else [cpv]
        cpv0 = str(cpv[0]) if cpv else ""
        val = to_eur(n.get("tender-value"))
        cur = first_eng(n.get("tender-value-cur")) or "EUR"
        buyer = first_eng(n.get("buyer-name")) or "?"
        date = (n.get("dispatch-date") or "")[:10]
        n_win = len([w for w in winners if w and w.strip()])
        pan_eu = (
            bool(PAN_EU_HINT.search(buyer))
            or (n_win > 1 and val > PAN_EU_VALUE)
            or any(PAN_EU_HINT.search(w) for w in winners)
        )
        pub = n.get("publication-number")
        if not winners:  # keep the award notice even with no parsed winner (provenance)
            winners, ids = [None], ids or [None]
        for i, w in enumerate(winners):
            ident = str(ids[i]) if i < len(ids) and ids[i] is not None else None
            rows.append(
                {
                    "publication_number": pub,
                    "notice_url": SOURCE["notice_url_template"].format(publication_number=pub) if pub else None,
                    "buyer_name": buyer,
                    "winner_name": (w or None),
                    "winner_identifier_raw": ident,
                    "winner_identifier_digits": clean_identifier(ident) if ident else None,
                    "award_value_eur": val if val > 0 else None,
                    "currency": cur,
                    "n_winners": n_win,
                    "is_multi_supplier_framework": n_win > 1,
                    "is_pan_eu_outlier": pan_eu,
                    "value_kind": "framework_or_dps_ceiling" if n_win > 1 else "contract_award_value",
                    "cpv_code": cpv0 or None,
                    "cpv_division": CPV_DIV.get(cpv0[:2], "Other/Unknown"),
                    "dispatch_date": date or None,
                    "year": int(date[:4]) if date[:4].isdigit() else None,
                    "month": date[:7] or None,
                }
            )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=PAGE_CAP)
    ap.add_argument("--refresh", action="store_true", help="ignore raw cache, re-pull API")
    args = ap.parse_args()

    hr("PULL TED — Irish contract-award notices (eForms era, 2024+)")
    raw = load_raw(args.max_pages, args.refresh)
    print(f"\nnotices: {len(raw):,}")

    # Graceful skip: an external-API outage (TED down / network) must NOT fail the whole
    # pipeline. If we got nothing AND have no prior silver to keep, exit 0 with a warning;
    # if a prior silver exists it simply stays in place (this run is a no-op).
    if not raw:
        print(
            "WARNING: TED API returned no notices and no cache is available — skipping this "
            "run (pipeline continues; prior silver, if any, is left untouched)."
        )
        return

    df = pl.DataFrame(build_rows(raw), infer_schema_length=None)

    # ---- winner classification + privacy (sole-trader quarantine flag, NOT dropped) ----
    df = (
        df.with_columns(
            name_norm_expr("winner_name").alias("winner_name_norm"),
            pl.col("winner_name")
            .map_elements(lambda s: bool(COMPANY_SUFFIX.search(s or "")), return_dtype=pl.Boolean)
            .alias("_co"),
            pl.col("winner_name")
            .map_elements(lambda s: bool(FOREIGN_FORM.search(s or "")), return_dtype=pl.Boolean)
            .alias("_for"),
        )
        .with_columns(
            pl.when(pl.col("winner_name").is_null())
            .then(pl.lit("unknown"))
            .when(pl.col("_co"))
            .then(pl.lit("company"))
            .when(pl.col("_for"))
            .then(pl.lit("foreign_company"))
            .otherwise(pl.lit("sole_trader_or_individual"))
            .alias("supplier_class"),
        )
        .drop(["_co", "_for"])
    )
    # privacy_status deferred until AFTER the CRO join — a CRO match is decisive evidence the
    # winner is a registered company, not an individual (see below).

    # ---- CRO match: by winner-identifier (exact reg number) THEN by normalised name ----
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num", "company_status"])
    cro_num = (
        cro.select(
            pl.col("company_num")
            .cast(pl.Utf8)
            .str.replace_all(r"\D", "")
            .str.strip_chars_start("0")
            .alias("num_digits"),
            pl.col("company_num").alias("company_num_id"),
            pl.col("company_status").alias("status_by_id"),
        )
        .filter(pl.col("num_digits").str.len_chars() >= 4)
        .unique(subset=["num_digits"])
    )
    cro_name = cro.filter(pl.col("name_norm").str.len_chars() >= 4).unique(subset=["name_norm"])

    df = (
        df.join(cro_num, left_on="winner_identifier_digits", right_on="num_digits", how="left")
        .join(cro_name, left_on="winner_name_norm", right_on="name_norm", how="left")
        .with_columns(
            pl.coalesce(["company_num_id", "company_num"]).alias("cro_company_num"),
            pl.when(pl.col("company_num_id").is_not_null())
            .then(pl.lit("identifier"))
            .when(pl.col("company_num").is_not_null())
            .then(pl.lit("name"))
            .otherwise(pl.lit("none"))
            .alias("cro_match_method"),
            pl.coalesce(["status_by_id", "company_status"]).alias("cro_company_status"),
        )
        .drop(["company_num_id", "company_num", "status_by_id", "company_status"])
    )

    # CRO-evidence upgrade: a winner that joins the company register IS a registered company,
    # even if its TED name dropped the suffix word (Sweeney Consultancy, Three Ireland, Savills,
    # Cruinn Diagnostics...). Upgrade those from sole_trader_or_individual -> company so the
    # privacy flag isn't inflated by real firms. privacy_status computed AFTER this.
    df = df.with_columns(
        pl.when((pl.col("supplier_class") == "sole_trader_or_individual") & (pl.col("cro_match_method") != "none"))
        .then(pl.lit("company"))
        .otherwise(pl.col("supplier_class"))
        .alias("supplier_class"),
    ).with_columns(
        pl.when(pl.col("supplier_class") == "sole_trader_or_individual")
        .then(pl.lit("review_personal_data"))
        .otherwise(pl.lit("ok"))
        .alias("privacy_status"),
    )

    # ---- value flags ----------------------------------------------------------------
    # TED award values are ceiling/award-grade, not transactions. Even SINGLE-winner
    # notices above EU thresholds are routinely multi-year framework/operating CEILINGS
    # (e.g. Version1 €10.3bn IT framework for Education; NTA bus operating contracts €1-2bn).
    # So a "single-winner" test is NOT enough — gate large awards out of value_safe_to_sum
    # and flag them for review. The trustworthy metrics here are COUNT and MEDIAN, never a
    # naive sum (per doc/PROCUREMENT_INVESTIGATION.md). Threshold is deliberately blunt and
    # documented; a later view should refine it with TED's framework-agreement field.
    LARGE_AWARD = 50_000_000
    df = df.with_columns(
        (pl.col("award_value_eur") >= LARGE_AWARD).alias("is_large_award_review"),
    ).with_columns(
        (
            (pl.col("value_kind") == "contract_award_value")
            & ~pl.col("is_multi_supplier_framework")
            & ~pl.col("is_pan_eu_outlier")
            & ~pl.col("is_large_award_review")  # likely multi-year ceiling, not a transaction
            & pl.col("award_value_eur").is_not_null()
            & (pl.col("award_value_eur") > 0)
        ).alias("value_safe_to_sum"),
        pl.lit("TED").alias("source"),
        pl.lit(datetime.now(UTC).strftime("%Y-%m-%d")).alias("retrieved_utc"),
    )

    OUT_SILVER.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_SILVER)

    hr("SILVER WRITTEN")
    print(f"rows (notice x winner): {df.height:,}  ->  {OUT_SILVER}")
    print(f"distinct notices: {df['publication_number'].n_unique():,}")
    print(df.group_by("supplier_class").len().sort("len", descending=True))
    print(df.group_by("cro_match_method").len().sort("len", descending=True))

    safe = df.filter(pl.col("value_safe_to_sum"))
    cro_hit = df.filter(pl.col("cro_match_method") != "none")
    by_id = df.filter(pl.col("cro_match_method") == "identifier")
    print(
        f"\nvalue_safe_to_sum rows: {safe.height:,}  €{(safe['award_value_eur'].sum() or 0):,.0f} "
        f"(single-winner awards only; frameworks + pan-EU excluded)"
    )
    print(
        f"CRO matched: {cro_hit.height:,} ({cro_hit.height / df.height:.0%})  "
        f"of which by exact identifier: {by_id.height:,}"
    )

    cov = {
        "rows_notice_x_winner": df.height,
        "distinct_notices": int(df["publication_number"].n_unique()),
        "rows_with_value": int(df["award_value_eur"].is_not_null().sum()),
        "multi_supplier_framework_rows": int(df["is_multi_supplier_framework"].sum()),
        "pan_eu_outlier_rows": int(df["is_pan_eu_outlier"].sum()),
        "value_safe_to_sum_rows": safe.height,
        "value_safe_to_sum_total_eur": float(safe["award_value_eur"].sum() or 0),
        "value_naive_sum_eur_DO_NOT_USE": float(df["award_value_eur"].sum() or 0),
        "large_award_review_rows_ge_50m": int(df["is_large_award_review"].sum()),
        "median_award_eur": float(df.filter(pl.col("award_value_eur") > 0)["award_value_eur"].median() or 0),
        "trustworthy_metrics": "COUNT of awards + MEDIAN award value; never the naive sum (ceiling/award-grade values, tail-dominated)",
        "supplier_class_counts": {
            r["supplier_class"]: r["len"] for r in df.group_by("supplier_class").len().iter_rows(named=True)
        },
        "cro_match_counts": {
            r["cro_match_method"]: r["len"] for r in df.group_by("cro_match_method").len().iter_rows(named=True)
        },
        "cro_match_rate": round(cro_hit.height / max(1, df.height), 3),
        "rows_review_personal_data": int((df["privacy_status"] == "review_personal_data").sum()),
        "date_span": [df["dispatch_date"].min(), df["dispatch_date"].max()],
        "layer": "silver",
        "next_step": "build sql_views/ted_*.sql (reconcile frameworks + CRO) before any gold/UI",
        "source": SOURCE,
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "SILVER (cleaned, not frontend-exposed). One row per notice x winner. "
        "tender-value is a NOTICE-level figure: for multi-supplier frameworks it is the "
        "framework CEILING, never per-winner — only value_safe_to_sum (single-winner, "
        "non-framework, non-pan-EU) may be totalled, labelled 'awarded', not spend. "
        "winner-identifier matched to CRO company_num after digit-strip; bare personal-name "
        "winners flagged review_personal_data (quarantine deferred). A contract award is a "
        "fact, not evidence of influence.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage {OUT_COV}")
    print("\nLAYER=silver. Gold only when a sql_views/ted_*.sql view exposes it to the UI.")


if __name__ == "__main__":
    main()
