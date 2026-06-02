"""Corporate notices enrichment — produces data/gold/parquet/corporate_notices.parquet.

Source: data/silver/iris_oifigiuil/iris_notice_events_clean.csv
Output: data/gold/parquet/corporate_notices.parquet
        data/_meta/corporate_notices_coverage.json    (A5 coverage gate)

Civic frame: corporate notices only. Personal insolvency (individual
bankruptcies) is excluded by policy — see
[[feedback_personal_insolvency_privacy]]. The exclusion is:
  - notice_category == "bankruptcy" (the whole bucket)
  - PLUS any row in another bucket whose raw_text carries personal-bankruptcy
    wording (~277 leak rows the classifier didn't move)

Brand tagging: for each notice, scan raw_text against a curated alias list
(data/_meta/loan_book_fund_aliases.csv) and tag brand_mentions +
parent_fund_mentions as list columns. Used by the Corporate page's featured
"who's calling in Irish loans" panel. Coverage is honestly reported in the
A5 JSON: of 2,624 receivership notices, only a minority name a known major
appointing party — the rest are appointed by smaller institutions or
individuals.

CLI:
    python corporate_notices_enrichment.py            # print summary, no write
    python corporate_notices_enrichment.py --write    # write parquet + coverage JSON
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
import sys

import polars as pl

from config import DATA_DIR, GOLD_PARQUET_DIR, SILVER_DIR
from paths import PROJECT_ROOT as _ROOT

sys.path.insert(0, str(_ROOT))

_SRC = SILVER_DIR / "iris_oifigiuil" / "iris_notice_events_clean.csv"
_OUT = GOLD_PARQUET_DIR / "corporate_notices.parquet"
_META = DATA_DIR / "_meta" / "corporate_notices_coverage.json"
_BRAND_CSV = DATA_DIR / "_meta" / "loan_book_fund_aliases.csv"

# Categories in scope for the Corporate page.
CORPORATE_CATEGORIES = [
    "corporate_insolvency",
    "corporate_notice",
    "corporate_rescue",
    "investment_vehicle_register_notice",
]

# Personal-insolvency wording that must be excluded even when it leaks into a
# corporate category (~277 such rows observed across the corpus).
_PERSONAL_INSOL_RE = re.compile(
    r"\bA BANKRUPT\b|ADJUDICATED BANKRUPT|BANKRUPT IN MAIN PROCEEDINGS|"
    r"PERSONAL INSOLVENCY|DEBT SETTLEMENT ARRANGEMENT|DEBT RELIEF NOTICE|"
    r"PROTECTIVE CERTIFICATE",
    re.I,
)


def _load_brand_map() -> dict[str, tuple[str, str]]:
    """Return { BRAND_UPPER : (parent_fund, fund_type) }."""
    df = pl.read_csv(_BRAND_CSV)
    out = {}
    for r in df.iter_rows(named=True):
        brand = str(r.get("brand", "")).strip().upper()
        if brand:
            out[brand] = (str(r.get("parent_fund", "")).strip(), str(r.get("fund_type", "")).strip())
    return out


def _tag_row(raw_upper: str, brand_map: dict) -> tuple[list[str], list[str], list[str]]:
    """Return (brand_mentions, parent_fund_mentions, fund_type_mentions) for one notice."""
    brands: list[str] = []
    parents: list[str] = []
    types: list[str] = []
    seen_parents: set[str] = set()
    for brand, (parent, ftype) in brand_map.items():
        if brand in raw_upper:
            brands.append(brand)
            if parent and parent not in seen_parents:
                parents.append(parent)
                types.append(ftype)
                seen_parents.add(parent)
    return brands, parents, types


def enrich(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    brand_map = _load_brand_map()
    print(f"[1/4] loaded brand map: {len(brand_map)} aliases → {len(set(p for p, _ in brand_map.values()))} parents")

    print(f"[2/4] filtering to corporate scope ({len(CORPORATE_CATEGORIES)} categories)...")
    raw_in_scope = df.filter(pl.col("notice_category").is_in(CORPORATE_CATEGORIES))
    n_in_scope = raw_in_scope.height
    print(f"   in scope before personal-insolvency exclusion: {n_in_scope:,}")

    print("[3/4] applying personal-insolvency exclusion (privacy rule)...")
    raw_texts = raw_in_scope["raw_text"].cast(pl.Utf8).fill_null("").to_list()
    personal_mask = [bool(_PERSONAL_INSOL_RE.search(t)) for t in raw_texts]
    n_excluded = sum(personal_mask)
    keep_mask = pl.Series("_keep", [not p for p in personal_mask])
    corp = raw_in_scope.with_columns(keep_mask).filter(pl.col("_keep")).drop("_keep")
    print(f"   excluded {n_excluded:,} personal-insolvency-wording rows | kept {corp.height:,}")

    print("[4/4] tagging brand_mentions / parent_fund_mentions...")
    brands_col: list[list[str]] = []
    parents_col: list[list[str]] = []
    types_col: list[list[str]] = []
    for t in corp["raw_text"].cast(pl.Utf8).fill_null("").to_list():
        b, p, ft = _tag_row(t.upper(), brand_map)
        brands_col.append(b)
        parents_col.append(p)
        types_col.append(ft)

    out = corp.select(
        [
            "notice_ref",
            "issue_date",
            "issue_number",
            "notice_category",
            "notice_subtype",
            "entity_name",
            "display_title",
            "title",
            "raw_text",
            "iris_source_pdf" if "iris_source_pdf" in corp.columns else "source_file",
        ]
    ).rename({"source_file": "iris_source_pdf"} if "source_file" in corp.columns else {})

    out = out.with_columns(
        [
            pl.Series("brand_mentions", brands_col, dtype=pl.List(pl.Utf8)),
            pl.Series("parent_fund_mentions", parents_col, dtype=pl.List(pl.Utf8)),
            pl.Series("fund_type_mentions", types_col, dtype=pl.List(pl.Utf8)),
        ]
    )

    # Receivership-shaped subset (used for the brand-coverage stat).
    recv_mask = (out["notice_subtype"] == "receivership") | out["raw_text"].cast(
        pl.Utf8
    ).str.to_uppercase().str.contains("APPOINTMENT OF (?:STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER")
    n_recv = int(recv_mask.sum())
    n_recv_tagged = int(((out["brand_mentions"].list.len() > 0) & recv_mask).sum())

    # A5 coverage gate JSON.
    coverage = {
        "as_of": _dt.datetime.now().isoformat(timespec="seconds"),
        "source": str(_SRC.relative_to(_ROOT)),
        "rows_in_scope_before_exclusion": n_in_scope,
        "rows_excluded_personal_insolvency": n_excluded,
        "rows_in_final_parquet": out.height,
        "category_counts": dict(out["notice_category"].value_counts(sort=True).iter_rows()),
        "subtype_counts": dict(out["notice_subtype"].value_counts(sort=True).head(20).iter_rows()),
        "receivership_brand_tagging": {
            "receivership_notices_total": n_recv,
            "receivership_notices_with_known_brand": n_recv_tagged,
            "coverage_pct": round(100 * n_recv_tagged / max(n_recv, 1), 1),
            "honest_note": (
                "Of receivership notices, only a minority name a known major loan-book "
                "buyer or Irish bank. The rest are appointed by smaller institutions or "
                "under private debentures where no major fund is named, OR by brands "
                "not yet in data/_meta/loan_book_fund_aliases.csv."
            ),
        },
        "entity_name_quality": {
            "null_or_empty": int(
                (out["entity_name"].is_null() | (out["entity_name"].cast(pl.Utf8).str.strip_chars() == "")).sum()
            ),
            "junk_pattern_count": int(
                out.filter(
                    pl.col("entity_name")
                    .cast(pl.Utf8)
                    .str.to_uppercase()
                    .str.contains(r"NOTICE IS HEREBY|ABOVE NAMED|IN THE MATTER|COMPANIES ACT|ICAV ACT|COLLECTIVE ASSET")
                ).height
            ),
        },
        "personal_insolvency_exclusion_rule": (
            "notice_category == 'bankruptcy' (excluded upstream) PLUS personal-bankruptcy "
            "text patterns: A BANKRUPT, ADJUDICATED BANKRUPT, BANKRUPT IN MAIN PROCEEDINGS, "
            "PERSONAL INSOLVENCY, DEBT SETTLEMENT ARRANGEMENT, DEBT RELIEF NOTICE, "
            "PROTECTIVE CERTIFICATE."
        ),
    }
    return out, coverage


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--write", action="store_true", help="write the gold parquet + coverage JSON")
    args = ap.parse_args()

    src = pl.read_csv(_SRC, infer_schema_length=20000)
    out, coverage = enrich(src)

    print()
    print("=" * 64)
    print("CORPORATE NOTICES ENRICHMENT — SUMMARY")
    print("=" * 64)
    print(f"  final rows:                       {out.height:,}")
    print(f"  excluded (personal insolvency):   {coverage['rows_excluded_personal_insolvency']:,}")
    print(
        f"  receivership brand-tag coverage:  {coverage['receivership_brand_tagging']['coverage_pct']}%  "
        f"({coverage['receivership_brand_tagging']['receivership_notices_with_known_brand']:,} of "
        f"{coverage['receivership_brand_tagging']['receivership_notices_total']:,})"
    )
    print(f"  entity_name nulls:                {coverage['entity_name_quality']['null_or_empty']:,}")
    print(f"  entity_name junk-pattern hits:    {coverage['entity_name_quality']['junk_pattern_count']:,}")
    print()
    print("  by category:")
    for k, v in coverage["category_counts"].items():
        print(f"    {k:38s} {v:,}")

    if args.write:
        _OUT.parent.mkdir(parents=True, exist_ok=True)
        _META.parent.mkdir(parents=True, exist_ok=True)
        out.write_parquet(_OUT, compression="zstd", compression_level=3, statistics=True)
        _META.write_text(json.dumps(coverage, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nwrote {out.height:,} rows -> {_OUT.relative_to(_ROOT)}")
        print(f"wrote coverage JSON   -> {_META.relative_to(_ROOT)}")


if __name__ == "__main__":
    main()
