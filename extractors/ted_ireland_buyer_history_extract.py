"""TED (EU procurement journal) -> Irish AWARD-NOTICE activity, BUYER-SIDE -> SILVER parquet.

A SIBLING to ted_ireland_extract.py, deliberately a SEPARATE artifact (NEVER merged into
the winner-centric silver). Reason (verified 2026-06-08, see doc/TED_ENRICHMENT.md §3.5):
the TED Search API serves Irish award notices back to 2016, but for pre-2024 legacy notices
the WINNER is unavailable (winner-name / winner-identifier / organisation-name-tenderer = 0%),
while buyer-name = 100%, total-value ~= 62-83%, procedure-type = 100%. So the API can feed a
BUYER-SIDE history (which authority published the most/largest EU award notices, via what
procedure type, over time) — but it cannot feed a winner/supplier history. The winner backfill
needs the per-notice-XML lane (doc/TED_ENRICHMENT.md §3.5 "bulk legacy lane").

GRAIN: ONE ROW PER NOTICE (no winner dimension — the winner is not in this layer).

VALUE DISCIPLINE: total-value is the notice's TOTAL awarded value (all lots/winners). Without
a winner count we cannot reliably tell a single contract from a framework CEILING, so framework
detection here is VALUE-THRESHOLD-ONLY (blunter than the winner silver). The trustworthy metric
is COUNT of notices (and MEDIAN value); value_safe_to_sum is conservative (excludes large +
pan-EU) and totals must be labelled "awarded (notice totals)", never "spend". A published award
notice is a fact, not evidence of influence.

NOT wired into pipeline.py. Silver is regenerable from the API (left untracked). Gold only when
a sql_views/ted_*.sql view exposes it.

Run:
  ./.venv/Scripts/python.exe extractors/ted_ireland_buyer_history_extract.py
  ./.venv/Scripts/python.exe extractors/ted_ireland_buyer_history_extract.py --max-pages 4   # quick
  ./.venv/Scripts/python.exe extractors/ted_ireland_buyer_history_extract.py --refresh        # ignore cache
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# Reuse the sibling's pure helpers + reference tables (single source of truth; no duplication).
from extractors.ted_ireland_extract import (  # noqa: E402
    CPV_DIV,
    PAN_EU_HINT,
    UNCOMPETITIVE_PROCEDURES,
    first_eng,
    hr,
    to_eur,
)
from services.parquet_io import save_parquet  # noqa: E402
from shared.buyer_clean import clean_buyer_display  # noqa: E402
from services.ted_search import fetch_ted_search  # noqa: E402
from shared.name_norm import name_norm_expr  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

RAW_CACHE = ROOT / "data/bronze/ted/ted_ie_buyer_history_raw.json"
OUT_SILVER = ROOT / "data/silver/parquet/ted_ie_buyer_history.parquet"
OUT_COV = ROOT / "data/_meta/ted_ie_buyer_history_coverage.json"

URL = "https://api.ted.europa.eu/v3/notices/search"
# total-value (NOT tender-value) is the field that populates for legacy notices; the winner
# fields are deliberately NOT requested (they are 0% pre-2024 and absent by design here).
FIELDS = [
    "publication-number",
    "buyer-name",
    "total-value",
    "total-value-cur",
    "classification-cpv",
    "procedure-type",
    "notice-type",
    "dispatch-date",
]
# 2016 is the API's Irish data wall (verified 2026-06-08). Full range so authority trends are
# continuous from a single source; 2024+ overlaps the winner silver but is a DIFFERENT grain.
QUERY = "buyer-country=IRL AND notice-type=can-standard AND publication-date>=20160101"

LARGE_AWARD = 50_000_000  # blunt ceiling guard (no winner-count available here)

SOURCE = {
    "dataset": "TED — Tenders Electronic Daily (award-notice activity, buyer-side, Ireland)",
    "publisher": "Publications Office of the European Union",
    "api": URL,
    "query": QUERY,
    "landing_page": "https://ted.europa.eu/",
    "notice_url_template": "https://ted.europa.eu/en/notice/-/detail/{publication_number}",
    "license": "EU open data — reuse authorised under Commission Decision 2011/833/EU",
    "attribution": "Contains information from TED (© European Union), reused under Decision 2011/833/EU.",
    "winner_caveat": "BUYER-SIDE ONLY. The winner is NOT in this layer: the API returns 0% "
    "winner fields for pre-2024 legacy notices. Use ted_ie_awards.parquet (2024+) for "
    "supplier-side analysis; never join this layer to suppliers.",
}


def pull(max_pages: int | None) -> list[dict]:
    """Full 2016+ scroll via the shared ITERATION paginator (services/ted_search.py).

    ~16k notices exceeds PAGE_NUMBER's 15k cap, so ITERATION is mandatory here, not optional.
    max_pages is a smoke-test bound (None = all pages, with the completeness assertion).
    """
    return fetch_ted_search(QUERY, FIELDS, label="ted-buyer-history", max_pages=max_pages)


def load_raw(max_pages: int | None, refresh: bool) -> list[dict]:
    if RAW_CACHE.exists() and not refresh:
        with contextlib.suppress(Exception):
            data = json.loads(RAW_CACHE.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                print(f"using cached raw capture: {RAW_CACHE} ({len(data):,} notices)")
                return data
    raw = pull(max_pages)
    RAW_CACHE.parent.mkdir(parents=True, exist_ok=True)
    RAW_CACHE.write_text(json.dumps(raw), encoding="utf-8")
    print(f"wrote raw capture (bronze) -> {RAW_CACHE}")
    return raw


def build_rows(raw: list[dict]) -> list[dict]:
    rows = []
    for n in raw:
        cpv = n.get("classification-cpv") or []
        cpv = cpv if isinstance(cpv, list) else [cpv]
        cpv0 = str(cpv[0]) if cpv else ""
        proc = n.get("procedure-type")
        proc = (proc[0] if proc else None) if isinstance(proc, list) else proc
        val = to_eur(n.get("total-value"))
        buyer = first_eng(n.get("buyer-name")) or "?"
        date = (n.get("dispatch-date") or "")[:10]
        pub = n.get("publication-number")
        is_large = val >= LARGE_AWARD
        pan_eu = bool(PAN_EU_HINT.search(buyer))
        rows.append(
            {
                "publication_number": pub,
                "notice_url": SOURCE["notice_url_template"].format(publication_number=pub) if pub else None,
                "buyer_name": buyer,
                "total_value_eur": val if val > 0 else None,
                "currency": first_eng(n.get("total-value-cur")) or "EUR",
                "value_kind": "framework_or_dps_ceiling" if (is_large or pan_eu) else "contract_award_value",
                "is_large_award_review": is_large,
                "is_pan_eu_outlier": pan_eu,
                "procedure_type": proc,
                "is_uncompetitive_procedure": (proc in UNCOMPETITIVE_PROCEDURES) if proc else None,
                "cpv_code": cpv0 or None,
                "cpv_division": CPV_DIV.get(cpv0[:2], "Other/Unknown"),
                "dispatch_date": date or None,
                "year": int(date[:4]) if date[:4].isdigit() else None,
                "month": date[:7] or None,
                "notice_type": n.get("notice-type"),
            }
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=None, help="smoke-test bound; default None = ALL pages (ITERATION)")
    ap.add_argument("--refresh", action="store_true", help="ignore raw cache, re-pull API")
    args = ap.parse_args()

    hr("PULL TED — Irish award-notice activity (BUYER-SIDE, 2016+; no winner)")
    raw = load_raw(args.max_pages, args.refresh)
    print(f"\nnotices: {len(raw):,}")

    if not raw:
        print(
            "WARNING: TED API returned no notices and no cache is available — skipping this "
            "run (pipeline continues; prior silver, if any, is left untouched)."
        )
        return

    # Clean buyer_name (strip OGP org-id / school-roll debris) BEFORE deriving the norm, so the
    # join key groups the same authority together rather than splitting it by org id.
    df = clean_buyer_display(
        pl.DataFrame(build_rows(raw), infer_schema_length=None), "buyer_name"
    ).with_columns(
        name_norm_expr("buyer_name").alias("buyer_name_norm"),
    )
    # Conservative sum-safe gate: value present, single-grade (not large), not pan-EU. Framework
    # detection is threshold-only here (no winner count) — totals stay "awarded (notice totals)".
    df = df.with_columns(
        (
            (pl.col("value_kind") == "contract_award_value")
            & ~pl.col("is_large_award_review")
            & ~pl.col("is_pan_eu_outlier")
            & pl.col("total_value_eur").is_not_null()
            & (pl.col("total_value_eur") > 0)
        ).alias("value_safe_to_sum"),
        pl.lit("TED").alias("source"),
        pl.lit("buyer_side_award_notice").alias("grain"),
        pl.lit(datetime.now(UTC).strftime("%Y-%m-%d")).alias("retrieved_utc"),
    )

    OUT_SILVER.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_SILVER)

    hr("SILVER WRITTEN (buyer-side)")
    print(f"rows (one per notice): {df.height:,}  ->  {OUT_SILVER}")
    print(f"distinct notices: {df['publication_number'].n_unique():,}  distinct buyers: {df['buyer_name_norm'].n_unique():,}")
    print(df.group_by("year").len().sort("year"))
    safe = df.filter(pl.col("value_safe_to_sum"))
    has_val = df.filter(pl.col("total_value_eur").is_not_null())
    print(
        f"\nvalue present: {has_val.height:,} ({has_val.height / df.height:.0%})  "
        f"| value_safe_to_sum: {safe.height:,}  €{(safe['total_value_eur'].sum() or 0):,.0f} (notice totals, excl. large + pan-EU)"
    )
    print("\ntop buyers by notice count:")
    print(df.group_by("buyer_name").len().sort("len", descending=True).head(8))

    cov = {
        "rows_per_notice": df.height,
        "distinct_notices": int(df["publication_number"].n_unique()),
        "distinct_buyers": int(df["buyer_name_norm"].n_unique()),
        "rows_with_value": int(df["total_value_eur"].is_not_null().sum()),
        "value_fill_rate": round(int(df["total_value_eur"].is_not_null().sum()) / max(1, df.height), 3),
        "value_safe_to_sum_rows": safe.height,
        "value_safe_to_sum_total_eur": float(safe["total_value_eur"].sum() or 0),
        "value_naive_sum_eur_DO_NOT_USE": float(df["total_value_eur"].sum() or 0),
        "large_award_review_rows_ge_50m": int(df["is_large_award_review"].sum()),
        "median_value_eur": float(df.filter(pl.col("total_value_eur") > 0)["total_value_eur"].median() or 0),
        "by_year": {str(r["year"]): r["len"] for r in df.group_by("year").len().sort("year").iter_rows(named=True)},
        "by_procedure_type": {
            str(r["procedure_type"]): r["len"]
            for r in df.group_by("procedure_type").len().sort("len", descending=True).iter_rows(named=True)
        },
        "date_span": [df["dispatch_date"].min(), df["dispatch_date"].max()],
        "layer": "silver",
        "grain": "one row per award notice (BUYER-SIDE; NO winner dimension)",
        "trustworthy_metrics": "COUNT of notices + MEDIAN value + procedure-type mix; never the naive sum",
        "sibling_winner_layer": "data/silver/parquet/ted_ie_awards.parquet (2024+, winner-centric)",
        "next_step": "sql_views/ted_buyer_*.sql for authority-activity + procedure-type trends; "
        "winner backfill is a separate per-notice-XML build (doc/TED_ENRICHMENT.md §3.5)",
        "source": SOURCE,
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "BUYER-SIDE award-notice activity. One row per notice; the WINNER is NOT present "
        "(API returns 0% winner fields for pre-2024 legacy notices). total-value is the notice's "
        "total awarded value; framework ceilings cannot be separated without a winner count, so "
        "framework detection is value-threshold-only and value_safe_to_sum is conservative. Lead "
        "with COUNT. Never merge with or sum against the winner silver or eTenders.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage {OUT_COV}")
    print("\nLAYER=silver (buyer-side sibling). Gold only when a sql_views/ted_*.sql view exposes it.")


if __name__ == "__main__":
    main()
