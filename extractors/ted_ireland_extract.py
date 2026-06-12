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

Wired into pipeline.py as the `ted` chain. Silver parquet is regenerable from the API
(left untracked). Surfaced via sql_views/procurement_ted_*.sql on the Procurement page —
"gold" here = exposed as-is through a SQL view that reads this silver directly.

Run:
  ./.venv/Scripts/python.exe extractors/ted_ireland_extract.py
  ./.venv/Scripts/python.exe extractors/ted_ireland_extract.py --max-pages 4   # quick
  ./.venv/Scripts/python.exe extractors/ted_ireland_extract.py --refresh        # ignore cache
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from extractors.ted_enrich import enrich_winner_rows  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402
from services.ted_search import fetch_ted_search  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

RAW_CACHE = ROOT / "data/bronze/ted/ted_ie_awards_raw.json"  # bronze: raw API capture (portable, headless-safe)
OUT_SILVER = ROOT / "data/silver/parquet/ted_ie_awards.parquet"
OUT_COV = ROOT / "data/_meta/ted_ie_awards_coverage.json"

# Cache TTL: a bronze capture reused whenever it merely EXISTS makes a routine `ted` chain
# run silently rebuild silver from a stale pull (the bug that left the sibling tenders lane
# 10 weeks behind while looking freshly retrieved — fixed both lanes 2026-06-11/12; same
# class as DAIL-160/162). Awards arrive steadily, so 7d is enough; --refresh ignores the
# cache entirely. Override with TED_RAW_CACHE_MAX_AGE_DAYS.
RAW_CACHE_MAX_AGE_DAYS: float = float(os.environ.get("TED_RAW_CACHE_MAX_AGE_DAYS", "7"))


def _cache_is_fresh(refresh: bool, max_age_days: float = RAW_CACHE_MAX_AGE_DAYS) -> bool:
    if refresh or not RAW_CACHE.exists():
        return False
    age_days = (time.time() - RAW_CACHE.stat().st_mtime) / 86400.0
    if age_days > max_age_days:
        print(f"TED awards raw cache {age_days:.1f}d old (> {max_age_days}d) — re-pulling.")
        return False
    print(f"TED awards raw cache {age_days:.1f}d old (<= {max_age_days}d) — reusing {RAW_CACHE}.")
    return True


URL = "https://api.ted.europa.eu/v3/notices/search"
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
    # ── competition-intensity layer (eForms, 2024+; fill rates measured 2026-06-08) ──
    "procedure-type",  # 99.6% — open / restricted / negotiated / single-offer
    "received-submissions-type-code",  # 99.2% — submission-type taxonomy (see TENDER_SUBMISSION_CODES)
    "received-submissions-type-val",  # 99.2% — the per-lot submission COUNT (the single-bid signal)
    "award-criterion-type-lot",  # 95.6% — price / cost / quality
    # ── contract-term layer (expiring-contracts signal; fill rates measured 2026-06-11 on
    #    2025H2+ CANs — early-2024 eForms-transition notices are sparser) ──
    "contract-conclusion-date",  # BT-145, ~50% — when the contract was signed
    "contract-duration-period-lot",  # BT-36,  ~50% — [{unit: MONTH/YEAR/DAY, value: N}] per lot
    "contract-duration-start-date-lot",  # BT-536, ~4% — explicit start
    "contract-duration-end-date-lot",  # BT-537, ~2% — explicit end (rare but authoritative)
    "renewal-maximum-lot",  # BT-58,  ~2% — max renewals advertised
    "procedure-identifier",  # BT-04, 100% — future join to the CN tender lane
]
# 2024+ is deliberate, NOT an API limit. The API reaches back to 2016, BUT for pre-2024 legacy
# notices the WINNER is unavailable: verified 2026-06-08 winner-name / winner-identifier /
# organisation-name-tenderer = 0% for 2016-2023 (vs ~55% in 2024+), while buyer-name=100% and
# total-value≈62-83%. This silver is winner-centric (winner->CRO, supplier rankings, per-winner
# value_safe_to_sum), so a winner-less backfill would corrupt the grain. Pre-2024 winner+value
# only exist in the bulk legacy TED_EXPORT XML packages. See doc/TED_ENRICHMENT.md §3.5.
QUERY = "buyer-country=IRL AND notice-type=can-standard AND publication-date>=20240101"

# received-submissions-type-code is an eForms taxonomy (BT-759/760). VERIFIED 2026-06-08: codes
# are never mixed within a single notice (0/250). Every value below is a count of TENDERS
# received; 'part-req' (requests-to-participate — a restricted/negotiated FIRST-stage count, not
# tenders) is deliberately excluded so the single-bid signal isn't polluted.
TENDER_SUBMISSION_CODES = {"tenders", "t-esubm", "t-sme", "t-small", "t-med", "t-oth-eea", "t-non-eea", "t-eea"}
# procedure types that ran WITHOUT an open competitive call (a factual signal, never a verdict).
UNCOMPETITIVE_PROCEDURES = {"neg-wo-call", "oth-single"}

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


def pull(max_pages: int | None) -> list[dict]:
    """Full Irish-awards scroll via the shared ITERATION paginator (services/ted_search.py).

    Replaces the old PAGE_NUMBER loop, which the API caps at 15k notices and which capped
    itself at 10k with no completeness check (silent truncation). ITERATION has no notice
    limit and asserts against totalNoticeCount. max_pages is a smoke-test bound (None = all).
    """
    return fetch_ted_search(QUERY, FIELDS, label="ted-awards", max_pages=max_pages)


def load_raw(max_pages: int | None, refresh: bool) -> list[dict]:
    if _cache_is_fresh(refresh):
        try:
            data = json.loads(RAW_CACHE.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass
    raw = pull(max_pages)
    if not raw:
        # API outage: keep the existing capture (stale beats empty) rather than
        # clobbering bronze with [] — downstream still rebuilds from it.
        if RAW_CACHE.exists():
            with contextlib.suppress(Exception):
                data = json.loads(RAW_CACHE.read_text(encoding="utf-8"))
                if isinstance(data, list) and data:
                    print(f"API returned nothing — falling back to stale capture ({len(data):,} notices)")
                    return data
        return raw
    RAW_CACHE.parent.mkdir(parents=True, exist_ok=True)
    RAW_CACHE.write_text(json.dumps(raw), encoding="utf-8")
    print(f"wrote raw capture (bronze) -> {RAW_CACHE}")
    return raw


def clean_identifier(s: str) -> str:
    """winner-identifier is mixed (CRO num / VAT / name). Reduce to digits, drop leading
    zeros so it can join CRO company_num (the investigation's ~84% id-match finding)."""
    digits = re.sub(r"\D", "", s or "")
    return digits.lstrip("0")


def competition_fields(n: dict) -> dict:
    """Notice-level competition-intensity signals (same for every winner-row of the notice).

    All eForms fields arrive as PER-LOT arrays. Rules (measured 2026-06-08):
      * procedure_type: a scalar/first value.
      * n_tenders_received: the MIN tender count across the notice's lots (the least-competitive
        lot) — derived only from TENDER_SUBMISSION_CODES, never from 'part-req'. is_single_bid
        means that least-competitive lot received exactly one tender. ⚠️ KEPT for backward
        compatibility, but it OVER-states single-bid for multi-lot notices (a 10-lot notice is
        flagged single-bid if any ONE lot drew a single bidder). For an honest competition rate
        use the LOT-LEVEL counts below and aggregate single-bid LOTS / total LOTS, not notices.
      * n_lots_with_bidcount / n_single_bid_lots: the per-lot truth — how many of the notice's
        lots reported a tender count, and how many of those had exactly one bidder. Summing these
        across a buyer gives the real single-bid rate (each contract part counted once).
      * award_criteria_kind: the distinct price/cost/quality types used; is_price_only flags a
        lowest-price-only award (no quality criterion). All neutral facts, never verdicts.
    """
    proc = n.get("procedure-type")
    if isinstance(proc, list):
        proc = proc[0] if proc else None

    codes = n.get("received-submissions-type-code") or []
    vals = n.get("received-submissions-type-val") or []
    tender_counts = []
    for c, v in zip(codes, vals, strict=False):
        if c in TENDER_SUBMISSION_CODES:
            with contextlib.suppress(TypeError, ValueError):
                count = int(str(v))
                # A lot reporting 0 tenders on an AWARD notice is a cancelled/failed
                # lot, not a bid count — an awarded lot has ≥1 by definition. Keeping
                # the 0 made min() report nonsense (n_tenders_received=0, 539 rows)
                # and deflated every single-bid denominator downstream.
                if count >= 1:
                    tender_counts.append(count)
    n_tenders = min(tender_counts) if tender_counts else None

    crit = n.get("award-criterion-type-lot") or []
    crit = crit if isinstance(crit, list) else [crit]
    crit_set = sorted({str(c) for c in crit if c})

    return {
        "procedure_type": proc,
        "is_uncompetitive_procedure": (proc in UNCOMPETITIVE_PROCEDURES) if proc else None,
        "n_tenders_received": n_tenders,
        "is_single_bid": (n_tenders == 1) if n_tenders is not None else None,
        # lot-level competition truth (count each contract part once, not the whole notice)
        "n_lots_with_bidcount": len(tender_counts) if tender_counts else None,
        "n_single_bid_lots": sum(1 for t in tender_counts if t == 1) if tender_counts else None,
        "award_criteria_kind": "+".join(crit_set) if crit_set else None,
        "is_price_only": (crit_set == ["price"]) if crit_set else None,
    }


# eForms duration units -> months. DAY/WEEK normalised on the average-month basis;
# durations are advertised terms (whole months dominate), not accounting periods.
_DURATION_UNIT_MONTHS = {"MONTH": 1.0, "YEAR": 12.0, "DAY": 1 / 30.44, "WEEK": 7 / 30.44, "QUARTER": 3.0}


def _first_date(v) -> str | None:
    """First ISO date from a TED list/scalar value ('2024-01-09Z' -> '2024-01-09')."""
    if isinstance(v, list):
        v = v[0] if v else None
    if not v:
        return None
    s = str(v)[:10]
    return s if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s) else None


def _add_months_approx(iso_date: str, months: float) -> str | None:
    """iso_date + months. Whole months move the calendar month (clamping the day);
    any fractional remainder is added as average-month days."""
    from datetime import date, timedelta

    try:
        d = date.fromisoformat(iso_date)
    except ValueError:
        return None
    whole = int(months)
    frac_days = round((months - whole) * 30.44)
    y, m = divmod(d.month - 1 + whole, 12)
    y += d.year
    m += 1
    # clamp day to the target month's length (Jan 31 + 1 month -> Feb 28/29)
    for day in (d.day, 30, 29, 28):
        try:
            out = date(y, m, day)
            break
        except ValueError:
            continue
    else:
        return None
    return (out + timedelta(days=frac_days)).isoformat()


def duration_fields(n: dict) -> dict:
    """Notice-level contract-term facts + the derived estimated end date.

    eForms duration arrives per lot; we take the LONGEST lot term (the latest a part of
    the contract is advertised to run). The estimate anchors on, in order of preference:
    explicit end date (BT-537) > explicit start (BT-536) + duration > conclusion date
    (BT-145) + duration. contract_end_basis records which path produced the estimate so
    the UI can present it as the advertised term it is — never a verified end date.
    Renewal options (renewal_max) are surfaced, NOT folded into the estimate.
    """
    periods = n.get("contract-duration-period-lot") or []
    periods = periods if isinstance(periods, list) else [periods]
    months_vals = []
    for p in periods:
        if isinstance(p, dict) and p.get("value") is not None:
            unit = _DURATION_UNIT_MONTHS.get(str(p.get("unit", "")).upper())
            with contextlib.suppress(TypeError, ValueError):
                if unit:
                    months_vals.append(float(str(p["value"])) * unit)
    duration_months = round(max(months_vals), 1) if months_vals else None

    conclusion = _first_date(n.get("contract-conclusion-date"))
    start = _first_date(n.get("contract-duration-start-date-lot"))
    end_explicit = _first_date(n.get("contract-duration-end-date-lot"))

    end_est, basis = None, None
    if end_explicit:
        end_est, basis = end_explicit, "explicit_end_date"
    elif duration_months is not None and start:
        end_est, basis = _add_months_approx(start, duration_months), "start_plus_duration"
    elif duration_months is not None and conclusion:
        end_est, basis = _add_months_approx(conclusion, duration_months), "conclusion_plus_duration"
    if end_est is None:
        basis = None

    renewals = n.get("renewal-maximum-lot") or []
    renewals = renewals if isinstance(renewals, list) else [renewals]
    renewal_vals = []
    for r in renewals:
        with contextlib.suppress(TypeError, ValueError):
            renewal_vals.append(int(str(r)))
    proc_id = n.get("procedure-identifier")
    if isinstance(proc_id, list):
        proc_id = proc_id[0] if proc_id else None

    return {
        "procedure_id": str(proc_id) if proc_id else None,
        "contract_conclusion_date": conclusion,
        "contract_start_date": start,
        "contract_end_date_explicit": end_explicit,
        "contract_duration_months": duration_months,
        "renewal_max": max(renewal_vals) if renewal_vals else None,
        "contract_end_date_est": end_est,
        "contract_end_basis": basis,
    }


def build_rows(raw: list[dict]) -> list[dict]:
    rows = []
    for n in raw:
        comp = competition_fields(n)
        term = duration_fields(n)
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
                    **comp,
                    **term,
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
    ap.add_argument(
        "--max-pages", type=int, default=None, help="smoke-test bound; default None = ALL pages (ITERATION)"
    )
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

    # Shared winner classification + CRO match + privacy + value flags (extractors/ted_enrich.py)
    # — byte-identical with the legacy per-notice-XML lane so the two silvers UNION cleanly.
    df = enrich_winner_rows(df)

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
        "competition_signal": {
            "rows_with_procedure_type": int(df["procedure_type"].is_not_null().sum()),
            "rows_with_tenders_received": int(df["n_tenders_received"].is_not_null().sum()),
            "single_bid_rows": int((df["is_single_bid"] == True).sum()),  # noqa: E712 (polars mask)
            "uncompetitive_procedure_rows": int((df["is_uncompetitive_procedure"] == True).sum()),  # noqa: E712
            "price_only_rows": int((df["is_price_only"] == True).sum()),  # noqa: E712
            "note": "n_tenders_received = MIN tenders across the notice's lots (least-competitive "
            "lot); is_single_bid = that min is 1. Derived from tender-count submission codes only "
            "('part-req' excluded). A factual competition signal, never a verdict.",
        },
        "contract_term_signal": {
            "rows_with_duration": int(df["contract_duration_months"].is_not_null().sum()),
            "rows_with_end_estimate": int(df["contract_end_date_est"].is_not_null().sum()),
            "end_basis_counts": {
                r["contract_end_basis"]: r["len"]
                for r in df.group_by("contract_end_basis").len().iter_rows(named=True)
                if r["contract_end_basis"] is not None
            },
            "note": "contract_end_date_est = advertised term projected from the notice "
            "(explicit end > start+duration > conclusion+duration). An advertised term, "
            "never a verified end date; renewal options are NOT folded in.",
        },
        "date_span": [df["dispatch_date"].min(), df["dispatch_date"].max()],
        "layer": "silver",
        "next_step": "build sql_views/ted_*.sql (reconcile frameworks + CRO) before any gold/UI",
        "source": SOURCE,
        "schema_version": 2,
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
