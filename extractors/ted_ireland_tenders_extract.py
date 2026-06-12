"""TED (EU procurement journal) -> Irish COMPETITION / TENDER notices -> SILVER parquet.

Sibling of ted_ireland_extract.py (which does *award* notices, `can-standard`). This pulls
`cn-standard` — the PRE-AWARD stage: what Irish bodies are putting out to tender, under which
procedure, by when. A DIFFERENT GRAIN from awards and payments:

  award   (can-standard / eTenders)  — a contract was won, by whom, for how much
  TENDER  (cn-standard)  <- THIS      — a competition is open/recent; estimated value is a
                                        BUYER ESTIMATE, never an award and never a payment
  payment (public_payments_fact)      — money actually paid to a named supplier

⚠️ value_safe_to_sum is ALWAYS FALSE here: estimated-value is a pre-award estimate/ceiling, not
money awarded or paid. It is carried for context only and must NEVER be summed with award or
payment figures (the three-grain firewall, see doc/PROCUREMENT_MASTER.md).

Grain: ONE ROW PER NOTICE (lots aggregated — earliest deadline, summed estimate). eForms
fields arrive as per-lot arrays; we reduce them to a notice-level summary for a pipeline listing.

Wired into pipeline.py as the `ted_tenders` chain. Silver is regenerable from the API.

Run:
  ./.venv/Scripts/python.exe extractors/ted_ireland_tenders_extract.py
  ./.venv/Scripts/python.exe extractors/ted_ireland_tenders_extract.py --max-pages 4 --refresh
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

RAW_CACHE = ROOT / "data/bronze/ted/ted_ie_tenders_raw.json"
OUT_SILVER = ROOT / "data/silver/parquet/ted_ie_tenders.parquet"
OUT_COV = ROOT / "data/_meta/ted_ie_tenders_coverage.json"

# Cache TTL: the old code reused the bronze raw capture whenever it merely EXISTED, so a
# routine `ted_tenders` chain run silently rebuilt silver from a months-old pull and stamped
# a fresh retrieved_utc over stale notices (observed 2026-06-11: retrieved 06-08, newest
# dispatch 03-25 — a ~10-week-stale forward pipeline that LOOKED fresh). Same silent-staleness
# class as DAIL-160/162 and the old eTenders CSV cache; same fix. A cache older than this is
# re-pulled; --refresh ignores it entirely. The tenders lane is the live opportunity feed, so
# the TTL is short. Override with TED_RAW_CACHE_MAX_AGE_DAYS.
RAW_CACHE_MAX_AGE_DAYS: float = float(os.environ.get("TED_RAW_CACHE_MAX_AGE_DAYS", "3"))


def _cache_is_fresh(refresh: bool, max_age_days: float = RAW_CACHE_MAX_AGE_DAYS) -> bool:
    if refresh or not RAW_CACHE.exists():
        return False
    age_days = (time.time() - RAW_CACHE.stat().st_mtime) / 86400.0
    if age_days > max_age_days:
        print(f"TED tenders raw cache {age_days:.1f}d old (> {max_age_days}d) — re-pulling.")
        return False
    print(f"TED tenders raw cache {age_days:.1f}d old (<= {max_age_days}d) — reusing {RAW_CACHE}.")
    return True


URL = "https://api.ted.europa.eu/v3/notices/search"
H = {"User-Agent": "dail-tracker research probe", "Accept": "application/json"}
FIELDS = [
    "publication-number",
    "buyer-name",
    "classification-cpv",
    "procedure-type",
    "deadline-receipt-tender-date-lot",
    "estimated-value-lot",
    "estimated-value-cur-lot",
    "dispatch-date",
]
QUERY = "buyer-country=IRL AND notice-type=cn-standard AND publication-date>=20240101"
PAGE_CAP = 120  # 250/page; ~28k cn-standard notices all-time, ~recent slice when date-floored

# CPV division labels (kept in sync with ted_ireland_extract.py).
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
UNCOMPETITIVE_PROCEDURES = {"neg-wo-call", "oth-single"}

SOURCE = {
    "dataset": "TED — Tenders Electronic Daily (contract/competition notices, Ireland)",
    "publisher": "Publications Office of the European Union",
    "api": URL,
    "query": QUERY,
    "notice_url_template": "https://ted.europa.eu/en/notice/-/detail/{publication_number}",
    "license": "EU open data — reuse authorised under Commission Decision 2011/833/EU",
    "attribution": "Contains information from TED (© European Union), reused under Decision 2011/833/EU.",
}


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
    if _cache_is_fresh(refresh):
        with contextlib.suppress(Exception):
            data = json.loads(RAW_CACHE.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                return data
    raw = pull(max_pages)
    if not raw:
        # API outage: keep the existing capture (stale beats empty) — main() will still
        # rebuild silver from it rather than clobbering bronze with [].
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


def _min_date(v) -> str | None:
    """Earliest submission deadline across the notice's lots (YYYY-MM-DD)."""
    vals = v if isinstance(v, list) else [v]
    days = sorted(str(x)[:10] for x in vals if x)
    return days[0] if days else None


def build_rows(raw: list[dict]) -> list[dict]:
    rows = []
    for n in raw:
        cpv = n.get("classification-cpv") or []
        cpv = cpv if isinstance(cpv, list) else [cpv]
        cpv0 = str(cpv[0]) if cpv else ""
        proc = n.get("procedure-type")
        if isinstance(proc, list):
            proc = proc[0] if proc else None
        est = to_eur(n.get("estimated-value-lot"))
        date = (n.get("dispatch-date") or "")[:10]
        pub = n.get("publication-number")
        rows.append(
            {
                "publication_number": pub,
                "notice_url": SOURCE["notice_url_template"].format(publication_number=pub) if pub else None,
                "buyer_name": first_eng(n.get("buyer-name")) or "?",
                "cpv_code": cpv0 or None,
                "cpv_division": CPV_DIV.get(cpv0[:2], "Other/Unknown"),
                "procedure_type": proc,
                "is_uncompetitive_procedure": (proc in UNCOMPETITIVE_PROCEDURES) if proc else None,
                "submission_deadline": _min_date(n.get("deadline-receipt-tender-date-lot")),
                "estimated_value_eur": est if est > 0 else None,
                "currency": first_eng(n.get("estimated-value-cur-lot")) or "EUR",
                # pre-award estimate — a BUYER ESTIMATE, never an award/payment; never summable.
                "value_kind": "pre_award_estimate",
                "value_safe_to_sum": False,
                "dispatch_date": date or None,
                "year": int(date[:4]) if date[:4].isdigit() else None,
                "month": date[:7] or None,
                "source": "TED_CN",
                "retrieved_utc": datetime.now(UTC).strftime("%Y-%m-%d"),
            }
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=PAGE_CAP)
    ap.add_argument("--refresh", action="store_true", help="ignore raw cache, re-pull API")
    args = ap.parse_args()

    print("PULL TED — Irish competition/tender notices (cn-standard, eForms era 2024+)")
    raw = load_raw(args.max_pages, args.refresh)
    print(f"notices: {len(raw):,}")
    if not raw:
        print("WARNING: TED API returned no tender notices and no cache — skipping (pipeline continues).")
        return

    df = pl.DataFrame(build_rows(raw), infer_schema_length=None)
    OUT_SILVER.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_SILVER)

    n_open = int((df["submission_deadline"] >= datetime.now(UTC).strftime("%Y-%m-%d")).sum())
    cov = {
        "rows_notices": df.height,
        "distinct_notices": int(df["publication_number"].n_unique()),
        "rows_with_estimated_value": int(df["estimated_value_eur"].is_not_null().sum()),
        "rows_with_deadline": int(df["submission_deadline"].is_not_null().sum()),
        "still_open_by_deadline": n_open,
        "uncompetitive_procedure_rows": int((df["is_uncompetitive_procedure"] == True).sum()),  # noqa: E712
        "date_span": [df["dispatch_date"].min(), df["dispatch_date"].max()],
        "grain": "one row per cn-standard notice (lots aggregated: earliest deadline, summed estimate)",
        "layer": "silver",
        "source": SOURCE,
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "PRE-AWARD competition notices. estimated_value_eur is a BUYER ESTIMATE — never an "
        "award value and never a payment; value_safe_to_sum is always FALSE. NEVER sum with award or "
        "payment figures (three-grain firewall). A tender notice is a procurement opportunity, not a contract.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"SILVER WRITTEN: {df.height:,} notices -> {OUT_SILVER}")
    print(f"  with estimated value: {cov['rows_with_estimated_value']:,}  with deadline: {cov['rows_with_deadline']:,}")
    print(f"wrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
