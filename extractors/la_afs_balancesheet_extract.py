"""INGEST (sandbox): per-LA AFS BALANCE SHEET — full Statement of Financial Position (long).

Third AFS sibling, after la_afs_extract.py (REVENUE Income & Expenditure) and
la_afs_capital_extract.py (CAPITAL account by division). Those two are I&E FLOWS (money spent
in a year). This one reads the Balance Sheet / Statement of Financial Position — the council's
financial POSITION at 31 December: what it owns (fixed assets, debtors, cash), what it owes
(creditors, loans), and its reserves.

WHY it matters — and why NOAC does NOT already cover it: NOAC publishes ~125 operational/ratio
indicators (collection rates, cost-per-unit, etc.) but ZERO balance-sheet line items (verified:
no loan/asset/creditor/reserve/debt series in noac_indicators_long). So the whole financial
POSITION of a council — how much it owns, owes and has borrowed — is uncaptured until here.

Emitted LONG (one row per council, year, item) — the balance sheet is a labelled vertical list,
so a tidy long fact lets the view pivot/sum without a wide, sparse schema. `item` is a canonical
key; `section` groups them (fixed_assets / assets / current_assets / current_liabilities /
long_term_creditors / net). Each value is the printed figure for that (year, line).

GRAIN — every value here is a STOCK (a position at a point in time), NOT a flow. NEVER sum or
reconcile any of it with AFS revenue/capital expenditure, procurement-awarded, payments or
budget euros; and NEVER sum one item across years for one council (that double-counts a carried
balance). Loans Payable is one item (item='loans_payable') — the debt headline that motivated
the fact: Galway City stepped 51,417,617 (2021) -> 94,483,841 (2022), the ~€43m Crown Square
acquisition loan the capital account only showed as a bland division total.

Method (no reconcile gate — these are directly-PRINTED audited lines, not reconstructed sums):
  - Find the Balance Sheet page: a balance-sheet anchor ("FIXED ASSETS"/"FINANCED BY"/
    "NET ASSETS") AND a parseable Loans Payable row (rejects the Financial-Review narrative page
    that merely says "loans payable" in prose, e.g. offaly p2).
  - Read the two YEAR columns from the header ("Notes | 2022 | 2021 | € | €"). One statement
    yields TWO years (current + prior comparative), both audited.
  - Word-geometry: cluster each row; if its label maps to a known balance-sheet item, take the
    TWO RIGHTMOST numeric tokens by x as (current_year, prior_year). The leftmost small integer
    is the Note reference and is dropped by construction. Sub-total lines carry no label -> not
    captured (the view sums the labelled components instead).

Coverage: only Wexford (scanned-image AFS, no text layer) misses — the same council the revenue
fact cannot reach either.

Reuses la_afs_extract wholesale (REGISTRY, cached bronze PDFs, title_year, MIN_AFS_YEAR). Reads
the SAME cached PDFs; writes a SEPARATE parquet. The Note-7 loan-movement FLOW fact is a sibling
(la_afs_loanmovement_extract.py) that imports the geometry helpers here.

Run:
  ./.venv/Scripts/python.exe extractors/la_afs_balancesheet_extract.py
  ./.venv/Scripts/python.exe extractors/la_afs_balancesheet_extract.py --only galway_city,dublin_city
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import la_afs_extract as rev  # noqa: E402  (CACHE, REGISTRY, title_year, MIN_AFS_YEAR)

import config  # noqa: E402
from services.coverage_io import save_coverage  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_afs_balance_sheet.parquet"
OUT_COV = ROOT / "data/_meta/la_afs_balance_sheet_coverage.json"

NUMRE = re.compile(r"^\(?-?[\d,]+(?:\.\d+)?\)?\*?$")
YEARRE = re.compile(r"^(20[12]\d)$")
BS_ANCHORS = ("FIXED ASSETS", "FINANCED BY", "NET ASSETS", "CAPITALISATION")

# Canonical balance-sheet line items (ACoP-standard labels, normalised: lowercased, '&'->'and',
# whitespace-collapsed). Matched by exact-or-startswith so "net current assets / (liabilities)"
# still lands. Sub-total lines carry no label and are intentionally NOT captured — the view sums
# these labelled components. Ambiguous repeats ("finance leases" x2, bare "other") are omitted
# rather than mis-assigned, per the never-guess rule.
BS_ITEMS: dict[str, tuple[str, str]] = {
    "operational": ("fa_operational", "fixed_assets"),
    "infrastructural": ("fa_infrastructural", "fixed_assets"),
    "community": ("fa_community", "fixed_assets"),
    "non-operational": ("fa_non_operational", "fixed_assets"),
    "work in progress and preliminary expenses": ("work_in_progress", "assets"),
    "long term debtors": ("long_term_debtors", "assets"),
    "stocks": ("stocks", "current_assets"),
    "trade debtors and prepayments": ("trade_debtors_prepayments", "current_assets"),
    "bank investments": ("bank_investments", "current_assets"),
    "cash at bank": ("cash_at_bank", "current_assets"),
    "cash in transit": ("cash_in_transit", "current_assets"),
    "creditors and accruals": ("creditors_accruals", "current_liabilities"),
    "loans payable": ("loans_payable", "long_term_creditors"),
    "refundable deposits": ("refundable_deposits", "long_term_creditors"),
    "net current assets": ("net_current_assets", "net"),
}


def tonum(s: str) -> float | None:
    s = s.strip().rstrip("*")
    if s in ("-", "", "*"):
        return None
    neg = s.startswith("(")
    s = s.strip("()").replace(",", "")
    try:
        v = float(s)
    except ValueError:
        return None
    return -v if neg else v


def _norm_label(text_tokens: list[str]) -> str:
    txt = " ".join(t for t in text_tokens if not NUMRE.match(t)).lower()
    txt = txt.replace("&", "and")
    return re.sub(r"\s+", " ", txt).strip()


def _match_item(norm: str) -> tuple[str, str] | None:
    if norm in BS_ITEMS:
        return BS_ITEMS[norm]
    for key, val in BS_ITEMS.items():
        if norm.startswith(key):
            return val
    return None


def _cluster_rows(words: list[tuple], tol: float = 4.0) -> list[dict]:
    rows: list[dict] = []
    for w in sorted(words, key=lambda w: (w[1], w[0])):
        yc = (w[1] + w[3]) / 2
        for r in rows:
            if abs(r["y"] - yc) <= tol:
                r["w"].append(w)
                break
        else:
            rows.append({"y": yc, "w": [w]})
    for r in rows:
        r["w"].sort(key=lambda w: w[0])
    rows.sort(key=lambda r: r["y"])
    return rows


def _rightmost_two(row_words: list[tuple]) -> tuple[float, float] | None:
    """(current_year, prior_year) = the two RIGHTMOST numeric tokens by x on this row; the
    leftmost small integer (a Note ref) is dropped by construction. None if <2 figures."""
    nums = [((w[0] + w[2]) / 2, tonum(w[4])) for w in row_words if NUMRE.match(w[4]) and tonum(w[4]) is not None]
    if len(nums) < 2:
        return None
    nums.sort(key=lambda p: p[0])
    return nums[-2][1], nums[-1][1]


def _loans_row(page) -> tuple[float, float] | None:
    """The Loans Payable row's (current, prior) — used by the page-finder to confirm this is the
    real statement, not a narrative page."""
    for r in _cluster_rows([(w[0], w[1], w[2], w[3], w[4]) for w in page.get_text("words")]):
        if _norm_label([w[4] for w in r["w"]]).startswith("loans payable"):
            return _rightmost_two(r["w"])
    return None


def _find_balance_sheet(doc) -> int | None:
    """Page index of the Balance Sheet statement, else None. Requires a balance-sheet anchor AND
    that the Loans Payable *row* parses — rejects Financial-Review narrative pages."""
    for i in range(doc.page_count):
        u = doc[i].get_text("text").upper()
        if "LOANS PAYABLE" not in u or not any(a in u for a in BS_ANCHORS):
            continue
        if _loans_row(doc[i]) is not None:
            return i
    return None


def _header_years(page) -> tuple[int, int] | None:
    """The two year columns from the header ('Notes | 2022 | 2021 | € | €') as (current, prior)."""
    toks: list[int] = []
    for ln in page.get_text("text").splitlines():
        m = YEARRE.match(ln.strip())
        if m:
            toks.append(int(m.group(1)))
        if "FIXED ASSETS" in ln.upper():
            break
    if len(toks) >= 2 and toks[0] > toks[1]:
        return toks[0], toks[1]
    return None


def _parse_items(page) -> dict[str, tuple[str, float, float]]:
    """{item_key: (section, current_value, prior_value)} for every labelled balance-sheet line
    on the page. First match per item wins (guards the rare duplicate label)."""
    out: dict[str, tuple[str, float, float]] = {}
    for r in _cluster_rows([(w[0], w[1], w[2], w[3], w[4]) for w in page.get_text("words")]):
        hit = _match_item(_norm_label([w[4] for w in r["w"]]))
        if hit is None:
            continue
        item, section = hit
        if item in out:
            continue
        vals = _rightmost_two(r["w"])
        if vals is not None:
            out[item] = (section, vals[0], vals[1])
    return out


def _ingest_one(path: Path, slug: str, cf: dict) -> tuple[list[dict], dict]:
    """Parse ONE cached AFS PDF -> long-format balance-sheet rows (current + prior year)."""
    stat = {"council": cf["council"], "slug": slug}
    doc = fitz.open(path)
    pg = _find_balance_sheet(doc)
    if pg is None:
        doc.close()
        stat.update(status="no-balance-sheet-page", year=rev.title_year(path.stem))
        return [], stat
    yrs = _header_years(doc[pg])
    items = _parse_items(doc[pg])
    doc.close()
    if not items:
        stat.update(status="no-items", year=rev.title_year(path.stem), page=pg)
        return [], stat
    if yrs is None:
        cy = rev.title_year(path.stem)
        yrs = (cy, cy - 1) if cy else None
    if yrs is None:
        stat.update(status="no-year-header", page=pg)
        return [], stat
    cur_yr, prior_yr = yrs

    def _row(item: str, section: str, year: int, value: float, is_current: bool) -> dict:
        return {
            "council": cf["council"],
            "slug": slug,
            "entity": cf["entity"],
            "region": cf["region"],
            "year": year,
            "section": section,
            "item": item,
            "value_eur": value,
            "is_statement_year": is_current,
            "statement_year": cur_yr,
            "source_file_url": cf["landing"][0],
            "source_page_number": pg,
            "parse_method": "geom",
            "value_kind": "balance_sheet_stock",
            "scope": "single-LA Balance Sheet / Statement of Financial Position (position @ 31 Dec)",
            "source": "Local Authority audited AFS (own website), Balance Sheet",
        }

    rows: list[dict] = []
    for item, (section, cur_val, prior_val) in items.items():
        if cur_val is not None and cur_yr >= rev.MIN_AFS_YEAR:
            rows.append(_row(item, section, cur_yr, cur_val, True))
        if prior_val is not None and prior_yr >= rev.MIN_AFS_YEAR:
            rows.append(_row(item, section, prior_yr, prior_val, False))
    stat.update(status="ok", year=cur_yr, page=pg, items=len(items), rows=len(rows))
    return rows, stat


def ingest(slug: str, cf: dict) -> tuple[list[dict], dict]:
    """Parse EVERY cached AFS for this council -> multi-year long balance sheet. Dedup by
    (year, item) preferring the STATEMENT-year read over a later statement's prior-year column."""
    pdfs = sorted((rev.CACHE / slug).glob("*.pdf"), reverse=True)  # newest first
    stat = {"council": cf["council"], "slug": slug}
    if not pdfs:
        stat["status"] = "no-cached-pdf"
        return [], stat
    by_key: dict[tuple[int, str], dict] = {}
    last_status = "no-balance-sheet-page"
    latest_stat: dict | None = None
    for path in pdfs:
        if not (path.stem.isdigit() and int(path.stem) >= rev.MIN_AFS_YEAR):
            continue
        rows, sub = _ingest_one(path, slug, cf)
        last_status = sub.get("status", last_status)
        if rows and latest_stat is None:
            latest_stat = sub
        for r in rows:
            key = (r["year"], r["item"])
            prev = by_key.get(key)
            if prev is None or (r["is_statement_year"] and not prev["is_statement_year"]):
                by_key[key] = r
    if not by_key:
        stat.update(status=last_status)
        return [], stat
    rows = sorted(by_key.values(), key=lambda r: (r["year"], r["item"]))
    years = sorted({r["year"] for r in rows})
    loans = next(
        (r["value_eur"] for r in rows if r["item"] == "loans_payable" and r["year"] == max(years)),
        None,
    )
    stat.update(
        status="ok",
        year=latest_stat["year"] if latest_stat else max(years),
        page=latest_stat["page"] if latest_stat else None,
        n_years=len(years),
        years=years,
        n_items=len({r["item"] for r in rows}),
        loans_payable_latest=round(loans, 0) if loans is not None else None,
    )
    return rows, stat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated slugs")
    args = ap.parse_args()
    only = {s.strip() for s in args.only.split(",")} if args.only else None

    print("=" * 74)
    print("PER-LA AFS — BALANCE SHEET (full Statement of Financial Position, long)")
    print("=" * 74)
    all_rows: list[dict] = []
    stats: list[dict] = []
    for cf in rev.REGISTRY:
        slug = cf["slug"]
        if only and slug not in only:
            continue
        if not (rev.CACHE / slug).exists():
            continue
        rows, stat = ingest(slug, cf)
        stats.append(stat)
        all_rows.extend(rows)
        if stat["status"] == "ok":
            lp = stat["loans_payable_latest"]
            print(
                f"  {cf['council']:<15} ok  latest={stat['year']}  pg={stat['page']:<3} "
                f"items={stat['n_items']}  yrs={stat['n_years']}  "
                f"loansPayable={(lp / 1e6 if lp else 0):>7.1f}m"
            )
        else:
            print(f"  {cf['council']:<15} {stat['status']}")

    if not all_rows:
        print("\nno rows — nothing written")
        return

    df = pl.DataFrame(all_rows, infer_schema_length=None).sort(["council", "year", "section", "item"])
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_PARQUET)

    ok = [s for s in stats if s["status"] == "ok"]
    cov = {
        "councils_attempted": len(stats),
        "councils_with_rows": len(ok),
        "rows": len(all_rows),
        "distinct_items": sorted({r["item"] for r in all_rows}),
        "by_council": stats,
        "value_kind": "balance_sheet_stock",
        "scope": "per-LA audited AFS Balance Sheet — financial position line items (long) at 31 December",
        "generated_at": datetime.now(UTC).isoformat(),
        "caveat": (
            "STOCK grain — positions at a point in time, NOT flows. NEVER sum or reconcile with "
            "any AFS expenditure (la_afs_divisions / la_afs_capital_divisions), procurement-awarded, "
            "payments or budget euros, and NEVER sum one item across years for one council (double-"
            "counts a carried balance). Read directly from the printed Balance Sheet (no reconcile "
            "gate). One value per (council, year, item); the prior-year comparative column is kept "
            "only where no statement-year read exists for that year. Sub-total lines are not "
            "captured — sum the labelled components (e.g. fixed assets = the fa_* items)."
        ),
    }
    save_coverage(cov, OUT_COV)
    print(f"\n  rows: {len(all_rows)}  councils: {len(ok)}/{len(stats)}  items: {len(cov['distinct_items'])}")
    print(f"wrote {OUT_PARQUET}")
    print(f"      {OUT_COV}")


if __name__ == "__main__":
    main()
