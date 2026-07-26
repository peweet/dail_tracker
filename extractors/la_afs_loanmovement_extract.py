"""INGEST (sandbox): per-LA AFS NOTE 7 — Loan Movement (the borrow/repay FLOW behind the debt).

Sibling of la_afs_balancesheet_extract.py. That fact reads the Balance Sheet Loans Payable
STOCK; this one reads Note 7 "Movement in Loans Payable" — the in-year FLOW that moved the
stock: opening balance, NEW borrowings, principal repayments, early redemptions, closing.

WHY it matters: the stock says how much a council owes; the movement says whether it is
RAMPING borrowing or paying debt down. "New borrowings in 2022 = €49.7m" (Galway, the Crown
Square loan) is the discretionary-decision signal the stock alone can't show.

GRAIN — MIXED, tagged per row: opening/closing are STOCKS; borrowings/repayments/redemptions
are FLOWS (in-year). Repayments and redemptions are printed negative (outflows) and kept with
their sign. NEVER sum a flow with a stock, nor either with any spend fact.

⚠️ The Note-7 total is GROSS borrowing and legitimately differs from the Balance-Sheet Loans
Payable line, which reclassifies mortgage-related loans against Long-Term Debtors (galway_city
2022: Note-7 closing 97,852,660 vs balance-sheet 94,483,841). Do NOT reconcile the two facts
against each other. The reconcile gate here is INTERNAL: opening + borrowings + repayments +
redemptions + other == closing, checked per (council, year).

Method: reuse the balance-sheet module's word-geometry helpers. Find the Note-7 page (Loans
Payable + Borrowings + Repayment). The movement table's two RIGHTMOST columns are the total
current-year and total prior-year balances (the left columns split by lender HFA/OPW/Other),
so _rightmost_two gives (current, prior) regardless of how many lender columns collapse. One
note yields TWO years (current + prior comparative).

Run:
  ./.venv/Scripts/python.exe extractors/la_afs_loanmovement_extract.py
  ./.venv/Scripts/python.exe extractors/la_afs_loanmovement_extract.py --only galway_city
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

import la_afs_balancesheet_extract as bs  # noqa: E402  (_cluster_rows, _norm_label, _rightmost_two)
import la_afs_extract as rev  # noqa: E402  (CACHE, REGISTRY, title_year, MIN_AFS_YEAR)

import config  # noqa: E402
from services.coverage_io import save_coverage  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_afs_loan_movement.parquet"
OUT_COV = ROOT / "data/_meta/la_afs_loan_movement_coverage.json"

RECON_TOL = 1000.0  # euro — Note-7 movement rows are round thousands; allow small print rounding

# (label prefixes, item_key, is_flow) in match order. opening/closing are stocks; the rest are
# in-year flows. Opening/closing carry several label variants across councils ("Balance @ 1/1"
# vs "Opening Balance" vs "Opening Balance at 01/01/2024"); each still means the same line.
MOVE_ITEMS: list[tuple[tuple[str, ...], str, bool]] = [
    (("balance @ 1/1", "balance at 1/1", "opening balance"), "opening_balance", False),
    (("borrowings",), "borrowings", True),
    (("repayment of principal", "repayments of principal", "repayment"), "repayment_of_principal", True),
    (("early redemptions", "early redemption"), "early_redemptions", True),
    (("other adjustments", "other adjustment"), "other_adjustments", True),
    (("balance @ 31/12", "balance at 31/12", "closing balance"), "closing_balance", False),
]
_EOY = re.compile(r"31/12/(20[12]\d)")


def _find_note7(doc) -> tuple[int, dict] | None:
    """(page index, movement items) of the Note-7 movement table, else None. Validated on the
    PARSED rows (opening + closing present) rather than a header string, so councils that write
    '31 December' instead of '31/12' still land."""
    for i in range(doc.page_count):
        u = doc[i].get_text("text").upper()
        if "LOANS PAYABLE" not in u or "BORROWINGS" not in u or "REPAYMENT" not in u:
            continue
        items = _parse_movement(doc[i])
        if "opening_balance" in items and "closing_balance" in items:
            return i, items
    return None


def _note7_years(page) -> tuple[int, int] | None:
    """(current, prior) from the two '31/12/YYYY' total-column headers."""
    yrs: list[int] = []
    for m in _EOY.finditer(page.get_text("text")):
        y = int(m.group(1))
        if y not in yrs:
            yrs.append(y)
    if len(yrs) >= 2 and yrs[0] > yrs[1]:
        return yrs[0], yrs[1]
    return None


def _parse_movement(page) -> dict[str, tuple[bool, float, float]]:
    """{item_key: (is_flow, current_total, prior_total)} — first match per item wins."""
    out: dict[str, tuple[bool, float, float]] = {}
    for r in bs._cluster_rows([(w[0], w[1], w[2], w[3], w[4]) for w in page.get_text("words")]):
        norm = bs._norm_label([w[4] for w in r["w"]])
        for prefixes, item, is_flow in MOVE_ITEMS:
            if item not in out and any(norm.startswith(p) for p in prefixes):
                vals = bs._rightmost_two(r["w"])
                if vals is not None:
                    out[item] = (is_flow, vals[0], vals[1])
                break
    return out


def _reconciles(items: dict[str, tuple[bool, float, float]], idx: int) -> bool:
    """opening + borrowings + repayments + redemptions + other == closing, for column idx
    (1 = current year, 2 = prior year)."""
    if "opening_balance" not in items or "closing_balance" not in items:
        return False
    moved = sum(items[k][idx] for k in items if items[k][0])  # is_flow rows
    got = items["opening_balance"][idx] + moved
    return abs(got - items["closing_balance"][idx]) < RECON_TOL


def _ingest_one(path: Path, slug: str, cf: dict) -> tuple[list[dict], dict]:
    stat = {"council": cf["council"], "slug": slug}
    doc = fitz.open(path)
    found = _find_note7(doc)
    if found is None:
        doc.close()
        stat.update(status="no-note7-page", year=rev.title_year(path.stem))
        return [], stat
    pg, items = found
    yrs = _note7_years(doc[pg])
    doc.close()
    if yrs is None:
        cy = rev.title_year(path.stem)
        yrs = (cy, cy - 1) if cy else None
    if yrs is None:
        stat.update(status="no-year-header", page=pg)
        return [], stat
    cur_yr, prior_yr = yrs
    # RECONCILE GATE: only a year whose opening + flows == closing is trustworthy. On messy
    # reversed-header layouts (fingal/clare) a wrong column can be picked, and a wrong closing
    # balance is a false statement about a council's debt — so a non-reconciling year is dropped
    # entirely, never published with a caveat.
    recon_cy = _reconciles(items, 1)
    recon_py = _reconciles(items, 2)

    def _row(item: str, is_flow: bool, year: int, value: float, is_current: bool) -> dict:
        return {
            "council": cf["council"],
            "slug": slug,
            "entity": cf["entity"],
            "region": cf["region"],
            "year": year,
            "item": item,
            "value_eur": value,
            "is_flow": is_flow,  # True = in-year flow (borrow/repay); False = opening/closing stock
            "is_statement_year": is_current,
            "statement_year": cur_yr,
            "source_file_url": cf["landing"][0],
            "source_page_number": pg,
            "parse_method": "geom",
            "value_kind": "loan_movement",
            "scope": "single-LA AFS Note 7 — movement in Loans Payable (gross borrowing)",
            "source": "Local Authority audited AFS (own website), Note 7 Loans Payable",
        }

    rows: list[dict] = []
    for item, (is_flow, cur_val, prior_val) in items.items():
        if recon_cy and cur_val is not None and cur_yr >= rev.MIN_AFS_YEAR:
            rows.append(_row(item, is_flow, cur_yr, cur_val, True))
        if recon_py and prior_val is not None and prior_yr >= rev.MIN_AFS_YEAR:
            rows.append(_row(item, is_flow, prior_yr, prior_val, False))
    status = "ok" if rows else ("no-reconcile" if items else "no-movement-rows")
    stat.update(
        status=status,
        year=cur_yr,
        page=pg,
        items=len(items),
        reconciled_cy=recon_cy,
        borrowings=round(items.get("borrowings", (True, 0.0, 0.0))[1], 0),
    )
    return rows, stat


def ingest(slug: str, cf: dict) -> tuple[list[dict], dict]:
    pdfs = sorted((rev.CACHE / slug).glob("*.pdf"), reverse=True)
    stat = {"council": cf["council"], "slug": slug}
    if not pdfs:
        stat["status"] = "no-cached-pdf"
        return [], stat
    by_key: dict[tuple[int, str], dict] = {}
    last_status = "no-note7-page"
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
    latest_borrow = next((r["value_eur"] for r in rows if r["item"] == "borrowings" and r["year"] == max(years)), 0.0)
    stat.update(
        status="ok",
        year=latest_stat["year"] if latest_stat else max(years),
        page=latest_stat["page"] if latest_stat else None,
        n_years=len(years),  # reconciled years only — a non-reconciling year is dropped
        years=years,
        borrowings_latest=round(latest_borrow or 0, 0),
    )
    return rows, stat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated slugs")
    args = ap.parse_args()
    only = {s.strip() for s in args.only.split(",")} if args.only else None

    print("=" * 74)
    print("PER-LA AFS — NOTE 7 LOAN MOVEMENT (borrow / repay flow)")
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
            print(
                f"  {cf['council']:<15} ok  latest={stat['year']}  pg={stat['page']:<3} "
                f"reconYrs={stat['n_years']}  newBorrow(latest)={stat['borrowings_latest'] / 1e6:>6.1f}m"
            )
        else:
            print(f"  {cf['council']:<15} {stat['status']}")

    if not all_rows:
        print("\nno rows — nothing written")
        return

    df = pl.DataFrame(all_rows, infer_schema_length=None).sort(["council", "year", "item"])
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_PARQUET)

    ok = [s for s in stats if s["status"] == "ok"]
    cov = {
        "councils_attempted": len(stats),
        "councils_with_rows": len(ok),
        "rows": len(all_rows),
        "note": "reconcile-gated — only (council, year) where opening + flows == closing are kept",
        "by_council": stats,
        "value_kind": "loan_movement",
        "scope": "per-LA audited AFS Note 7 — movement in Loans Payable (gross borrowing) by year",
        "generated_at": datetime.now(UTC).isoformat(),
        "caveat": (
            "MIXED grain, tagged by is_flow: opening_balance/closing_balance are STOCKS; "
            "borrowings/repayment_of_principal/early_redemptions/other_adjustments are in-year "
            "FLOWS (repayments/redemptions negative). Reconcile gate is INTERNAL (opening + flows "
            "== closing per year); the Note-7 closing is GROSS and legitimately differs from the "
            "Balance-Sheet Loans Payable line — never reconcile the two facts. Never sum a flow "
            "with a stock, nor either with any spend fact."
        ),
    }
    save_coverage(cov, OUT_COV)
    print(f"\n  rows: {len(all_rows)}  councils (≥1 reconciled year): {len(ok)}/{len(stats)}")
    print(f"wrote {OUT_PARQUET}")
    print(f"      {OUT_COV}")


if __name__ == "__main__":
    main()
