"""PHASE-4 BESPOKE PARSERS (PRE-ETL): HSE + Tusla PO/payment PDFs, with a DQ check.

The generic header-anchored reader (sample_extract_procurement_pdf.py) hit its ceiling on
these two: HSE fuses amount+quarter+date, Tusla's left-aligned vendor bleeds into the
right-aligned amount column. Per the LA lesson ("each publisher = bespoke column order"),
the fix is a small per-publisher column-x SPEC measured from the actual word geometry
(see the coords dump in the build notes), not a smarter generic heuristic.

Layouts (x = word centre):
  HSE   VENDOR(<180) | DOC REF(180-270) | GL DESC(270-450) | € AMOUNT(450-515) | Qx YYYY(>515)
  TUSLA YEAR(<110) | QTR(110-160) | DATE(160-290) | AMOUNT(290-375) | VENDOR(375-570) | DESC(>570)

Both emit the same normalised schema and then run a data-quality check (plan §6/Phase-3
battery + supplier-name quality + period coverage + top suppliers). Nothing is written to
gold or wired into pipeline.py — output is c:/tmp/procurement_publishers/.

Run:  ./.venv/Scripts/python.exe extractors/procurement_hse_tusla_parser.py
"""

from __future__ import annotations

import contextlib
import json
import re
import sys
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from sample_extract_procurement_pdf import (  # noqa: E402
    MONEY_RE, TMP, cluster_word_rows, fetch, to_eur,
)

PROBE = TMP / "procurement_publishers_probe.json"
OUT_DQ = TMP / "hse_tusla_dq_report.json"

COMPANY_SUFFIX = re.compile(
    r"\b(ltd|limited|dac|plc|clg|llp|teo|teoranta|t/a|uc|inc|llc|gmbh|nv|company|co\b|"
    r"group|services|solutions|consult|engineer|partners|associates|holdings|university|"
    r"college|council|hse|board|institute|ireland|hospital|pharma|medical|systems|&)\b", re.I)
TOTAL_RE = re.compile(r"^\s*(grand\s+)?total\b|^\s*sum\b|^\s*subtotal\b", re.I)
LEAD_DIGITS = re.compile(r"^(?:\d{3,}\s+)+")
TRAIL_DIGITS = re.compile(r"\s+\d{4,}$")


def cols_by_xcuts(words: list, cuts: list[float]) -> list[str]:
    """Bucket a row's words into columns by word-centre x-cuts (len(cuts)+1 buckets)."""
    buckets: list[list[str]] = [[] for _ in range(len(cuts) + 1)]
    for w in sorted(words, key=lambda w: w[0]):
        c = (w[0] + w[2]) / 2
        idx = 0
        while idx < len(cuts) and c > cuts[idx]:
            idx += 1
        buckets[idx].append(w[4])
    return [" ".join(b).strip(" -:|") for b in buckets]


def norm_name(s: str) -> str:
    """Light supplier normalisation for distinct counts / dq (NOT the CRO join key)."""
    s = LEAD_DIGITS.sub("", s or "")
    s = TRAIL_DIGITS.sub("", s)
    return re.sub(r"\s+", " ", s).strip(" .,").upper()


def url_for(pid: str) -> str:
    d = json.loads(PROBE.read_text(encoding="utf-8"))
    return next(p["sample"] for p in d["publishers"] if p["publisher_id"] == pid)


# ---------------------------------------------------------------- HSE -----------------
def hse_row(cells: list[str], page: int, idx: int) -> dict | None:
    vendor, doc_ref, desc, amount_s, period_s = (cells + [""] * 5)[:5]
    amt = to_eur(amount_s)
    if amt is None:
        return None
    m = re.search(r"Q([1-4])\s*(\d{4})", period_s.replace(" ", ""))
    quarter = f"Q{m.group(1)}" if m else None
    year = int(m.group(2)) if m else None
    return {
        "publisher_id": "ie_hse", "year": year, "quarter": quarter,
        "supplier_raw": vendor, "supplier_norm": norm_name(vendor),
        "amount_eur": amt, "amount_semantics": "payment_incl_vat",
        "description": desc, "doc_ref": doc_ref.strip(),
        "source_page": page + 1, "source_row": idx,
    }


# --------------------------------------------------------------- TUSLA ----------------
def tusla_row(cells: list[str], page: int, idx: int) -> dict | None:
    year_s, qtr_s, date_s, amount_s, vendor, desc = (cells + [""] * 6)[:6]
    amt = to_eur(amount_s)
    if amt is None:
        return None
    year = int(year_s) if re.fullmatch(r"\d{4}", year_s.strip()) else None
    quarter = qtr_s.strip() if re.fullmatch(r"Q[1-4]", qtr_s.strip()) else None
    return {
        "publisher_id": "ie_tusla", "year": year, "quarter": quarter,
        "supplier_raw": vendor, "supplier_norm": norm_name(vendor),
        "amount_eur": amt, "amount_semantics": "invoice_payment",
        "description": desc, "doc_ref": date_s.strip(),
        "source_page": page + 1, "source_row": idx,
    }


SPECS = {
    "ie_hse": {"cuts": [180, 270, 450, 515], "build": hse_row, "name": "HSE"},
    "ie_tusla": {"cuts": [110, 160, 290, 375, 570], "build": tusla_row, "name": "Tusla"},
}


def parse(pid: str) -> pl.DataFrame:
    spec = SPECS[pid]
    b = fetch(url_for(pid))
    doc = fitz.open(stream=b, filetype="pdf")
    rows: list[dict] = []
    for pi in range(doc.page_count):
        for wrow in cluster_word_rows(doc[pi]):
            if not any(MONEY_RE.search(w[4]) for w in wrow):
                continue
            rec = spec["build"](cols_by_xcuts(wrow, spec["cuts"]), pi, len(rows))
            if rec:
                rows.append(rec)
    doc.close()
    return pl.DataFrame(rows)


# --------------------------------------------------------------- DQ -------------------
def dq(df: pl.DataFrame, name: str) -> dict:
    n = df.height
    amt = df["amount_eur"]
    sup = df["supplier_raw"]
    total = float(amt.sum())
    biggest = float(amt.max())
    # supplier-name quality
    empty_sup = int((sup.str.strip_chars().str.len_chars() == 0).sum())
    lead_digit = int(sup.str.contains(r"^\d{3,}\s").sum())
    trail_digit = int(sup.str.contains(r"\s\d{4,}$").sum())
    very_short = int((df["supplier_norm"].str.len_chars() < 3).sum())
    total_like = int(sup.str.contains(r"(?i)^\s*(grand\s+)?total\b|^\s*sum\b").sum())
    # personal-name risk: company-suffix-free, 1-3 words (over-counts; conservative)
    pdf = df.with_columns(
        pl.col("supplier_norm").map_elements(
            lambda s: bool(s) and not COMPANY_SUFFIX.search(s) and 1 <= len(s.split()) <= 3,
            return_dtype=pl.Boolean).alias("indiv"))
    person_risk = round(float(pdf["indiv"].mean()), 3) if n else None
    # top suppliers + concentration
    top = (df.group_by("supplier_norm").agg(pl.col("amount_eur").sum().alias("eur"),
                                            pl.len().alias("rows"))
           .sort("eur", descending=True).head(10))
    # dedup on CONTENT (not source_row/page, which are unique by construction). Identical
    # content rows can still be legitimately distinct invoices (same vendor/amount/date),
    # so this is a flag to inspect, not an error count.
    content = ["year", "quarter", "supplier_raw", "amount_eur", "description", "doc_ref"]
    dups = n - df.select(content).unique().height
    sum_excl_total = round(float(df.filter(~pl.col("supplier_raw").str.contains(
        r"(?i)^\s*(grand\s+)?total\b|^\s*sum\b"))["amount_eur"].sum()), 2)
    period_cov = sorted({f"{r['year']}{r['quarter']}" for r in df.iter_rows(named=True)
                         if r["year"] and r["quarter"]})
    report = {
        "publisher": name, "rows": n,
        "distinct_suppliers_norm": int(df["supplier_norm"].n_unique()),
        "amount_sum_eur": round(total, 2), "amount_sum_excl_total_rows_eur": sum_excl_total,
        "amount_max_eur": round(biggest, 2),
        "amount_mean_eur": round(float(amt.mean()), 2),
        "negatives": int((amt < 0).sum()), "zeros": int((amt == 0).sum()),
        "gt_10m_rows": int((amt > 1e7).sum()),
        "largest_share_of_total": round(biggest / total, 3) if total else None,
        "outlier_warning": bool(total and biggest / total > 0.5),
        "missing_supplier": empty_sup, "supplier_leading_digits": lead_digit,
        "supplier_trailing_digits": trail_digit, "supplier_very_short": very_short,
        "total_like_rows": total_like, "duplicate_rows": dups,
        "personal_name_risk_frac": person_risk,
        "period_coverage": period_cov,
        "top_suppliers": [{"supplier": r["supplier_norm"], "eur": round(r["eur"], 2),
                           "rows": r["rows"]} for r in top.iter_rows(named=True)],
    }
    # print
    print(f"\n{'=' * 74}\nDQ — {name}\n{'=' * 74}")
    print(f"  rows={n:,}  distinct suppliers={report['distinct_suppliers_norm']:,}  "
          f"periods={period_cov}")
    print(f"  amount: sum=€{total:,.0f}  (excl total-rows €{sum_excl_total:,.0f})  "
          f"max=€{biggest:,.0f}  mean=€{report['amount_mean_eur']:,.0f}")
    print(f"  amount flags: neg={report['negatives']} zero={report['zeros']} "
          f">10m={report['gt_10m_rows']}  largest_share={report['largest_share_of_total']:.1%}"
          + ("  !! OUTLIER" if report["outlier_warning"] else ""))
    print(f"  supplier quality: empty={empty_sup} lead_digits={lead_digit} "
          f"trail_digits={trail_digit} very_short={very_short} total_rows={total_like}")
    print(f"  duplicate full rows={dups}  personal_name_risk={person_risk}")
    print("  top 5 suppliers by €:")
    for r in report["top_suppliers"][:5]:
        print(f"    €{r['eur']:>16,.0f}  ({r['rows']:>4} rows)  {r['supplier'][:48]}")
    return report


def main() -> None:
    reports = {}
    for pid in SPECS:
        df = parse(pid)
        reports[pid] = dq(df, SPECS[pid]["name"])
        # show a few parsed rows so the extraction is eyeball-checkable
        print("  5 sample parsed rows:")
        for r in df.head(5).iter_rows(named=True):
            print(f"    {str(r['year'])+' '+str(r['quarter']):<9} | €{r['amount_eur']:>12,.2f} | "
                  f"{r['supplier_raw'][:34]:<34} | {r['description'][:24]}")
    OUT_DQ.write_text(json.dumps(reports, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {OUT_DQ}")
    print("PRE-ETL. Bespoke per-publisher parsers; not wired to pipeline.py.")


if __name__ == "__main__":
    main()
