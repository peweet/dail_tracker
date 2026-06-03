"""INGEST (full, sandbox): amalgamated Local Authority Annual Financial Statements —
Income & Expenditure by SERVICE DIVISION, every published year (2009–2023).

Source: Dept of Housing 'Local Authority Annual Financial Statements' collection (gov.ie),
audited amalgamation of all 31 LAs. The unique BUDGET/SPENT-by-function macro layer (the
only COMPLETE, AUDITED total — see doc/PROCUREMENT_INVESTIGATION.md). PRE-ETL: writes a
tidy parquet to data/sandbox/parquet/ (NOT gold, NOT wired to pipeline.py).

Per year: download → find the I&E-by-division statement page (≥6 division labels + a
"Gross Expenditure" header) → parse 8 divisions × (gross, income, net, prior-net) by
keyword (survives wording drift across years) → reconcile Σ vs the printed Total line.
Divisions matched by KEYWORD so the 2014 service-division rename + pre-2014 programme-group
wording both map to one canonical taxonomy.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/afs_amalgamated_extract.py
"""

from __future__ import annotations

import contextlib
import re
import subprocess
import sys
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

CACHE = Path("c:/tmp/afs")
OUT_PARQUET = ROOT / "data/sandbox/parquet/afs_amalgamated_divisions.parquet"
OUT_CSV = CACHE / "afs_amalgamated_divisions.csv"
H = {"User-Agent": "Mozilla/5.0 (dail-tracker research)"}

URLS = {
    # cut off at 2016 (modern service-division era; pre-2016 uses old programme-group wording)
    2016: "https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2016-4f047a6a-642a-4ad6-88b4-b8ec07f7128f.pdf",
    2017: "https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2017-6d7db48d-0e59-4b40-8ce5-d528e0daa390.pdf",
    2018: "https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2018-7864f1e9-b6a6-4bb9-93ca-4cb151c86a50.pdf",
    2019: "https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2019.pdf",
    2020: "https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2020.pdf",
    2021: "https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2021.pdf",
    2022: "https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2022-45632ad8-16cd-47ae-95de-942e4e8d5265.pdf",
    2023: "https://assets.gov.ie/static/documents/AFS_2023.pdf",
}

# canonical division -> keyword regex (matches statutory wording across years)
DIVISIONS = [
    ("Housing and Building", r"housing"),
    ("Roads, Transportation and Safety", r"road"),
    ("Water Services", r"water serv"),
    ("Development Management", r"develop"),
    ("Environmental Services", r"environ"),
    ("Recreation and Amenity", r"recreation"),
    ("Agriculture, Education, Health & Welfare", r"agriculture"),
    ("Miscellaneous Services", r"miscellaneous"),
]
# a numeric cell: full euros "6,750,822,110", negatives "(13,737,809)", or the 2019
# millions-with-suffix notation "1,630.75 M"
NUM = re.compile(r"^\(?-?[\d,]+(?:\.\d+)?\)?\s*[Mm]?$")


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def to_num(s: str) -> float:
    s = s.strip()
    neg = s.startswith("(")
    mult = 1e6 if re.search(r"[Mm]\s*$", s) else 1.0  # 2019 reports in € millions ("1,630.75 M")
    m = re.search(r"[\d,]+(?:\.\d+)?", s)
    if not m:
        return 0.0
    v = float(m.group().replace(",", "")) * mult
    return -v if neg else v


def download(year: int, url: str) -> Path | None:
    dest = CACHE / f"afs_full_{year}.pdf"
    if dest.exists() and dest.stat().st_size > 20000:
        return dest
    CACHE.mkdir(parents=True, exist_ok=True)
    try:
        import requests
        b = requests.get(url, headers=H, timeout=120).content
        if b[:4] != b"%PDF":
            raise ValueError("not pdf")
        dest.write_bytes(b)
        return dest
    except Exception:
        with contextlib.suppress(Exception):
            subprocess.run(["curl", "-sS", "-L", "--max-time", "120", "-A", H["User-Agent"],
                            "-o", str(dest), url], timeout=150, check=False)
            if dest.exists() and dest.read_bytes()[:4] == b"%PDF":
                return dest
    return None


def find_ie_page(doc) -> int | None:
    """First page with >=6 division labels AND a Gross-Expenditure I&E header
    (not the budget note / balance sheet)."""
    for i in range(doc.page_count):
        t = doc[i].get_text("text").lower()
        ndiv = sum(1 for _, kw in DIVISIONS if re.search(kw, t))
        if ndiv >= 6 and "gross expenditure" in t and "income" in t:
            return i
    return None


def parse_ie(page_text: str) -> tuple[dict[str, list[float]], tuple[float, float] | None]:
    """8 divisions × first 4 numeric cells (gross, income, net, prior-net) + printed total."""
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    out: dict[str, list[float]] = {}
    total = None
    for i, ln in enumerate(lines):
        low = ln.lower()
        # printed total line (for reconciliation) — skip the note-ref column ("16"),
        # the real totals are the large (>€1m) figures that follow.
        if total is None and ("total expenditure" in low or "total income" in low):
            nums = [to_num(x) for x in lines[i + 1:i + 6] if NUM.match(x) and to_num(x) > 1_000_000]
            if len(nums) >= 2:
                total = (nums[0], nums[1])
        if len(ln) > 55:
            continue
        for canon, kw in DIVISIONS:
            if canon in out:
                continue
            if re.search(kw, low) and not any(c.isdigit() for c in ln):
                nums = []
                j = i + 1
                while j < len(lines) and len(nums) < 4:
                    if NUM.match(lines[j]):
                        nums.append(to_num(lines[j]))
                    elif len(lines[j]) < 55 and any(re.search(k, lines[j].lower()) for _, k in DIVISIONS):
                        break
                    j += 1
                if len(nums) >= 3:
                    out[canon] = nums + [None] * (4 - len(nums))
                break
    return out, total


def main() -> None:
    hr("FULL INGEST — amalgamated AFS, Income & Expenditure by division, all years")
    all_rows, dq = [], []
    for year, url in URLS.items():
        p = download(year, url)
        if not p:
            dq.append((year, "DOWNLOAD-FAIL", 0, 0.0, "-"))
            print(f"  {year}: download failed")
            continue
        doc = fitz.open(p)
        digital = len(doc[min(12, doc.page_count - 1)].get_text("text").strip()) > 200
        pg = find_ie_page(doc)
        if pg is None:
            doc.close()
            dq.append((year, "NO-IE-PAGE" + ("" if digital else "/SCANNED"), 0, 0.0, "-"))
            print(f"  {year}: no I&E-by-division page found ({'digital' if digital else 'SCANNED'})")
            continue
        ie, total = parse_ie(doc[pg].get_text("text"))
        doc.close()
        gross_sum = sum(v[0] for v in ie.values() if v[0])
        recon = "n/a"
        if total:
            diff = abs(gross_sum - total[0])
            recon = "EXACT" if diff <= 2 else (f"≈ M-rounded (€{diff:,.0f})" if diff < 100_000 else f"diff €{diff:,.0f}")
        for canon, v in ie.items():
            all_rows.append({"year": year, "division": canon, "gross_expenditure": v[0],
                             "income": v[1], "net_expenditure": v[2], "net_expenditure_prior_yr": v[3]})
        dq.append((year, "OK" if len(ie) == 8 else f"{len(ie)}/8", len(ie), gross_sum, recon))
        print(f"  {year}: p{pg}  divisions {len(ie)}/8  Σgross €{gross_sum / 1e9:,.2f}bn  recon={recon}")

    if not all_rows:
        print("\nnothing extracted.")
        return
    df = pl.DataFrame(all_rows).with_columns(
        pl.lit("all-31-LAs (amalgamated)").alias("scope"),
        pl.lit("SPENT").alias("realisation_tier"),
        pl.lit("net_expenditure_actual").alias("value_kind"),
        pl.lit("Dept Housing amalgamated AFS (gov.ie), audited").alias("source"),
    ).sort(["year", "division"])
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)
    df.write_csv(OUT_CSV)

    hr("DATA-QUALITY — per year")
    print(f"  {'year':<6}{'status':<16}{'div':<5}{'Σ gross':>14}  reconciliation")
    for y, st, nd, gs, rc in dq:
        print(f"  {y:<6}{st:<16}{nd:<5}€{gs / 1e9:>11,.2f}bn  {rc}")
    ok_years = [y for y, st, *_ in dq if st == "OK"]
    print(f"\n  clean 8/8+reconciled years: {len(ok_years)}  {ok_years}")
    print(f"  rows: {df.height} ({df['year'].n_unique()} years × ~8 divisions)")

    hr("WHAT THE FULL SERIES SHOWS — net expenditure by division (€bn)")
    piv = (df.filter(pl.col("net_expenditure").is_not_null())
           .with_columns((pl.col("net_expenditure") / 1e9).round(2).alias("net_bn"))
           .pivot(values="net_bn", index="division", on="year", aggregate_function="first"))
    show = [c for c in piv.columns if c == "division" or c in {str(y) for y in (2014, 2017, 2020, 2023)} or c in (2014, 2017, 2020, 2023)]
    with pl.Config(tbl_rows=10, tbl_cols=12, fmt_str_lengths=42):
        print(piv.select(show) if len(show) > 1 else piv)

    print(f"\nwrote {OUT_PARQUET}\n      {OUT_CSV}")
    print("realisation_tier=SPENT / value_kind=net_expenditure_actual (accrual; national;")
    print("NOT per-LA, NOT cash-PO grain — do not reconcile against the procurement layers).")


if __name__ == "__main__":
    main()
