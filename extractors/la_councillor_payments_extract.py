"""INGEST: s.142 councillor payments — ACTUAL money paid to named councillors, from the
councils that publish their statutory s.142 register as OPEN DATA (CC-BY CSV on data.gov.ie).

Local Government Act 2001 s.142 obliges every council to keep a public register of payments
to elected members (representational payment, allowances, travel, conference, chair/mayor
allowances). Only a handful publish it machine-readably:
  - South Dublin CC — quarterly ArcGIS CSVs, 2022-Q1 → 2025-Q4 (16 datasets; includes the
    representational payment and meeting attendance counts)
  - Dublin City Council — monthly CSV per year (2024; expenses/allowances only)
The other councils publish PDFs/HTML or nothing — HARD SCOPE CAP per the council-targeting
assessment: structured open-data publishers only, never 31 bespoke parsers. Coverage is stated
honestly in the UI ("published as open data by N of 31 councils").

Output: git-tracked data/_meta/la_councillor_payments.csv, LONG form —
(local_authority, councillor, year, period, category, value, unit) with unit ∈ {EUR, meetings}.
Councillor names keep the printed form when not resolvable (same keep-as-printed rule as the
named-votes fact); category labels are normalised via token rules so header drift across
quarters doesn't fork the taxonomy.

Run:  ./.venv/Scripts/python.exe extractors/la_councillor_payments_extract.py
"""

from __future__ import annotations

import contextlib
import csv
import io
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

OUT_CSV = ROOT / "data/_meta/la_councillor_payments.csv"
OUT_COV = ROOT / "data/_meta/la_councillor_payments_coverage.json"
CKAN = "https://data.gov.ie/api/3/action/package_show?id="
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# SDCC quarterly packages (naming convention changed 2025); DCC one package, resource per year.
SDCC_PACKAGES = [
    *(f"s142-register-q{q}-{y}-sdcc1" for y in (2022, 2023) for q in (1, 2, 3, 4)),
    *(f"s142-register-q{q}-2024-sdcc" for q in (1, 2, 3, 4)),
    *(f"councillor-allowance-and-expenses-q{q}-2025-sdcc" for q in (1, 2, 3, 4)),
]
DCC_PACKAGE = "councillor-allowance-and-expenses-dcc"

# header-token → canonical category. Matched on the folded header (lowercase, alnum runs);
# FIRST hit wins, so more specific tokens come first. unit=meetings for attendance counts.
CATEGORY_RULES: list[tuple[str, str, str]] = [
    ("meetings to attend", "meetings_to_attend", "meetings"),
    ("meetings attended", "meetings_attended", "meetings"),
    ("rep payment", "representational_payment", "EUR"),  # matches Rep_Payments_Q4 AND Rep_Payment_Q1
    ("allowance for attendanc", "attendance_allowance", "EUR"),
    ("local representational", "local_representation_allowance", "EUR"),
    ("spc chair", "spc_chair_allowance", "EUR"),
    ("deputy lord mayor", "deputy_mayor_allowance", "EUR"),
    ("deputy mayor", "deputy_mayor_allowance", "EUR"),
    ("mayor allowance", "mayor_allowance", "EUR"),
    ("subistence", "travel_subsistence", "EUR"),  # DCC's own spelling
    ("subsistence", "travel_subsistence", "EUR"),
    ("petty cash", "petty_cash", "EUR"),
    ("vouched expenses", "vouched_expenses", "EUR"),
    ("training", "training", "EUR"),
    ("conference", "conferences", "EUR"),
    ("international travel", "international_travel", "EUR"),
    ("security", "security_allowance", "EUR"),
    ("deduction", "deductions", "EUR"),
    ("total", "total_payment", "EUR"),
]
SKIP_HEADERS = ("objectid", "fid", "month", "councillor")  # ArcGIS row-id columns are not money


def fetch(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": UA})
    return urlopen(req, timeout=90).read()


def csv_resource_urls(package: str) -> list[tuple[str, str]]:
    """(resource_name, url) for every CSV resource of a CKAN package."""
    d = json.loads(fetch(CKAN + package))
    return [
        (r.get("name") or r.get("url", ""), r["url"])
        for r in d["result"]["resources"]
        if str(r.get("format", "")).upper() == "CSV"
    ]


def fold_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", h.lower()).strip()


def categorise(header: str) -> tuple[str, str] | None:
    f = fold_header(header)
    if f in SKIP_HEADERS or not f:
        return None
    for token, canon, unit in CATEGORY_RULES:
        if token in f:
            return canon, unit
    return re.sub(r"\s+", "_", f), "EUR"  # unknown money column — keep, slugified (flagged in coverage)


def to_val(cell: str) -> float | None:
    s = str(cell).strip().replace("€", "").replace(",", "").replace(" ", "")
    if not s or s in ("-", "0.00-"):
        return None
    neg = s.startswith("(") and s.endswith(")")
    with contextlib.suppress(ValueError):
        v = float(s.strip("()"))
        return -v if neg else v
    return None


def clean_name(name: str) -> str:
    n = re.sub(r"^\s*(Councillor|Cllr\.?|An tArdmh[eé]ara)\s+", "", str(name).strip(), flags=re.I)
    return re.sub(r"\s+", " ", n).strip()


def parse_csv_bytes(raw: bytes) -> list[dict]:
    text = raw.decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(io.StringIO(text)))


def ingest_sdcc(rows_out: list[dict], cov: list[dict]) -> None:
    for pkg in SDCC_PACKAGES:
        m = re.search(r"q(\d)-(20\d{2})", pkg)
        quarter, year = int(m.group(1)), int(m.group(2))
        try:
            urls = csv_resource_urls(pkg)
        except Exception as exc:  # noqa: BLE001 — a missing quarter must not kill the run
            cov.append({"source": pkg, "status": f"package-fetch-fail: {exc}"})
            continue
        if not urls:
            cov.append({"source": pkg, "status": "no-csv-resource"})
            continue
        n0 = len(rows_out)
        unknown: set[str] = set()
        for _name, url in urls[:1]:  # one CSV per SDCC package
            for rec in parse_csv_bytes(fetch(url)):
                who = clean_name(rec.get("Councillors") or rec.get("﻿Councillors") or "")
                if not who:
                    continue
                for h, cell in rec.items():
                    cat_unit = categorise(h or "")
                    if cat_unit is None:
                        continue
                    v = to_val(cell)
                    if v is None:
                        continue
                    cat, unit = cat_unit
                    if cat not in {c for _t, c, _u in CATEGORY_RULES}:
                        unknown.add(cat)
                    rows_out.append(
                        {
                            "local_authority": "South Dublin",
                            "councillor": who,
                            "year": year,
                            "period": f"{year}-Q{quarter}",
                            "category": cat,
                            "value": round(v, 2),
                            "unit": unit,
                            "source_url": url,
                        }
                    )
        cov.append(
            {"source": pkg, "status": "ok", "rows": len(rows_out) - n0, "unknown_categories": sorted(unknown)}
        )


def ingest_dcc(rows_out: list[dict], cov: list[dict]) -> None:
    try:
        urls = csv_resource_urls(DCC_PACKAGE)
    except Exception as exc:  # noqa: BLE001
        cov.append({"source": DCC_PACKAGE, "status": f"package-fetch-fail: {exc}"})
        return
    for name, url in urls:
        ym = re.search(r"(20\d{2})", f"{name} {url}")
        year = int(ym.group(1)) if ym else 0
        n0 = len(rows_out)
        unknown: set[str] = set()
        for rec in parse_csv_bytes(fetch(url)):
            who = clean_name(rec.get("Councillor") or "")
            month = str(rec.get("Month") or "").strip()
            if not who or not month:
                continue
            for h, cell in rec.items():
                cat_unit = categorise(h or "")
                if cat_unit is None:
                    continue
                v = to_val(cell)
                if v is None:
                    continue
                cat, unit = cat_unit
                if cat not in {c for _t, c, _u in CATEGORY_RULES}:
                    unknown.add(cat)
                rows_out.append(
                    {
                        "local_authority": "Dublin City",
                        "councillor": who,
                        "year": year,
                        "period": f"{year}-{month[:3]}",
                        "category": cat,
                        "value": round(v, 2),
                        "unit": unit,
                        "source_url": url,
                    }
                )
        cov.append(
            {"source": f"{DCC_PACKAGE}:{name}", "status": "ok", "rows": len(rows_out) - n0,
             "unknown_categories": sorted(unknown)}
        )


def main() -> None:
    rows: list[dict] = []
    cov: list[dict] = []
    ingest_sdcc(rows, cov)
    ingest_dcc(rows, cov)
    if not rows:
        print("no rows extracted — refusing to overwrite the fact")
        return
    rows.sort(key=lambda r: (r["local_authority"], r["councillor"], r["period"], r["category"]))
    fields = ["local_authority", "councillor", "year", "period", "category", "value", "unit", "source_url"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    councils = sorted({r["local_authority"] for r in rows})
    years = sorted({r["year"] for r in rows})
    total_eur = round(sum(r["value"] for r in rows if r["unit"] == "EUR" and r["category"] == "total_payment"))
    OUT_COV.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "grain": "s.142 payments to elected members, LONG (council, councillor, period, category)",
                "scope_cap": "structured open-data publishers ONLY (assessment rule — never 31 bespoke parsers)",
                "councils": councils,
                "years": years,
                "rows": len(rows),
                "sum_total_payment_eur": total_eur,
                "by_source": cov,
                "caveat": "Actual register amounts as published by each council; categories vary by "
                "publisher. NOT comparable to the national rate schedule line-by-line; never mix with "
                "procurement/AFS/budget money grains.",
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"  rows: {len(rows)} | councils: {councils} | years: {years}")
    print(f"  Σ total_payment: €{total_eur:,}")
    print(f"  wrote {OUT_CSV}\n        {OUT_COV}")


if __name__ == "__main__":
    main()
