"""EXPERIMENTAL sandbox — catalog EVERY per-LA indicator in the NOAC 2024 report.

Complements noac_extend_extract.py (which curates 11 keepers). This is the completeness
pass: sweep the whole 240-page PDF, grab every table that has a per-LA results grid, label
its columns from the page headers, parse values, and emit:
  * one parquet per indicator table (raw, generically parsed)
  * a master catalog (catalog_all.json) of every indicator with its source page + columns,
    so we can SEE the full menu before deciding what to surface to a user.

NOTHING here touches gold or the pipeline. find_tables, no OCR.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
PDF_URL = "https://cdn.noac.ie/wp-content/uploads/2025/09/NOAC-Local-Authority-Performance-Indicator-Report-2024.pdf"
SRC = ROOT / "doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf"
OUT = ROOT / "pipeline_sandbox/noac_accountability/catalog"
OUT.mkdir(exist_ok=True)

_LA_STEMS = (
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal", "Dublin City", "DLR",
    "Dun Laoghaire", "Fingal", "Galway City", "Galway County", "Kerry", "Kildare", "Kilkenny",
    "Laois", "Leitrim", "Limerick", "Longford", "Louth", "Mayo", "Meath", "Monaghan", "Offaly",
    "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford", "Westmeath", "Wexford", "Wicklow",
)
FAMILY = {"H": "Housing", "R": "Roads", "W": "Water", "E": "Environment", "P": "Planning",
          "F": "Fire", "L": "Library", "Y": "Youth/Community", "C": "Corporate", "M": "Finance"}
CODE_RE = re.compile(r"\b([HRWEPFLYCM])\s?(\d{1,2})\b")


def _fold(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _clean(s) -> str:
    return re.sub(r"\s+", " ", ("" if s is None else str(s)).replace("\n", " ")).strip()


def _is_la(name: str) -> bool:
    n = _fold(re.sub(r"[*†‡\s]+$", "", _clean(name)))
    return bool(n) and any(n.startswith(_fold(s)) for s in _LA_STEMS)


def _num(s: str):
    s = _clean(s)
    if not s or s in {"-", "n/a", "N/A", "*"}:
        return None
    neg = "(" in s or "-" in s
    digits = re.sub(r"[^\d.]", "", s)
    if not digits or digits == ".":
        return None
    try:
        return -float(digits) if neg else float(digits)
    except ValueError:
        return None


def _slug(label: str, idx: int) -> str:
    s = _fold(_clean(label)).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return (s[:48] or f"col{idx}")


def main() -> None:
    doc = fitz.open(SRC)
    catalog = []
    for p in range(20, doc.page_count):
        page = doc[p]
        txt = page.get_text()
        try:
            tables = page.find_tables().tables
        except Exception:
            continue
        codes = [a + b for a, b in CODE_RE.findall(txt[:800])]
        code = max(set(codes), key=codes.count) if codes else ""
        family = FAMILY.get(code[:1], "") if code else ""
        for ti, t in enumerate(tables):
            rows = [[_clean(c) for c in r] for r in t.extract()]
            la_rows = [r for r in rows if r and _is_la(r[0])]
            if len(la_rows) < 25:
                continue
            ncol = max(len(r) for r in rows)
            # label each value column from the header row(s); fall back to row 1 if blank
            hdr = rows[0] if rows else []
            hdr2 = rows[1] if len(rows) > 1 else []
            labels, numeric_fill = [], []
            used, recs = {}, [{"la": re.sub(r"[*†‡\s]+$", "", _clean(r[0]))} for r in la_rows]
            for ci in range(1, ncol):
                raw = hdr[ci] if ci < len(hdr) and hdr[ci] else (hdr2[ci] if ci < len(hdr2) else "")
                slug = _slug(raw, ci)
                used[slug] = used.get(slug, 0) + 1
                slug = slug if used[slug] == 1 else f"{slug}_{used[slug]}"
                labels.append({"col": ci, "slug": slug, "header": raw[:120]})
                vals = [_num(r[ci]) if ci < len(r) else None for r in la_rows]
                numeric_fill.append(round(sum(v is not None for v in vals) / max(len(vals), 1) * 100))
                for rec, v in zip(recs, vals):
                    rec[slug] = v
            # write the parsed table so "all data" is literally on disk
            tag = (code or "x").lower()
            pl.DataFrame(recs).write_parquet(OUT / f"p{p-1}_{tag}_t{ti}.parquet")
            catalog.append({
                "source_pdf_url": PDF_URL,
                "deep_link": f"{PDF_URL}#page={p+1}",
                "doc_page": p, "printed_page": p - 1, "indicator_code": code, "family": family,
                "n_la": len(la_rows), "n_value_cols": ncol - 1,
                "numeric_fill_pct": numeric_fill,
                "columns": labels,
                "table_title": _clean(next((l for l in txt.splitlines() if l.strip()
                                            and "Performance Indicator Report" not in l
                                            and l.strip() != str(p - 1)), ""))[:90],
            })
    (OUT / "catalog_all.json").write_text(json.dumps(catalog, indent=2), encoding="utf-8")

    # console summary grouped by family
    print(f"{len(catalog)} per-LA indicator tables found\n")
    by_fam = {}
    for c in catalog:
        by_fam.setdefault(c["family"] or "?", []).append(c)
    for fam in ["Housing", "Roads", "Water", "Environment", "Planning", "Fire", "Library",
                "Youth/Community", "Corporate", "Finance", "?"]:
        items = by_fam.get(fam, [])
        if not items:
            continue
        print(f"=== {fam} ({len(items)} tables) ===")
        for c in items:
            cols = ", ".join(l["slug"][:26] for l in c["columns"][:4])
            more = "" if c["n_value_cols"] <= 4 else f" +{c['n_value_cols']-4}"
            print(f"  p{c['printed_page']:<3} {c['indicator_code']:<3} [{c['n_la']}LA] {cols}{more}")
    print(f"\nwrote {OUT/'catalog_all.json'}")


if __name__ == "__main__":
    main()
