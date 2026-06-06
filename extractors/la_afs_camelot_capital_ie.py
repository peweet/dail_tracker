"""Camelot-based per-LA AFS CAPITAL-account extractor (runs in the ISOLATED camelot venv).

Counterpart to la_afs_camelot_ie.py, for the "Analysis of Expenditure & Income on Capital
Account" appendix on councils whose layout the main-venv word-geometry parser can't reconcile
(note-reference column shifts indices, label-after-data ordering, no clean TOTAL row). camelot's
structured cell grid handles these. Per council: pypdf finds candidate pages (>=5 division
keywords + 'capital' + 'expenditure'); camelot reads them; a multi-row header is joined per
column to NAME the columns; the Expenditure + Total-Income columns are picked by name; the
TOTAL row reconciles. Emits reconciling councils to data/_meta/la_afs_capital_camelot_rows.json
for the main-venv capital extractor (la_afs_capital_extract.merge_camelot) to merge.

Build the isolated venv (one-off, paths are examples — same venv as la_afs_camelot_ie.py):
    uv venv c:/tmp/afs_camelot_venv --python <64-bit python3.12>
    uv pip install --python c:/tmp/afs_camelot_venv "camelot-py[base]" pypdf
Run standalone:  <venv>/python.exe extractors/la_afs_camelot_capital_ie.py monaghan kildare …
"""
from __future__ import annotations

import contextlib
import json
import re
import sys
from pathlib import Path

import camelot
from pypdf import PdfReader

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

# repo-relative paths (portable — resurrectable on any clone), derived from this file's location
ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze" / "pdfs" / "la_afs"
OUT = ROOT / "data" / "_meta" / "la_afs_capital_camelot_rows.json"

DIVISIONS = [
    ("Housing and Building", "housing"),
    ("Roads, Transportation and Safety", "road"),
    ("Water Services", "water serv"),
    ("Development Management", "develop"),
    ("Environmental Services", "environ"),
    ("Recreation and Amenity", "recreation"),
    ("Agriculture, Education, Health & Welfare", "agricult"),
    ("Miscellaneous Services", "miscellaneous"),
]
COUNCILS = sys.argv[1:] or ["monaghan", "kildare", "clare", "fingal", "dlr", "cork_county"]


def num(s: str) -> float | None:
    s = str(s).strip()
    if s in ("-", "", "*", "nan"):
        return None
    neg = s.startswith("(") and ")" in s
    m = re.search(r"-?\d[\d,]*(?:\.\d+)?", s.replace("\n", " "))
    if not m:
        return None
    digits = m.group().replace(",", "").lstrip("-")
    if not digits:  # degenerate match (e.g. a lone comma) — guard float('')
        return None
    v = float(digits)
    return -v if neg else v


def candidate_pages(pdf: Path) -> list[int]:
    reader = PdfReader(str(pdf))
    out = []
    for i, page in enumerate(reader.pages):
        with contextlib.suppress(Exception):
            t = (page.extract_text() or "").lower()
            ndiv = sum(1 for _, kw in DIVISIONS if kw.split()[0] in t)
            if ndiv >= 5 and "capital" in t and "expenditure" in t:
                out.append(i + 1)
    return out


def _label_col(cells: list[str]) -> tuple[int, str] | None:
    """(column index, canonical division) for the cell holding a division name, or None."""
    for i, c in enumerate(cells):
        low = c.lower()
        for canon, kw in DIVISIONS:
            if kw.split()[0] in low:
                return i, canon
    return None


def _is_total_label(cells: list[str]) -> bool:
    """A TOTAL row's own label cell — guard against the 'Total Income' COLUMN header."""
    for c in cells:
        s = c.strip().lower()
        if s == "total" or s.startswith("total ") and "income" not in s and "expend" not in s:
            return True
    return False


def extract_page(pdf: Path, page1: int):
    """-> (rows{canon:(exp,income)}, total_exp) or (None,None).

    Robust to (a) a NOTE-REFERENCE column at col0 with the LABEL at col1, and (b) camelot
    header rows being MISALIGNED with the data columns. So columns are located POSITIONALLY
    from the data, not the header: division name in `label_col`, then Opening Balance, then
    EXPENDITURE = label_col+2 (the appendix's fixed order). The total row is the one whose
    own label is 'Total' (NOT the 'Total Income' column header) or a trailing all-numeric row.
    Validated by reconciling Σdiv(exp) vs the total row's exp cell."""
    for flavor in ("stream", "lattice"):
        with contextlib.suppress(Exception):
            tables = camelot.read_pdf(str(pdf), pages=str(page1), flavor=flavor)
            for t in tables:
                grid = [[str(c).strip() for c in row] for row in t.df.itertuples(index=False)]
                div_rows: dict[str, tuple[int, list]] = {}
                total_row = None
                for r in grid:
                    nums_in = sum(1 for c in r if num(c) is not None)
                    lc = _label_col(r)
                    if lc and lc[1] not in div_rows and nums_in >= 3:
                        div_rows[lc[1]] = (lc[0], r)
                    elif total_row is None and nums_in >= 6 and (
                        _is_total_label(r) or not any(ch.isalpha() for ch in " ".join(r))
                    ):
                        total_row = r
                if len(div_rows) < 6:
                    continue
                # expenditure column = (modal label column) + 2  [skip the opening-balance col]
                label_c = max(set(lc for lc, _ in div_rows.values()),
                              key=lambda x: [lc for lc, _ in div_rows.values()].count(x))
                exp_c = label_c + 2

                def cell(row, c):
                    return num(row[c]) if c is not None and 0 <= c < len(row) else None

                exps = {k: cell(r, exp_c) for k, (_, r) in div_rows.items()}
                div_exp = sum(e for e in exps.values() if e is not None)
                total_exp = cell(total_row, exp_c) if total_row else None
                if total_exp is None or abs(div_exp - total_exp) >= 100_000:
                    continue
                # total income column = first col right of expenditure whose div-sum reconciles
                inc_c = None
                for c in range(exp_c + 1, len(total_row)):
                    ds = sum(v for k, (_, r) in div_rows.items() if (v := cell(r, c)) is not None)
                    tv = cell(total_row, c)
                    if tv is not None and abs(ds - tv) < 100_000 and tv >= 0.5 * total_exp:
                        inc_c = c
                        break
                rows = {k: (cell(r, exp_c), cell(r, inc_c)) for k, (_, r) in div_rows.items()}
                return rows, total_exp
    return None, None


def main() -> None:
    all_rows = []
    print(f"{'council':<14}{'page':>6}{'div':>5}{'capEXP':>12}{'printed':>12}  reconcile")
    print("-" * 64)
    for slug in COUNCILS:
        files = list((BRONZE / slug).glob("*.pdf"))
        if not files:
            print(f"{slug:<14}  (no bronze pdf)")
            continue
        pdf = files[0]
        best = None
        for pg in candidate_pages(pdf):
            rows, total_exp = extract_page(pdf, pg)
            if not rows:
                continue
            div_exp = sum(e for e, _ in rows.values() if e is not None)
            recon = bool(total_exp and abs(div_exp - total_exp) < 100_000)
            cand = (int(recon), len(rows), pg, rows, total_exp, div_exp)
            if best is None or cand[:2] > best[:2]:
                best = cand
        if not best:
            print(f"{slug:<14}  no candidate pages parsed")
            continue
        recon, ndiv, pg, rows, total_exp, div_exp = best
        flag = "EXACT" if recon else "FAIL"
        h = rows.get("Housing and Building", (None, None))[0]
        print(f"{slug:<14}{pg:>6}{ndiv:>5}{div_exp/1e6:>11.1f}m{(total_exp/1e6 if total_exp else 0):>11.1f}m  {flag}  housing={(h or 0)/1e6:.1f}m")
        if recon:
            for canon, (exp, inc) in rows.items():
                all_rows.append({"slug": slug, "division": canon,
                                 "capital_expenditure": exp, "capital_income": inc,
                                 "source_page_number": pg - 1,
                                 "printed_total_expenditure": total_exp})
    OUT.write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT}  ({len({r['slug'] for r in all_rows})} councils, {len(all_rows)} rows)")


if __name__ == "__main__":
    main()
