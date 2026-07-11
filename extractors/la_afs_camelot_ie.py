"""Camelot-based per-LA AFS I&E extractor — runs in an ISOLATED camelot venv (not the main
.venv: avoids the `uv sync` churn + the opencv/cv2 clash that would break SIPO).

For councils whose layout the main extractor's fitz `parse_ie` mis-reads (it grabs the wrong
cells → ~2× inflated Σgross), extract the I&E-by-division statement with camelot's structured
cell grid instead. Per council: pypdf finds candidate pages (≥5 division keywords — pypdf
renders 'gross expenditure' inconsistently, so the reconcile gate filters false pages);
camelot (stream/lattice) reads them; map rows → divisions (gross/income/net/prior) + the
printed Total line; keep the page that reconciles Σgross to the printed total.

Invoked as a SUBPROCESS by la_afs_extract.merge_camelot() with the fitz-fail slugs as argv.
Writes reconciling rows to data/_meta/la_afs_camelot_rows.json for the main venv to merge.

Build the isolated venv (one-off, paths are examples):
    uv venv c:/tmp/afs_camelot_venv --python <64-bit python3.12>
    uv pip install --python c:/tmp/afs_camelot_venv "camelot-py[base]" pypdf
Run standalone:  <venv>/python.exe extractors/la_afs_camelot_ie.py monaghan kildare …
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
OUT = ROOT / "data" / "_meta" / "la_afs_camelot_rows.json"

# canonical division -> keyword (same taxonomy as afs_amalgamated_extract.DIVISIONS)
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
# argv tokens are 'slug' (all bronze PDFs for that council) or 'slug:filename.pdf' (that
# file only — the per-YEAR fail-set la_afs_extract now passes); else a default set + a
# control council for standalone runs.
_TOKENS = sys.argv[1:] or ["monaghan", "kildare", "clare", "fingal", "dlr", "cork_county"]
TARGETS: dict[str, list[str] | None] = {}  # slug -> filenames (None = every bronze pdf)
for _t in _TOKENS:
    if ":" in _t:
        _slug, _name = _t.split(":", 1)
        if TARGETS.get(_slug) is None and _slug in TARGETS:
            continue  # bare slug already requested all files
        TARGETS.setdefault(_slug, []).append(_name)
    else:
        TARGETS[_t] = None


def num(s: str) -> float | None:
    s = str(s).strip()
    neg = s.startswith("(") and ")" in s
    m = re.search(r"-?\d[\d,]*(?:\.\d+)?", s)  # require a leading digit — a lone ',' must not match
    if not m:
        return None
    v = float(m.group().replace(",", ""))
    return -v if neg else v


def candidate_pages(pdf: Path) -> list[int]:
    """1-indexed pages with ≥5 division keywords (loose — the reconcile gate filters false pages)."""
    reader = PdfReader(str(pdf))
    out = []
    for i, page in enumerate(reader.pages):
        with contextlib.suppress(Exception):
            t = (page.extract_text() or "").lower()
            ndiv = sum(1 for _, kw in DIVISIONS if kw.split()[0] in t)
            if ndiv >= 5:
                out.append(i + 1)
    return out


def extract_page(pdf: Path, page1: int) -> tuple[dict, tuple | None]:
    """Return ({canon: (gross,income,net,prior)}, (tot_gross, tot_income))."""
    divs: dict[str, tuple] = {}
    total = None
    for flavor in ("stream", "lattice"):
        with contextlib.suppress(Exception):
            tables = camelot.read_pdf(str(pdf), pages=str(page1), flavor=flavor)
            for t in tables:
                for row in t.df.itertuples(index=False):
                    cells = [str(c) for c in row]
                    label = cells[0].lower()
                    nums = [num(c) for c in cells[1:] if num(c) is not None and abs(num(c)) > 100]
                    if ("total" in label and "expenditure" in label) and len(nums) >= 2 and total is None:
                        total = (nums[0], nums[1])
                    kw = next((c for c, k in DIVISIONS if k.split()[0] in label), None)
                    if kw and kw not in divs and len(nums) >= 3:
                        divs[kw] = (nums[0], nums[1], nums[2], nums[3] if len(nums) >= 4 else None)
            if len(divs) >= 8 and total:
                return divs, total
    return divs, total


def main() -> None:
    all_rows = []
    print(f"{'council/file':<34}{'page':>6}{'div':>5}{'Sgross':>12}{'printed':>12}  reconcile")
    print("-" * 82)
    for slug, names in TARGETS.items():
        if names is None:
            files = sorted((BRONZE / slug).glob("*.pdf"))
        else:
            files = [BRONZE / slug / n for n in names]
            files = [f for f in files if f.exists()]
        if not files:
            print(f"{slug:<34}  (no bronze pdf)")
            continue
        for pdf in files:  # one AFS file = one statement year → best page PER FILE
            label = f"{slug}/{pdf.name}"
            best = None
            for pg in candidate_pages(pdf):
                divs, total = extract_page(pdf, pg)
                if len(divs) < 6:
                    continue
                gross = sum(v[0] for v in divs.values())
                recon = bool(total and abs(gross - total[0]) < 100_000)
                cand = (int(recon), len(divs), pg, divs, total, gross)
                if best is None or cand[:2] > best[:2]:
                    best = cand
            if not best:
                print(f"{label:<34}  no candidate pages parsed")
                continue
            recon, ndiv, pg, divs, total, gross = best
            printed = total[0] if total else None
            flag = "EXACT" if recon else "FAIL"
            print(
                f"{label:<34}{pg:>6}{ndiv:>5}{gross / 1e6:>11.1f}m{(printed / 1e6 if printed else 0):>11.1f}m  {flag}"
            )
            if recon:
                for canon, v in divs.items():
                    all_rows.append(
                        {
                            "slug": slug,
                            "source_file": pdf.name,  # lets the merger derive THIS file's year
                            "division": canon,
                            "gross_expenditure": v[0],
                            "income": v[1],
                            "net_expenditure": v[2],
                            "net_expenditure_prior_yr": v[3],
                            "source_page_number": pg - 1,
                            "printed_total_eur": printed,
                        }
                    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
    n_files = len({(r["slug"], r.get("source_file")) for r in all_rows})
    print(f"\nwrote {OUT}  ({len({r['slug'] for r in all_rows})} councils, {n_files} files, {len(all_rows)} rows)")


if __name__ == "__main__":
    main()
