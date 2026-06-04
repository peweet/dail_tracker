"""INGEST (sandbox): per-LA AFS CAPITAL ACCOUNT — Expenditure & Income by service division.

Sibling to la_afs_extract.py (the REVENUE Income & Expenditure account). This parses the
"Analysis of Expenditure and Income on Capital Account" appendix (the build / acquire /
infrastructure programme) — the layer the revenue I&E account does NOT capture.

WHY a separate fact: the revenue account shows housing netting to ~EUR0 (HAP/RAS recoupment +
rents pass straight through). The ACTUAL housing investment lives here, in the capital
account, and is dominated by central (DHLGH) grants. Capital is a DISTINCT GRAIN from revenue
net-expenditure — never union or reconcile the two (a capital build EUR and a revenue net EUR
are not the same money).

Method (per the dual-parser rule, every council is reconcile-gated to its own printed TOTAL):
  - PRIMARY = word-geometry. get_text("words") -> cluster rows by y -> cluster number tokens
    into columns by x (a blank "-" cell leaves a column gap rather than shifting positions,
    which is what defeats naive line-parsing on these 9-10 column tables). The appendix column
    order is standardised: [Opening Balance @ 1/1, EXPENDITURE, <income sub-cols>, Total
    Income, <transfers>, Closing Balance @ 31/12]. Expenditure is the first data column after
    the opening balance (c1); it is VALIDATED by reconciling the division sum to the printed
    TOTAL. Total Income is identified as the income column whose printed total is closest to
    the expenditure total (capital income ~= capital expenditure, being grant-financed).
  - FALLBACK = line-parse, for councils whose appendix is a rotated/landscape table
    (Tipperary) where word y-clustering collapses (division labels stack vertically).

Reuses la_afs_extract wholesale (REGISTRY, cached bronze PDFs, statement_year/best_ie_page for
a year consistent with the revenue fact). Reads the SAME cached PDFs; writes a SEPARATE parquet.

Run:
  ./.venv/Scripts/python.exe pipeline_sandbox/la_afs_capital_extract.py
  ./.venv/Scripts/python.exe pipeline_sandbox/la_afs_capital_extract.py --only cork_city,meath
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "pipeline_sandbox"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import la_afs_extract as rev  # noqa: E402  (CACHE, REGISTRY, best_ie_page, statement_year, title_year)
import config  # noqa: E402

OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_afs_capital_divisions.parquet"
OUT_COV = ROOT / "data/_meta/la_afs_capital_coverage.json"

DIV_KEYS = {
    "housing": "Housing and Building",
    "road": "Roads, Transportation and Safety",
    "water": "Water Services",
    "development": "Development Management",
    "environ": "Environmental Services",
    "recreation": "Recreation and Amenity",
    "agricultur": "Agriculture, Education, Health & Welfare",
    "miscellan": "Miscellaneous Services",
}
NUMRE = re.compile(r"^\(?-?[\d,]+(?:\.\d+)?\)?\*?$")
RECON_TOL = 3.0  # euro


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


def _row_label(text_tokens: list[str]) -> str | None:
    txt = " ".join(t for t in text_tokens if not NUMRE.match(t)).lower()
    if "total" in txt:
        return "TOTAL"
    for k, name in DIV_KEYS.items():
        if k in txt:
            return name
    return None


# ---- word-geometry parser -------------------------------------------------
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


def _parse_geom(page) -> tuple[dict, list, dict] | None:
    """-> (mat {division: [col values]}, colx, total_row) or None if no division rows."""
    words = [(w[0], w[1], w[2], w[3], w[4]) for w in page.get_text("words")]
    rows = _cluster_rows(words)
    drows: dict[str, list] = {}
    total: list | None = None
    for r in rows:
        lab = _row_label([w[4] for w in r["w"]])
        nums = [((w[0] + w[2]) / 2, tonum(w[4])) for w in r["w"] if NUMRE.match(w[4])]
        if not nums:
            continue
        if lab == "TOTAL" and total is None:
            total = nums
        elif lab in DIV_KEYS.values() and lab not in drows:
            drows[lab] = nums
    if len(drows) < 6:
        return None
    allx = sorted(x for src in [*drows.values(), total or []] for (x, _) in src)
    colx: list[float] = []
    cluster: list[float] = []
    for x in allx:
        if cluster and x - cluster[-1] > 12:
            colx.append(sum(cluster) / len(cluster))
            cluster = []
        cluster.append(x)
    if cluster:
        colx.append(sum(cluster) / len(cluster))

    def place(src: list) -> list:
        row: list = [None] * len(colx)
        for x, v in src:
            row[min(range(len(colx)), key=lambda i: abs(colx[i] - x))] = v
        return row

    mat = {k: place(v) for k, v in drows.items()}
    totrow = place(total) if total else [None] * len(colx)
    return mat, colx, totrow


# ---- line parser (rotated/landscape fallback) -----------------------------
def _parse_lines(page) -> tuple[dict, list, dict] | None:
    lines = [ln.strip() for ln in page.get_text("text").splitlines() if ln.strip()]
    drows: dict[str, list] = {}
    total: list | None = None
    cur: str | None = None
    buf: list[str] = []

    def flush():
        nonlocal total
        if cur and buf:
            vals = [tonum(x) for x in buf]
            if cur == "TOTAL" and total is None:
                total = vals
            elif cur in DIV_KEYS.values() and cur not in drows:
                drows[cur] = vals

    for ln in lines:
        lab = _row_label([ln])
        if lab:
            flush()
            cur, buf = lab, []
        elif cur and NUMRE.match(ln.replace(" ", "")):
            buf.append(ln)
        elif cur and buf:
            flush()
            cur, buf = None, []
    flush()
    if len(drows) < 6:
        return None
    ncol = max((len(v) for v in drows.values()), default=0)
    aligned = {k: v for k, v in drows.items() if len(v) == ncol}
    if len(aligned) < 6:
        return None
    colx = list(range(ncol))
    totrow = (total + [None] * ncol)[:ncol] if total else [None] * ncol
    return aligned, colx, totrow


def _reconciles(mat: dict, totrow: list, c: int) -> bool:
    tv = totrow[c] if c < len(totrow) else None
    if tv is None:
        return False
    ds = sum(mat[k][c] for k in mat if c < len(mat[k]) and mat[k][c] is not None)
    return abs(ds - tv) < RECON_TOL


def _find_capital(doc, skip_pages: set[int]) -> tuple[int, str, dict, list, list] | None:
    """Scan for the by-division capital-account page; return the one whose Expenditure column
    (c1) reconciles to its printed TOTAL. (mat, colx, totrow) by word-geom else line-parse."""
    best = None
    for i in range(doc.page_count):
        if i in skip_pages:
            continue
        U = doc[i].get_text("text").upper()
        if "CAPITAL" not in U or "EXPENDITURE" not in U:
            continue
        for method, fn in (("geom", _parse_geom), ("line", _parse_lines)):
            res = fn(doc[i])
            if not res:
                continue
            mat, colx, totrow = res
            if len(colx) >= 3 and _reconciles(mat, totrow, 1):  # c1 = Expenditure
                score = (len(mat), method == "geom")
                if best is None or score > best[0]:
                    best = (score, i, method, mat, colx, totrow)
                break
    if best is None:
        return None
    _, pg, method, mat, colx, totrow = best
    return pg, method, mat, colx, totrow


def _income_col(totrow: list, exp_c: int = 1, open_c: int = 0) -> int | None:
    """Total-Income column = the one (not opening/expenditure/closing) whose printed total is
    closest to the expenditure total (capital income ~= expenditure, being grant-financed)."""
    last = len(totrow) - 1
    exp_total = totrow[exp_c]
    if exp_total is None:
        return None
    cands = [c for c in range(len(totrow)) if c not in (open_c, exp_c, last) and totrow[c] is not None]
    if not cands:
        return None
    return min(cands, key=lambda c: abs(totrow[c] - exp_total))


def ingest(slug: str, cf: dict) -> tuple[list[dict], dict]:
    pdfs = sorted((rev.CACHE / slug).glob("*.pdf"))
    stat = {"council": cf["council"], "slug": slug}
    if not pdfs:
        stat["status"] = "no-cached-pdf"
        return [], stat
    path = pdfs[0]
    doc = fitz.open(path)
    # authoritative year (same derivation as the revenue fact)
    pg_ie, _, _ = rev.best_ie_page(doc)
    year = None
    if pg_ie is not None:
        year = rev.statement_year(doc[pg_ie].get_text("text"))
    year = year or rev.title_year(path.stem) or None
    found = _find_capital(doc, skip_pages={pg_ie} if pg_ie is not None else set())
    doc.close()
    if not found:
        stat.update(status="no-capital-page", year=year)
        return [], stat
    pg, method, mat, colx, totrow = found
    last = len(colx) - 1
    inc_c = _income_col(totrow)
    exp_total = totrow[1]
    inc_total = totrow[inc_c] if inc_c is not None else None
    rows = []
    for div, vals in mat.items():
        rows.append(
            {
                "council": cf["council"],
                "slug": slug,
                "entity": cf["entity"],
                "region": cf["region"],
                "year": year,
                "division": div,
                "capital_expenditure": vals[1],
                "capital_income": vals[inc_c] if inc_c is not None else None,
                "opening_balance": vals[0],
                "closing_balance": vals[last],
                "source_file_url": cf.get("picked_url"),
                "source_page_number": pg,
                "parse_method": method,
                "printed_total_expenditure": exp_total,
                "reconciled": True,
                "realisation_tier": "SPENT",
                "value_kind": "capital_expenditure_actual",
                "scope": "single-LA capital account (by service division)",
                "source": "Local Authority audited AFS (own website), Capital Account appendix",
            }
        )
    div_sum = sum(r["capital_expenditure"] for r in rows if r["capital_expenditure"] is not None)
    stat.update(
        status="ok",
        year=year,
        method=method,
        page=pg,
        divisions=len(rows),
        capital_expenditure_total=round(div_sum, 0),
        printed_total=round(exp_total, 0) if exp_total else None,
        income_total=round(inc_total, 0) if inc_total else None,
        reconciled=abs(div_sum - (exp_total or 0)) < RECON_TOL,
    )
    return rows, stat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated slugs")
    args = ap.parse_args()
    only = {s.strip() for s in args.only.split(",")} if args.only else None

    print("=" * 74)
    print("PER-LA AFS — CAPITAL ACCOUNT (by service division)")
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
            recon = "EXACT" if stat["reconciled"] else "**OFF**"
            hv = next((r["capital_expenditure"] for r in rows if r["division"].startswith("Housing")), None)
            print(
                f"  {cf['council']:<15} ok  yr={stat['year']}  {stat['method']:<4} pg={stat['page']:<3} "
                f"div={stat['divisions']}  recon={recon:<6} "
                f"capEXP={stat['capital_expenditure_total'] / 1e6:>7.1f}m  "
                f"housing={(hv or 0) / 1e6:>6.1f}m"
            )
        else:
            print(f"  {cf['council']:<15} {stat['status']}")

    if not all_rows:
        print("\nno rows — nothing written")
        return

    df = pl.DataFrame(all_rows).sort(["council", "division"])
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)

    ok = [s for s in stats if s["status"] == "ok"]
    cov = {
        "councils_attempted": len(stats),
        "councils_with_rows": len(ok),
        "councils_reconciled": sum(1 for s in ok if s["reconciled"]),
        "rows": len(all_rows),
        "by_council": stats,
        "realisation_tier": "SPENT",
        "value_kind": "capital_expenditure_actual",
        "scope": "per-LA audited AFS Capital Account, expenditure by service division",
        "generated_at": datetime.now(UTC).isoformat(),
        "caveat": (
            "Capital (build/acquire) expenditure by division — a DISTINCT grain from the "
            "revenue I&E net-expenditure fact (la_afs_divisions). NEVER union or reconcile the "
            "two. Capital income is overwhelmingly DHLGH/central grants. Sum only within a "
            "(council, year)."
        ),
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"\n  rows: {len(all_rows)}  councils: {len(ok)}  reconciled: {cov['councils_reconciled']}/{len(ok)}")
    print(f"wrote {OUT_PARQUET}")
    print(f"      {OUT_COV}")


if __name__ == "__main__":
    main()
