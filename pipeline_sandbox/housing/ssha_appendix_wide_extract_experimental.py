"""SSHA Appendix A1.1-A1.9 — header-preserving WIDE extraction (no camelot).

Supersedes the positional `ssha_appendix_full_extract_experimental.py` (which
discarded headers and emitted anonymous col_idx/value) and the camelot variant
(`ssha_appendix_camelot_extract_experimental.py`, which needs ghostscript).

`fitz.Page.find_tables()` already returns clean labelled grids for every SSHA
appendix table, so this reads the header row directly and emits one tidy WIDE
parquet per table:  la, year, <labelled categories...>, total.

Tables are located by their "Table A1.x:" caption (robust to page drift — the
earlier page-range hardcoding mis-assigned A1.2/A1.3/A1.4 headers). Two tables
(A1.4 household size, A1.5 main need) use lettered codes A-M with a legend key;
those letters are mapped to labels via the hardcoded legends below (transcribed
from the 2025 report — re-check if the source schema changes).

Validation gate (per table): sum(category counts) == reported Total for every
(la, year) row. On the 2025 report this passes 32/32 LAs x 2 years = 64/64.

Reads  : doc/source_pdfs/SSHA_2025_FINAL.pdf   (repo-root, via parents[2])
Writes : data/gold/parquet/ssha_<table>_wide.parquet   (with --write, green only)

NOTE: sandbox/experimental — sandbox->vet->promote. Default run is dry (no write).
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import fitz
import polars as pl

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

# This file lives at pipeline_sandbox/housing/ -> repo root is parents[2].
_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "doc" / "source_pdfs" / "SSHA_2025_FINAL.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet"

# Appendix table ids -> the caption token find_tables pages are matched against.
# (A1.6a is a separate small Traveller-identifier table; handled by its own
#  extractor, excluded here so its 3-col shape doesn't trip the sum gate.)
TABLE_IDS = {
    "A1.1": "a1_1_age",
    "A1.2": "a1_2_employment",
    "A1.3": "a1_3_income",
    "A1.4": "a1_4_household_size",
    "A1.5": "a1_5_main_need",
    "A1.6": "a1_6_accom_req",
    "A1.7": "a1_7_tenure",
    "A1.8": "a1_8_time_on_list",
    "A1.9": "a1_9_citizenship",
}

# Appendix sits in this 0-indexed page window (whole section ~ PDF pages 46-75).
_PAGE_WINDOW = range(44, 76)

# Letter legends for the two coded tables (transcribed from SSHA 2025 report).
_LEGEND_A1_4 = {
    "A": "1 adult", "B": "1 adult, 1-2 children", "C": "Couple, 1-2 children",
    "D": "Couple", "E": "1 adult, 3 or more children",
    "F": "Couple, 3 or more children", "G": "2 adults",
    "H": "2 adults, with children", "I": "Couple, 1 or more adults, 1-2 children",
    "J": "Couple with 1 or more other adults", "K": "3 or more adults",
    "L": "3 or more adults, with children",
    "M": "Couple, 1 or more adults, 3 or more children",
}
_LEGEND_A1_5 = {
    "A": "Unsuitable - particular household circumstance",
    "B": "Requires Rent Supplement",
    "C": "Requirement for separate accommodation",
    "D": "Homeless / institution / emergency accommodation",
    "E": "Overcrowded accommodation", "F": "Intellectual disability",
    "G": "Physical disability", "H": "Mental health disability",
    "I": "Unfit accommodation", "J": "Unsustainable mortgage",
    "K": "Sensory disability", "L": "Medical or compassionate grounds",
    "M": "Other form of disability",
}
_LEGENDS = {"a1_4_household_size": _LEGEND_A1_4, "a1_5_main_need": _LEGEND_A1_5}

# Targeted fitz character-pair-transposition fixes seen in SSHA headers.
_GLITCH = {
    "citzi en": "citizen", "citzien": "citizen", "eaa": "eea",
    "benefti": "benefit", "retri ed": "retired", "accommodatoi n": "accommodation",
    "accommodatoin": "accommodation", "accomm odation": "accommodation",
    "accomm -odation": "accommodation", "accomm- odation": "accommodation",
    "relatvi es": "relatives",
}

_CAPTION_RE = re.compile(r"Table\s+(A1\.\d+a?)", re.IGNORECASE)

EXPECTED_LAS = 31  # 31 local authorities (+ a possible "State" total row)


def _clean(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").replace("\n", " ").replace("\r", " ")).strip()
    low = s.lower()
    for bad, good in _GLITCH.items():
        if bad in low:
            low = low.replace(bad, good)
            s = low
    return s.strip()


def _slug(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", _clean(s).lower())
    return s.strip("_") or "col"


def _to_int(c):
    s = str(c or "").replace(",", "").replace(" ", "").strip()
    if not s or s in {"-", "—", "–"}:
        return None
    m = re.match(r"-?\d+", s)
    return int(m.group(0)) if m else None


_LA_TYPO_FIXES = {"Ofaf ly": "Offaly", "Ofafly": "Offaly"}


def _la_canon(s: str) -> str:
    n = _clean(s).replace("Dún", "Dun")
    for typo, fix in _LA_TYPO_FIXES.items():
        n = n.replace(typo, fix)
    n = re.sub(r"\s+Council$", "", n)
    return n


def _is_letter_header(headers: list[str]) -> bool:
    cats = [h for h in headers if h.upper() not in ("", "TOTAL")]
    if not cats:
        return False
    letters = sum(1 for h in cats if re.fullmatch(r"[A-M]", h.strip().upper()))
    return letters >= max(3, len(cats) // 2)


def _page_caption(page) -> str | None:
    m = _CAPTION_RE.search(page.get_text() or "")
    return m.group(1).replace("A1.6a", "A1.6A").upper().replace("A1.6A", "A1.6a") if m else None


def extract_table(doc, table_id: str) -> pl.DataFrame:
    """Find every grid for this table across the appendix window and merge."""
    want_caption = next(k for k, v in TABLE_IDS.items() if v == table_id)
    headers: list[str] | None = None
    records: list[dict] = []
    legend = _LEGENDS.get(table_id)

    for pi in _PAGE_WINDOW:
        if pi >= doc.page_count:
            break
        page = doc[pi]
        cap = _page_caption(page)
        if cap != want_caption:
            continue
        for tab in page.find_tables().tables:
            data = tab.extract()
            if not data or len(data) < 4:
                continue
            head = [_clean(c) for c in data[0]]
            if not head[0].lower().startswith(("local", "authority")):
                continue
            # second col should be the Year column
            if len(head) < 4 or "year" not in head[1].lower():
                continue
            if headers is None:
                headers = head
            # Lock column identity to the FIRST page's header — continuation pages
            # can render the same header with cosmetic glitches (e.g. "accomm
            # odation" vs "accommodation"), which would otherwise split one logical
            # column into two. Same table => same column order, so skip any page
            # whose shape disagrees rather than mis-align it.
            if len(head) != len(headers):
                continue
            cur_la = None
            for r in data[1:]:
                cells = [(c or "").strip() for c in r]
                first = _la_canon(cells[0])
                if first and not first.replace(",", "").isdigit() and "local" not in first.lower():
                    cur_la = first
                yr = cells[1].strip() if len(cells) > 1 else ""
                if cur_la and yr in ("2024", "2025"):
                    rec = {"la": cur_la, "year": int(yr)}
                    for ci in range(2, len(headers)):
                        raw = headers[ci]
                        if legend and raw.strip().upper() in legend:
                            col = _slug(legend[raw.strip().upper()])
                        elif raw.lower() == "total":
                            col = "total"
                        else:
                            col = _slug(raw) or f"col{ci}"
                        rec[col] = _to_int(cells[ci]) if ci < len(cells) else None
                    records.append(rec)
    if not records:
        return pl.DataFrame()
    df = pl.DataFrame(records).unique(subset=["la", "year"], keep="first")
    df = df.filter(pl.col("la").str.to_lowercase() != "total")
    return df.sort(["la", "year"])


def fidelity_check(df: pl.DataFrame, table_id: str) -> dict:
    rpt: dict = {"table": table_id, "rows": df.height, "checks": {}}
    if df.is_empty():
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        rpt["green"] = False
        return rpt
    n_la = df["la"].n_unique()
    rpt["checks"]["1_la_coverage"] = {"unique_LAs": n_la, "pass": n_la >= EXPECTED_LAS}
    years = sorted(df["year"].unique().to_list())
    rpt["checks"]["2_years"] = {"years": years, "pass": 2024 in years and 2025 in years}
    cat_cols = [c for c in df.columns if c not in ("la", "year", "total")]
    if "total" in df.columns and cat_cols:
        chk = df.with_columns(
            pl.sum_horizontal([pl.col(c).fill_null(0) for c in cat_cols]).alias("_sum")
        )
        bad = chk.filter(pl.col("_sum") != pl.col("total")).height
        rpt["checks"]["3_sum_eq_total"] = {
            "rows_ok": chk.height - bad, "rows": chk.height, "pass": bad == 0,
        }
    else:
        rpt["checks"]["3_sum_eq_total"] = {"pass": False, "note": "no total/cats"}
    rpt["checks"]["4_categories"] = {"n_category_cols": len(cat_cols), "cols": cat_cols}
    rpt["green"] = all(c.get("pass", True) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="write green tables to gold")
    ap.add_argument("--tables", nargs="*", default=list(TABLE_IDS.values()))
    args = ap.parse_args()

    if not _SRC.exists():
        print(f"ERROR: source missing: {_SRC}")
        sys.exit(1)

    doc = fitz.open(str(_SRC))
    results = []
    for tid in args.tables:
        df = extract_table(doc, tid)
        rpt = fidelity_check(df, tid)
        print(f"\n=== ssha_{tid}_wide — {df.height} rows ===")
        for name, chk in rpt["checks"].items():
            mark = "GREEN" if chk.get("pass", True) else "FAIL"
            print(f"  [{mark}] {name}: {chk}")
        print(f"  >>> {'GREEN' if rpt['green'] else 'AMBER'}")
        if args.write and rpt["green"]:
            path = _OUT / f"ssha_{tid}_wide.parquet"
            _write_parquet(df, path)
            print(f"  Wrote {path.relative_to(_ROOT)}")
        results.append((tid, df.height, rpt["green"]))
    doc.close()

    print("\n" + "=" * 60 + "\nSUMMARY")
    for tid, n, green in results:
        print(f"  {'OK ' if green else 'AMB'} ssha_{tid:22s} {n:>4} rows")
    if not args.write:
        print("\n(dry-run — pass --write to land green tables in gold)")


if __name__ == "__main__":
    main()
