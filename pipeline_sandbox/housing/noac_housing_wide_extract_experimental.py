"""NOAC LA Performance Indicator Report 2024 — housing H1-H7 LABELLED WIDE extract.

Supersedes the positional `noac_housing_extract_experimental.py` (which emitted
anonymous `col_idx`/`value` long rows, losing all column meaning) by mirroring the
SSHA Tier-A pattern (`ssha_appendix_wide_extract_experimental.py`):
  - locate each indicator's per-LA matrix by scanning pages with fitz
    `find_tables()` and matching the 31-LA row signature (NOT hardcoded pages);
  - classify every column via a hardcoded legend (A./B./H5A-style truncated
    headers -> full human label + unit, transcribed from the report key text);
  - emit one tidy WIDE parquet per indicator with slugified, unit-suffixed
    columns (`_weeks`, `_eur`, `_pct`, `_count`, `_mwh`, `_tco2`).

NOAC has NO `Total` column to validate against (unlike SSHA's sum==Total). Gates:
  - LA coverage >= 30 (>= 4 for H6 — H6 reports all 31 but several are 0.00);
  - range sanity per unit (% in 0-100; costs/times/counts non-negative);
  - median per indicator reported for eyeballing.

CRITICAL FINDING: the sibling text dump (NOAC_LA_PerfInd_2024.txt) is OFF-BY-ONE
for the single-column tables (H2/H4/H6): get_text() linearization drops Carlow's
value and shifts every LA up one, appending the State average to Wicklow. fitz
`find_tables()` aligns each value to its LA by geometry and is authoritative
(verified against page word-coordinates). This extractor reads tables, never text.

H1A/H1B/H1C ("Total Social Housing Output ... by local authority and AHB") are
DESPITE their captions NATIONAL year-series tables (2018-2024 rows, national
totals), not per-LA matrices. They are extracted separately as a national
year-series parquet (noac_h1_output_national_year).

Reads  : doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf  (repo root via parents[2])
Writes : data/gold/parquet/_noac_eval/noac_<indicator>_wide.parquet  (--write)
         ISOLATED eval dir — NEVER the real gold names. sandbox->vet->promote.
Default run is dry (no write).
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
_SRC = _ROOT / "doc" / "source_pdfs" / "NOAC_LA_PerfInd_2024.pdf"
# ISOLATED eval dir — never the real gold parquet names.
_OUT = _ROOT / "data" / "gold" / "parquet" / "_noac_eval"

# ---------------------------------------------------------------------------
# 31 local authorities. Tables print 30 named rows + a "Totals" row (H1/H5/H7)
# or just 30 named rows (H2/H3/H4/H6). DLR prints as "Dun Laoghaire-Rathdown".
# ---------------------------------------------------------------------------
EXPECTED_LAS = {
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal",
    "Dublin City", "Dun Laoghaire", "Fingal", "Galway City", "Galway County",
    "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford",
    "Louth", "Mayo", "Meath", "Monaghan", "Offaly", "Roscommon", "Sligo",
    "South Dublin", "Tipperary", "Waterford", "Westmeath", "Wexford", "Wicklow",
}
MIN_LA_DEFAULT = 30
MIN_LA_H6 = 4

# ---------------------------------------------------------------------------
# Column legend per indicator. Keys are the LETTER code that prefixes each
# truncated header ("A.", "B.", "A. (1)", ...). Values: (label, unit, slug).
# Transcribed from the report key text (lines ~1693-2103 of the .txt dump and
# the indicator definitions at lines ~2394-2773). Units drive the slug suffix
# and the range-sanity gate.
# ---------------------------------------------------------------------------
# unit -> slug suffix
_UNIT_SUFFIX = {
    "count": "_count", "eur": "_eur", "pct": "_pct", "weeks": "_weeks",
    "mwh": "_mwh", "tco2": "_tco2",
}

NOAC_LEGEND: dict[str, dict[str, tuple[str, str]]] = {
    # H1 Social Housing Stock — per-LA matrix, cols A-F (page 34)
    "h1_stock": {
        "A": ("Dwellings in LA ownership at 01/01/2024", "count"),
        "B": ("Dwellings added to LA owned stock in 2024 (built or acquired)", "count"),
        "C": ("LA owned dwellings sold in 2024", "count"),
        "D": ("LA owned dwellings demolished in 2024", "count"),
        "E": ("Dwellings in LA ownership at 31/12/2024", "count"),
        "F": ("LA owned dwellings planned for demolition under DHLGH approved scheme", "count"),
    },
    # H2 Housing Vacancies — single col A (page 35)
    "h2_vacancies": {
        "A": ("Percentage of LA owned dwellings vacant on 31/12/2024", "pct"),
    },
    # H3 Average Re-letting Time & Cost — cols A (weeks) B (eur) (page 36)
    "h3_reletting": {
        "A": ("Average re-letting time (vacation to re-tenant) in 2024", "weeks"),
        "B": ("Average cost of getting re-tenanted dwellings ready in 2024", "eur"),
    },
    # H4 Housing Maintenance Direct Cost — single col A (page 37)
    "h4_maintenance": {
        "A": ("Maintenance expenditure 2024 per dwelling (H1E less H1F)", "eur"),
    },
    # H5 Private Rented Sector Inspections — cols A-E, mixed units (page 38)
    "h5_prs_inspections": {
        "A": ("Total registered tenancies in LA area at end Dec 2024", "count"),
        "B": ("Rented dwellings inspected in 2024", "count"),
        "C": ("Percentage of inspected dwellings not compliant with Standards Regs", "pct"),
        "D": ("Dwellings deemed compliant in 2024 (incl. originally non-compliant)", "count"),
        "E": ("Inspections undertaken in 2024 (including re-inspections)", "count"),
    },
    # H6 Long-term Homeless Adults — single col A (page 39)
    "h6_homeless": {
        "A": ("Long-term homeless adults as pct of total homeless adults in emergency accom at end 2024", "pct"),
    },
    # H7 Social Housing Retrofit — cols A(1),A(2),A(3),B,C, mixed units (page 40)
    "h7_retrofit": {
        "A1": ("Houses retrofitted 01/01/2024-31/12/2024", "count"),
        "A2": ("Retrofitted houses achieving BER B2 or above", "count"),
        "A3": ("Heat pumps installed in retrofitted houses", "count"),
        "B": ("Total annual energy savings (MWh) from retrofitted houses", "mwh"),
        "C": ("Total carbon emission reduction (tCO2) from retrofitted houses", "tco2"),
    },
}

# Indicators in low->high complexity order (low wins first).
INDICATOR_ORDER = [
    "h2_vacancies", "h3_reletting", "h4_maintenance", "h6_homeless",
    "h7_retrofit", "h1_stock", "h5_prs_inspections",
]

# Per-LA caption used to anchor each indicator's matrix page.
INDICATOR_CAPTION = {
    "h1_stock": re.compile(r"H1:\s*Social Housing Stock", re.I),
    "h2_vacancies": re.compile(r"H2:\s*Housing Vacancies", re.I),
    "h3_reletting": re.compile(r"H3:\s*Average Re-?letting", re.I),
    "h4_maintenance": re.compile(r"H4:\s*Housing Maintenance", re.I),
    "h5_prs_inspections": re.compile(r"H5:\s*Private Rented", re.I),
    "h6_homeless": re.compile(r"H6:\s*Long-?term Homeless", re.I),
    "h7_retrofit": re.compile(r"H7:\s*Social Housing Retrofit", re.I),
}


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\n", " ").replace("\r", " ")).strip()


def _to_float(c):
    """Coerce a cell to float; strip €, %, commas, the fitz euro-glitch char."""
    if c is None:
        return None
    s = str(c).replace(",", "").replace("€", "").replace("%", "")
    s = s.replace("�", "").replace(" ", "").strip()  # � = euro mojibake
    if not s or s in {"-", "—", "–", "None", "N/A", "n/a"}:
        return None
    m = re.match(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def _la_canon(name: str) -> str:
    """Canonicalise an LA name, PRESERVING the City/County distinction.

    NOAC prints e.g. "Cork City" and "Cork County" as two separate authorities;
    "Galway City"/"Galway County" likewise. We must NOT strip the City/County
    qualifier (that would collapse them and lose 5 LAs). We only:
      - normalise the euro/accent mojibake in "Dún Laoghaire";
      - drop a trailing " Council" wording;
      - collapse "City and County" -> "City and County" (Limerick/Waterford are
        unitary "City and County" authorities; keep the full form).
    """
    n = (name or "").replace("\n", " ").replace("\r", " ")
    n = re.sub(r"\s+", " ", n).strip()
    n = n.replace("Dún", "Dun").replace("D�n", "Dun").replace("D�n", "Dun")
    # H1 stock table abbreviates DLR; every other table spells it out. Normalise
    # so the LA joins across indicators.
    if n.strip().upper() == "DLR":
        return "Dun Laoghaire-Rathdown"
    # NOAC prints clean, distinct authority names ("Cork City"/"Cork County",
    # "Limerick City and County"); only drop a literal trailing " Council" word
    # if present — never the City/County qualifier (that loses 5 LAs).
    n = re.sub(r"\s+Council\s*$", "", n)
    return n.strip()


def _is_la_row(text: str) -> bool:
    if not text:
        return False
    t = _la_canon(text).lower()
    if not t or t.replace(".", "").replace(",", "").isdigit():
        return False
    return any(la.lower() in t for la in EXPECTED_LAS)


def _is_total_row(text: str) -> bool:
    return _clean(text).lower().startswith(("total", "state"))


def _header_letter_code(header_cell: str) -> str | None:
    """Map a truncated header cell to its letter code key in NOAC_LEGEND.

    Handles 'A. ...', 'B. ...', 'A. (1) ...' (=> 'A1'), 'C. ...' etc.
    """
    h = _clean(header_cell)
    m = re.match(r"^([A-F])\.\s*\((\d)\)", h)  # A. (1) ...
    if m:
        return f"{m.group(1)}{m.group(2)}"
    m = re.match(r"^([A-F])\.", h)  # A. ...
    if m:
        return m.group(1)
    return None


def _slug_for(label: str, unit: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    # keep slug compact: first ~5 meaningful tokens + unit suffix
    toks = [t for t in base.split("_") if t and t not in {"the", "of", "in", "a", "to", "and", "at"}]
    base = "_".join(toks[:6]) or "col"
    return base + _UNIT_SUFFIX.get(unit, "")


def extract_indicator(doc, indicator: str) -> tuple[pl.DataFrame, list[dict]]:
    """Find the per-LA matrix for `indicator` and emit a labelled wide frame.

    Returns (df, col_meta) where col_meta describes each emitted column.
    """
    caption_re = INDICATOR_CAPTION[indicator]
    legend = NOAC_LEGEND[indicator]
    records: list[dict] = []
    col_meta: list[dict] = []
    chosen_cols: list[tuple[int, str, str, str, str]] | None = None  # (idx, code, slug, label, unit)

    for pi in range(doc.page_count):
        page = doc[pi]
        txt = page.get_text() or ""
        if not caption_re.search(txt) or "Carlow" not in txt:
            continue
        for tab in page.find_tables().tables:
            data = tab.extract()
            if not data or len(data) < 10:
                continue
            head = [_clean(c) for c in data[0]]
            if not head or not head[0].lower().startswith("authorit"):
                continue
            # count LA rows in this table; must look like the 30/31-row matrix
            la_rows = sum(1 for r in data[1:] if _is_la_row((r[0] or "")))
            if la_rows < 25:
                continue

            # Classify each column (1..) via its letter code -> legend.
            cols: list[tuple[int, str, str, str, str]] = []
            unclassified = 0
            for ci in range(1, len(head)):
                code = _header_letter_code(head[ci])
                if code and code in legend:
                    label, unit = legend[code]
                    slug = _slug_for(label, unit)
                    cols.append((ci, code, slug, label, unit))
                else:
                    unclassified += 1
                    slug = f"unclassified_col_{ci}"
                    cols.append((ci, code or "?", slug, head[ci] or "(empty header)", "unknown"))
            chosen_cols = cols

            cur_la = None
            for r in data[1:]:
                cells = [(c or "").strip() for c in r]
                first = cells[0] if cells else ""
                if _is_total_row(first):
                    continue
                if _is_la_row(first):
                    cur_la = _la_canon(first)
                else:
                    # continuation of multi-line LA name with no value -> skip
                    continue
                rec = {"la": cur_la, "year": 2024}
                for (ci, code, slug, label, unit) in cols:
                    rec[slug] = _to_float(cells[ci]) if ci < len(cells) else None
                records.append(rec)
            break  # one matrix table per indicator page
        if records:
            break  # found and consumed the matrix; stop scanning

    if chosen_cols:
        for (ci, code, slug, label, unit) in chosen_cols:
            col_meta.append({"col": slug, "code": code, "label": label, "unit": unit})

    if not records:
        return pl.DataFrame(), col_meta
    df = pl.DataFrame(records).unique(subset=["la"], keep="first").sort("la")
    return df, col_meta


def extract_h1_output_national(doc) -> tuple[pl.DataFrame, list[dict]]:
    """H1A/H1B/H1C are NATIONAL year-series (2018-2024) despite 'by LA' captions.

    They share a Year-keyed grain; merge into one wide national table. Labels are
    transcribed from the report (lines ~1110-1175). All values are dwelling counts.
    """
    # Each sub-table identified by a DISTINGUISHING header token (not just the
    # page caption — page 23 hosts BOTH H1A and H1B grids, so caption alone
    # mis-pairs them). H1A: "Overall Totals"; H1B: "AHB New Build";
    # H1C: "AHB Acquisition".
    grid_sig = {
        "h1a": "overall totals",
        "h1b": "ahb new build",
        "h1c": "ahb acquisition",
    }
    # year -> merged record
    year_rec: dict[int, dict] = {}
    col_meta: list[dict] = []
    seen_slugs: set[str] = set()
    found_keys: set[str] = set()

    for pi in range(doc.page_count):
        page = doc[pi]
        for tab in page.find_tables().tables:
            data = tab.extract()
            if not data or len(data) < 5:
                continue
            head = [_clean(c) for c in data[0]]
            if not head or "year" not in (head[0] or "").lower():
                continue
            yr_rows = sum(1 for r in data[1:] if re.match(r"^20\d\d", (r[0] or "").strip()))
            if yr_rows < 4:
                continue
            head_low = " | ".join(h.lower() for h in head)
            key = next((k for k, sig in grid_sig.items() if sig in head_low), None)
            if key is None or key in found_keys:
                continue
            found_keys.add(key)
            cols = []
            for ci in range(1, len(head)):
                raw = _clean(head[ci]) or f"col{ci}"
                slug = f"{key}_" + (re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_") or f"col{ci}")
                slug = slug[:48] + "_count"
                if slug not in seen_slugs:
                    seen_slugs.add(slug)
                    col_meta.append({"col": slug, "code": key.upper(), "label": raw, "unit": "count"})
                cols.append((ci, slug))
            for r in data[1:]:
                cells = [(c or "").strip() for c in r]
                m = re.match(r"^(20\d\d)", cells[0] if cells else "")
                if not m:
                    continue
                yr = int(m.group(1))
                rec = year_rec.setdefault(yr, {"year": yr})
                for (ci, slug) in cols:
                    v = _to_float(cells[ci]) if ci < len(cells) else None
                    if v is not None:
                        rec[slug] = v
        if len(found_keys) == 3:
            break
    if not year_rec:
        return pl.DataFrame(), col_meta
    df = pl.DataFrame(list(year_rec.values())).sort("year")
    return df, col_meta


def fidelity_check(df: pl.DataFrame, indicator: str, col_meta: list[dict]) -> dict:
    rpt: dict = {"indicator": indicator, "rows": df.height, "checks": {}}
    if df.is_empty():
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        rpt["green"] = False
        return rpt

    is_la = "la" in df.columns
    if is_la:
        n_la = df["la"].n_unique()
        min_la = MIN_LA_H6 if indicator == "h6_homeless" else MIN_LA_DEFAULT
        rpt["checks"]["1_la_coverage"] = {"unique_LAs": n_la, "min": min_la, "pass": n_la >= min_la}
    else:
        n_yr = df["year"].n_unique()
        rpt["checks"]["1_year_coverage"] = {"years": sorted(df["year"].to_list()), "pass": n_yr >= 4}

    # range sanity per unit
    range_ok = True
    range_notes = {}
    unit_by_col = {m["col"]: m["unit"] for m in col_meta}
    for col in df.columns:
        if col in ("la", "year"):
            continue
        unit = unit_by_col.get(col, "unknown")
        s = df[col].drop_nulls()
        if s.is_empty():
            range_notes[col] = "all-null"
            continue
        mn, mx = s.min(), s.max()
        bad = False
        if unit == "pct" and (mn < 0 or mx > 100):
            bad = True
        if unit in ("eur", "weeks", "count", "mwh", "tco2") and mn < 0:
            bad = True
        range_notes[col] = {"min": round(float(mn), 2), "max": round(float(mx), 2), "ok": not bad}
        range_ok = range_ok and not bad
    rpt["checks"]["2_range_sanity"] = {"per_col": range_notes, "pass": range_ok}

    # median per value column (eyeballing)
    medians = {}
    for col in df.columns:
        if col in ("la", "year"):
            continue
        s = df[col].drop_nulls()
        medians[col] = round(float(s.median()), 2) if not s.is_empty() else None
    rpt["checks"]["3_medians"] = {"median": medians, "pass": True}

    # unclassified columns
    unclass = [m["col"] for m in col_meta if m["col"].startswith("unclassified_col_")]
    rpt["checks"]["4_classification"] = {
        "n_cols": len([c for c in df.columns if c not in ("la", "year")]),
        "unclassified": unclass,
        "pass": len(unclass) == 0,
    }
    rpt["green"] = all(c.get("pass", True) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="write parquet (to _noac_eval/ unless --gold)")
    ap.add_argument("--gold", action="store_true",
                    help="promote: write to data/gold/parquet/ instead of the _noac_eval sandbox dir")
    ap.add_argument("--indicators", nargs="*", default=INDICATOR_ORDER + ["h1_output_national"])
    args = ap.parse_args()
    out_dir = (_ROOT / "data" / "gold" / "parquet") if args.gold else _OUT

    if not _SRC.exists():
        print(f"ERROR: source missing: {_SRC}")
        sys.exit(1)

    doc = fitz.open(str(_SRC))
    results = []
    for ind in args.indicators:
        if ind == "h1_output_national":
            df, col_meta = extract_h1_output_national(doc)
        else:
            df, col_meta = extract_indicator(doc, ind)
        rpt = fidelity_check(df, ind, col_meta)
        print(f"\n=== noac_{ind}_wide — {df.height} rows, {len([c for c in df.columns if c not in ('la','year')])} value cols ===")
        if col_meta:
            for m in col_meta:
                print(f"    {m['col']:48s} <- [{m['code']}] {m['label'][:60]} ({m['unit']})")
        for name, chk in rpt["checks"].items():
            mark = "GREEN" if chk.get("pass", True) else "FAIL "
            # keep printed dicts short
            short = {k: v for k, v in chk.items() if k != "per_col" and k != "median"}
            print(f"  [{mark}] {name}: {short}")
        print(f"  >>> {'GREEN' if rpt['green'] else 'AMBER'}")
        if args.write:
            path = out_dir / f"noac_{ind}_wide.parquet"
            _write_parquet(df, path)
            print(f"  Wrote {path.relative_to(_ROOT)} ({df.height} rows)")
        results.append((ind, df.height, rpt["green"]))
    doc.close()

    print("\n" + "=" * 64 + "\nSUMMARY")
    for ind, n, green in results:
        print(f"  {'GREEN' if green else 'AMBER'}  noac_{ind:24s} {n:>4} rows")
    if not args.write:
        print("\n(dry-run — pass --write to land eval parquet under _noac_eval/)")


if __name__ == "__main__":
    main()
