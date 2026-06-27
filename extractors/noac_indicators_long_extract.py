"""NOAC — full indicator set (long) gold extractor: every per-LA series, raw values.

The scorecard surfaces ~7 headline metrics; the NOAC report actually publishes ~46
indicators / ~144 per-LA data series. This promotes ALL of them to a single long gold table
so they are queryable / exportable / surfaceable, feeding the "All NOAC indicators"
reference drill-down on the council dossier (one expander — the primary view is untouched).

Values are stored as the PUBLISHED RAW STRING ("€38.19", "06:13", "Yes", "12.3") so every
type renders correctly without per-column parsing; a numeric_value is added where it parses.

Reads : doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf  (born-digital, PyMuPDF find_tables)
Writes: data/gold/parquet/noac_indicators_long.parquet
        (local_authority, family, indicator_code, series_label, raw_value, numeric_value,
         source_page, deep_link, year)
"""
from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

SRC = ROOT / "doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf"
DEST = ROOT / "data/gold/parquet/noac_indicators_long.parquet"
PDF_URL = ("https://cdn.noac.ie/wp-content/uploads/2025/09/"
           "NOAC-Local-Authority-Performance-Indicator-Report-2024.pdf")

# noac label -> page `local_authority` key (same map as v_la_noac_scorecard), squish-matched.
_PAIRS = {
    "Carlow County": "Carlow", "Cavan County": "Cavan", "Clare County": "Clare",
    "Cork City": "Cork City", "Cork County": "Cork County", "Donegal County": "Donegal",
    "Dublin City": "Dublin City", "Dun Laoghaire-Rathdown": "Dun Laoghaire-Rathdown",
    "Fingal County": "Fingal", "Galway City": "Galway City", "Galway County": "Galway County",
    "Kerry County": "Kerry", "Kildare County": "Kildare", "Kilkenny County": "Kilkenny",
    "Laois County": "Laois", "Leitrim County": "Leitrim", "Limerick City and County": "Limerick",
    "Longford County": "Longford", "Louth County": "Louth", "Mayo County": "Mayo",
    "Meath County": "Meath", "Monaghan County": "Monaghan", "Offaly County": "Offaly",
    "Roscommon County": "Roscommon", "Sligo County": "Sligo", "South Dublin County": "South Dublin",
    "Tipperary County": "Tipperary", "Waterford City and County": "Waterford",
    "Westmeath County": "Westmeath", "Wexford County": "Wexford", "Wicklow County": "Wicklow",
}
FAMILY = {"H": "Housing", "R": "Roads", "W": "Water", "E": "Environment", "P": "Planning",
          "F": "Fire", "L": "Library", "Y": "Youth/Community", "C": "Corporate", "M": "Finance",
          "J": "Economic"}
CODE_RE = re.compile(r"\b([HRWEPFLYCMJ])\s?(\d{1,2})\b")


def _squish(s: str) -> str:
    folded = "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", folded.lower())


_SQ = {_squish(k): v for k, v in _PAIRS.items()}


def _la(raw: str) -> str | None:
    sq = _squish(raw)
    return "Dun Laoghaire-Rathdown" if sq == "dlr" else _SQ.get(sq)


def _clean(s) -> str:
    return re.sub(r"\s+", " ", ("" if s is None else str(s)).replace("\n", " ")).strip()


def _label(header: str) -> str:
    """Turn a raw NOAC column header into a readable series label."""
    s = _clean(header)
    # Strip enumerators ONLY when a real delimiter is present — 'A. ' / 'A) ' / 'B (b): '.
    # The delimiter ( or . must NOT be optional: an all-optional pattern matched a bare
    # leading capital + the next letter and ate the first chars of ordinary prose headers
    # ("Buildings…" -> "ildings…", "Net expenditure" -> "t expenditure").
    s = re.sub(r"^[A-Z]\s*[\(\.]\s*[a-z0-9]?\)?[:.]?\s*", "", s)    # 'A. ' / 'B (b): '
    s = re.sub(r"^[A-Z]\d{1,2}[\.\)]?\s+", "", s)                   # 'H1 ' / 'R2. ' code prefix
    s = re.sub(r"\s*(for|in|during|by|as at|to)\s*\d{0,2}/?\d{0,2}/?20\d\d.*$", "", s, flags=re.I)  # trailing dates
    s = re.sub(r"\s*\(?based on .*?census\)?", "", s, flags=re.I)
    return (s.strip(" .,-") or header)[:90]


def _numeric(raw: str) -> float | None:
    s = _clean(raw)
    if re.match(r"^\d{1,2}:\d{2}$", s):  # MM:SS -> minutes
        m = re.match(r"^(\d{1,2}):(\d{2})$", s)
        return round(int(m.group(1)) + int(m.group(2)) / 60, 3)
    neg = "(" in s or "-" in s
    d = re.sub(r"[^\d.]", "", s)
    if not d or d == ".":
        return None
    try:
        return -float(d) if neg else float(d)
    except ValueError:
        return None


def main() -> None:
    doc = fitz.open(SRC)
    recs = []
    for p in range(doc.page_count):
        txt = doc[p].get_text()
        try:
            tables = doc[p].find_tables().tables
        except Exception:
            continue
        codes = [a + b for a, b in CODE_RE.findall(txt[:800])]
        code = max(set(codes), key=codes.count) if codes else ""
        family = FAMILY.get(code[:1], "Other") if code else "Other"
        for t in tables:
            rows = [[_clean(c) for c in r] for r in t.extract()]
            la_rows = [r for r in rows if r and _la(r[0])]
            if len(la_rows) < 25 or not rows:
                continue
            ncol = max(len(r) for r in rows)
            h0, h1 = rows[0], (rows[1] if len(rows) > 1 else [])
            for ci in range(1, ncol):
                raw_head = h0[ci] if ci < len(h0) and h0[ci] else (h1[ci] if ci < len(h1) else "")
                label = _label(raw_head)
                for r in la_rows:
                    if ci >= len(r) or not r[ci]:
                        continue
                    recs.append({
                        "local_authority": _la(r[0]), "family": family, "indicator_code": code,
                        "series_label": label, "raw_value": r[ci], "numeric_value": _numeric(r[ci]),
                        "source_page": p + 1, "deep_link": f"{PDF_URL}#page={p + 1}", "year": 2024,
                    })
    df = pl.DataFrame(recs).unique(["local_authority", "family", "indicator_code", "series_label"]).sort(
        ["local_authority", "family", "indicator_code", "series_label"])
    save_parquet(df, DEST, min_rows=2000)
    print(f"wrote {DEST}  ({df.height} rows)")
    print(f"  series: {df.select(pl.struct('family','indicator_code','series_label')).n_unique()}  "
          f"councils: {df['local_authority'].n_unique()}  families: {sorted(df['family'].unique())}")


if __name__ == "__main__":
    main()
