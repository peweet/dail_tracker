"""NOAC scorecard — MULTI-YEAR history extractor (layout-robust, header-driven).

The single-year extractor (noac_scorecard_extract.py) keys each metric off a fixed page
index — fine for one report, useless across years because every annual report paginates
differently and the indicator set grows (44 indicators in 2022, 46 in 2024). This extractor
instead LOCATES each metric by a predicate over the table's column header text, so it works
on any year's report. It powers the trend sparklines on the council dossier.

Reads : the NOAC PI report PDFs in doc/source_pdfs/ (one per year, born-digital).
Writes: data/gold/parquet/noac_scorecard_history.parquet  (la x year x the 7 metrics).

Coverage is honest: where a year's report phrases a header too differently (or omits an
indicator), that (metric, year) cell is simply absent — never guessed. Per-year coverage
is printed and saved to data/_meta/noac_scorecard_history_coverage.json.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

DEST = ROOT / "data/gold/parquet/noac_scorecard_history.parquet"
COV = ROOT / "data/_meta/noac_scorecard_history_coverage.json"

# (year, pdf filename, source url). Add a year by dropping its PDF here — nothing else changes.
REPORTS = [
    (2024, "NOAC_LA_PerfInd_2024.pdf",
     "https://cdn.noac.ie/wp-content/uploads/2025/09/NOAC-Local-Authority-Performance-Indicator-Report-2024.pdf"),
    (2023, "NOAC_PI_2023.pdf",
     "https://cdn.noac.ie/wp-content/uploads/2024/10/NOAC-PI-Report-2023-FINAL.pdf"),
    (2022, "NOAC_PI_2022.pdf",
     "https://cdn.noac.ie/wp-content/uploads/2023/10/20231009-NOAC-PI-Report-2022-FINAL.pdf"),
]

CANON = {
    "Carlow County", "Cavan County", "Clare County", "Cork City", "Cork County",
    "Donegal County", "Dublin City", "Dun Laoghaire-Rathdown", "Fingal County",
    "Galway City", "Galway County", "Kerry County", "Kildare County", "Kilkenny County",
    "Laois County", "Leitrim County", "Limerick City and County", "Longford County",
    "Louth County", "Mayo County", "Meath County", "Monaghan County", "Offaly County",
    "Roscommon County", "Sligo County", "South Dublin County", "Tipperary County",
    "Waterford City and County", "Westmeath County", "Wexford County", "Wicklow County",
}
def _squish(s: str) -> str:
    """Accent-fold + lowercase + drop every non-alphanumeric char, so council labels match
    regardless of footnote marks, hyphen spacing, or cell line-wraps ('Dún Laoghaire-\\nRathdown')."""
    folded = "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", folded.lower())


_CF = {_squish(n): n for n in CANON}


def _canon(raw: str) -> str | None:
    sq = _squish(raw)
    if sq == "dlr":
        return "Dun Laoghaire-Rathdown"
    return _CF.get(sq)


def _num(s: str) -> float | None:
    s = ("" if s is None else str(s)).strip()
    if not s or s in {"-", "n/a", "N/A", "*"}:
        return None
    neg = "(" in s or "-" in s
    d = re.sub(r"[^\d.]", "", s)
    if not d or d == ".":
        return None
    try:
        return -float(d) if neg else float(d)
    except ValueError:
        return None


def _has(*tokens):
    return lambda h: all(t in h for t in tokens)


def _not(*tokens):
    return lambda h: not any(t in h for t in tokens)


def _all(*preds):
    return lambda h: all(p(h) for p in preds)


# metric -> column-header predicate. Distinctive tokens that survive phrasing drift across
# years, with exclusions so the right column wins (e.g. local primary, not regional/secondary).
PREDICATES = {
    "revenue_balance_pct": _all(_has("total income"), _has("cumulative")),
    "m3_claims_per_capita_eur": _has("per capita", "settled claims"),
    "m4_central_mgmt_charge_pct": _has("central management charge", "percentage"),
    "sickness_certified_pct": _all(_has("sickness absence"), _has("medical"), _not("self")),
    "roads_poor_pct": _all(_has("primary road", "1-4"), _not("regional", "secondary", "tertiary")),
    "fire_within_10min_pct": _all(_has("within 10 minutes"), _has("fire"), _not("other emergency")),
    "litter_moderate_pct": _has("area moderately polluted"),
    "litter_significant_pct": _has("area significantly polluted"),
    "litter_grossly_pct": _has("area grossly polluted"),
}


def _locate(path: Path) -> dict[str, dict[str, float]]:
    doc = fitz.open(path)
    out: dict[str, dict[str, float]] = {k: {} for k in PREDICATES}
    for p in range(doc.page_count):
        try:
            tables = doc[p].find_tables().tables
        except Exception:
            continue
        for t in tables:
            rows = [[("" if c is None else str(c).replace("\n", " ").strip()) for c in r] for r in t.extract()]
            la_rows = [r for r in rows if r and _canon(r[0])]
            if len(la_rows) < 25 or not rows:
                continue
            ncol = max(len(r) for r in rows)
            h0, h1 = rows[0], (rows[1] if len(rows) > 1 else [])
            for ci in range(1, ncol):
                head = ((h0[ci] if ci < len(h0) else "") + " " + (h1[ci] if ci < len(h1) else "")).lower()
                for metric, pred in PREDICATES.items():
                    if not out[metric] and pred(head):
                        out[metric] = {_canon(r[0]): _num(r[ci]) for r in la_rows if ci < len(r) and _canon(r[0])}
    return out


def main() -> None:
    recs, coverage = [], {}
    for year, fname, url in REPORTS:
        path = ROOT / "doc/source_pdfs" / fname
        if not path.exists():
            print(f"  {year}: MISSING {fname} — skipped")
            continue
        loc = _locate(path)
        per = {}
        for la in CANON:
            rec = {"la": la, "year": year, "source_file_url": url}
            for m in ("revenue_balance_pct", "m3_claims_per_capita_eur", "m4_central_mgmt_charge_pct",
                      "sickness_certified_pct", "roads_poor_pct", "fire_within_10min_pct"):
                rec[m] = loc[m].get(la)
            lm, ls, lg = (loc["litter_moderate_pct"].get(la), loc["litter_significant_pct"].get(la),
                          loc["litter_grossly_pct"].get(la))
            rec["litter_problem_pct"] = None if lm is None and ls is None and lg is None else \
                (lm or 0) + (ls or 0) + (lg or 0)
            recs.append(rec)
            per[la] = rec
        coverage[str(year)] = {m: sum(1 for la in CANON if per[la][m] is not None)
                               for m in ("revenue_balance_pct", "m3_claims_per_capita_eur",
                                         "m4_central_mgmt_charge_pct", "sickness_certified_pct",
                                         "roads_poor_pct", "fire_within_10min_pct", "litter_problem_pct")}

    df = pl.DataFrame(recs).select(
        "la", "year", "revenue_balance_pct", "m3_claims_per_capita_eur", "m4_central_mgmt_charge_pct",
        "sickness_certified_pct", "roads_poor_pct", "fire_within_10min_pct", "litter_problem_pct",
        "source_file_url",
    ).sort(["la", "year"])
    save_parquet(df, DEST, min_rows=60)
    COV.write_text(json.dumps(coverage, indent=2), encoding="utf-8")
    print(f"wrote {DEST}  ({df.height} rows, {df['year'].n_unique()} years)")
    for yr, c in coverage.items():
        print(f"  {yr}: " + "  ".join(f"{m.split('_')[0]}={n}" for m, n in c.items()))


if __name__ == "__main__":
    main()
