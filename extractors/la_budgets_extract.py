"""INGEST: ADOPTED local-authority budgets — DHLGH consolidated Local Authority Budget
publications → (council, year, division) BUDGETED expenditure/income for ALL 31 councils.

One national publication per year (gov.ie "local-authority-budgets" collection, 2019-2026)
carries every council's adopted revenue budget by service division at sub-service granularity
— so ONE parser over ~8 national PDFs replaces 31 bespoke council budget-book scrapes (several
of which are scanned, e.g. Kerry's book). Divisions A-H map 1:1 onto the AFS service-division
taxonomy, so this fact joins la_afs_divisions for budget-vs-actual per (council, year, division).

⚠️ FOURTH MONEY GRAIN — realisation_tier='BUDGETED', value_kind='budget_adopted',
value_safe_to_sum=False on every row. An adopted budget is a PLAN: never union or sum it with
the SPENT/COMMITTED payment grains or the AFS accounts grains. Compare side-by-side only
(v_procurement_la_budget_vs_actual sets them beside each other, never adds).

RECONCILE GATES (clean-by-construction, same discipline as the AFS facts):
  1. HARD per-column gate: every table unit prints a Total row; a unit is admitted only if,
     for EVERY numeric column, Σ(council values) equals the printed total (≤ €5). This
     validates row/column alignment against the document's own arithmetic.
  2. ADVISORY summary cross-check: Σ divisions per council vs the publication's own
     summary-table gross expenditure, recorded in coverage as a delta (the summary's
     "*Expenditure" can include items outside Divisions A-H, so it is not a hard drop).

Run:
  ./.venv/Scripts/python.exe extractors/la_budgets_extract.py               # all years
  ./.venv/Scripts/python.exe extractors/la_budgets_extract.py --years 2026  # one year
"""

from __future__ import annotations

import argparse
import contextlib
import re
import subprocess
import sys
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import config  # noqa: E402
from services.coverage_io import save_coverage  # noqa: E402
from services.extract_runner import run_extractor  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

CACHE = config.BRONZE_PDF_DIR / "la_budgets"
OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_budget_divisions.parquet"
OUT_COV = ROOT / "data/_meta/la_budgets_coverage.json"

# One consolidated DHLGH publication per adopted-budget year (gov.ie collection
# "local-authority-budgets"; assets.gov.ie serves to a browser UA — gov.ie WAF blocks
# datacenter/plain-python fetches, so refresh runs from the local box only).
URLS: dict[int, str] = {
    2019: "https://assets.gov.ie/static/documents/local-authority-budget-2019-177-mb.pdf",
    2020: "https://assets.gov.ie/static/documents/local-authority-budget-2020.pdf",
    2021: "https://assets.gov.ie/static/documents/local-authority-budget-2021.pdf",
    2022: "https://assets.gov.ie/static/documents/local-authority-budgets-2022.pdf",
    2023: "https://assets.gov.ie/static/documents/local-authority-budgets-2023.pdf",
    2024: "https://assets.gov.ie/static/documents/local-authority-budgets-2024.pdf",
    2025: "https://assets.gov.ie/static/documents/FINAL_Local_Authority_Budget_Publication_2025.pdf",
    2026: "https://assets.gov.ie/static/documents/82d163b3/FINAL_Local_Authority_Budget_Publication_2026.docx.pdf",
}

# Service Division A-H → the AFS fact's canonical division strings (keyword-keyed because the
# publication's wording drifts across editions: 'Road Transportation and Safety' vs the AFS
# 'Roads, Transportation and Safety'; 'Environmental Protection' vs 'Environmental Services').
DIV_CANON: list[tuple[str, str]] = [
    ("housing", "Housing and Building"),
    ("road", "Roads, Transportation and Safety"),
    ("water", "Water Services"),
    ("develop", "Development Management"),
    ("environ", "Environmental Services"),
    ("recreation", "Recreation and Amenity"),
    ("agricult", "Agriculture, Education, Health & Welfare"),
    ("miscellaneous", "Miscellaneous Services"),
]

# The 31 canonical council names (must match la_afs_divisions / crosswalk spelling exactly —
# plain-ASCII DLR, no 'County Council' suffixes).
CANON_31 = {
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal", "Dublin City",
    "Dun Laoghaire-Rathdown", "Fingal", "Galway City", "Galway County", "Kerry", "Kildare",
    "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth", "Mayo", "Meath",
    "Monaghan", "Offaly", "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
}

NUM_RE = re.compile(r"^\(?-?\d[\d,]*\)?$")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def _fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


# suffix words the publication appends (and abbreviates) after the council name proper —
# observed across editions: 'County Council', 'City and County Council', 'County Co',
# 'City & Co Co', 'Cou Co', 'DLR', 'Sth Dublin'. Prefix-matching the canonical name and
# allowing only these as the remainder kills the whole abbreviation class in one place.
_SUFFIX_JUNK = {"county", "city", "council", "co", "cou", "and", "&"}
_CANON_LOOKUP = sorted(CANON_31, key=len, reverse=True)  # longest first: 'Cork County' before any 'Cork…' prefix clash


def canon_council(name: str) -> str | None:
    """'Dún Laoghaire-Rathdown Cou Co' → 'Dun Laoghaire-Rathdown'; None if not a council.
    A label is a council iff it STARTS WITH a canonical name and everything after it is
    council-suffix boilerplate (however abbreviated this edition felt like printing it)."""
    n = _fold(re.sub(r"\s+", " ", name)).strip(" .")
    n = re.sub(r"^DLR\b", "Dun Laoghaire-Rathdown", n)
    n = re.sub(r"^Sth\.?\b", "South", n)
    low = n.lower().replace("-", " ")
    for canon in _CANON_LOOKUP:
        cl = canon.lower().replace("-", " ")
        if low == cl or low.startswith(cl + " "):
            rest = low[len(cl):].replace(".", " ").split()
            if all(t in _SUFFIX_JUNK for t in rest):
                return canon
    return None


def to_num(tok: str) -> float | None:
    tok = tok.strip()
    if not NUM_RE.match(tok):
        return None
    neg = tok.startswith("(") and tok.endswith(")")
    v = float(tok.strip("()").replace(",", "").replace("-", "-") or 0)
    return -v if neg else v


def fetch(url: str, dest: Path) -> Path | None:
    if dest.exists() and dest.stat().st_size > 100_000:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    body = b""
    with contextlib.suppress(Exception):
        req = Request(url, headers={"User-Agent": UA})
        body = urlopen(req, timeout=90).read()
    if body[:4] != b"%PDF":  # WAF fallback
        with contextlib.suppress(Exception):
            p = subprocess.run(
                ["curl", "-sS", "-k", "-L", "--max-time", "120", "-A", UA, url],
                capture_output=True, timeout=150, check=False,
            )
            body = p.stdout
    if body[:4] != b"%PDF":
        return None
    dest.write_bytes(body)
    return dest


# ── page geometry ──────────────────────────────────────────────────────────────────────────────
def page_rows(page) -> list[list[tuple[float, float, str]]]:
    """Words y-clustered into visual rows (±3px), each row x-sorted, as (x0, x1, word). A
    wrapped council name (name-only row directly above its numbers row) is merged forward."""
    words = sorted(page.get_text("words"), key=lambda w: (w[1], w[0]))
    rows: list[tuple[float, list[tuple[float, float, str]]]] = []
    for x0, y0, x1, _y1, w, *_ in words:
        if rows and abs(rows[-1][0] - y0) <= 3:
            rows[-1][1].append((x0, x1, w))
        else:
            rows.append((y0, [(x0, x1, w)]))
    out: list[list[tuple[float, float, str]]] = []
    name_buf: list[tuple[float, float, str]] = []
    for _y, items in rows:
        items = sorted(items)
        has_num = any(to_num(w) is not None for _a, _b, w in items)
        if not has_num:
            name_buf = items if canon_council(" ".join(w for _a, _b, w in items)) else []
            if name_buf:
                continue
        if name_buf and has_num:
            items = name_buf + items
            name_buf = []
        out.append(items)
    return out


def split_row(items: list[tuple[float, float, str]]) -> tuple[str, list[tuple[float, float]]]:
    """Leading alpha tokens = label; numeric tokens = (right_edge_x, value). The right edge
    keys column identity — the tables right-align numbers, so x1 is stable per column while
    x0 wanders with digit count. Editions ≤2023 print truly EMPTY cells (no '0'), so values
    must be mapped to columns by position, never by order."""
    label_toks, vals = [], []
    for _x0, x1, t in items:
        v = to_num(t)
        if v is None and not vals:
            label_toks.append(t)
        elif v is not None:
            vals.append((x1, v))
        # alpha token AFTER numbers (footnote letters) — ignore
    return " ".join(label_toks), vals


def column_anchors(rows: list[list[tuple[float, float]]], gap: float = 14.0) -> list[float]:
    """Consensus column positions for one table unit: cluster every numeric right-edge across
    ALL its rows (numbers are right-aligned, so x1 is stable per column; deriving anchors from
    the whole unit rather than the Total row alone survives the bold/offset Total typography
    some editions use). Clusters separated by <gap px merge; anchor = cluster mean."""
    xs = sorted(x1 for row in rows for x1, _v in row)
    if not xs:
        return []
    clusters: list[list[float]] = [[xs[0]]]
    for x in xs[1:]:
        if x - clusters[-1][-1] <= gap:
            clusters[-1].append(x)
        else:
            clusters.append([x])
    return [sum(c) / len(c) for c in clusters]


def align_to_anchors(vals: list[tuple[float, float]], anchors: list[float], tol: float = 25.0) -> list[float] | None:
    """Map (x1, value) pairs onto the unit's column anchors; absent cells → 0.0. None if any
    value lands >tol from every anchor or two values claim one column."""
    out = [0.0] * len(anchors)
    claimed = [False] * len(anchors)
    for x1, v in vals:
        k = min(range(len(anchors)), key=lambda i: abs(anchors[i] - x1))
        if abs(anchors[k] - x1) > tol or claimed[k]:
            return None
        out[k] = v
        claimed[k] = True
    return out


def parse_year(pdf: Path, year: int, url: str) -> tuple[list[dict], dict]:
    doc = fitz.open(pdf)
    division = None  # canonical division of the section being read
    acc: dict[tuple[str, str], list[float]] = {}  # (council, division) -> [exp, inc] from sub-service cols
    div_total: dict[tuple[str, str], tuple[float, float]] = {}  # from the printed 'Total for Service Division' pair
    seen_divs: set[str] = set()
    subs_failed: set[str] = set()  # divisions whose sub-service accumulation is incomplete
    div_total_ok: set[str] = set()  # divisions whose printed total pair passed its column gate
    unit: dict[str, list[float]] | None = None  # council -> (x1, value) pairs, current table unit
    unit_div: str | None = None
    unit_has_total_col = False
    stat = {"year": year, "units_ok": 0, "units_dropped": 0, "drop_pages": []}

    def close_unit(total_pairs: list[tuple[float, float]], page_no: int) -> None:
        nonlocal unit
        if unit is None or unit_div is None:
            unit = None
            return
        # Two independent row→column strategies, each validated by the same exact per-column
        # printed-total gate (a false pass would require the column sums to coincide — not a
        # real risk). ORDER-based handles most editions; POSITIONAL (right-edge anchors) rescues
        # editions that print truly empty cells (≤2023), where order slips a column.
        total_vals = [v for _x1, v in total_pairs]
        ncols = len(total_vals)

        def col_passes(rows: dict[str, list[float]], totals: list[float]) -> list[bool]:
            if len(rows) < 25:
                return [False] * len(totals)
            return [abs(sum(v[k] for v in rows.values()) - totals[k]) <= 5 for k in range(len(totals))]

        rows_ok: dict[str, list[float]] = {
            c: [v for _x1, v in pairs] for c, pairs in unit.items() if len(pairs) == ncols
        }
        passes = col_passes(rows_ok, total_vals)
        if not all(passes):  # positional fallback — keep whichever strategy passes more columns
            anchors = column_anchors([*unit.values(), total_pairs])
            aligned_total = align_to_anchors(total_pairs, anchors)
            if aligned_total is not None and len(aligned_total) % 2 == 0:
                pos_rows: dict[str, list[float]] = {}
                for c, pairs in unit.items():
                    aligned = align_to_anchors(pairs, anchors)
                    if aligned is not None:
                        pos_rows[c] = aligned
                pos_passes = col_passes(pos_rows, aligned_total)
                if sum(pos_passes) > sum(passes):
                    rows_ok, total_vals, ncols, passes = pos_rows, aligned_total, len(aligned_total), pos_passes
        # Gate per (Expenditure, Income) column PAIR, not per unit: a misprint in one printed
        # column (the 2020 edition's division-total column omits a cell its own grand total
        # includes) must not discard the five sub-service columns that reconcile exactly.
        if ncols % 2:
            passes = [False] * ncols
        pair_fail = False
        for p in range(0, ncols, 2):
            pair_ok = passes[p] and passes[p + 1]
            is_total_pair = unit_has_total_col and p == ncols - 2
            if pair_ok:
                for c, v in rows_ok.items():
                    if is_total_pair:
                        div_total[(c, unit_div)] = (v[p], v[p + 1])
                    else:
                        slot = acc.setdefault((c, unit_div), [0.0, 0.0])
                        slot[0] += v[p]
                        slot[1] += v[p + 1]
                if is_total_pair:
                    div_total_ok.add(unit_div)
            else:
                pair_fail = True
                if not is_total_pair:
                    subs_failed.add(unit_div)  # accumulation now incomplete for this division
        if pair_fail or ncols == 0:
            stat["units_dropped"] += 1
            stat["drop_pages"].append(page_no + 1)
        else:
            stat["units_ok"] += 1
        unit = None

    summary_gross: dict[str, float] = {}
    in_summary = False
    for i in range(doc.page_count):
        text = doc[i].get_text("text")
        flat = re.sub(r"\s+", " ", text)
        m = re.search(r"Service Division\s+[A-H]\s+([A-Za-z ,&/]+)", flat)
        if m and "REVENUE EXPENDITURE" not in flat[:200]:
            division = next((canon for kw, canon in DIV_CANON if kw in m.group(1).lower()), division)
            in_summary = False
            continue
        if re.search(r"ANNUAL RATE ON VALUATION", flat[:220]):
            in_summary = True
        elif re.search(r"REVENUE EXPENDITURE AND INCOME", flat[:120]):
            in_summary = False
            if unit is not None:  # header page starts a new unit; previous never closed → drop
                stat["units_dropped"] += 1
                stat["drop_pages"].append(i)
                if unit_div:
                    subs_failed.add(unit_div)  # a lost unit = missing sub-services
                unit = None
            if division:
                unit, unit_div = {}, division
                seen_divs.add(division)
                unit_has_total_col = bool(re.search(r"Total\s+for\s+Service\s+Division", flat, re.I))
        for items in page_rows(doc[i]):
            label, vals = split_row(items)
            council = canon_council(label)
            if in_summary:
                if council and vals:
                    summary_gross.setdefault(council, vals[0][1])
                continue
            if unit is None:
                continue
            if label.strip().lower().startswith("total") and vals:
                close_unit(vals, i)
            elif council and vals:
                unit[council] = vals
    doc.close()

    # Per (council, division): the printed 'Total for Service Division' pair is authoritative
    # where its column gate passed; otherwise fall back to the sub-service accumulation — but
    # ONLY when every sub-service pair of that division passed (a partial accumulation would
    # silently undercount). Divisions satisfying neither are dropped whole and reported.
    final: dict[tuple[str, str], tuple[float, float]] = {}
    for key, tot in div_total.items():
        final[key] = tot
    for (c, d), (exp, inc) in acc.items():
        if (c, d) not in final and d not in div_total_ok and d not in subs_failed:
            final[(c, d)] = (exp, inc)
    # advisory: where BOTH exist and accumulation is complete, the two should agree
    xchk = [
        abs(acc[k][0] - div_total[k][0])
        for k in div_total
        if k in acc and k[1] not in subs_failed
    ]
    stat["subservice_vs_total_max_delta_eur"] = round(max(xchk), 2) if xchk else None
    stat["divisions_failed"] = sorted(seen_divs - {d for _c, d in final})
    rows = [
        {
            "council": c,
            "year": year,
            "division": d,
            "expenditure_adopted": exp,
            "income_adopted": inc,
            "source_url": url,
            "realisation_tier": "BUDGETED",
            "value_kind": "budget_adopted",
            "value_safe_to_sum": False,
            "parser": "geom",
        }
        for (c, d), (exp, inc) in sorted(final.items())
    ]
    # advisory cross-check vs the publication's own summary table
    per_c: dict[str, float] = {}
    for (c, _d), (exp, _inc) in final.items():
        per_c[c] = per_c.get(c, 0.0) + exp
    stat["councils"] = len(per_c)
    stat["divisions"] = len({d for _c, d in final})
    stat["summary_delta_pct"] = {
        c: round(100 * (per_c[c] - summary_gross[c]) / summary_gross[c], 2)
        for c in sorted(per_c)
        if summary_gross.get(c)
    }
    return rows, stat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="", help="comma-separated years (default: all)")
    args = ap.parse_args()
    years = [int(y) for y in args.years.split(",") if y.strip()] or sorted(URLS)

    all_rows, stats = [], []
    for year in years:
        url = URLS[year]
        p = fetch(url, CACHE / f"{year}.pdf")
        if not p:
            stats.append({"year": year, "status": "download-fail(WAF?)"})
            print(f"  {year}: download failed")
            continue
        rows, stat = parse_year(p, year, url)
        all_rows.extend(rows)
        stats.append(stat)
        print(
            f"  {year}: councils={stat.get('councils', 0)}/31 divisions={stat.get('divisions', 0)}/8 "
            f"units {stat['units_ok']} ok / {stat['units_dropped']} dropped"
        )

    if not all_rows:
        print("no rows extracted")
        return
    df = pl.DataFrame(all_rows).sort(["year", "council", "division"])
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_PARQUET)
    print(f"\n  rows: {df.height} | councils: {df['council'].n_unique()} | years: {sorted(df['year'].unique())}")
    cov = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "grain": "adopted budget (BUDGETED) by (council, year, division) — DHLGH consolidated publication",
        "caveat": "A plan, not spend. NEVER union/sum with payment (SPENT/COMMITTED) or AFS accounts "
        "grains; compare side-by-side only. Per-column printed-total gate enforced per table unit; "
        "summary_delta_pct is the advisory gap vs the publication's own summary gross.",
        "by_year": stats,
    }
    save_coverage(cov, OUT_COV)
    print(f"  wrote {OUT_PARQUET}\n        {OUT_COV}")


if __name__ == "__main__":
    run_extractor(main)
