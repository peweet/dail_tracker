"""SI legal-state from the eISB Legislation Directory chronological tables.
A full multi-year extractor that writes the gold parquet + coverage JSON and
self-tests join coverage against gold. Lives in pipeline_sandbox/ (per the
sandbox rule) but is now a SHIPPING extractor: it owns the gold table
si_current_state.parquet, which sql_views/legislation_si_current_state.sql
(v_si_current_state) reads and legislation_si_index.sql LEFT-JOINs into
v_statutory_instruments. NOT yet wired into pipeline.py's iris chain (deferred);
re-run by hand on an eISB refresh.

The parsing contract (derive_state + affecting_sis) is locked by
test/test_si_legal_state.py — keep them in sync.

Output:
  data/gold/parquet/si_current_state.parquet
  data/_meta/si_current_state_coverage.json
HTML cached to: data/bronze/eisb_directory/

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/si_legislation_directory_extract.py
      (optional args: start_year end_year ; defaults to gold's range)
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

BASE = "https://www.irishstatutebook.ie/isbc"
HDRS = {"User-Agent": "dail-tracker research/enrichment (planning; contact via repo)"}
GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"
CACHE_DIR = ROOT / "data/bronze/eisb_directory"
OUT_PARQUET = ROOT / "data/gold/parquet/si_current_state.parquet"
OUT_COVERAGE = ROOT / "data/_meta/si_current_state_coverage.json"

# A provision marker scopes a verb down to part of an SI (e.g. "Reg. 2 revoked"
# = partial). It must carry the abbreviation dot (Reg./Art./Sch./s./para.) AND
# sit immediately before the verb — the pattern is end-anchored ($) because it's
# only ever .search()ed against the lookbehind window in derive_state(). The dot
# requirement + anchoring stop a bare year ("…2026 revoked") or an "S.I."/"SI"
# token from mis-scoping a WHOLE revocation as partial. See test_si_legal_state.
PROVISION_MARKER = re.compile(r"\b(reg|regs|art|arts|sch|para|paras|s|ss)\.\s*\d[\w()]*[\s,]*$", re.I)
SI_CITE = re.compile(r"S\.?I\.?\s*No\.?\s*(\d+)\s*of\s*(\d{4})", re.I)
ACT_CITE = re.compile(r"\b(?:No\.?\s*\d+\s*of\s*\d{4}|[A-Z][A-Za-z ]+Act\s+\d{4})")
UPDATED_TO = re.compile(r"Updated to\s+([0-9]{1,2}\s+\w+\s+\d{4})", re.I)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def fetch(url: str, cache_name: str | None = None) -> str:
    if cache_name:
        cp = CACHE_DIR / cache_name
        if cp.exists() and cp.stat().st_size > 500:
            return cp.read_text(encoding="utf-8", errors="ignore")
    r = requests.get(url, headers=HDRS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    txt = r.text
    if cache_name:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        (CACHE_DIR / cache_name).write_text(txt, encoding="utf-8")
        time.sleep(0.35)  # polite only on real fetch
    return txt


def range_urls(year: int) -> tuple[list[str], str | None]:
    html = fetch(f"{BASE}/si{year}.html", f"si{year}_index.html")
    soup = BeautifulSoup(html, "html.parser")
    upd = UPDATED_TO.search(soup.get_text(" ", strip=True))
    urls = []
    for a in soup.find_all("a", href=True):
        if re.search(rf"si{year}_\d+-\d+\.html$", a["href"]):
            urls.append(a["href"].split("/")[-1])
    return sorted(set(urls)), (upd.group(1) if upd else None)


def derive_state(how: str) -> str:
    """Classify the eISB 'How Affected' cell into a legal-state enum.

    IMPORTANT: pass ONLY the 'How Affected' text (column 2), never the
    'Affecting Provision' column (column 3). That column holds the *affecting*
    SI's own provisions (e.g. "S.I. No. 332 of 2025 , reg. 16") which, if mixed
    in, would mis-read a whole revocation as partial.

    Severity ladder (most severe wins for the headline state):
    revoked > partially_revoked > amended > in_force. A "revoked" verb counts as
    WHOLE unless a provision marker sits immediately before it (then partial).
    "Whole S.I. other than reg. X revoked" is the one whole-prefixed form that
    stays partial (reg. X survives) — handled by the marker rule, conservatively.

    Contract locked by test/test_si_legal_state.py.
    """
    h = (how or "").strip()
    if not h or h.lower().startswith("not affected"):
        return "in_force_as_made"
    low = h.lower()
    # split into sub-clauses and judge severity of each
    has_whole_revoke = False
    has_partial_revoke = False
    has_amend = False
    # iterate over "revoked"/"amended" occurrences, check what precedes each
    for m in re.finditer(r"\b(revoked|amended)\b", low):
        verb = m.group(1)
        pre = low[max(0, m.start() - 18):m.start()]
        scoped = bool(PROVISION_MARKER.search(pre))
        if verb == "revoked":
            if scoped:
                has_partial_revoke = True
            else:
                has_whole_revoke = True
        else:
            has_amend = True
    if has_whole_revoke:
        return "revoked"
    if has_partial_revoke and has_amend:
        return "amended_and_partially_revoked"
    if has_partial_revoke:
        return "partially_revoked"
    if has_amend:
        return "amended"
    return "other_affected"


def confidence_for(state: str, how: str) -> float:
    if state == "in_force_as_made":
        return 0.95
    if state == "revoked":
        # bare "Revoked" cell = very high; revoked-with-detail slightly lower
        return 0.92 if how.strip().lower() == "revoked" else 0.85
    if state in ("amended", "partially_revoked", "amended_and_partially_revoked"):
        return 0.8
    return 0.4


def affecting_sis(*texts: str) -> list[str]:
    found: set[str] = set()
    for t in texts:
        for n, y in SI_CITE.findall(t or ""):
            found.add(f"{int(n)}/{int(y)}")
    return sorted(found, key=lambda s: (int(s.split('/')[1]), int(s.split('/')[0])))


def eli_url(si_year: int, si_number: int) -> str:
    """Direct, citable eISB ELI link to an SI's made text — the 'confirm' link."""
    return f"https://www.irishstatutebook.ie/eli/{si_year}/si/{si_number}/made/en/html"


def affecting_urls(affecting: list[str]) -> list[str]:
    out = []
    for ref in affecting:
        num, yr = ref.split("/")
        out.append(eli_url(int(yr), int(num)))
    return out


def parse_table(html: str, year: int, src_url: str, updated_to: str | None) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tab in soup.find_all("table"):
        for tr in tab.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            m = re.match(r"^(\d{1,4})\b", cells[0].strip())
            if not m:
                continue
            si_no = int(m.group(1))
            title = cells[1] if len(cells) > 1 else ""
            how = cells[2] if len(cells) > 2 else ""
            affecting = cells[3] if len(cells) > 3 else ""
            state = derive_state(how)
            aff = affecting_sis(how, affecting)
            out.append({
                "si_id": f"{year}-{si_no:03d}",
                "si_year": year,
                "si_number": si_no,
                "directory_title": title[:160],
                "current_state": state,
                "affecting_sis": aff,
                "affecting_si_urls": affecting_urls(aff),   # confirm: read the revoking/amending SI
                "this_si_eli_url": eli_url(year, si_no),     # confirm: this SI's own made text
                "how_affected_raw": (how + (" || " + affecting if affecting else ""))[:500],
                "state_source": "eISB Legislation Directory",
                "state_source_url": src_url,                 # confirm: the directory row itself
                "directory_updated_to": updated_to,
                "confidence": confidence_for(state, how),
            })
    return out


def main() -> None:
    gold = pl.read_parquet(GOLD)
    y0 = int(sys.argv[1]) if len(sys.argv) > 1 else int(gold["si_year"].min())
    y1 = int(sys.argv[2]) if len(sys.argv) > 2 else int(gold["si_year"].max())
    hr(f"CRAWL eISB Legislation Directory {y0}..{y1}")

    rows: list[dict] = []
    updated_map: dict[int, str | None] = {}
    for year in range(y0, y1 + 1):
        try:
            names, upd = range_urls(year)
            updated_map[year] = upd
            for name in names:
                html = fetch(f"{BASE}/{name}", name)
                rows += parse_table(html, year, f"{BASE}/{name}", upd)
            print(f"  {year}: {len(names)} pages, updated_to={upd}")
        except Exception as e:
            print(f"  {year}: ERROR {e!r}")

    df = pl.DataFrame(rows).unique(subset=["si_year", "si_number"], keep="first")
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)
    hr("WROTE PARQUET")
    print(f"{OUT_PARQUET}  ({df.height:,} rows)")

    hr("STATE DISTRIBUTION (all years)")
    print(df.group_by("current_state").len().sort("len", descending=True))

    # ---- self-test: join coverage vs gold -------------------------------
    g = gold.select(["si_id", "si_year", "si_number", "si_title"])
    j = g.join(df.select(["si_year", "si_number", "current_state"]), on=["si_year", "si_number"], how="left")
    matched = j.filter(pl.col("current_state").is_not_null()).height
    hr("SELF-TEST: gold join coverage")
    print(f"gold SIs           : {g.height:,}")
    print(f"matched to directory: {matched:,}  ({matched / g.height:.1%})")
    by_year = (
        j.group_by("si_year")
        .agg(pl.len().alias("gold"), pl.col("current_state").is_not_null().sum().alias("matched"))
        .with_columns((pl.col("matched") / pl.col("gold")).alias("cov"))
        .sort("si_year")
    )
    print(by_year)
    not_made = j.filter(pl.col("current_state").is_in(
        ["revoked", "partially_revoked", "amended", "amended_and_partially_revoked"])).height
    print(f"gold SIs with a non-'made' legal state: {not_made:,}  ({not_made / g.height:.1%})")

    # ---- coverage JSON ---------------------------------------------------
    cov = {
        "as_of_years": [y0, y1],
        "directory_pages_updated_to": {str(k): v for k, v in updated_map.items()},
        "rows": df.height,
        "state_distribution": {r["current_state"]: r["len"] for r in df.group_by("current_state").len().iter_rows(named=True)},
        "gold_si_count": g.height,
        "gold_join_coverage_pct": round(100 * matched / g.height, 2),
        "gold_with_non_made_state": not_made,
        "source": "eISB Legislation Directory chronological tables (isbc/siYYYY_*.html)",
        "caveat": "Discovery/indexing only — verify the official eISB entry before legal reliance. "
                  "Whole vs provision-level revocation is heuristic from the 'How Affected' column.",
    }
    OUT_COVERAGE.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"\nwrote coverage: {OUT_COVERAGE}")

    hr("SAMPLES: whole-revoked + partially_revoked + amended")
    for st in ["revoked", "partially_revoked", "amended", "amended_and_partially_revoked"]:
        s = df.filter(pl.col("current_state") == st).head(2)
        for r in s.iter_rows(named=True):
            print(f"  [{st}] {r['si_id']} {r['directory_title'][:50]!r}  affecting={r['affecting_sis']}")
            print(f"        how: {r['how_affected_raw'][:110]}")
            print(f"        confirm (this SI): {r['this_si_eli_url']}")
            print(f"        confirm (directory row): {r['state_source_url']}")
            if r["affecting_si_urls"]:
                print(f"        confirm (affecting SI): {r['affecting_si_urls'][0]}")


if __name__ == "__main__":
    main()
