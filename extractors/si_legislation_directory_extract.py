"""SI legal-state from the eISB Legislation Directory chronological tables.
A full multi-year extractor that writes the gold parquet + coverage JSON and
self-tests join coverage against gold. Lives in extractors/ (per the
sandbox rule) but is now a SHIPPING extractor: it owns the gold table
si_current_state.parquet, which sql_views/legislation/legislation_si_current_state.sql
(v_si_current_state) reads and legislation_si_index.sql LEFT-JOINs into
v_statutory_instruments. Wired into the iris chain as iris_refresh.step_si_legal_state
(runs under `python pipeline.py --select iris`).

Freshness: runs are freshness-gated by default — each year INDEX is re-fetched
to read its current "Updated to" date, and a year's range pages are re-crawled
only when that date has moved since the last run (recorded in the coverage JSON).
Steady-state re-runs are therefore near-instant (≈11 index requests, no full
crawl) yet pick up new revocations the day eISB publishes them. `--offline`
serves entirely from cache (fast dev/test).

The parsing contract (derive_state + affecting_sis) is locked by
test/test_si_legal_state.py — keep them in sync.

Output:
  data/gold/parquet/si_current_state.parquet
  data/_meta/si_current_state_coverage.json
HTML cached to: data/bronze/eisb_directory/

Run:  ./.venv/Scripts/python.exe extractors/si_legislation_directory_extract.py
      # full gold-range sweep, merge-on-write (the pipeline's call)
        ... --year 2014            # backfill one year, merged into the parquet
        ... --year 2012 2013 2014  # backfill several years (mirrors the Iris poller)
        ... 2016 2026              # explicit range
        ... --offline              # cache-only, no freshness re-check
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

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


def fetch(url: str, cache_name: str | None = None, *, force: bool = False) -> str:
    if cache_name and not force:
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


def range_urls(year: int, *, force_index: bool = False) -> tuple[list[str], str | None]:
    html = fetch(f"{BASE}/si{year}.html", f"si{year}_index.html", force=force_index)
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
        pre = low[max(0, m.start() - 18) : m.start()]
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
    return sorted(found, key=lambda s: (int(s.split("/")[1]), int(s.split("/")[0])))


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
            out.append(
                {
                    "si_id": f"{year}-{si_no:03d}",
                    "si_year": year,
                    "si_number": si_no,
                    "directory_title": title[:160],
                    "current_state": state,
                    "affecting_sis": aff,
                    "affecting_si_urls": affecting_urls(aff),  # confirm: read the revoking/amending SI
                    "this_si_eli_url": eli_url(year, si_no),  # confirm: this SI's own made text
                    "how_affected_raw": (how + (" || " + affecting if affecting else ""))[:500],
                    "state_source": "eISB Legislation Directory",
                    "state_source_url": src_url,  # confirm: the directory row itself
                    "directory_updated_to": updated_to,
                    "confidence": confidence_for(state, how),
                }
            )
    return out


def _prior_updated_map() -> dict[str, str | None]:
    """Last run's per-year 'Updated to' dates, from the coverage JSON. Drives the
    freshness gate: a year whose directory date hasn't moved is served from
    cache. Missing/unreadable coverage → empty map → full re-crawl."""
    if OUT_COVERAGE.exists():
        try:
            return json.loads(OUT_COVERAGE.read_text(encoding="utf-8")).get("directory_pages_updated_to", {})
        except Exception:
            return {}
    return {}


def _merge_years(new_df: pl.DataFrame, crawl_years: list[int]) -> pl.DataFrame:
    """Merge freshly-crawled years into the existing gold parquet instead of
    clobbering it. Rows for the crawled years are REPLACED; every other year
    already on disk is preserved. This makes a subset run (``--year 2014``) a
    true backfill that *extends* coverage, while the default full-range run
    behaves exactly like a full replace (it crawls every gold year, so nothing
    older survives to keep — unless gold's floor later rises, in which case the
    older years are correctly retained)."""
    crawled = set(crawl_years)
    if OUT_PARQUET.exists():
        old = pl.read_parquet(OUT_PARQUET)
        kept = old.filter(~pl.col("si_year").is_in(list(crawled)))
        merged = pl.concat([kept, new_df], how="diagonal_relaxed") if new_df.height else kept
    else:
        merged = new_df
    return merged.unique(subset=["si_year", "si_number"], keep="first").sort(["si_year", "si_number"])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("start_year", nargs="?", type=int, help="first SI year of a range (default: gold min)")
    ap.add_argument("end_year", nargs="?", type=int, help="last SI year of a range (default: gold max)")
    ap.add_argument(
        "--year",
        nargs="+",
        type=int,
        metavar="YYYY",
        help="crawl specific year(s) and merge them into the existing parquet "
        "(backfill). Mirrors iris_oifigiuil_poller --year; takes precedence over "
        "the positional range. e.g. --year 2014  /  --year 2012 2013 2014",
    )
    ap.add_argument(
        "--offline",
        action="store_true",
        help="serve entirely from cached HTML — skip the freshness re-check (fast dev/test re-run)",
    )
    args = ap.parse_args()

    gold = pl.read_parquet(GOLD)
    if args.year:
        crawl_years = sorted(set(args.year))
    else:
        y0 = args.start_year if args.start_year is not None else int(gold["si_year"].min())
        y1 = args.end_year if args.end_year is not None else int(gold["si_year"].max())
        crawl_years = list(range(y0, y1 + 1))

    # Freshness gate (default; --offline bypasses it). Each year INDEX is
    # re-fetched (1 cheap request/year) to read its current "Updated to" date;
    # a year's range pages are re-crawled only when that date has moved since
    # the last run. This keeps a pipeline re-run near-instant in steady state
    # yet picks up new revocations/amendments the day eISB publishes them.
    prior = _prior_updated_map()
    mode = "offline (cache only)" if args.offline else "freshness-gated"
    span = f"{crawl_years[0]}..{crawl_years[-1]}" if crawl_years else "(none)"
    hr(f"CRAWL eISB Legislation Directory {span}  ({len(crawl_years)} year(s), {mode})")

    rows: list[dict] = []
    updated_map: dict[int, str | None] = {}
    for year in crawl_years:
        try:
            names, upd = range_urls(year, force_index=not args.offline)
            updated_map[year] = upd
            # Re-crawl this year's range pages when its directory date moved
            # (or we've no prior record). Offline always trusts the cache.
            changed = (not args.offline) and (str(upd) != str(prior.get(str(year))))
            for name in names:
                html = fetch(f"{BASE}/{name}", name, force=changed)
                rows += parse_table(html, year, f"{BASE}/{name}", upd)
            print(f"  {year}: {len(names)} pages, updated_to={upd} [{'refetched' if changed else 'cache'}]")
        except Exception as e:
            print(f"  {year}: ERROR {e!r}")

    new_df = pl.DataFrame(rows).unique(subset=["si_year", "si_number"], keep="first") if rows else pl.DataFrame()
    df = _merge_years(new_df, crawl_years)
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_PARQUET)
    hr("WROTE PARQUET (merged)")
    print(f"{OUT_PARQUET}  ({df.height:,} rows; crawled {len(crawl_years)} year(s), {new_df.height:,} rows refreshed)")

    hr("STATE DISTRIBUTION (all years)")
    print(df.group_by("current_state").len().sort("len", descending=True))

    # ---- self-test: join coverage vs gold (over the MERGED frame) -------
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
    not_made = j.filter(
        pl.col("current_state").is_in(["revoked", "partially_revoked", "amended", "amended_and_partially_revoked"])
    ).height
    print(f"gold SIs with a non-'made' legal state: {not_made:,}  ({not_made / g.height:.1%})")

    # ---- coverage JSON ---------------------------------------------------
    # directory_pages_updated_to merges this run's crawled years over the prior
    # map, so years we DIDN'T crawl keep their last-known dates (the freshness
    # gate on the next run still works for them). as_of_years reflects the full
    # merged span, not just what this run touched.
    merged_updated: dict[str, str | None] = {str(k): v for k, v in prior.items()}
    for y, upd in updated_map.items():
        merged_updated[str(y)] = upd
    yrs_all = sorted(int(x) for x in df["si_year"].unique().to_list()) if df.height else crawl_years
    cov = {
        "as_of_years": [yrs_all[0], yrs_all[-1]] if yrs_all else None,
        "last_crawled_years": crawl_years,
        "directory_pages_updated_to": merged_updated,
        "rows": df.height,
        "state_distribution": {
            r["current_state"]: r["len"] for r in df.group_by("current_state").len().iter_rows(named=True)
        },
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
