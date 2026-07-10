"""CURATED-META INGEST: Local Property Tax — Local Adjustment Factor (LAF), all 31 LAs.

Each row is a whole-council annual money decision: under s.20 Finance (Local Property
Tax) Act 2012 the elected members may vary the basic LPT rate by up to +/-15% for the
following year (a RESERVED function — councillors vote it, the CE only executes).
Revenue publishes the adopted factor per local authority per year.

Source (live, current year): revenue.ie "Valuation bands and rates" page — the
"Local Adjustment Factor" table. Earlier years (2023-2025) are recovered best-effort
from web.archive.org snapshots of the predecessor page ("Determining your LPT charge",
2022-2025 era); a snapshot failing to fetch/parse degrades coverage, never the run.

Output: git-trackable data/_meta/lpt_local_adjustment_factors.csv
(local_authority, year, adjustment_pct, source_url, retrieved_date) — the
curated-meta pattern (cf. la_chief_executives.csv), read at runtime by
sql_views/constituency/constituency_la_lpt_adjustment.sql (v_la_lpt_adjustment).

local_authority is normalised to EXACTLY the 31 canonical names used by
data/_meta/constituency_la_crosswalk.csv ("Dun Laoghaire-Rathdown" no fada,
"Limerick"/"Waterford" without the "City & County" suffix, "Cork County" vs
"Cork City") and the run ASSERTS set-equality per year before writing.

Run:  ./.venv/Scripts/python.exe extractors/lpt_laf_extract.py
"""

from __future__ import annotations

import contextlib
import html as html_mod
import re
import subprocess
import sys
import unicodedata
from datetime import date
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

OUT_CSV = ROOT / "data" / "_meta" / "lpt_local_adjustment_factors.csv"
CROSSWALK = ROOT / "data" / "_meta" / "constituency_la_crosswalk.csv"

# revenue.ie fetches fine with a plain browser UA; curl fallback kept for parity with
# the gov.ie GOVIE_HEADERS precedent (some networks 403 python-requests).
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"}

LIVE_URL = "https://www.revenue.ie/en/property/local-property-tax/valuing-your-property/valuation-bands-rates.aspx"
# Immutable web.archive.org snapshots of the 2022-2025-era page carrying each year's
# adopted LAF table (verified to parse). Best-effort: failure skips the year.
ARCHIVE_URLS = {
    2023: "https://web.archive.org/web/20230115231340/https://www.revenue.ie/en/property/local-property-tax/valuing-your-property/determining-lpt-charge.aspx",
    2024: "https://web.archive.org/web/20240404131538/https://www.revenue.ie/en/property/local-property-tax/valuing-your-property/determining-lpt-charge.aspx",
    2025: "https://web.archive.org/web/20250121014850/https://www.revenue.ie/en/property/local-property-tax/valuing-your-property/determining-lpt-charge.aspx",
}

# header cell of the LAF table, e.g. "2023 increase or decrease on Base Rate",
# "2026 percentage increase or decrease on base rate"
YEAR_HDR = re.compile(r"(20\d{2})\s+(?:percentage\s+)?increase\s+or\s+decrease", re.I)
PCT = re.compile(r"^([+-]?)\s*(\d+(?:\.\d+)?)\s*%$")


def fetch(url: str) -> str | None:
    try:
        import requests

        r = requests.get(url, headers=H, timeout=90)
        if r.status_code == 200 and "<table" in r.text:
            return r.text
    except Exception:
        pass
    with contextlib.suppress(Exception):  # browser-UA curl fallback (GOVIE_HEADERS precedent)
        p = subprocess.run(
            ["curl", "-sS", "-L", "--max-time", "90", "-A", H["User-Agent"], url],
            capture_output=True,
            timeout=120,
            check=False,
        )
        text = p.stdout.decode("utf-8", errors="replace")
        if "<table" in text:
            return text
    return None


def canonical_la(raw: str) -> str:
    """Revenue table label -> canonical crosswalk local_authority (NFKD accent-fold)."""
    s = unicodedata.normalize("NFKD", html_mod.unescape(raw))
    s = s.encode("ascii", "ignore").decode()  # Dún -> Dun
    s = re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()
    if re.search(r"City\s*(?:&|and)\s*County Council$", s, re.I):  # Limerick, Waterford
        return re.sub(r"\s*City\s*(?:&|and)\s*County Council$", "", s, flags=re.I)
    if re.search(r"City Council$", s, re.I):  # Cork City, Dublin City, Galway City
        return re.sub(r"\s*Council$", "", s, flags=re.I)
    base = re.sub(r"\s*County Council$", "", s, flags=re.I)
    if base in {"Cork", "Galway"}:  # disambiguate from the city councils
        return f"{base} County"
    if re.fullmatch(r"Dun Laoghaire[- ]?Rathdown", base, re.I):
        return "Dun Laoghaire-Rathdown"
    return base


def parse_laf_table(page_html: str) -> tuple[int, list[tuple[str, float]]] | None:
    """Find the LAF table (header 'Local Authority' + '<year> increase or decrease')."""
    for tb in re.findall(r"<table.*?</table>", page_html, re.S):
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tb, re.S)
        if not rows:
            continue

        def cells(row_html: str) -> list[str]:
            return [
                html_mod.unescape(re.sub(r"<[^>]+>", " ", c)).replace("\xa0", " ").strip()
                for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.S)
            ]

        head = cells(rows[0])
        m = next((YEAR_HDR.search(h) for h in head if YEAR_HDR.search(h)), None)
        if not (m and any("local authority" in h.lower() for h in head)):
            continue
        year = int(m.group(1))
        out: list[tuple[str, float]] = []
        for r in rows[1:]:
            c = cells(r)
            if len(c) < 2:
                continue
            pm = PCT.match(re.sub(r"\s+", " ", c[1]).strip())
            if not pm:
                continue
            pct = float(pm.group(2)) * (-1.0 if pm.group(1) == "-" else 1.0)
            out.append((canonical_la(c[0]), pct))
        if out:
            return year, out
    return None


def main() -> None:
    canonical = set(pl.read_csv(CROSSWALK)["local_authority"].unique().to_list())
    assert len(canonical) == 31, f"crosswalk should carry 31 LAs, has {len(canonical)}"
    retrieved = date.today().isoformat()
    rows: list[dict] = []

    # live page (current year) — hard requirement
    live = fetch(LIVE_URL)
    if live is None:
        raise SystemExit(f"FATAL: could not fetch {LIVE_URL}")
    parsed = parse_laf_table(live)
    if parsed is None:
        raise SystemExit("FATAL: LAF table not found on the live Revenue page")
    got_years: dict[int, str] = {parsed[0]: LIVE_URL}
    year_rows: dict[int, list[tuple[str, float]]] = {parsed[0]: parsed[1]}

    # archived years — best-effort, never fatal
    for year, url in ARCHIVE_URLS.items():
        if year in year_rows:
            continue
        try:
            page = fetch(url)
            p = parse_laf_table(page) if page else None
            if p is None:
                print(f"  {year}: archive snapshot unavailable/unparseable — skipped")
                continue
            if p[0] != year:
                print(f"  {year}: snapshot header says {p[0]} — skipped (year mismatch)")
                continue
            got_years[year] = url
            year_rows[year] = p[1]
        except Exception as e:  # noqa: BLE001 — archive recovery must never block the run
            print(f"  {year}: archive fetch failed ({e}) — skipped")

    for year in sorted(year_rows):
        las = [la for la, _ in year_rows[year]]
        # the never-break rule: every name EXACTLY in the canonical-31 set, no dupes, all 31
        assert set(las) == canonical and len(las) == 31, (
            f"{year}: LA set mismatch — extra={sorted(set(las) - canonical)}, "
            f"missing={sorted(canonical - set(las))}, n={len(las)}"
        )
        for la, pct in year_rows[year]:
            assert -15.0 <= pct <= 15.0, f"{year} {la}: adjustment {pct}% outside the statutory +/-15% band"
            rows.append(
                {
                    "local_authority": la,
                    "year": year,
                    "adjustment_pct": pct,
                    "source_url": got_years[year],
                    "retrieved_date": retrieved,
                }
            )

    df = pl.DataFrame(rows).sort(["year", "local_authority"])
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(OUT_CSV)
    print(f"\nwrote {OUT_CSV}  ({df.height} rows, years {sorted(year_rows)})")
    for year in sorted(year_rows):
        y = df.filter(pl.col("year") == year)
        up = y.filter(pl.col("adjustment_pct") > 0).height
        down = y.filter(pl.col("adjustment_pct") < 0).height
        flat = y.filter(pl.col("adjustment_pct") == 0).height
        print(f"  {year}: 31 councils — {up} raised, {down} cut, {flat} held at base rate")


if __name__ == "__main__":
    main()
