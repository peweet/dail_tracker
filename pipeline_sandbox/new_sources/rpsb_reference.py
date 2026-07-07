"""CSO Register of Public Sector Bodies (RPSB) — reference dim (SANDBOX).

Extracts the entity list from the CSO RPSB 2024-final release sector sub-pages
(the authoritative universe of Irish public-sector bodies with S.13 sector
classification). Closes the public-body reference gap the crosswalk exposed.

CSO release pages are HTML tables (no direct file download); each sector page
carries an "Entity Name" column. Open CC-BY data.
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import fetch, now_iso, write_silver  # noqa: E402

BASE = "https://www.cso.ie/en/releasesandpublications/ep/p-rpbi/registerofpublicsectorbodies2024-final/"
# leaf sectors (avoid the publicsector/generalgovernment rollups to reduce dup)
SECTORS = ["centralgovernment", "localgovernment", "socialsecurityfunds", "publiccorporations"]
_SKIP = {"", "nil", "n/a", "entity name", "total", "-", "—"}


def entities_from_table(tbl) -> list[dict]:
    rows = tbl.find_all("tr")
    if not rows:
        return []
    # find the header row + the Entity Name column index
    hdr_idx = None
    ename_col = None
    sub_col = None
    for ri, tr in enumerate(rows[:3]):
        cells = [c.get_text(" ", strip=True).lower() for c in tr.find_all(["td", "th"])]
        for ci, c in enumerate(cells):
            if "entity name" in c:
                ename_col = ci
                hdr_idx = ri
            if "sub sector" in c or "sub-sector" in c:
                sub_col = ci
        if ename_col is not None:
            break
    if ename_col is None:
        return []
    out = []
    last_sub = None
    for tr in rows[hdr_idx + 1:]:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if ename_col >= len(cells):
            continue
        name = cells[ename_col].strip()
        if sub_col is not None and sub_col < len(cells) and cells[sub_col].strip():
            last_sub = cells[sub_col].strip()
        if name.lower() in _SKIP or len(name) < 3:
            continue
        out.append({"entity_name": name, "sub_sector": last_sub})
    return out


def run() -> None:
    rows: list[dict] = []
    for sector in SECTORS:
        url = BASE + sector + "/"
        try:
            html, meta = fetch(url)
        except Exception as e:  # noqa: BLE001
            print(f"  {sector}: fetch error {type(e).__name__}: {e}")
            continue
        s = BeautifulSoup(html, "html.parser")
        got = 0
        for tbl in s.find_all("table"):
            for e in entities_from_table(tbl):
                rows.append({**e, "sector": sector, "source_url": url})
                got += 1
        print(f"  {sector}: +{got} entities")

    df = (pl.DataFrame(rows)
          .unique(subset=["entity_name"], keep="first")
          .with_columns([
              pl.lit(now_iso()).alias("fetched_at"),
              pl.lit("html_scrape").alias("extraction_method"),
              pl.lit("high").alias("confidence"),
              pl.lit("public").alias("privacy_tier"),
          ]))
    out = write_silver("rpsb_bodies", df)
    print(f"\nSILVER: {out}  rows={df.height}")
    by = df.group_by("sector").len().sort("len", descending=True)
    for r in by.to_dicts():
        print(f"  {r['len']:>4}  {r['sector']}")
    print("  samples:", df["entity_name"].head(6).to_list())


if __name__ == "__main__":
    run()
