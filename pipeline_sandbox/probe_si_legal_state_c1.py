"""PROBE (throwaway): is SI legal-state (C1) extractable from the eISB
Legislation Directory chronological tables?

Per-SI eISB pages carry no structured status, but each year has a chronological
table (https://www.irishstatutebook.ie/isbc/siYYYY.html -> siYYYY_1-50.html ...)
with columns: SI Year/Number | Title | How Affected | Affecting Provision.
"How Affected" records amend/revoke per provision, citing the affecting SI.

This probe crawls ONE year, parses the table, derives a coarse current_state,
measures the distribution, and tests join-back coverage to our gold SI table.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_si_legal_state_c1.py [year]
Reads gold SI parquet + fetches eISB Directory pages. Writes nothing.
"""

from __future__ import annotations

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

YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2018
BASE = "https://www.irishstatutebook.ie/isbc"
HDRS = {"User-Agent": "dail-tracker research probe (planning)"}
GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def get(url: str) -> str:
    r = requests.get(url, headers=HDRS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"  # server mislabels; force utf-8
    return r.text


def range_urls(year: int) -> list[str]:
    idx = get(f"{BASE}/si{year}.html")
    soup = BeautifulSoup(idx, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        if re.search(rf"si{year}_\d+-\d+\.html$", a["href"]):
            urls.append(f"{BASE}/{a['href'].lstrip('/').split('/')[-1]}")
    return sorted(set(urls))


def derive_state(how: str) -> str:
    h = how.strip()
    if not h or h.lower().startswith("not affected"):
        return "in_force_as_made"
    low = h.lower()
    # whole-instrument revocation: "Revoked" not tied to a Reg./Art. provision
    whole_revoked = bool(re.search(r"(?<![A-Za-z\.])\brevoked\b", low)) and not re.search(r"\b(reg|art|sch|para)\b", low[: low.find("revoked")])
    has_rev = "revoked" in low
    has_amd = "amended" in low
    if whole_revoked:
        return "revoked"
    if has_rev and has_amd:
        return "amended_and_partially_revoked"
    if has_rev:
        return "partially_revoked"
    if has_amd:
        return "amended"
    return "other_affected"


def parse_table(html: str, year: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tab in soup.find_all("table"):
        for tr in tab.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            num_raw = cells[0].strip()
            m = re.match(r"^(\d{1,4})\b", num_raw)
            if not m:
                continue
            si_no = int(m.group(1))
            title = cells[1] if len(cells) > 1 else ""
            how = cells[2] if len(cells) > 2 else ""
            affecting = cells[3] if len(cells) > 3 else ""
            out.append({
                "si_year": year, "si_number": si_no, "title": title[:90],
                "how_affected": how, "affecting": affecting[:80],
                "current_state": derive_state(how),
            })
    return out


def main() -> None:
    hr(f"C1 LEGAL-STATE PROBE — year {YEAR}")
    urls = range_urls(YEAR)
    print(f"chronological-table range pages: {len(urls)}")
    rows: list[dict] = []
    for u in urls:
        try:
            rows += parse_table(get(u), YEAR)
            time.sleep(0.4)  # be polite
        except Exception as e:
            print("  fetch/parse err", u, repr(e))
    df = pl.DataFrame(rows).unique(subset=["si_year", "si_number"], keep="first") if rows else pl.DataFrame()
    print(f"parsed SI rows: {df.height:,}")
    if not df.height:
        return

    hr("DERIVED current_state DISTRIBUTION")
    print(df.group_by("current_state").len().sort("len", descending=True))

    hr("SAMPLE: revoked / partially_revoked / amended")
    for st in ["revoked", "partially_revoked", "amended", "amended_and_partially_revoked"]:
        s = df.filter(pl.col("current_state") == st).head(3)
        for r in s.iter_rows(named=True):
            print(f"  [{st}] {r['si_year']}/{r['si_number']} {r['title'][:55]!r}")
            print(f"        how_affected: {r['how_affected'][:120]}")

    # join-back coverage to gold
    hr("JOIN-BACK to gold v_statutory_instruments")
    gold = pl.read_parquet(GOLD).filter(pl.col("si_year") == YEAR).select(["si_id", "si_year", "si_number", "si_title"])
    j = gold.join(df, on=["si_year", "si_number"], how="left")
    matched = j.filter(pl.col("current_state").is_not_null()).height
    print(f"gold SIs for {YEAR}        : {gold.height:,}")
    print(f"  matched to directory     : {matched:,}  ({matched / max(1, gold.height):.1%})")
    print(f"  of those, NOT in_force_as_made: {j.filter(pl.col('current_state').is_in(['revoked','partially_revoked','amended','amended_and_partially_revoked'])).height:,}")

    hr("VERDICT")
    rev = df.filter(pl.col("current_state") == "revoked").height
    print(f"  parseable: YES — {df.height:,} SIs, {len(urls)} pages for {YEAR}")
    print(f"  whole-SI revoked detected: {rev:,}")
    print(f"  gold join coverage: {matched / max(1, gold.height):.1%}")
    print("  caveat: provision-level vs whole-instrument revocation needs careful")
    print("  wording rules; 'revoked' here is heuristic — verify against eISB before reliance.")


if __name__ == "__main__":
    main()
