"""Uisce Éireann capacity registers (water.ie) -> one attribute parquet.

UÉ publishes per-county "capacity register" pages — an INDICATIVE Green/Amber/Red per treatment
plant ("spare capacity now, or on completion of a project underway") — refreshed roughly annually
(current tables carry <time datetime> Published April 2026). There is NO open-data or download
version (checked data.gov.ie + the pages themselves, 2026-07-30): the only machine-readable form
is the HTML table on each county page, so this scraper exists. The adjacent open sources cover
different ground: EPA UWWT agglomeration boundaries (already ingested; spatial + load PE) and
EEA Waterbase-UWWTD (numeric design capacity, but a compliance snapshot years behind).

GRAIN: one row per (register, county_page, settlement). A plant can serve several settlements
(Oranmore -> Mutton Island WWTP), so reg_code REPEATS; never count plants by row. The wastewater
table's "Reg #" is the EPA wastewater-discharge authorisation code — the SAME code family as
RegCd in epa_uww_agglomeration, so that join is deterministic. The water-supply register has no
such key; joining it means settlement-name matching (not attempted here).

The RAG is UÉ's own wording: "indicative only, non-binding and should not be relied upon" — store
it, surface it with attribution, never harden it into a verdict.

    python pipeline_sandbox/ue_capacity_register.py            # both registers
    python pipeline_sandbox/ue_capacity_register.py --register wastewater
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before numpy/pandas load anywhere downstream. Ordering is the
# contract; see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import logging
import re
import time
from datetime import UTC, datetime

import polars as pl
import requests
from bs4 import BeautifulSoup

from extractors.planning_layers_ingest import OUT  # land beside the layers this joins to
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("ue_capacity_register")
H = {"User-Agent": "dail-tracker-planning-ingest/1.0"}
BASE = "https://www.water.ie"
REGISTERS = {
    "wastewater": "/connections/developer-services/capacity-registers/wastewater-treatment-capacity-register",
    "water_supply": "/connections/developer-services/capacity-registers/water-supply-capacity-register",
}
RAG_DOMAIN = {"Green", "Amber", "Red"}
# One row per settlement and ~30 pages per register: a healthy national sweep is several hundred
# rows. The floor guards against a half-broken sweep silently replacing a good parquet.
MIN_ROWS = {"wastewater": 250, "water_supply": 150}


def _get(url: str) -> str:
    r = requests.get(url, headers=H, timeout=60)
    r.raise_for_status()
    return r.text


def county_pages(register: str) -> list[str]:
    """County slugs discovered from the register index page (29 wastewater pages on 2026-07-30 —
    city/county splits like galway vs galway-city included)."""
    path = REGISTERS[register]
    soup = BeautifulSoup(_get(BASE + path), "html.parser")
    slugs = set()
    for a in soup.find_all("a", href=True):
        # slugs are mostly lowercase but the index links a few capitalised (Dublin, Fingal —
        # found 2026-07-30 when the lowercase-only pattern swept 27 of 29 pages). Fetch with the
        # case AS LINKED (a lowercased URL redirect-loops); lowercase only the stored row key.
        m = re.fullmatch(re.escape(path) + r"/([A-Za-z-]+)/?", a["href"].split("?")[0])
        if m:
            slugs.add(m.group(1))
    return sorted(slugs, key=str.lower)


def _header_key(text: str) -> str | None:
    t = text.strip().lower()
    if not t:
        return None
    if "reg" in t and "#" in t:
        return "reg_code"
    if "capacity" in t:
        return "rag"
    if "project" in t:
        return "project"
    if t == "region":
        return "region"
    if t == "county":
        return "county"
    if "settlement" in t or "scheme" in t:
        return "settlement"
    if "plant" in t or "wwtp" in t or "wtp" in t:
        return "plant"
    return None


def parse_page(html: str, register: str, slug: str, url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    t = soup.find("time", attrs={"datetime": True})
    published = t["datetime"][:10] if t else None
    rows: list[dict] = []
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        keys = [_header_key(h) for h in headers]
        if "rag" not in keys:  # not a capacity table (some pages carry unrelated tables)
            continue
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(cells) != len(keys):
                continue
            rec = {k: v for k, v in zip(keys, cells, strict=True) if k}
            rag = rec.get("rag", "").replace("•", "").strip().title()
            # placeholder reg codes: some rows print a bare dash (ASCII or U+2010/2013/2014) where
            # no authorisation code exists — store null, not a fake shared key (found 2026-07-31:
            # "-" appeared as one "code" shared by two unrelated plants)
            reg_code = (rec.get("reg_code") or "").strip().strip("-‐–—").strip() or None
            rows.append(
                {
                    "register": register,
                    "county_page": slug,
                    "region": rec.get("region") or None,
                    "county": rec.get("county") or None,
                    "settlement": rec.get("settlement") or None,
                    "plant": rec.get("plant") or None,
                    "reg_code": reg_code,
                    "rag": rag or None,
                    "rag_raw": rec.get("rag") or None,
                    "project_planned": rec.get("project", "").strip().lower().startswith("yes"),
                    "published": published,
                    "source_url": url,
                }
            )
    return rows


def sweep(register: str) -> pl.DataFrame:
    slugs = county_pages(register)
    LOG.info("%s: %d county pages", register, len(slugs))
    rows: list[dict] = []
    for slug in slugs:
        url = f"{BASE}{REGISTERS[register]}/{slug}"
        try:
            page_rows = parse_page(_get(url), register, slug.lower(), url)
        except requests.HTTPError as e:  # fail loudly at the end, keep sweeping
            LOG.error("%s: HTTP %s", url, e.response.status_code if e.response is not None else "?")
            continue
        if not page_rows:
            LOG.warning("%s: 0 rows parsed", url)
        rows.extend(page_rows)
        time.sleep(0.5)
    df = pl.DataFrame(rows)
    off = df.filter(~pl.col("rag").is_in(RAG_DOMAIN) | pl.col("rag").is_null())
    if off.height:
        LOG.warning("%s: %d rows with off-domain RAG (kept, rag_raw preserved)", register, off.height)
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--register", choices=[*REGISTERS, "both"], default="both")
    args = ap.parse_args()
    wanted = list(REGISTERS) if args.register == "both" else [args.register]
    frames = [sweep(r) for r in wanted]
    df = pl.concat(frames, how="vertical").with_columns(
        pl.lit(datetime.now(UTC).isoformat(timespec="seconds")).alias("fetched_at")
    )
    floor = sum(MIN_ROWS[r] for r in wanted)
    dest = save_parquet(df, OUT / "ue_capacity_register.parquet", min_rows=floor)
    for r in wanted:
        sub = df.filter(pl.col("register") == r)
        LOG.info(
            "%s: %d rows, %d distinct reg codes, RAG %s",
            r,
            sub.height,
            sub["reg_code"].drop_nulls().n_unique(),
            dict(sub.group_by("rag").len().iter_rows()),
        )
    LOG.info("wrote %s (%d rows)", dest, df.height)


if __name__ == "__main__":
    setup_standalone_logging("ue_capacity_register")
    main()
