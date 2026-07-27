"""Census 2022 Small Area Population Statistics — fetched straight from the CSO.

`extractors/census_saps_land_extract.py` lands SAPS from CSVs already sitting in
doc/source_pdfs/_samples/. Those inputs are not in the working tree, so nothing was ever
landed. This module fetches the published files instead, so the fact can be rebuilt from
nothing on any checkout.

Why it matters for siting: two live uses, not demographic colour.
  1. Small-area population/household context for the point (what competitor reports print).
  2. The draft rural-housing National Planning Statement proposes to lift local-need criteria
     for settlements under 1,500 population — so the BUA (built-up area) table is the
     population half of that test, with osi_settlements the geometry half.

Source : https://www.cso.ie/en/census/census2022/census2022smallareapopulationstatistics
Writes : data/silver/parquet/census_saps_2022_<level>.parquet   (sa | ed | bua | lea | county)

    python -m extractors.census_saps_2022_fetch
    python -m extractors.census_saps_2022_fetch --level bua
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before polars/numpy load. Ordering is the contract;
# see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import logging
from pathlib import Path

import polars as pl
import requests

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("census_saps_2022")

OUT = Path(__file__).resolve().parents[1] / "data/silver/parquet"
_BASE = "https://www.cso.ie/en/media/csoie/census/census2022"
_UA = {"User-Agent": "Mozilla/5.0 (compatible; dail-tracker/1.0)"}

# level -> (filename, expected row count as published, row floor)
FILES: dict[str, tuple[str, int, int]] = {
    "sa": ("SAPS_2022_Small_Area_UR_171024.csv", 18_919, 15_000),
    "ed": ("SAPS_2022_CSOED3270923.csv", 3_420, 3_000),
    "bua": ("SAPS_2022_BUA_270923.csv", 867, 700),
    "lea": ("SAPS_2022_CSOLEA270923.csv", 166, 150),
    "county": ("SAPS_2022_county_270923.csv", 31, 26),
}


def fetch_level(level: str) -> pl.DataFrame:
    fname, expect, _ = FILES[level]
    url = f"{_BASE}/{fname}"
    LOG.info("[%s] %s", level, url)
    resp = requests.get(url, headers=_UA, timeout=300)
    resp.raise_for_status()
    # SAPS ships with a handful of non-UTF8 bytes in Irish placenames; lossy keeps every row.
    df = pl.read_csv(resp.content, infer_schema_length=0, encoding="utf8-lossy")
    if df.height != expect:
        LOG.warning("[%s] %d rows, published count was %d — CSO may have revised", level, df.height, expect)
    return df


# The handful of SAPS columns a site report actually needs, out of 793. Measured 2026-07-27:
# `pl.read_parquet` of the full small-area table costs 328 MB of private bytes, against 38 MB
# for a pushdown read of three columns. Landing a slim table removes the footgun rather than
# relying on every future caller remembering to push down.
_SLIM = {
    "GEOGID": "geogid",
    "GEOGDESC": "geogdesc",
    "UR_Category_Desc": "urban_rural",
    "T1_1AGETT": "population",
    "T6_1_TH": "households",
}


def _slim(df: pl.DataFrame, level: str) -> pl.DataFrame:
    lookup = {c.strip().lower(): c for c in df.columns}
    cols = {lookup[k.lower()]: v for k, v in _SLIM.items() if k.lower() in lookup}
    out = df.select(list(cols)).rename(cols)
    for numeric in ("population", "households"):
        if numeric in out.columns:
            out = out.with_columns(pl.col(numeric).cast(pl.Int64, strict=False))
    return out.with_columns(pl.lit(level).alias("level"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--level", default="all", choices=[*FILES, "all"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    setup_standalone_logging("census_saps_2022")

    levels = list(FILES) if args.level == "all" else [args.level]
    slim_parts: list[pl.DataFrame] = []
    OUT.mkdir(parents=True, exist_ok=True)
    for level in levels:
        df = fetch_level(level)
        # T1_1AGETT is the SAPS total-population column; log it so a caller can see the fact
        # arrived intact rather than trusting a row count alone.
        pop_col = next((c for c in df.columns if c.upper() == "T1_1AGETT"), None)
        total = int(df[pop_col].cast(pl.Int64, strict=False).sum()) if pop_col else -1
        LOG.info("[%s] %d rows x %d cols, total population %s", level, df.height, df.width, f"{total:,}")
        if args.dry_run:
            continue
        save_parquet(df, OUT / f"census_saps_2022_{level}.parquet", min_rows=FILES[level][2])
        LOG.info("[%s] wrote census_saps_2022_%s.parquet", level, level)
        slim_parts.append(_slim(df, level))

    if slim_parts and not args.dry_run:
        slim = pl.concat(slim_parts, how="diagonal_relaxed")
        save_parquet(slim, OUT / "census_saps_2022_slim.parquet", min_rows=1_000)
        LOG.info("wrote census_saps_2022_slim.parquet (%d rows x %d cols)", slim.height, slim.width)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
