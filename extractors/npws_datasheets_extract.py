"""NPWS SAC/SPA datasheets — the qualifying interests of every European site.

Why this exists: the siting engine holds SAC/SPA *boundaries* (npws_sac / npws_spa carry
SITECODE, SITE_NAME, COUNTY and nothing else), so an AA flag could only ever say "you are
2.1 km from The Murrough Wetlands SAC". It could not say what that site is protected *for* —
which is the fact that decides whether Appropriate Assessment screening is arguable. These
datasheets close that: per site code, the Annex I habitats and Annex II / bird species the
site was designated for, plus the statutory instrument and the site-specific conservation
objectives document URL, so a report can cite the source rather than assert.

Source : https://www.npws.ie/sites/default/files/files/SAC_datasheets_20231017.zip
         https://www.npws.ie/sites/default/files/files/SPA_datasheets_20231017.zip
         (data.gov.ie "SAC Datasheets" / "SPA Datasheets", CC-BY 4.0, NPWS, data to Mar 2023)
Writes : data/silver/parquet/npws_qualifying_interests.parquet  (long: one row per site x QI)
         data/silver/parquet/npws_site_objectives.parquet       (one row per site)

    python -m extractors.npws_datasheets_extract
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before polars/numpy load. Ordering is the contract;
# see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import io
import logging
import zipfile
from pathlib import Path

import polars as pl

from services.http_engine import fetch_bytes
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("npws_datasheets")

OUT = Path(__file__).resolve().parents[1] / "data/silver/parquet"
_BASE = "https://www.npws.ie/sites/default/files/files"
SOURCES: dict[str, str] = {
    "SAC": f"{_BASE}/SAC_datasheets_20231017.zip",
    "SPA": f"{_BASE}/SPA_datasheets_20231017.zip",
}

# Row floors: the SAC pull is 441 sites / 1,493 habitat rows / 347 species rows as of the
# Oct-2023 edition. Floors sit well under those so a genuine NPWS revision passes, but a
# truncated or empty harvest cannot overwrite a good file.
_FLOOR_QI = 1_200
_FLOOR_SITES = 500


def _csvs(url: str) -> dict[str, pl.DataFrame]:
    """Download one datasheet zip and return {csv basename: frame}."""
    body = fetch_bytes(url, timeout=180, validate=lambda b: b[:2] == b"PK")
    if body is None:
        raise RuntimeError(f"NPWS datasheet download failed: {url}")
    frames: dict[str, pl.DataFrame] = {}
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as fh:
                # NPWS ships these latin-1-ish; utf8-lossy keeps Irish placenames readable
                # instead of failing the whole file on one byte.
                frames[Path(name).name] = pl.read_csv(
                    fh.read(), infer_schema_length=5_000, ignore_errors=True, encoding="utf8-lossy"
                )
    return frames


def _pick(frames: dict[str, pl.DataFrame], token: str) -> pl.DataFrame | None:
    for name, df in frames.items():
        if token.lower() in name.lower():
            return df
    return None


def _norm(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    """Rename the columns we want (case/space-insensitive) and drop the rest."""
    lookup = {c.strip().lower(): c for c in df.columns}
    keep, rename = [], {}
    for want, out in mapping.items():
        src = lookup.get(want)
        if src is not None:
            keep.append(src)
            rename[src] = out
    return df.select(keep).rename(rename)


def _qi_rows(frames: dict[str, pl.DataFrame], site_type: str) -> list[pl.DataFrame]:
    out: list[pl.DataFrame] = []
    # SAC zips carry Habitat_Data + Species_Data; SPA zips carry Bird_SCI_Data (the special
    # conservation interest bird species) + a Wetland_SCI_Data table that is site-level only
    # (no species column) and is folded into the site table instead.
    for token, qi_kind, code_col, name_col in (
        ("Habitat_Data", "habitat", "hab code", "habitat name"),
        ("Species_Data", "species", "species code", "species name"),
        ("Bird_SCI_Data", "bird", "species code", "species name"),
    ):
        df = _pick(frames, token)
        if df is None or df.is_empty():
            LOG.warning("[%s] no %s table in zip", site_type, token)
            continue
        norm = _norm(
            df,
            {
                "site code": "site_code",
                "site name": "site_name",
                code_col: "qi_code",
                name_col: "qi_name",
                "date": "listed_date",
            },
        )
        missing = {"site_code", "qi_code", "qi_name"} - set(norm.columns)
        if missing:
            LOG.warning("[%s] %s missing %s — skipped", site_type, token, sorted(missing))
            continue
        out.append(
            norm.with_columns(
                pl.lit(site_type).alias("site_type"),
                pl.lit(qi_kind).alias("qi_kind"),
            )
        )
    return out


def _site_rows(frames: dict[str, pl.DataFrame], site_type: str) -> pl.DataFrame | None:
    df = _pick(frames, "Site_Data")
    if df is None or df.is_empty():
        return None
    norm = _norm(
        df,
        {
            "site code": "site_code",
            "site name": "site_name",
            "longitude": "longitude",
            "latitude": "latitude",
            "site area": "site_area",
            "s. i. number": "si_number",
            "s.i. number": "si_number",
            "s.i. date": "si_date",
            "s.i. url": "si_url",
            "ssco version": "ssco_version",
            "ssco version date": "ssco_date",
            "ssco url": "ssco_url",
        },
    )
    # A site appearing in Wetland_SCI_Data is designated for the "wetlands and waterbirds"
    # special conservation interest — the distinction the AA screening question turns on.
    wet = _pick(frames, "Wetland_SCI")
    codes: list[str] = []
    if wet is not None and not wet.is_empty():
        col = next((c for c in wet.columns if c.strip().lower() == "site code"), None)
        if col:
            codes = [str(v) for v in wet[col].drop_nulls().unique().to_list()]
    return norm.with_columns(
        pl.lit(site_type).alias("site_type"),
        pl.col("site_code").cast(pl.Utf8).is_in(codes).alias("wetland_sci"),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="parse and report counts, write nothing")
    args = ap.parse_args()
    setup_standalone_logging("npws_datasheets")

    qi_parts: list[pl.DataFrame] = []
    site_parts: list[pl.DataFrame] = []
    for site_type, url in SOURCES.items():
        LOG.info("[%s] %s", site_type, url)
        frames = _csvs(url)
        LOG.info("[%s] csvs: %s", site_type, sorted(frames))
        qi_parts.extend(_qi_rows(frames, site_type))
        sites = _site_rows(frames, site_type)
        if sites is not None:
            site_parts.append(sites)

    if not qi_parts:
        LOG.error("no qualifying-interest rows parsed — refusing to write")
        return 1

    qi = pl.concat(qi_parts, how="diagonal_relaxed").select(
        "site_type", "site_code", "site_name", "qi_kind", "qi_code", "qi_name", "listed_date"
    )
    sites = (
        pl.concat(site_parts, how="diagonal_relaxed")
        if site_parts
        else pl.DataFrame({"site_code": [], "site_type": []})
    )

    LOG.info(
        "qualifying interests: %d rows over %d sites (habitats %d, species %d)",
        qi.height,
        qi["site_code"].n_unique(),
        qi.filter(pl.col("qi_kind") == "habitat").height,
        qi.filter(pl.col("qi_kind") == "species").height,
    )
    LOG.info("site objectives: %d rows", sites.height)
    if args.dry_run:
        return 0

    OUT.mkdir(parents=True, exist_ok=True)
    save_parquet(qi, OUT / "npws_qualifying_interests.parquet", min_rows=_FLOOR_QI)
    save_parquet(sites, OUT / "npws_site_objectives.parquet", min_rows=_FLOOR_SITES)
    LOG.info("wrote %s + %s", "npws_qualifying_interests.parquet", "npws_site_objectives.parquet")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
