"""Residential Property Price Register — every recorded residential sale since 2010.

Why this exists: competitor site reports carry a "nearby property sales" section and ours has
none. The PSRA register is the only public record of actual consideration paid.

⚠ The register has NO COORDINATES. Address is free text plus county, with a sparse Eircode
column, and Eircode → point needs the licensed ECAD, which we do not hold. So this extractor
lands the register faithfully and stops there; placing a sale near a site is a separate
matching problem (the honest cheap route is county + townland-name matching against the
osi_townlands layer, which gives "sales in this townland", never "sale 140 m away").

Source : https://www.propertypriceregister.ie (PSRA), bulk PPR-ALL.zip
Writes : data/silver/parquet/ppr_sales.parquet

    python -m extractors.ppr_sales_extract
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

from services.http_engine import fetch_bytes, polite_headers
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("ppr_sales")

OUT = Path(__file__).resolve().parents[1] / "data/silver/parquet"
URL = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

# ~700k sales 2010-2026. Floor well below that: a genuine month's growth passes, a truncated
# or challenge-page harvest cannot overwrite a good file.
_FLOOR = 400_000

_RENAME = {
    "date of sale (dd/mm/yyyy)": "sale_date",
    "address": "address",
    "county": "county",
    "eircode": "eircode",
    "price (€)": "price_eur",
    "not full market price": "not_full_market_price",
    "vat exclusive": "vat_exclusive",
    "description of property": "property_description",
    "property size description": "property_size_description",
}


def _clean_price(col: pl.Expr) -> pl.Expr:
    # "€350,000.00" -> 350000.0 ; the euro sign arrives mangled under some encodings, so strip
    # everything that is not a digit or a decimal point rather than matching the symbol.
    return col.cast(pl.Utf8).str.replace_all(r"[^0-9.]", "").cast(pl.Float64, strict=False)


def fetch() -> pl.DataFrame:
    # The register is served to browsers only (a bare python UA gets a challenge page),
    # and the challenge arrives as HTTP 200 HTML — the zip-magic validate treats it as a
    # miss so it can never be parsed as a truncated harvest.
    body = fetch_bytes(URL, headers=polite_headers(browser=True), timeout=300, validate=lambda b: b[:2] == b"PK")
    if body is None:
        raise RuntimeError("PPR download failed (challenge page or network)")
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise RuntimeError(f"no CSV in PPR zip: {zf.namelist()}")
        LOG.info("csv in zip: %s", names[0])
        raw = zf.read(names[0])
    # PSRA ships this cp1252/latin-1; utf8-lossy keeps the rows and mangles only stray bytes.
    df = pl.read_csv(raw, infer_schema_length=0, encoding="utf8-lossy")
    lookup = {c.strip().lower(): c for c in df.columns}
    keep, rename = [], {}
    for want, out in _RENAME.items():
        src = lookup.get(want)
        if src is not None:
            keep.append(src)
            rename[src] = out
    # The price header is "Price (€)" and the euro sign does not survive the lossy decode, so
    # exact matching drops it. Fall back to a prefix match on the one column that starts "price".
    if "price_eur" not in rename.values():
        for low, src in lookup.items():
            if low.startswith("price"):
                keep.append(src)
                rename[src] = "price_eur"
                break
    missing = set(_RENAME.values()) - set(rename.values())
    if missing:
        LOG.warning("PPR columns absent this edition: %s", sorted(missing))
    df = df.select(keep).rename(rename)
    if "price_eur" in df.columns:
        df = df.with_columns(_clean_price(pl.col("price_eur")).alias("price_eur"))
    if "sale_date" in df.columns:
        df = df.with_columns(pl.col("sale_date").str.strptime(pl.Date, "%d/%m/%Y", strict=False).alias("sale_date"))
    return df


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="parse and report counts, write nothing")
    args = ap.parse_args()
    setup_standalone_logging("ppr_sales")

    df = fetch()
    LOG.info("PPR: %d sales, columns %s", df.height, df.columns)
    if "sale_date" in df.columns:
        LOG.info("date span: %s .. %s", df["sale_date"].min(), df["sale_date"].max())
    if "county" in df.columns:
        LOG.info("counties: %d", df["county"].n_unique())
    if args.dry_run:
        return 0

    OUT.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT / "ppr_sales.parquet", min_rows=_FLOOR)
    LOG.info("wrote ppr_sales.parquet")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
