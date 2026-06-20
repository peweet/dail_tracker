"""Contract tests for the NOAC + SSHA wide housing gold tables.

These 17 tables (``noac_*_wide.parquet``, ``ssha_a1_*_wide.parquet``) were the gold
files with NO coverage at all before this — neither a regression baseline entry nor a
schema test. They share one uniform shape: a ``(la, year)`` grain plus N numeric
measure columns, so a single generic contract covers every one:

  * ``la`` is a non-empty string;
  * ``year`` is an integer in a plausible range;
  * ``(la, year)`` is unique — the grain (a duplicate means a bad join/append);
  * every numeric measure is non-negative (these are counts, stocks, and collection
    percentages — a negative is a parser/sign error).

Marked ``@sql`` so it runs in the sql-contracts lane against the committed gold on
every push (no pipeline build needed).
"""

import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from config import GOLD_PARQUET_DIR  # noqa: E402

HOUSING_WIDE_GLOBS = ("noac_*_wide.parquet", "ssha_a1_*_wide.parquet")


def _s(data) -> pl.Series:
    return data.lazyframe.select(pl.col(data.key)).collect()[data.key]


def _df(data) -> pl.DataFrame:
    return data.lazyframe.collect()


class HousingWideSchema(pa.DataFrameModel):
    """Generic contract for a ``(la, year)``-grain wide housing table."""

    la: str = pa.Field(nullable=False)
    year: int = pa.Field(ge=1990, le=2035, nullable=False)

    class Config:
        strict = False
        name = "housing_wide"

    @pa.check("la")
    def _la_non_empty(cls, data) -> bool:
        return bool((_s(data).drop_nulls().str.strip_chars() != "").all())

    @pa.dataframe_check
    def _la_year_unique(cls, data) -> bool:
        df = _df(data)
        return df.select(pl.struct(["la", "year"]).n_unique()).item() == len(df)

    @pa.dataframe_check
    def _measures_non_negative(cls, data) -> bool:
        df = _df(data)
        measures = [
            c for c, dt in df.schema.items() if c != "year" and dt in (pl.Int64, pl.Float64, pl.Int32, pl.Float32)
        ]
        for c in measures:
            col = df[c]
            # null is allowed (a year a council didn't report); negatives are not.
            if col.drop_nulls().lt(0).any():
                return False
        return True


def _housing_files() -> list[Path]:
    files: list[Path] = []
    for g in HOUSING_WIDE_GLOBS:
        files.extend(sorted(GOLD_PARQUET_DIR.glob(g)))
    return files


# Sample frames for the always-on unit tests (no committed data needed).
_GOOD = pl.DataFrame({"la": ["Cork", "Mayo"], "year": [2022, 2022], "total": [10, 20], "pct": [98.5, 101.2]})
_DUP = pl.DataFrame({"la": ["Cork", "Cork"], "year": [2022, 2022], "total": [10, 20]})
_NEG = pl.DataFrame({"la": ["Cork"], "year": [2022], "total": [-5]})


def test_housing_schema_accepts_good_sample():
    HousingWideSchema.validate(_GOOD)


def test_housing_schema_rejects_duplicate_grain():
    with pytest.raises(pa.errors.SchemaError):
        HousingWideSchema.validate(_DUP)


def test_housing_schema_rejects_negative_measure():
    with pytest.raises(pa.errors.SchemaError):
        HousingWideSchema.validate(_NEG)


@pytest.mark.sql
def test_housing_wide_tables_exist():
    files = _housing_files()
    assert len(files) >= 16, f"expected the NOAC+SSHA wide gold tables, found {[f.name for f in files]}"


@pytest.mark.sql
@pytest.mark.parametrize("path", _housing_files(), ids=lambda p: p.name)
def test_housing_wide_table_satisfies_contract(path):
    HousingWideSchema.validate(pl.read_parquet(path))
