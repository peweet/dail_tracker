"""Contract tests for the SIPO GE2024 political-finance money facts (gold).

The ten ``sipo_*`` gold facts are OCR-derived from scanned SIPO returns, so the
failure mode is the classic silent one: a re-OCR mis-reads a figure (a negative
amount, or a wrong election tag) and it ships green into the donations / election-
expenses UI. The facts vary in width — donor rows, candidate summaries, national
line-items, party aggregates — but they share two universal money invariants, so a
single generic contract covers every one:

  * every euro column (``*_eur``) is non-negative — a negative is an OCR sign /
    parse error (anchored 2026-06-27: 0 negatives across all 10 facts);
  * ``election_event`` (where the fact carries it) is closed to the events we have
    actually OCR'd — currently only ``GE2024``; a new tag is unverified drift.

Marked ``@sql`` — gold IS committed, so this runs against the real files on every
push with no pipeline build needed.
"""

import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from config import GOLD_PARQUET_DIR  # noqa: E402

# The events we have ground-truth OCR for. Promote-to-gold stamps this; a value
# outside the set means a fact was built from a source we have not validated.
ELECTION_EVENTS: frozenset[str] = frozenset({"GE2024"})

_NUMERIC = (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.UInt32, pl.UInt64)


def _df(data) -> pl.DataFrame:
    """Unwrap the pl.DataFrame from a Pandera-Polars @pa.dataframe_check argument."""
    return data.lazyframe.collect()


class SipoMoneySchema(pa.DataFrameModel):
    """Generic contract for any ``sipo_*`` gold money fact.

    strict=False and no required columns: the facts differ in shape, so the contract
    is expressed purely as cross-frame checks over whatever euro / election columns
    each one carries.
    """

    class Config:
        strict = False
        name = "sipo_money"

    @pa.dataframe_check
    def _eur_columns_non_negative(cls, data) -> bool:
        df = _df(data)
        for col, dtype in df.schema.items():
            # null is allowed (an un-extracted OCR cell); a negative is not.
            if col.endswith("_eur") and dtype in _NUMERIC and df[col].drop_nulls().lt(0).any():
                return False
        return True

    @pa.dataframe_check
    def _election_event_in_vocab(cls, data) -> bool:
        df = _df(data)
        if "election_event" not in df.columns:
            return True  # five of the facts predate the stamp — absence is fine.
        nn = df["election_event"].drop_nulls().cast(pl.Utf8)
        return bool(nn.is_in(list(ELECTION_EVENTS)).all()) if len(nn) else True


# --------------------------------------------------------------------------- samples
_GOOD = pl.DataFrame({"party": ["Fianna Fáil"], "value_eur": [1000.0], "election_event": ["GE2024"]})
_NEG_EUR = pl.DataFrame({"party": ["Fine Gael"], "value_eur": [-5.0], "election_event": ["GE2024"]})
_BAD_EVENT = pl.DataFrame({"party": ["Labour"], "cost_eur": [10.0], "election_event": ["GE2099"]})
# A fact with no election_event column and only positive money — must pass.
_NO_EVENT = pl.DataFrame({"party": ["Aontú"], "total_eur": [24006.59]})


def test_sipo_schema_accepts_good_sample():
    SipoMoneySchema.validate(_GOOD)


def test_sipo_schema_accepts_fact_without_election_event():
    SipoMoneySchema.validate(_NO_EVENT)


def test_sipo_schema_rejects_negative_eur():
    with pytest.raises(pa.errors.SchemaError):
        SipoMoneySchema.validate(_NEG_EUR)


def test_sipo_schema_rejects_unknown_election_event():
    with pytest.raises(pa.errors.SchemaError):
        SipoMoneySchema.validate(_BAD_EVENT)


# --------------------------------------------------------------------------- integration
def _sipo_files() -> list[Path]:
    return sorted(GOLD_PARQUET_DIR.glob("sipo_*.parquet"))


@pytest.mark.sql
def test_sipo_money_facts_exist():
    files = _sipo_files()
    assert len(files) >= 8, f"expected the sipo_* gold money facts, found {[f.name for f in files]}"


@pytest.mark.sql
@pytest.mark.parametrize("path", _sipo_files(), ids=lambda p: p.name)
def test_sipo_money_fact_satisfies_contract(path):
    SipoMoneySchema.validate(pl.read_parquet(path))
