"""
Silver lobbying parquet schema tests.

All count columns in lobbying parquets are UInt32 — use Series[pl.UInt32].
All date columns are Datetime(us) — use Series[pl.Datetime] with year-range checks.
Plain list literals default to Int32/Int64; sample DataFrames must set dtype explicitly.

14 lower-priority tables are covered by a single parametrized integration test
rather than 14 copies of the same function.
"""

import sys
from pathlib import Path
from datetime import datetime

import pandera.polars as pa
import polars as pl
import pytest
from pandera.typing.polars import Series

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LOBBY_PARQUET_DIR


# ---------------------------------------------------------------------------
# SHARED HELPERS
# ---------------------------------------------------------------------------

def _s(data) -> pl.Series:
    """Extract pl.Series from a Pandera Polars @pa.check callback argument."""
    return data.lazyframe.select(pl.col(data.key)).collect()[data.key]


def _df(data) -> pl.DataFrame:
    """Extract pl.DataFrame from a Pandera Polars @pa.dataframe_check callback argument."""
    return data.lazyframe.collect()


def _no_encoding_artifacts(series: pl.Series) -> bool:
    """Reject strings containing Unicode replacement char (�) from bad PDF encoding."""
    return not series.drop_nulls().str.contains("�", literal=True).any()


# ---------------------------------------------------------------------------
# SCHEMAS
# ---------------------------------------------------------------------------

class ReturnsSchema(pa.DataFrameModel):
    """
    data/silver/lobbying/parquet/returns.parquet
    Primary source table — one row per lobbying return filing.
    """
    primary_key:                        int              = pa.Field(nullable=False)
    lobbyist_name:                      str              = pa.Field(nullable=False)
    was_this_a_grassroots_campaign:     bool             = pa.Field(nullable=True)
    lobbying_period_start_date:         Series[pl.Datetime] = pa.Field(nullable=False)
    lobbying_period_end_date:           Series[pl.Datetime] = pa.Field(nullable=True)
    lobby_url:                          str              = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "returns"

    @pa.check("lobbyist_name")
    def no_empty_lobbyist_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.check("lobby_url")
    def lobby_url_domain(cls, data) -> bool:
        non_null = _s(data).drop_nulls()
        if len(non_null) == 0:
            return True
        return non_null.str.starts_with("https://www.lobbying.ie/").all()

    @pa.dataframe_check
    def primary_key_unique(cls, data) -> bool:
        df = _df(data)
        return df["primary_key"].n_unique() == len(df)

    @pa.dataframe_check
    def start_date_year_plausible(cls, data) -> bool:
        return (_df(data)["lobbying_period_start_date"].dt.year() >= 2015).all()

    @pa.dataframe_check
    def no_fully_duplicate_rows(cls, data) -> bool:
        return _df(data).is_duplicated().sum() == 0


class ReturnsMasterSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/returns_master.parquet"""
    primary_key:                int              = pa.Field(nullable=False)
    lobbyist_name:              str              = pa.Field(nullable=False)
    lobbying_period_start_date: Series[pl.Datetime] = pa.Field(nullable=False)

    class Config:
        strict = False
        name = "returns_master"

    @pa.check("lobbyist_name")
    def no_empty_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.dataframe_check
    def primary_key_unique(cls, data) -> bool:
        df = _df(data)
        return df["primary_key"].n_unique() == len(df)

    @pa.dataframe_check
    def start_date_year_plausible(cls, data) -> bool:
        return (_df(data)["lobbying_period_start_date"].dt.year() >= 2015).all()


class MostLobbiedPoliticiansSchema(pa.DataFrameModel):
    """
    data/silver/lobbying/parquet/most_lobbied_politicians.parquet
    Narrow table (5 cols) — strict=True so any new column is a deliberate contract change.
    """
    full_name:               str               = pa.Field(nullable=False)
    chamber:                 str               = pa.Field(nullable=False)
    lobby_returns_targeting: Series[pl.UInt32] = pa.Field(ge=0)
    distinct_orgs:           Series[pl.UInt32] = pa.Field(ge=0)
    total_returns:           Series[pl.UInt32] = pa.Field(ge=0)

    class Config:
        strict = True
        name = "most_lobbied_politicians"

    @pa.check("full_name")
    def no_empty_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.dataframe_check
    def no_duplicate_politicians(cls, data) -> bool:
        df = _df(data)
        return df["full_name"].n_unique() == len(df)

    @pa.dataframe_check
    def no_fully_duplicate_rows(cls, data) -> bool:
        return _df(data).is_duplicated().sum() == 0

    @pa.dataframe_check
    def warn_on_unexpected_columns(cls, data) -> bool:
        declared = {"full_name", "chamber", "lobby_returns_targeting",
                    "distinct_orgs", "total_returns"}
        extra = set(_df(data).columns) - declared
        if extra:
            import warnings
            warnings.warn(f"Unexpected columns in most_lobbied_politicians: {extra}")
        return True


class PoliticianPolicyExposureSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/politician_policy_exposure.parquet"""
    full_name:          str               = pa.Field(nullable=False)
    public_policy_area: str               = pa.Field(nullable=False)
    returns_targeting:  Series[pl.UInt32] = pa.Field(ge=0)
    distinct_lobbyists: Series[pl.UInt32] = pa.Field(ge=0)

    class Config:
        strict = False
        name = "politician_policy_exposure"

    @pa.check("full_name")
    def no_empty_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.check("public_policy_area")
    def no_empty_policy_area(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()


class BilateralRelationshipsSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/bilateral_relationships.parquet"""
    lobbyist_name:           str               = pa.Field(nullable=False)
    full_name:               str               = pa.Field(nullable=False)
    returns_in_relationship: Series[pl.UInt32] = pa.Field(ge=1)
    relationship_start:      Series[pl.Datetime] = pa.Field(nullable=False)
    relationship_last_seen:  Series[pl.Datetime] = pa.Field(nullable=False)

    class Config:
        strict = False
        name = "bilateral_relationships"

    @pa.check("lobbyist_name")
    def no_empty_lobbyist(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.check("full_name")
    def no_empty_politician(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.dataframe_check
    def start_before_last_seen(cls, data) -> bool:
        df = _df(data)
        return (df["relationship_start"] <= df["relationship_last_seen"]).all()

    @pa.dataframe_check
    def start_year_plausible(cls, data) -> bool:
        return (_df(data)["relationship_start"].dt.year() >= 2015).all()


class RevolvingDoorDposSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/revolving_door_dpos.parquet"""
    dpos_or_former_dpos_who_carried_out_lobbying_name: str               = pa.Field(nullable=False)
    returns_involved_in:                               Series[pl.UInt32] = pa.Field(ge=0)
    distinct_lobbyist_firms:                           Series[pl.UInt32] = pa.Field(ge=0)
    distinct_policy_areas:                             Series[pl.UInt32] = pa.Field(ge=0)
    distinct_politicians_targeted:                     Series[pl.UInt32] = pa.Field(ge=0)

    class Config:
        strict = False
        name = "revolving_door_dpos"

    @pa.check("dpos_or_former_dpos_who_carried_out_lobbying_name")
    def no_empty_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()


class LobbyistPersistenceSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/lobbyist_persistence.parquet"""
    lobbyist_name:          str               = pa.Field(nullable=False)
    total_returns:          Series[pl.UInt32] = pa.Field(ge=1)
    distinct_periods_filed: Series[pl.UInt32] = pa.Field(ge=1)
    first_return_date:      Series[pl.Datetime] = pa.Field(nullable=False)
    last_return_date:       Series[pl.Datetime] = pa.Field(nullable=False)
    active_span_days:       int               = pa.Field(ge=0)

    class Config:
        strict = False
        name = "lobbyist_persistence"

    @pa.check("lobbyist_name")
    def no_empty_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.dataframe_check
    def first_before_last(cls, data) -> bool:
        df = _df(data)
        return (df["first_return_date"] <= df["last_return_date"]).all()


class QuarterlyTrendSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/quarterly_trend.parquet"""
    year_quarter:       str               = pa.Field(nullable=False)
    return_count:       Series[pl.UInt32] = pa.Field(ge=0)
    distinct_lobbyists: Series[pl.UInt32] = pa.Field(ge=0)

    class Config:
        strict = False
        name = "quarterly_trend"

    @pa.check("year_quarter")
    def year_quarter_format(cls, data) -> bool:
        return _s(data).drop_nulls().str.contains(r"^20\d{2}-Q[1-4]$").all()


class PolicyAreaBreakdownSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/policy_area_breakdown.parquet"""
    public_policy_area: str               = pa.Field(nullable=False)
    return_count:       Series[pl.UInt32] = pa.Field(ge=0)
    distinct_lobbyists: Series[pl.UInt32] = pa.Field(ge=0)

    class Config:
        strict = False
        name = "policy_area_breakdown"

    @pa.check("public_policy_area")
    def no_empty_policy_area(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()


class DistinctOrgsPerPoliticianSchema(pa.DataFrameModel):
    """data/silver/lobbying/parquet/distinct_orgs_per_politician.parquet"""
    full_name:             str               = pa.Field(nullable=False)
    distinct_orgs:         Series[pl.UInt32] = pa.Field(ge=0)
    distinct_returns:      Series[pl.UInt32] = pa.Field(ge=0)
    distinct_policy_areas: Series[pl.UInt32] = pa.Field(ge=0)

    class Config:
        strict = False
        name = "distinct_orgs_per_politician"

    @pa.check("full_name")
    def no_empty_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()


# ---------------------------------------------------------------------------
# SAMPLE DATA
# ---------------------------------------------------------------------------

SAMPLE_RETURNS = pl.DataFrame({
    "primary_key":   pl.Series([1001, 1002], dtype=pl.Int64),
    "lobbyist_name": ["IBEC", "Google Ireland"],
    "was_this_a_grassroots_campaign": [False, False],
    "lobbying_period_start_date": pl.Series(
        [datetime(2023, 1, 1), datetime(2023, 7, 1)], dtype=pl.Datetime
    ),
    "lobbying_period_end_date": pl.Series(
        [datetime(2023, 3, 31), datetime(2023, 9, 30)], dtype=pl.Datetime
    ),
    "lobby_url": [
        "https://www.lobbying.ie/return/10001",
        "https://www.lobbying.ie/return/10002",
    ],
})

SAMPLE_RETURNS_DUPLICATE_PK = pl.DataFrame({
    "primary_key":   pl.Series([1001, 1001], dtype=pl.Int64),  # duplicate — must fail
    "lobbyist_name": ["IBEC", "IBEC"],
    "was_this_a_grassroots_campaign": [False, False],
    "lobbying_period_start_date": pl.Series(
        [datetime(2023, 1, 1), datetime(2023, 1, 1)], dtype=pl.Datetime
    ),
    "lobbying_period_end_date": pl.Series(
        [datetime(2023, 3, 31), datetime(2023, 3, 31)], dtype=pl.Datetime
    ),
    "lobby_url": [
        "https://www.lobbying.ie/return/10001",
        "https://www.lobbying.ie/return/10001",
    ],
})

SAMPLE_MOST_LOBBIED = pl.DataFrame({
    "full_name": ["Mary Murphy", "Sean O Brien"],
    "chamber":   ["Dáil", "Seanad"],
    "lobby_returns_targeting": pl.Series([12, 5],  dtype=pl.UInt32),
    "distinct_orgs":           pl.Series([3,  2],  dtype=pl.UInt32),
    "total_returns":           pl.Series([20, 7],  dtype=pl.UInt32),
})

SAMPLE_BILATERAL = pl.DataFrame({
    "lobbyist_name":           ["IBEC", "Google Ireland"],
    "full_name":               ["Mary Murphy", "Sean O Brien"],
    "returns_in_relationship": pl.Series([3, 1], dtype=pl.UInt32),
    "relationship_start":      pl.Series(
        [datetime(2020, 1, 1), datetime(2021, 6, 1)], dtype=pl.Datetime
    ),
    "relationship_last_seen":  pl.Series(
        [datetime(2023, 6, 1), datetime(2023, 9, 1)], dtype=pl.Datetime
    ),
})

SAMPLE_BILATERAL_BAD_DATES = pl.DataFrame({
    "lobbyist_name":           ["IBEC"],
    "full_name":               ["Mary Murphy"],
    "returns_in_relationship": pl.Series([3], dtype=pl.UInt32),
    "relationship_start":      pl.Series([datetime(2023, 9, 1)], dtype=pl.Datetime),
    "relationship_last_seen":  pl.Series([datetime(2020, 1, 1)], dtype=pl.Datetime),
})


# ---------------------------------------------------------------------------
# UNIT TESTS
# ---------------------------------------------------------------------------

def test_returns_accepts_valid_data():
    ReturnsSchema.validate(SAMPLE_RETURNS)


def test_returns_rejects_duplicate_primary_key():
    with pytest.raises(pa.errors.SchemaError):
        ReturnsSchema.validate(SAMPLE_RETURNS_DUPLICATE_PK)


def test_most_lobbied_accepts_valid_data():
    MostLobbiedPoliticiansSchema.validate(SAMPLE_MOST_LOBBIED)


def test_bilateral_accepts_valid_data():
    BilateralRelationshipsSchema.validate(SAMPLE_BILATERAL)


def test_bilateral_rejects_inverted_dates():
    with pytest.raises(pa.errors.SchemaError):
        BilateralRelationshipsSchema.validate(SAMPLE_BILATERAL_BAD_DATES)


# ---------------------------------------------------------------------------
# INTEGRATION TESTS — full-schema tables (parametrized)
# Add new full-schema lobbying tables here — no other code changes needed.
# ---------------------------------------------------------------------------

def _parquet(filename):
    path = LOBBY_PARQUET_DIR / filename
    if not path.exists():
        pytest.skip(f"{filename} not found — run pipeline.py first")
    return pl.read_parquet(path)


SCHEMA_TESTS = [
    ("returns.parquet",                       ReturnsSchema),
    ("returns_master.parquet",                ReturnsMasterSchema),
    ("most_lobbied_politicians.parquet",      MostLobbiedPoliticiansSchema),
    ("politician_policy_exposure.parquet",    PoliticianPolicyExposureSchema),
    ("bilateral_relationships.parquet",       BilateralRelationshipsSchema),
    ("revolving_door_dpos.parquet",           RevolvingDoorDposSchema),
    ("lobbyist_persistence.parquet",          LobbyistPersistenceSchema),
    ("quarterly_trend.parquet",               QuarterlyTrendSchema),
    ("policy_area_breakdown.parquet",         PolicyAreaBreakdownSchema),
    ("distinct_orgs_per_politician.parquet",  DistinctOrgsPerPoliticianSchema),
]


@pytest.mark.integration
@pytest.mark.parametrize("filename,schema_cls", SCHEMA_TESTS)
def test_lobbying_schema(filename, schema_cls):
    schema_cls.validate(_parquet(filename))


# ---------------------------------------------------------------------------
# INTEGRATION TESTS — 14 lightweight tables (parametrized)
# Add new lobbying tables here; no other code changes needed.
# ---------------------------------------------------------------------------

LOBBYING_TABLES = [
    ("bilateral_returns_detail.parquet",        "lobbyist_name"),
    ("client_company_returns_detail.parquet",   "client_name"),
    ("delivery_method_mix.parquet",             "lobbyist_name"),
    ("grassroots_campaigns.parquet",            "lobbyist_name"),
    ("lobby_count_details.parquet",             "lobbyist_name"),
    ("lobbyist_returns_detail.parquet",         "lobbyist_name"),
    ("policy_area_quarterly_trend.parquet",     "public_policy_area"),
    ("politician_returns_detail.parquet",       "full_name"),
    ("reach_by_lobbyist.parquet",               "lobbyist_name"),
    ("return_description_lengths.parquet",      "lobbyist_name"),
    ("revolving_door_returns_detail.parquet",
     "dpos_or_former_dpos_who_carried_out_lobbying_name"),
    ("split_lobbyists.parquet",                 "lobbyist_name"),
    ("time_to_publish.parquet",                 "lobbyist_name"),
    ("top_client_companies.parquet",            "client_name"),
]


@pytest.mark.integration
@pytest.mark.parametrize("filename,key_col", LOBBYING_TABLES)
def test_lobbying_table_non_empty(filename, key_col):
    path = LOBBY_PARQUET_DIR / filename
    if not path.exists():
        pytest.skip(f"{filename} not found — run pipeline.py first")
    df = pl.read_parquet(path)
    assert len(df) > 0, f"{filename} is empty"
    assert df[key_col].null_count() == 0, f"{key_col} has nulls in {filename}"
    assert (df[key_col] != "").all(), f"{key_col} has empty strings in {filename}"
