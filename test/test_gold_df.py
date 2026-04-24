"""
Gold layer schema tests.

Pattern: each gold output has a Pandera DataFrameModel (the schema), a small
inline SAMPLE_* DataFrame (unit test, no files), and integration tests that
read the real file but skip if it doesn't exist.

Why Pandera DataFrameModel over hand-written asserts?
  - Column presence, type, nullability and range checks are declared once.
  - SchemaError messages tell you which column failed and why.
  - strict=False means schemas tolerate extra columns — safe for wide DataFrames.
"""

import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import GOLD_DIR, SILVER_DIR

LOBBY_OUTPUT_DIR = SILVER_DIR / "lobbying"


# ---------------------------------------------------------------------------
# SCHEMAS
# ---------------------------------------------------------------------------

class MasterTDSchema(pa.DataFrameModel):
    """
    data/gold/master_td_list.csv — one row per TD, driving table for joins.
    Uniqueness on identifier and join_key is critical: a duplicate silently
    multiplies rows in any LEFT JOIN downstream.
    """
    identifier: str = pa.Field(nullable=False)
    first_name: str = pa.Field(nullable=False)
    last_name: str = pa.Field(nullable=False)
    full_name: str = pa.Field(nullable=False)
    year_elected: int = pa.Field(ge=1900, le=2030, nullable=True)
    join_key: str = pa.Field(nullable=False)
    party: str = pa.Field(nullable=True)
    constituency: str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "master_td_list"

    @pa.dataframe_check
    def td_count_in_range(cls, df: pl.DataFrame) -> bool:
        # 160 Dáil seats; allow up to 174 for by-election churn
        return 1 <= len(df) <= 174

    @pa.dataframe_check
    def unique_identifiers(cls, df: pl.DataFrame) -> bool:
        return df["identifier"].n_unique() == len(df)

    @pa.dataframe_check
    def unique_join_keys(cls, df: pl.DataFrame) -> bool:
        return df["join_key"].n_unique() == len(df)


class EnrichedAttendanceSchema(pa.DataFrameModel):
    """
    data/gold/enriched_td_attendance.csv — 230+ columns.
    Only key columns are declared here; strict=False ignores the rest.
    Load with columns= to avoid reading the full 25 MB into memory in tests.
    """
    unique_member_code: str = pa.Field(nullable=False)
    full_name: str = pa.Field(nullable=False)
    party: str = pa.Field(nullable=True)
    sitting_days_count: int = pa.Field(ge=0, le=300, nullable=True)
    other_days_count: int = pa.Field(ge=0, le=300, nullable=True)
    join_key: str = pa.Field(nullable=False)

    class Config:
        strict = False
        name = "enriched_td_attendance"

    @pa.dataframe_check
    def sitting_total_consistent(cls, df: pl.DataFrame) -> bool:
        # sitting_total_days must equal sitting + other where all three are non-null
        if "sitting_total_days" not in df.columns:
            return True
        check = df.filter(
            pl.col("sitting_days_count").is_not_null()
            & pl.col("other_days_count").is_not_null()
            & pl.col("sitting_total_days").is_not_null()
        ).select(
            ((pl.col("sitting_days_count") + pl.col("other_days_count"))
             == pl.col("sitting_total_days")).alias("ok")
        )["ok"]
        return check.all() if len(check) > 0 else True

    @pa.dataframe_check
    def join_enriched_party_coverage(cls, df: pl.DataFrame) -> bool:
        # After enrichment >80% of rows should have a party value.
        # Lower coverage signals the join key is failing.
        non_null = df["party"].drop_nulls()
        return len(non_null) / len(df) > 0.80


class CommitteeAssignmentsSchema(pa.DataFrameModel):
    """data/gold/committee_assignments.csv"""
    unique_member_code: str = pa.Field(nullable=False)
    full_name: str = pa.Field(nullable=False)

    class Config:
        strict = False
        name = "committee_assignments"

    @pa.dataframe_check
    def unique_member_codes(cls, df: pl.DataFrame) -> bool:
        return df["unique_member_code"].n_unique() == len(df)


class MostLobbiedPoliticiansSchema(pa.DataFrameModel):
    """data/silver/lobbying/most_lobbied_politicians.csv"""
    full_name: str = pa.Field(nullable=False)
    lobby_returns_targeting: int = pa.Field(ge=0, nullable=False)
    distinct_orgs: int = pa.Field(ge=0, nullable=True)

    class Config:
        strict = False
        name = "most_lobbied_politicians"

    @pa.dataframe_check
    def no_duplicate_politicians(cls, df: pl.DataFrame) -> bool:
        return df["full_name"].n_unique() == len(df)


# ---------------------------------------------------------------------------
# SAMPLE DATA
# Inline DataFrames: fast unit tests that run without any pipeline output.
# They also document what a valid gold row looks like.
# join_key is the sorted-character key produced by normalise_join_key.py.
# ---------------------------------------------------------------------------

SAMPLE_MASTER_TD = pl.DataFrame({
    "identifier": ["MemberCode2020A", "MemberCode2016B"],
    "first_name": ["Mary", "Sean"],
    "last_name": ["Murphy", "O Brien"],
    "full_name": ["Mary Murphy", "Sean O Brien"],
    "year_elected": [2020, 2016],
    "join_key": ["ahmmprruyy", "abeeiinnors"],
    "party": ["Fianna Fáil", "Sinn Féin"],
    "constituency": ["Dublin South", "Cork North-West"],
})

SAMPLE_DUPLICATE_TD = pl.DataFrame({
    "identifier": ["MemberCode2020A", "MemberCode2020A"],  # duplicate — must fail
    "first_name": ["Mary", "Mary"],
    "last_name": ["Murphy", "Murphy"],
    "full_name": ["Mary Murphy", "Mary Murphy"],
    "year_elected": [2020, 2020],
    "join_key": ["ahmmprruyy", "ahmmprruyy"],
    "party": ["Fianna Fáil", None],
    "constituency": ["Dublin South", "Dublin South"],
})

SAMPLE_ATTENDANCE = pl.DataFrame({
    "unique_member_code": ["MC2020A", "MC2016B"],
    "full_name": ["Mary Murphy", "Sean O Brien"],
    "party": ["Fianna Fáil", "Sinn Féin"],
    "sitting_days_count": [87, 94],
    "other_days_count": [12, 8],
    "sitting_total_days": [99, 102],
    "join_key": ["ahmmprruyy", "abeeiinnors"],
})

SAMPLE_ATTENDANCE_BAD_DAYS = pl.DataFrame({
    "unique_member_code": ["MC2020A"],
    "full_name": ["Mary Murphy"],
    "party": ["Fianna Fáil"],
    "sitting_days_count": [999],  # mis-parsed PDF row — must fail
    "other_days_count": [0],
    "sitting_total_days": [999],
    "join_key": ["ahmmprruyy"],
})


# ---------------------------------------------------------------------------
# UNIT TESTS — no file I/O, always runnable
# ---------------------------------------------------------------------------

def test_master_td_schema_accepts_valid_data():
    MasterTDSchema.validate(SAMPLE_MASTER_TD)


def test_master_td_schema_rejects_duplicate_identifiers():
    with pytest.raises(pa.errors.SchemaError):
        MasterTDSchema.validate(SAMPLE_DUPLICATE_TD)


def test_enriched_attendance_schema_accepts_valid_data():
    EnrichedAttendanceSchema.validate(SAMPLE_ATTENDANCE)


def test_enriched_attendance_rejects_out_of_range_days():
    with pytest.raises(pa.errors.SchemaError):
        EnrichedAttendanceSchema.validate(SAMPLE_ATTENDANCE_BAD_DAYS)


# ---------------------------------------------------------------------------
# INTEGRATION TESTS — require pipeline to have run
# pytest.skip (not fail) when files are absent so CI can run unit tests alone.
# Load enriched attendance with columns= — avoids reading 230 cols / 25 MB.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def master_td_df():
    path = GOLD_DIR / "master_td_list.csv"
    if not path.exists():
        pytest.skip(f"Gold file not found: {path} — run pipeline.py first")
    return pl.read_csv(path)


@pytest.fixture(scope="module")
def enriched_attendance_df():
    path = GOLD_DIR / "enriched_td_attendance.csv"
    if not path.exists():
        pytest.skip(f"Gold file not found: {path} — run pipeline.py first")
    return pl.read_csv(path, columns=[
        "unique_member_code", "full_name", "party",
        "sitting_days_count", "other_days_count", "sitting_total_days", "join_key",
    ])


@pytest.fixture(scope="module")
def committee_df():
    path = GOLD_DIR / "committee_assignments.csv"
    if not path.exists():
        pytest.skip(f"Gold file not found: {path} — run pipeline.py first")
    return pl.read_csv(path)


@pytest.fixture(scope="module")
def most_lobbied_df():
    path = LOBBY_OUTPUT_DIR / "most_lobbied_politicians.csv"
    if not path.exists():
        pytest.skip(f"Silver/lobbying file not found: {path} — run pipeline.py first")
    return pl.read_csv(path)


@pytest.mark.integration
def test_master_td_schema(master_td_df):
    MasterTDSchema.validate(master_td_df)


@pytest.mark.integration
def test_master_td_count_in_range(master_td_df):
    count = len(master_td_df)
    assert 127 <= count <= 174, f"Unexpected TD count: {count}"


@pytest.mark.integration
def test_master_td_no_null_identifiers(master_td_df):
    n = master_td_df["identifier"].null_count()
    assert n == 0, f"{n} null identifiers in master_td_list"


@pytest.mark.integration
def test_master_td_no_null_join_keys(master_td_df):
    n = master_td_df["join_key"].null_count()
    assert n == 0, f"{n} null join_keys — these rows will silently drop from joins"


@pytest.mark.integration
def test_year_elected_plausible(master_td_df):
    years = master_td_df["year_elected"].drop_nulls()
    assert years.min() >= 1900
    assert years.max() <= 2030


@pytest.mark.integration
def test_enriched_attendance_schema(enriched_attendance_df):
    EnrichedAttendanceSchema.validate(enriched_attendance_df)


@pytest.mark.integration
def test_enriched_party_coverage(enriched_attendance_df):
    # After enrichment the vast majority of TDs should have a party value.
    n_total = len(enriched_attendance_df)
    n_null_party = enriched_attendance_df["party"].null_count()
    pct_null = n_null_party / n_total
    assert pct_null < 0.20, (
        f"{pct_null:.0%} of enriched rows have null party — join key may be failing"
    )


@pytest.mark.integration
def test_committee_schema(committee_df):
    CommitteeAssignmentsSchema.validate(committee_df)


@pytest.mark.integration
def test_most_lobbied_schema(most_lobbied_df):
    MostLobbiedPoliticiansSchema.validate(most_lobbied_df)
