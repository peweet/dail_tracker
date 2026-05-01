"""
Silver layer schema tests.

Silver outputs are the direct outputs of PDF scrapers and API flatteners —
the first point where raw data becomes structured. These tests catch:
  - PDF column mis-slicing (fixed iloc[:, :5] assumptions)
  - API field renames breaking the flatten step
  - Date parsing producing non-ISO strings (e.g. footer text leaking in)
  - Attendance counts that are obviously wrong (>300 days in a year)

Same pattern as test_gold_df.py: schema → sample data → unit test → integration test.
"""

import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import SILVER_DIR


# ---------------------------------------------------------------------------
# REUSABLE ISO DATE CHECK
# Attendance dates come from PDFs as strings. A mis-parsed footer row can
# produce values like "47" or "Andrews" that pass a plain string type check
# but break any downstream date arithmetic.
# ---------------------------------------------------------------------------

def _s(data) -> pl.Series:
    """Extract pl.Series from a Pandera Polars @pa.check callback argument."""
    return data.lazyframe.select(pl.col(data.key)).collect()[data.key]


def _df(data) -> pl.DataFrame:
    """Extract pl.DataFrame from a Pandera Polars @pa.dataframe_check callback argument."""
    return data.lazyframe.collect()


def _all_iso_dates(series: pl.Series) -> bool:
    """All non-null values must match YYYY-MM-DD."""
    non_null = series.drop_nulls()
    if len(non_null) == 0:
        return True
    return non_null.str.contains(r"^\d{4}-\d{2}-\d{2}$").all()


# ---------------------------------------------------------------------------
# SCHEMAS
# ---------------------------------------------------------------------------

class AttendanceSilverSchema(pa.DataFrameModel):
    """
    data/silver/aggregated_td_tables.csv
    One row per TD per year. The PDF scraper in attendance.py emits one row
    per sitting/other-day pair, aggregated by year after extraction.
    """
    identifier: str = pa.Field(nullable=False)
    first_name: str = pa.Field(nullable=False)
    last_name: str = pa.Field(nullable=False)
    year: int = pa.Field(ge=2000, le=2030, nullable=False)
    # Dates stored as ISO strings; use explicit check rather than polars Date
    # type because the CSV has no type metadata.
    iso_sitting_days_attendance: str = pa.Field(nullable=True)
    iso_other_days_attendance: str = pa.Field(nullable=True)
    sitting_days_count: int = pa.Field(ge=0, le=300, nullable=True)
    other_days_count: int = pa.Field(ge=0, le=300, nullable=True)

    class Config:
        strict = False
        name = "aggregated_td_tables"

    @pa.check("iso_sitting_days_attendance")
    def sitting_date_is_iso(cls, data) -> bool:
        return _all_iso_dates(_s(data))

    @pa.check("iso_other_days_attendance")
    def other_date_is_iso(cls, data) -> bool:
        return _all_iso_dates(_s(data))

    @pa.dataframe_check
    def no_null_identifiers(cls, data) -> bool:
        return _df(data)["identifier"].null_count() == 0


class FlattenedMembersSchema(pa.DataFrameModel):
    """
    data/silver/flattened_members.csv
    Only the identity columns are declared — the full file has 230+ columns.
    These are the ones used as join keys and display values downstream.
    """
    unique_member_code: str = pa.Field(nullable=False)
    full_name: str = pa.Field(nullable=False)
    first_name: str = pa.Field(nullable=False)
    last_name: str = pa.Field(nullable=False)
    party: str = pa.Field(nullable=True)
    constituency_name: str = pa.Field(nullable=True)
    year_elected: int = pa.Field(ge=1900, le=2030, nullable=True)

    class Config:
        strict = False
        name = "flattened_members"

    @pa.dataframe_check
    def unique_member_codes(cls, data) -> bool:
        df = _df(data)
        return df["unique_member_code"].n_unique() == len(df)


class MemberInterestsSchema(pa.DataFrameModel):
    """
    data/silver/dail_member_interests_combined.csv
    Combined across all years (2020–2025). One row per member/year/category entry.
    The PDF extraction for this file is the most fragile in the pipeline.
    """
    member_name: str = pa.Field(nullable=False)
    year: int = pa.Field(ge=2018, le=2030, nullable=False)
    category_code: int = pa.Field(ge=1, le=9, nullable=True)

    class Config:
        strict = False
        name = "dail_member_interests"

    @pa.dataframe_check
    def has_rows(cls, data) -> bool:
        return len(_df(data)) > 0

    @pa.dataframe_check
    def no_null_member_names(cls, data) -> bool:
        return _df(data)["member_name"].null_count() == 0


# ---------------------------------------------------------------------------
# SAMPLE DATA
# ---------------------------------------------------------------------------

SAMPLE_ATTENDANCE = pl.DataFrame({
    "identifier": ["Murphy_Mary", "OBrien_Sean"],
    "first_name": ["Mary", "Sean"],
    "last_name": ["Murphy", "O Brien"],
    "year": [2024, 2024],
    "iso_sitting_days_attendance": ["2024-01-17", "2024-01-17"],
    "iso_other_days_attendance": ["2024-01-03", "2024-01-03"],
    "sitting_days_count": [87, 94],
    "other_days_count": [12, 8],
})

SAMPLE_ATTENDANCE_BAD_DATE = pl.DataFrame({
    "identifier": ["Murphy_Mary"],
    "first_name": ["Mary"],
    "last_name": ["Murphy"],
    "year": [2024],
    "iso_sitting_days_attendance": ["47"],   # footer row leaked in — must fail
    "iso_other_days_attendance": ["2024-01-03"],
    "sitting_days_count": [87],
    "other_days_count": [12],
})

SAMPLE_ATTENDANCE_OVERFLOW = pl.DataFrame({
    "identifier": ["Murphy_Mary"],
    "first_name": ["Mary"],
    "last_name": ["Murphy"],
    "year": [2024],
    "iso_sitting_days_attendance": ["2024-01-17"],
    "iso_other_days_attendance": ["2024-01-03"],
    "sitting_days_count": [999],   # impossible — must fail
    "other_days_count": [0],
})

SAMPLE_MEMBERS = pl.DataFrame({
    "unique_member_code": ["MC2020A", "MC2016B"],
    "full_name": ["Mary Murphy", "Sean O Brien"],
    "first_name": ["Mary", "Sean"],
    "last_name": ["Murphy", "O Brien"],
    "party": ["Fianna Fáil", "Sinn Féin"],
    "constituency_name": ["Dublin South", "Cork North-West"],
    "year_elected": [2020, 2016],
})

SAMPLE_MEMBERS_DUPLICATE = pl.DataFrame({
    "unique_member_code": ["MC2020A", "MC2020A"],  # duplicate — must fail
    "full_name": ["Mary Murphy", "Mary Murphy"],
    "first_name": ["Mary", "Mary"],
    "last_name": ["Murphy", "Murphy"],
    "party": ["Fianna Fáil", None],
    "constituency_name": ["Dublin South", "Dublin South"],
    "year_elected": [2020, 2020],
})


# ---------------------------------------------------------------------------
# UNIT TESTS
# ---------------------------------------------------------------------------

def test_attendance_schema_accepts_valid_data():
    AttendanceSilverSchema.validate(SAMPLE_ATTENDANCE)


def test_attendance_schema_rejects_non_iso_date():
    with pytest.raises(pa.errors.SchemaError):
        AttendanceSilverSchema.validate(SAMPLE_ATTENDANCE_BAD_DATE)


def test_attendance_schema_rejects_overflow_days():
    with pytest.raises(pa.errors.SchemaError):
        AttendanceSilverSchema.validate(SAMPLE_ATTENDANCE_OVERFLOW)


def test_members_schema_accepts_valid_data():
    FlattenedMembersSchema.validate(SAMPLE_MEMBERS)


def test_members_schema_rejects_duplicate_codes():
    with pytest.raises(pa.errors.SchemaError):
        FlattenedMembersSchema.validate(SAMPLE_MEMBERS_DUPLICATE)


# ---------------------------------------------------------------------------
# INTEGRATION TESTS
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def attendance_df():
    path = SILVER_DIR / "aggregated_td_tables.csv"
    if not path.exists():
        pytest.skip(f"Silver file not found: {path} — run pipeline.py first")
    return pl.read_csv(path)


@pytest.fixture(scope="module")
def members_df():
    path = SILVER_DIR / "flattened_members.csv"
    if not path.exists():
        pytest.skip(f"Silver file not found: {path} — run pipeline.py first")
    return pl.read_csv(path, columns=[
        "unique_member_code", "full_name", "first_name", "last_name",
        "party", "constituency_name", "year_elected",
    ])


@pytest.fixture(scope="module")
def interests_df():
    path = SILVER_DIR / "dail_member_interests_combined.csv"
    if not path.exists():
        pytest.skip(f"Silver file not found: {path} — run pipeline.py first")
    return pl.read_csv(path)


@pytest.mark.integration
def test_attendance_silver_schema(attendance_df):
    AttendanceSilverSchema.validate(attendance_df)


@pytest.mark.integration
def test_attendance_unique_td_count(attendance_df):
    n = attendance_df["identifier"].n_unique()
    assert n >= 127, f"Only {n} unique TDs in attendance — expected >=127"


@pytest.mark.integration
def test_attendance_no_null_identifiers(attendance_df):
    n = attendance_df["identifier"].null_count()
    assert n == 0, f"{n} null identifiers in aggregated_td_tables"


@pytest.mark.integration
def test_attendance_iso_dates(attendance_df):
    for col in ("iso_sitting_days_attendance", "iso_other_days_attendance"):
        assert _all_iso_dates(attendance_df[col]), (
            f"{col} contains non-ISO date values — PDF footer row may have leaked in"
        )


@pytest.mark.integration
def test_attendance_days_within_bounds(attendance_df):
    for col in ("sitting_days_count", "other_days_count"):
        max_val = attendance_df[col].drop_nulls().max()
        assert max_val <= 300, f"{col} max={max_val} — likely a mis-parsed PDF row"


@pytest.mark.integration
def test_members_silver_schema(members_df):
    FlattenedMembersSchema.validate(members_df)


@pytest.mark.integration
def test_members_unique_codes(members_df):
    n_unique = members_df["unique_member_code"].n_unique()
    assert n_unique == len(members_df), (
        f"{len(members_df) - n_unique} duplicate unique_member_codes in flattened_members"
    )


@pytest.mark.integration
def test_interests_has_data(interests_df):
    assert len(interests_df) > 0, "dail_member_interests_combined.csv is empty"


@pytest.mark.integration
def test_interests_no_null_names(interests_df):
    # Column may be named member_name or full_name depending on extraction version
    name_col = next(
        (c for c in interests_df.columns if "name" in c.lower() and "member" in c.lower()),
        None,
    )
    if name_col is None:
        pytest.skip("Could not find member name column — update test with actual column name")
    n = interests_df[name_col].null_count()
    assert n == 0, f"{n} null values in {name_col}"
