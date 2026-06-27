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

sys.path.insert(0, str(Path(__file__).parents[2]))
from config import GOLD_DIR, SILVER_DIR

LOBBY_OUTPUT_DIR = SILVER_DIR / "lobbying"


def _df(data) -> pl.DataFrame:
    """Unwrap a pl.DataFrame from a Pandera Polars @pa.dataframe_check argument.

    Newer pandera-polars passes a PolarsData wrapper (with a .lazyframe), not the
    DataFrame directly. Mirrors the _df helper in test_silver_parquet.py.
    """
    return data.lazyframe.collect()


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
    def td_count_in_range(cls, data) -> bool:
        df = _df(data)
        # 174 Dáil seats; the membership record exceeds the seat count over a
        # term as mid-term replacements/by-elections add rows (the 34th Dáil
        # members API returns 176). Cap at 185 to still catch a runaway join.
        return 1 <= len(df) <= 185

    @pa.dataframe_check
    def unique_identifiers(cls, data) -> bool:
        df = _df(data)
        return df["identifier"].n_unique() == len(df)

    @pa.dataframe_check
    def unique_join_keys(cls, data) -> bool:
        df = _df(data)
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
    def sitting_total_consistent(cls, data) -> bool:
        df = _df(data)
        # sitting_total_days must equal sitting + other where all three are non-null
        if "sitting_total_days" not in df.columns:
            return True
        check = df.filter(
            pl.col("sitting_days_count").is_not_null()
            & pl.col("other_days_count").is_not_null()
            & pl.col("sitting_total_days").is_not_null()
        ).select(
            ((pl.col("sitting_days_count") + pl.col("other_days_count")) == pl.col("sitting_total_days")).alias("ok")
        )["ok"]
        return check.all() if len(check) > 0 else True

    @pa.dataframe_check
    def join_enriched_party_coverage(cls, data) -> bool:
        df = _df(data)
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
    def unique_member_codes(cls, data) -> bool:
        df = _df(data)
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
    def no_duplicate_politicians(cls, data) -> bool:
        df = _df(data)
        return df["full_name"].n_unique() == len(df)


# ---------------------------------------------------------------------------
# SAMPLE DATA
# Inline DataFrames: fast unit tests that run without any pipeline output.
# They also document what a valid gold row looks like.
# join_key is the sorted-character key produced by normalise_join_key.py.
# ---------------------------------------------------------------------------

SAMPLE_MASTER_TD = pl.DataFrame(
    {
        "identifier": ["MemberCode2020A", "MemberCode2016B"],
        "first_name": ["Mary", "Sean"],
        "last_name": ["Murphy", "O Brien"],
        "full_name": ["Mary Murphy", "Sean O Brien"],
        "year_elected": [2020, 2016],
        "join_key": ["ahmmprruyy", "abeeiinnors"],
        "party": ["Fianna Fáil", "Sinn Féin"],
        "constituency": ["Dublin South", "Cork North-West"],
    }
)

SAMPLE_DUPLICATE_TD = pl.DataFrame(
    {
        "identifier": ["MemberCode2020A", "MemberCode2020A"],  # duplicate — must fail
        "first_name": ["Mary", "Mary"],
        "last_name": ["Murphy", "Murphy"],
        "full_name": ["Mary Murphy", "Mary Murphy"],
        "year_elected": [2020, 2020],
        "join_key": ["ahmmprruyy", "ahmmprruyy"],
        "party": ["Fianna Fáil", None],
        "constituency": ["Dublin South", "Dublin South"],
    }
)

SAMPLE_ATTENDANCE = pl.DataFrame(
    {
        "unique_member_code": ["MC2020A", "MC2016B"],
        "full_name": ["Mary Murphy", "Sean O Brien"],
        "party": ["Fianna Fáil", "Sinn Féin"],
        "sitting_days_count": [87, 94],
        "other_days_count": [12, 8],
        "sitting_total_days": [99, 102],
        "join_key": ["ahmmprruyy", "abeeiinnors"],
    }
)

SAMPLE_ATTENDANCE_BAD_DAYS = pl.DataFrame(
    {
        "unique_member_code": ["MC2020A"],
        "full_name": ["Mary Murphy"],
        "party": ["Fianna Fáil"],
        "sitting_days_count": [999],  # mis-parsed PDF row — must fail
        "other_days_count": [0],
        "sitting_total_days": [999],
        "join_key": ["ahmmprruyy"],
    }
)


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
    return pl.read_csv(
        path,
        columns=[
            "unique_member_code",
            "full_name",
            "party",
            "sitting_days_count",
            "other_days_count",
            "sitting_total_days",
            "join_key",
        ],
    )


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
    # 174 Dáil seats; membership records exceed seats over a term as mid-term
    # replacements/by-elections add rows (34th Dáil members API returns 176).
    assert 127 <= count <= 185, f"Unexpected TD count: {count}"


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
    assert pct_null < 0.20, f"{pct_null:.0%} of enriched rows have null party — join key may be failing"


@pytest.mark.integration
def test_committee_schema(committee_df):
    CommitteeAssignmentsSchema.validate(committee_df)


@pytest.mark.integration
def test_most_lobbied_schema(most_lobbied_df):
    MostLobbiedPoliticiansSchema.validate(most_lobbied_df)


# ---------------------------------------------------------------------------
# SEANAD PARITY
# The Dáil has driver-table contracts (MasterTD + EnrichedAttendance) above; the
# Seanad twins had none, despite identical integrity stakes. master/attendance for
# the upper house get the same guards here.
# ---------------------------------------------------------------------------


class SeanadMasterSchema(pa.DataFrameModel):
    """
    data/gold/seanad_master_list.csv — one row per senator; the Seanad twin of
    master_td_list. Same stakes: a duplicate identifier or join_key silently
    multiplies rows in every downstream LEFT JOIN.
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
        name = "seanad_master_list"

    @pa.dataframe_check
    def senator_count_in_range(cls, data) -> bool:
        df = _df(data)
        # 60 Seanad seats; the record can edge above it over a term as casual
        # vacancies are filled (anchored 2026-06-27: 60). Cap at 75 to still
        # catch a runaway join.
        return 1 <= len(df) <= 75

    @pa.dataframe_check
    def unique_identifiers(cls, data) -> bool:
        df = _df(data)
        return df["identifier"].n_unique() == len(df)

    @pa.dataframe_check
    def unique_join_keys(cls, data) -> bool:
        df = _df(data)
        return df["join_key"].n_unique() == len(df)


class SeanadAttendanceSchema(pa.DataFrameModel):
    """
    data/gold/enriched_senator_attendance.csv — the Seanad twin of enriched_td_attendance.
    NOTE: this file is committee-exploded (≈122 rows per senator-year), so unlike the
    TD file it is NOT member-year unique. We therefore assert key non-nullness, party
    coverage and plausible day counts — NOT grain uniqueness.
    """

    unique_member_code: str = pa.Field(nullable=False)
    full_name: str = pa.Field(nullable=False)
    join_key: str = pa.Field(nullable=False)
    party: str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "enriched_senator_attendance"

    @pa.dataframe_check
    def party_coverage(cls, data) -> bool:
        df = _df(data)
        # Senators are overwhelmingly party-affiliated; low coverage means the join
        # key is failing (anchored 2026-06-27: 100%).
        non_null = df["party"].drop_nulls()
        return len(non_null) / len(df) > 0.80

    @pa.dataframe_check
    def day_counts_plausible(cls, data) -> bool:
        df = _df(data)
        # Day counts are read off the same PDFs as the TD file; a 999 is a mis-parse.
        # Typed loosely (Int vs Float varies with how the CSV nulls infer) — cast and
        # range-check the values rather than pinning a dtype.
        for c in ("sitting_days_count", "other_days_count"):
            if c in df.columns:
                col = df[c].drop_nulls().cast(pl.Float64)
                if len(col) and (col.lt(0).any() or col.gt(300).any()):
                    return False
        return True


SAMPLE_SEANAD_MASTER = pl.DataFrame(
    {
        "identifier": ["Sen2020A", "Sen2016B"],
        "first_name": ["Aoife", "Liam"],
        "last_name": ["Kelly", "Walsh"],
        "full_name": ["Aoife Kelly", "Liam Walsh"],
        "year_elected": [2020, 2016],
        "join_key": ["aaefikkloy", "aahilllmsw"],
        "party": ["Fianna Fáil", "Fine Gael"],
        "constituency": ["Agricultural Panel", "Industrial and Commercial Panel"],
    }
)

SAMPLE_SEANAD_MASTER_DUP = SAMPLE_SEANAD_MASTER.with_columns(
    pl.Series("join_key", ["aaefikkloy", "aaefikkloy"])  # duplicate join_key — must fail
)

SAMPLE_SEANAD_ATTENDANCE = pl.DataFrame(
    {
        "unique_member_code": ["SC2020A", "SC2016B"],
        "full_name": ["Aoife Kelly", "Liam Walsh"],
        "join_key": ["aaefikkloy", "aahilllmsw"],
        "party": ["Fianna Fáil", "Fine Gael"],
        "sitting_days_count": [42.0, 51.0],
        "other_days_count": [10.0, 6.0],
    }
)

SAMPLE_SEANAD_ATTENDANCE_BAD_DAYS = SAMPLE_SEANAD_ATTENDANCE.with_columns(
    pl.Series("sitting_days_count", [999.0, 51.0])  # mis-parsed PDF row — must fail
)


def test_seanad_master_schema_accepts_valid_data():
    SeanadMasterSchema.validate(SAMPLE_SEANAD_MASTER)


def test_seanad_master_schema_rejects_duplicate_join_keys():
    with pytest.raises(pa.errors.SchemaError):
        SeanadMasterSchema.validate(SAMPLE_SEANAD_MASTER_DUP)


def test_seanad_attendance_schema_accepts_valid_data():
    SeanadAttendanceSchema.validate(SAMPLE_SEANAD_ATTENDANCE)


def test_seanad_attendance_rejects_out_of_range_days():
    with pytest.raises(pa.errors.SchemaError):
        SeanadAttendanceSchema.validate(SAMPLE_SEANAD_ATTENDANCE_BAD_DAYS)


@pytest.fixture(scope="module")
def seanad_master_df():
    path = GOLD_DIR / "seanad_master_list.csv"
    if not path.exists():
        pytest.skip(f"Gold file not found: {path} — run pipeline.py first")
    return pl.read_csv(path)


@pytest.fixture(scope="module")
def seanad_attendance_df():
    path = GOLD_DIR / "enriched_senator_attendance.csv"
    if not path.exists():
        pytest.skip(f"Gold file not found: {path} — run pipeline.py first")
    return pl.read_csv(
        path,
        columns=["unique_member_code", "full_name", "join_key", "party", "sitting_days_count", "other_days_count"],
    )


@pytest.mark.integration
def test_seanad_master_schema(seanad_master_df):
    SeanadMasterSchema.validate(seanad_master_df)


@pytest.mark.integration
def test_seanad_master_no_null_join_keys(seanad_master_df):
    n = seanad_master_df["join_key"].null_count()
    assert n == 0, f"{n} null join_keys in seanad_master_list — these rows will silently drop from joins"


@pytest.mark.integration
def test_seanad_attendance_schema(seanad_attendance_df):
    SeanadAttendanceSchema.validate(seanad_attendance_df)
