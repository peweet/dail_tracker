"""
Silver layer parquet schema tests.

Parquets carry Polars-native types (UInt32, Float64, Date, Datetime, Boolean)
that differ from CSV reads. Annotations must match exactly or Pandera silently
mismatches. Key rules applied throughout:

  - Series[pl.UInt32] not int  — lobbying counts are UInt32
  - Series[pl.Date] not str    — Date_Paid is already a parsed Date
  - strict=False on wide tables; strict=True on narrow (≤7 col) tables
  - NaN ≠ null in Float64: check both
  - Empty string ≠ null in String: check identifier columns explicitly
  - Null/List(Struct)/List(Null) typed columns: skip entirely
"""

import sys
from pathlib import Path
from typing import Optional

import pandera.polars as pa
import polars as pl
import pytest
from pandera.typing.polars import Series

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
from config import SILVER_PARQUET_DIR
from test_silver_layer import AttendanceSilverSchema, FlattenedMembersSchema


# ---------------------------------------------------------------------------
# SHARED HELPERS
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


def _no_nan(series: pl.Series) -> bool:
    """Polars is_null() misses float NaN — check both."""
    return series.null_count() == 0 and series.is_nan().sum() == 0


def _no_encoding_artifacts(series: pl.Series) -> bool:
    """Reject strings containing Unicode replacement char from bad PDF encoding."""
    return not series.drop_nulls().str.contains("�", literal=True).any()


# ---------------------------------------------------------------------------
# SCHEMAS
# ---------------------------------------------------------------------------

class PaymentTableSchema(pa.DataFrameModel):
    """
    data/silver/parquet/aggregated_payment_tables.parquet
    Amount is a currency-formatted STRING ("€4,422.08") — NOT Float64.
    Date_Paid is pl.Date — NOT a string. Rows where a date leaked into the
    Amount column ("26/06/2020") have been observed in real data.
    """
    TAA_Band:  str            = pa.Field(nullable=True)
    Date_Paid: Series[pl.Date]= pa.Field(nullable=False)
    Amount:    str            = pa.Field(nullable=True)
    Full_Name: str            = pa.Field(nullable=False)
    join_key:  str            = pa.Field(nullable=False)

    class Config:
        strict = False
        name = "aggregated_payment_tables"

    @pa.check("Amount")
    def amount_is_currency_or_null(cls, data) -> bool:
        non_null = _s(data).drop_nulls()
        if len(non_null) == 0:
            return True
        return non_null.str.contains(r"^[€$]?[\d,]+\.\d{2}$").all()

    @pa.check("Full_Name")
    def no_empty_full_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.check("Full_Name")
    def no_encoding_in_full_name(cls, data) -> bool:
        return _no_encoding_artifacts(_s(data))

    @pa.check("join_key")
    def join_key_lowercase_only(cls, data) -> bool:
        return _s(data).drop_nulls().str.contains(r"^[a-z]+$").all()

    @pa.dataframe_check
    def date_paid_year_in_range(cls, data) -> bool:
        return (_df(data)["Date_Paid"].dt.year() >= 2018).all()

    @pa.dataframe_check
    def no_fully_duplicate_rows(cls, data) -> bool:
        return _df(data).is_duplicated().sum() == 0


class TopTDsByPaymentSchema(pa.DataFrameModel):
    """
    data/silver/parquet/top_tds_by_payment_since_2020.parquet
    Amount IS Float64 here (aggregated/parsed) — unlike aggregated_payment_tables.
    """
    Date_Paid:                     Series[pl.Date] = pa.Field(nullable=False)
    Amount:                        float           = pa.Field(ge=0.0, nullable=True)
    total_amount_paid_since_2020:  float           = pa.Field(ge=0.0, nullable=True)
    Full_Name:                     str             = pa.Field(nullable=False)
    join_key:                      str             = pa.Field(nullable=False)

    class Config:
        strict = False
        name = "top_tds_by_payment_since_2020"

    @pa.check("Full_Name")
    def no_empty_full_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.check("join_key")
    def join_key_lowercase_only(cls, data) -> bool:
        return _s(data).drop_nulls().str.contains(r"^[a-z]+$").all()

    @pa.dataframe_check
    def amount_at_most_2dp(cls, data) -> bool:
        col = _df(data)["Amount"].drop_nulls()
        if len(col) == 0:
            return True
        return (col - col.round(2)).abs().max() < 1e-9

    @pa.dataframe_check
    def total_amount_at_most_2dp(cls, data) -> bool:
        col = _df(data)["total_amount_paid_since_2020"].drop_nulls()
        if len(col) == 0:
            return True
        return (col - col.round(2)).abs().max() < 1e-9

    @pa.dataframe_check
    def no_nan_in_amount(cls, data) -> bool:
        return _df(data)["Amount"].is_nan().sum() == 0

    @pa.dataframe_check
    def date_paid_year_in_range(cls, data) -> bool:
        return (_df(data)["Date_Paid"].dt.year() >= 2018).all()

    @pa.dataframe_check
    def no_fully_duplicate_rows(cls, data) -> bool:
        return _df(data).is_duplicated().sum() == 0


class SponsorSchema(pa.DataFrameModel):
    """
    data/silver/parquet/sponsors.parquet
    bill_year is String in this table (unlike drop_cols_flattened_bills where it is Int64).
    """
    bill_no:            str  = pa.Field(nullable=False)
    bill_year:          str  = pa.Field(nullable=False)
    sponsor_is_primary: bool = pa.Field(nullable=True)
    status:             str  = pa.Field(nullable=True)
    unique_member_code: str  = pa.Field(nullable=True)
    bill_url:           str  = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "sponsors"

    @pa.check("bill_year")
    def bill_year_format(cls, data) -> bool:
        return _s(data).drop_nulls().str.contains(r"^[12]\d{3}$").all()

    @pa.check("bill_url")
    def bill_url_domain(cls, data) -> bool:
        non_null = _s(data).drop_nulls()
        if len(non_null) == 0:
            return True
        return non_null.str.starts_with("https://www.oireachtas.ie/en/bills/").all()

    @pa.check("unique_member_code")
    def member_code_format(cls, data) -> bool:
        non_null = _s(data).drop_nulls()
        if len(non_null) == 0:
            return True
        return non_null.str.contains(r"^.+\.[DS]\.\d{4}-\d{2}-\d{2}$").all()


class StageSchema(pa.DataFrameModel):
    """
    data/silver/parquet/stages.parquet
    event.dates is List(Struct) — skipped.
    billSort.* columns are Null type — skipped.
    """
    bill_no:               str  = pa.Field(nullable=False)
    bill_year:             str  = pa.Field(nullable=False)
    event_progressStage:   int  = pa.Field(ge=0, nullable=True, alias="event.progressStage")
    event_stageCompleted:  bool = pa.Field(nullable=True, alias="event.stageCompleted")

    class Config:
        strict = False
        name = "stages"


class DebateSchema(pa.DataFrameModel):
    """
    data/silver/parquet/debates.parquet
    date is a String (ISO format). billSort.* columns are Null type — skipped.
    """
    date:            str = pa.Field(nullable=False)
    debateSectionId: str = pa.Field(nullable=False)
    chamber_showAs:  str = pa.Field(nullable=True, alias="chamber.showAs")
    debate_url_web:  str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "debates"

    @pa.check("date")
    def date_is_iso(cls, data) -> bool:
        return _all_iso_dates(_s(data))

    @pa.check("debateSectionId")
    def no_empty_section_id(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.check("debate_url_web")
    def debate_url_domain(cls, data) -> bool:
        non_null = _s(data).drop_nulls()
        if len(non_null) == 0:
            return True
        return non_null.str.starts_with("https://www.oireachtas.ie/en/debates/").all()


class FlattenedSeanadMembersSchema(pa.DataFrameModel):
    """
    data/silver/parquet/flattened_seanad_members.parquet
    Wide (138 cols). date_of_death and membership_end_date are Null type — skipped.
    Committee end-date columns are Float64 (NaN sentinels) — skipped.
    Only identity columns declared.
    """
    unique_member_code: str = pa.Field(nullable=False)
    full_name:          str = pa.Field(nullable=False)
    first_name:         str = pa.Field(nullable=False)
    last_name:          str = pa.Field(nullable=False)
    party:              str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "flattened_seanad_members"

    @pa.check("unique_member_code")
    def member_code_format(cls, data) -> bool:
        return _s(data).drop_nulls().str.contains(r"^.+\.[DS]\.\d{4}-\d{2}-\d{2}$").all()

    @pa.check("full_name")
    def no_empty_full_name(cls, data) -> bool:
        return (_s(data).drop_nulls() != "").all()

    @pa.dataframe_check
    def unique_member_codes(cls, data) -> bool:
        df = _df(data)
        return df["unique_member_code"].n_unique() == len(df)

    @pa.dataframe_check
    def row_count_plausible(cls, data) -> bool:
        return 1 <= len(_df(data)) <= 300

    @pa.dataframe_check
    def no_fully_duplicate_rows(cls, data) -> bool:
        return _df(data).is_duplicated().sum() == 0


class RelatedDocSchema(pa.DataFrameModel):
    """
    data/silver/parquet/related_docs.parquet
    relatedDoc.formats.xml and billSort.* are Null type — skipped.
    """
    bill_billNo:        str = pa.Field(nullable=False, alias="bill.billNo")
    relatedDoc_docType: str = pa.Field(nullable=True,  alias="relatedDoc.docType")
    contextDate:        str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "related_docs"


class VersionSchema(pa.DataFrameModel):
    """
    data/silver/parquet/versions.parquet
    version.formats.xml and billSort.* are Null type — skipped.
    """
    bill_billNo:     str = pa.Field(nullable=False, alias="bill.billNo")
    version_docType: str = pa.Field(nullable=True,  alias="version.docType")
    version_date:    str = pa.Field(nullable=True,  alias="version.date")

    class Config:
        strict = False
        name = "versions"


class FlattenedBillsSchema(pa.DataFrameModel):
    """
    data/silver/parquet/drop_cols_flattened_bills.parquet
    bill.billNo and bill.billYear are Int64 here — String in sponsors.parquet.
    Float64 sentinel columns (bill.act, event.chamber, event.house) skipped.
    """
    bill_billNo:   int = pa.Field(nullable=True,            alias="bill.billNo")
    bill_billYear: int = pa.Field(ge=1900, le=2030,
                                  nullable=True,            alias="bill.billYear")
    bill_status:   str = pa.Field(nullable=True,            alias="bill.status")
    bill_source:   str = pa.Field(nullable=True,            alias="bill.source")

    class Config:
        strict = False
        name = "drop_cols_flattened_bills"


# ---------------------------------------------------------------------------
# SAMPLE DATA
# ---------------------------------------------------------------------------

SAMPLE_PAYMENT = pl.DataFrame({
    "TAA_Band":  ["Band A", "Band B"],
    "Date_Paid": pl.Series(["2024-01-15", "2023-06-30"]).str.to_date(),
    "Amount":    ["€4,422.08", "€2,100.00"],
    "Full_Name": ["Mary Murphy", "Sean O Brien"],
    "join_key":  ["ahmmprruyy", "abeeiinnors"],
    "Position":  ["TD", "TD"],
    "Narrative": ["TAA", "TAA"],
})

SAMPLE_PAYMENT_DATE_LEAK = pl.DataFrame({
    "TAA_Band":  ["Band A"],
    "Date_Paid": pl.Series(["2024-01-15"]).str.to_date(),
    "Amount":    ["26/06/2020"],   # date leaked into Amount — must fail
    "Full_Name": ["Mary Murphy"],
    "join_key":  ["ahmmprruyy"],
    "Position":  ["TD"],
    "Narrative": ["TAA"],
})

SAMPLE_PAYMENT_BAD_JOIN_KEY = pl.DataFrame({
    "TAA_Band":  ["Band A"],
    "Date_Paid": pl.Series(["2024-01-15"]).str.to_date(),
    "Amount":    ["€1,000.00"],
    "Full_Name": ["Mary Murphy"],
    "join_key":  ["Mary Murphy"],  # not lowercase sorted — must fail
    "Position":  ["TD"],
    "Narrative": ["TAA"],
})

SAMPLE_DEBATE = pl.DataFrame({
    "date":            ["2024-01-15", "2023-11-20"],
    "debateSectionId": ["debate.001", "debate.002"],
    "chamber.showAs":  ["Dáil Éireann", "Dáil Éireann"],
    "debate_url_web":  [
        "https://www.oireachtas.ie/en/debates/debate/dail/2024-01-15/1/",
        "https://www.oireachtas.ie/en/debates/debate/dail/2023-11-20/2/",
    ],
    "uri": ["uri1", "uri2"],
    "showAs": ["title1", "title2"],
})

SAMPLE_DEBATE_BAD_DATE = pl.DataFrame({
    "date":            ["not-a-date"],
    "debateSectionId": ["debate.001"],
    "chamber.showAs":  ["Dáil Éireann"],
    "debate_url_web":  ["https://www.oireachtas.ie/en/debates/debate/dail/2024-01-15/1/"],
    "uri": ["uri1"],
    "showAs": ["title1"],
})

SAMPLE_SEANAD_MEMBERS = pl.DataFrame({
    "unique_member_code": ["Mary-Murphy.S.2020-02-06", "Sean-OBrien.S.2016-04-25"],
    "full_name":          ["Mary Murphy", "Sean O Brien"],
    "first_name":         ["Mary", "Sean"],
    "last_name":          ["Murphy", "O Brien"],
    "party":              ["Fianna Fáil", "Sinn Féin"],
})

SAMPLE_SEANAD_DUPLICATE = pl.DataFrame({
    "unique_member_code": ["Mary-Murphy.S.2020-02-06", "Mary-Murphy.S.2020-02-06"],
    "full_name":          ["Mary Murphy", "Mary Murphy"],
    "first_name":         ["Mary", "Mary"],
    "last_name":          ["Murphy", "Murphy"],
    "party":              ["Fianna Fáil", None],
})


# ---------------------------------------------------------------------------
# UNIT TESTS
# ---------------------------------------------------------------------------

def test_payment_accepts_valid_data():
    PaymentTableSchema.validate(SAMPLE_PAYMENT)


def test_payment_rejects_date_leak_in_amount():
    with pytest.raises(pa.errors.SchemaError):
        PaymentTableSchema.validate(SAMPLE_PAYMENT_DATE_LEAK)


def test_payment_rejects_bad_join_key():
    with pytest.raises(pa.errors.SchemaError):
        PaymentTableSchema.validate(SAMPLE_PAYMENT_BAD_JOIN_KEY)


def test_debate_accepts_valid_data():
    DebateSchema.validate(SAMPLE_DEBATE)


def test_debate_rejects_non_iso_date():
    with pytest.raises(pa.errors.SchemaError):
        DebateSchema.validate(SAMPLE_DEBATE_BAD_DATE)


def test_seanad_members_accepts_valid_data():
    FlattenedSeanadMembersSchema.validate(SAMPLE_SEANAD_MEMBERS)


def test_seanad_members_rejects_duplicate_codes():
    with pytest.raises(pa.errors.SchemaError):
        FlattenedSeanadMembersSchema.validate(SAMPLE_SEANAD_DUPLICATE)


# ---------------------------------------------------------------------------
# INTEGRATION FIXTURES
# ---------------------------------------------------------------------------

def _parquet(filename):
    path = SILVER_PARQUET_DIR / filename
    if not path.exists():
        pytest.skip(f"{filename} not found — run pipeline.py first")
    return pl.read_parquet(path)


# Add new silver parquet tables here — no other code changes needed.
SCHEMA_TESTS = [
    ("aggregated_payment_tables.parquet",      PaymentTableSchema),
    ("top_tds_by_payment_since_2020.parquet",  TopTDsByPaymentSchema),
    ("sponsors.parquet",                       SponsorSchema),
    ("stages.parquet",                         StageSchema),
    ("debates.parquet",                        DebateSchema),
    ("flattened_seanad_members.parquet",       FlattenedSeanadMembersSchema),
    ("related_docs.parquet",                   RelatedDocSchema),
    ("versions.parquet",                       VersionSchema),
    ("drop_cols_flattened_bills.parquet",      FlattenedBillsSchema),
    ("aggregated_td_tables.parquet",           AttendanceSilverSchema),
    ("flattened_members.parquet",              FlattenedMembersSchema),
]


# ---------------------------------------------------------------------------
# INTEGRATION TESTS
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.parametrize("filename,schema_cls", SCHEMA_TESTS)
def test_parquet_schema(filename, schema_cls):
    schema_cls.validate(_parquet(filename))
