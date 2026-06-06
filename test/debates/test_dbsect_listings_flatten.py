"""Unit tests for debates/dbsect_listings_flatten.py.

``flatten_listings`` turns bronze debate-listing records into the silver
one-row-per-(date, chamber, debate_section_id) table. The tests build the
record dicts by hand and assert the derived columns: chamber resolution
(committee rows dropped), bill_ref extraction with event-uri fallback, the
constructed AKN-XML url, the public web url (dbsect_ prefix stripped), and
the composite-key de-duplication.
"""

from __future__ import annotations

import json

import pandas as pd

from debates.dbsect_listings_flatten import _col, _debate_records, flatten_listings


def _record(date, house_code, dbsect, *, chamber_type="house", bill=None, bill_event=None, xml_uri=None, show_as="X"):
    section: dict = {
        "debateSectionId": dbsect,
        "debateType": "bill",
        "showAs": show_as,
        "counts": {"speakerCount": 5, "speechCount": 12},
    }
    if bill is not None:
        section["bill"] = {"uri": bill}
    if bill_event is not None:
        section.setdefault("bill", {})["event"] = {"uri": bill_event}
    if xml_uri is not None:
        section["formats"] = {"xml": {"uri": xml_uri}}
    return {
        "date": date,
        "house": {"houseCode": house_code, "chamberType": chamber_type},
        "chamber": {"uri": f"https://data.oireachtas.ie/akn/ie/house/{house_code}"},
        "debateSections": [{"debateSection": section}],
    }


def test_flatten_basic_derived_columns():
    rows = flatten_listings([_record("2026-04-23", "dail", "dbsect_63", bill="https://x/bill/2026/75")])
    assert len(rows) == 1
    r = rows.iloc[0]
    assert r["debate_section_id"] == "dbsect_63"
    assert r["chamber"] == "dail"
    assert r["date"] == "2026-04-23"
    assert r["bill_ref"] == "2026_75"
    assert r["speaker_count"] == 5 and r["speech_count"] == 12
    # AKN url is constructed when no formats.xml.uri present.
    assert r["akn_xml_url"] == (
        "https://data.oireachtas.ie/akn/ie/debateRecord/dail/2026-04-23/debate/mul@/dbsect_63.xml"
    )
    # Public web url uses the bare section number (dbsect_ prefix stripped).
    assert r["debate_url_web"] == "https://www.oireachtas.ie/en/debates/debate/dail/2026-04-23/63/"


def test_explicit_xml_uri_overrides_constructed_akn():
    rows = flatten_listings(
        [_record("2026-04-23", "seanad", "dbsect_1", xml_uri="https://data.oireachtas.ie/real/xml/path.xml")]
    )
    assert rows.iloc[0]["akn_xml_url"] == "https://data.oireachtas.ie/real/xml/path.xml"
    assert rows.iloc[0]["chamber"] == "seanad"


def test_bill_ref_falls_back_to_event_uri():
    rows = flatten_listings([_record("2026-01-01", "dail", "dbsect_2", bill_event="https://x/bill/2019/100")])
    assert rows.iloc[0]["bill_ref"] == "2019_100"


def test_committee_rows_are_dropped():
    recs = [
        _record("2026-04-23", "dail", "dbsect_63"),
        _record("2026-04-23", "dail", "dbsect_99", chamber_type="committee"),
    ]
    rows = flatten_listings(recs)
    # The committee record resolves chamber → "" and is filtered out.
    assert list(rows["debate_section_id"]) == ["dbsect_63"]


def test_composite_key_dedup():
    # Same (date, chamber, debate_section_id) twice → collapsed to one row.
    recs = [
        _record("2026-04-23", "dail", "dbsect_63", show_as="first"),
        _record("2026-04-23", "dail", "dbsect_63", show_as="second"),
    ]
    rows = flatten_listings(recs)
    assert len(rows) == 1


def test_empty_input_returns_empty_frame():
    out = flatten_listings([])
    assert isinstance(out, pd.DataFrame)
    assert out.empty


# ── _debate_records (bronze loader) ──────────────────────────────────────────


def test_debate_records_flattens_pages_and_skips_non_records(tmp_path):
    bronze = tmp_path / "listings.json"
    bronze.write_text(
        json.dumps(
            [
                {"results": [{"debateRecord": {"id": 1}}, {"notADebate": {}}]},
                {"results": [{"debateRecord": {"id": 2}}]},
                {"results": None},  # tolerated
            ]
        ),
        encoding="utf-8",
    )
    recs = _debate_records(bronze)
    assert [r["id"] for r in recs] == [1, 2]


def test_debate_records_missing_file_returns_empty(tmp_path):
    assert _debate_records(tmp_path / "does_not_exist.json") == []


# ── _col helper ──────────────────────────────────────────────────────────────


def test_col_returns_string_series_or_all_na():
    df = pd.DataFrame({"present": [1, 2]})
    present = _col(df, "present")
    assert present.dtype == "string"
    assert list(present) == ["1", "2"]

    absent = _col(df, "missing")
    assert absent.dtype == "string"
    assert absent.isna().all()
    assert len(absent) == 2
