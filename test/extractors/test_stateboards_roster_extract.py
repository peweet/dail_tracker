"""Unit tests for the State Boards register parser (pure functions, no network).

Fixture HTML mirrors the live structure captured 2026-06-11 from
membership.stateboards.ie (board page = metadata <p> + one member <table>).

Run:  pytest test/extractors/test_stateboards_roster_extract.py -v
"""

from __future__ import annotations

from datetime import date

import polars as pl

from extractors.stateboards_roster_extract import (
    _cache_slug,
    apply_curated,
    parse_board,
    parse_dmy,
    parse_link_list,
)

_INDEX_HTML = """
<html><body><div id="main-content">
<ul>
  <li><a href="/en/department/Department%20of%20Agriculture,%20Food%20and%20the%20Marine/">Department of Agriculture, Food and the Marine</a></li>
  <li><a href="/en/department/Other%20Boards/">Other Boards</a></li>
</ul>
<a href="/updates/">Latest Changes</a>
</div></body></html>
"""

_BOARD_HTML = """
<html><body>
<div id="title"><h1>An Bord Bia/The Food Board</h1></div>
<div id="main-content">
<p class="breadcrumbs"><a href="/en/">All Boards</a> &ndash; <a href="/en/department/X/">X</a> &ndash; An Bord Bia</p>
<p>
  <b>Legal basis: </b><a href="http://www.irishstatutebook.ie/1994/en/act/pub/0022/sec0006.html">An Bord Bia Act 1994, s. 6</a>
  <br />
  <b>Maximum Number of Positions: </b>15
  <br />
  <b>Gender Balance Numbers: </b>Female (7), Male (7)
  <br />
  <b>Gender Balance Percentage: </b>Female (50%), Male (50%)
</p>
<table>
  <thead><tr>
    <th>Name</th><th>First Appointed</th><th>Reappointed</th><th>Expiry Date</th>
    <th>Position type</th><th>Basis of appointment</th>
  </tr></thead>
  <tr>
    <td>Denis Drennan</td><td>25/01/2024</td><td>01/07/2025</td><td>30/06/2028</td>
    <td>Board Member</td><td>Appointment by the Minister - Nominating body ICMSA</td>
  </tr>
  <tr>
    <td>Cliona Murphy</td><td>15/11/2023</td><td></td><td>14/11/2026</td>
    <td>Board Member</td><td></td>
  </tr>
</table>
</div></body></html>
"""

_EMPTY_BOARD_HTML = """
<html><body>
<div id="title"><h1>Quiet Board</h1></div>
<div id="main-content"><p class="breadcrumbs">crumbs</p></div>
</body></html>
"""


def test_parse_link_list_filters_by_prefix():
    links = parse_link_list(_INDEX_HTML, "/en/department/")
    assert [t for t, _ in links] == ["Department of Agriculture, Food and the Marine", "Other Boards"]
    # absolute URL built, percent-encoding preserved; the /updates/ link is excluded
    assert links[0][1].startswith("https://membership.stateboards.ie/en/department/")


def test_parse_dmy():
    assert parse_dmy("25/01/2024") == date(2024, 1, 25)
    assert parse_dmy("") is None
    assert parse_dmy(None) is None
    assert parse_dmy("Spring 2024") is None


def test_parse_board_members_and_meta():
    board, members = parse_board(_BOARD_HTML, "Dept X", "An Bord Bia", "https://x/board")
    assert board["body_full"] == "An Bord Bia/The Food Board"
    assert board["legal_basis"] == "An Bord Bia Act 1994, s. 6"
    assert board["legal_basis_url"].startswith("http://www.irishstatutebook.ie/")
    assert board["max_positions"] == 15
    assert board["gender_female_n"] == 7
    assert board["gender_male_pct"] == 50.0
    assert board["members_listed"] == 2

    drennan = next(m for m in members if m["member_name"] == "Denis Drennan")
    assert drennan["first_appointed"] == date(2024, 1, 25)
    assert drennan["reappointed"] == date(2025, 7, 1)
    assert drennan["expiry_date"] == date(2028, 6, 30)
    assert drennan["basis_of_appointment"] == "Appointment by the Minister - Nominating body ICMSA"

    murphy = next(m for m in members if m["member_name"] == "Cliona Murphy")
    assert murphy["reappointed"] is None
    assert murphy["basis_of_appointment"] is None


def test_parse_board_without_table_degrades():
    board, members = parse_board(_EMPTY_BOARD_HTML, "Dept X", "Quiet Board", "https://x")
    assert board["members_listed"] == 0
    assert board["legal_basis"] is None
    assert members == []


_ROSTER = pl.DataFrame(
    {
        "department": ["D1", "D1"],
        "body": ["B1", "B2"],
        "member_name": ["Verified Person", "Unknown Person"],
    }
)

_CURATED = pl.DataFrame(
    {
        "member_name": ["Verified Person"],
        "wikidata_qid": ["Q1"],
        "wikidata_url": ["https://www.wikidata.org/wiki/Q1"],
        "wikidata_label": ["Verified Person"],
        "wikidata_description": ["Irish economist"],
        "wikidata_occupations": ["economist"],
        "wikidata_employers": [""],
        "wikidata_positions_held": [""],
        "curation_note": ["verified: example"],
        "curated_on": ["2026-06-12"],
    }
)


def test_apply_curated_joins_only_curated_names():
    gold = apply_curated(_ROSTER, _CURATED)
    verified = gold.filter(pl.col("member_name") == "Verified Person").row(0, named=True)
    unknown = gold.filter(pl.col("member_name") == "Unknown Person").row(0, named=True)
    assert verified["wikidata_qid"] == "Q1"
    assert verified["wikidata_curation_note"] == "verified: example"
    assert verified["wikidata_employers"] is None  # CSV blank -> null
    assert unknown["wikidata_qid"] is None  # uncurated = null, never auto-matched
    assert "curated_on" not in gold.columns


def test_apply_curated_without_csv_is_shape_stable():
    gold = apply_curated(_ROSTER, None)
    assert gold.height == 2
    assert gold["wikidata_qid"].is_null().all()
    assert "wikidata_curation_note" in gold.columns


def test_cache_slug_stable_and_safe():
    url = "https://membership.stateboards.ie/en/board/An%20Bord%20Bia/"
    assert _cache_slug(url) == "en_board_an_bord_bia.html"
    assert _cache_slug("https://membership.stateboards.ie/") == "index.html"
