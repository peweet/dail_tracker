"""Unit + bare-mode render tests for ui/vote_explorer.py.

vote_explorer is the shared vote-evidence rendering module behind the Votes
page and the member-overview Votes expander. It was ~10% covered: a wall of
pure HTML/figure builders plus three st.*-driven render entry points.

Strategy (mirrors test_procurement_page_smoke.py):
  * pure helpers (_vote_icon / _outcome_chip / _fmt_date / _split_title_and_stage
    / the *_card_html / *_list_html builders / _party_chart) are asserted directly
    — deterministic, no Streamlit context needed;
  * the render entry points (render_division_panel / render_td_panel /
    render_member_votes) are called in Streamlit *bare mode*, where st.* calls
    warn-and-no-op, so the real branch logic executes without a script run.

Run:  pytest test/utility/test_vote_explorer.py -v
"""

from __future__ import annotations

import datetime
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

# Insert repo-root *and* utility/ (root first, so utility ends up ahead of it).
# Registering repo-root here means test_page_imports.py finds it already present
# and won't re-insert it at the front — which would otherwise push repo-root ahead
# of utility/ and make pages resolve the wrong (root) config.py. See the dual-config
# note in test_page_imports.py.
_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT), str(_ROOT / "utility")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ui.vote_explorer as ve  # noqa: E402


# ── _vote_icon / _outcome_chip ────────────────────────────────────────────────
@pytest.mark.parametrize(
    "vt,needle",
    [
        ("Voted Yes", "dt-vt-yes"),
        ("Voted No", "dt-vt-no"),
        ("Abstained", "dt-vt-abs"),
        ("", "dt-vt-abs"),
        (None, "dt-vt-abs"),
    ],
)
def test_vote_icon(vt, needle):
    out = ve._vote_icon(vt)
    assert needle in out
    assert out.startswith("<span")


def test_outcome_chip_variants():
    assert "carried" in ve._outcome_chip("Carried").lower()
    assert "lost" in ve._outcome_chip("Lost").lower()
    assert "other" in ve._outcome_chip("Withdrawn").lower()
    assert ve._outcome_chip("") == ""
    assert ve._outcome_chip(None) == ""


# ── _fmt_date ─────────────────────────────────────────────────────────────────
def test_fmt_date():
    assert ve._fmt_date(None) == "—"
    assert ve._fmt_date(datetime.date(2024, 7, 1)) == "01 Jul 2024"
    assert ve._fmt_date(pd.Timestamp("2024-07-01")) == "01 Jul 2024"
    # Plain string falls back to first-10-chars.
    assert ve._fmt_date("2024-07-01T10:00") == "2024-07-01"
    assert ve._fmt_date("None") == "—"


# ── _split_title_and_stage ────────────────────────────────────────────────────
def test_split_title_and_stage():
    assert ve._split_title_and_stage("No colon here") == ("No colon here", "")
    title, stage = ve._split_title_and_stage("Foo Bill 2025: Report and Final Stages")
    assert title == "Foo Bill 2025"
    assert stage == "Report and Final Stages"
    # Trailing [Private Members] is stripped from the stage half.
    title, stage = ve._split_title_and_stage("Bar Bill: Second Stage [Private Members]")
    assert stage == "Second Stage"
    # Trailing punctuation is trimmed.
    _, stage = ve._split_title_and_stage("Baz: Committee Stage;")
    assert stage == "Committee Stage"


# ── vt_division_card_html ─────────────────────────────────────────────────────
def test_vt_division_card_html_carried_private():
    row = {
        "vote_date": pd.Timestamp("2025-03-04"),
        "debate_title": "Housing Bill 2025: Second Stage [Private Members]",
        "vote_outcome": "Carried",
        "yes_count": 80,
        "no_count": 60,
        "abstained_count": 2,
        "margin": 20,
        "oireachtas_url": "https://oireachtas.ie/x",
    }
    html = ve.vt_division_card_html(row)
    assert "Housing Bill 2025" in html
    assert "[Private Members]" not in html  # jargon suffix lifted to a pill
    assert "Private Members" in html  # ...but the pill is present
    assert "Carried ✓" in html
    assert "won by 20" in html
    assert "04 Mar 2025" in html


def test_vt_division_card_html_lost_no_margin():
    row = {"debate_title": "Some Motion", "vote_outcome": "Lost", "margin": None}
    html = ve.vt_division_card_html(row)
    assert "Lost ✗" in html
    assert "won by" not in html and "lost by" not in html  # no margin pill


def test_vt_division_card_html_handles_missing_fields():
    # Empty row → no crash, em-dash title.
    html = ve.vt_division_card_html({})
    assert "—" in html


# ── member_vote_card_html ─────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "vt,needle",
    [
        ("Voted Yes", "vt-rec-card-yes"),
        ("Voted No", "vt-rec-card-no"),
        ("Abstained", "vt-rec-card-abs"),
        ("weird", "vt-rec-card-abs"),
    ],
)
def test_member_vote_card_html(vt, needle):
    html = ve.member_vote_card_html(
        vote_date=pd.Timestamp("2024-01-15"),
        debate_title="A Bill",
        vote_type=vt,
        vote_outcome="Carried",
        oireachtas_url="",
    )
    assert needle in html
    assert "A Bill" in html
    assert "15 Jan 2024" in html


def test_member_vote_card_html_escapes_title():
    html = ve.member_vote_card_html(
        vote_date=None, debate_title="<script>x</script>", vote_type="Voted Yes", vote_outcome=""
    )
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


# ── _render_td_history_html / _render_member_list_html ────────────────────────
def test_render_td_history_html():
    df = pd.DataFrame(
        [
            {
                "vote_date": pd.Timestamp("2024-02-02"),
                "debate_title": "Health Bill",
                "vote_type": "Voted Yes",
                "vote_outcome": "Carried",
                "oireachtas_url": "https://oireachtas.ie/y",
            }
        ]
    )
    html = ve._render_td_history_html(df)
    assert "Health Bill" in html
    assert "<table" in html
    assert "02 Feb 2024" in html


def test_render_member_list_html_with_and_without_id():
    df = pd.DataFrame(
        [
            {"member_name": "Jane Doe", "member_id": "m1", "party_name": "PartyA",
             "constituency": "Dublin", "vote_type": "Voted No"},
            {"member_name": "John Roe", "member_id": "", "party_name": "PartyB",
             "constituency": "Cork", "vote_type": "Voted Yes"},
        ]
    )
    html = ve._render_member_list_html(df)
    assert "Jane Doe" in html
    assert "John Roe" in html
    assert "<table" in html


# ── _party_chart ──────────────────────────────────────────────────────────────
def test_party_chart_returns_none_on_empty_or_missing_cols():
    assert ve._party_chart(pd.DataFrame()) is None
    assert ve._party_chart(pd.DataFrame([{"party_name": "X"}])) is None  # no vote_type
    assert ve._party_chart(pd.DataFrame([{"party_name": None, "vote_type": "Voted Yes"}])) is None


def test_party_chart_builds_figure():
    df = pd.DataFrame(
        [
            {"party_name": "PartyA", "vote_type": "Voted Yes", "member_count": 10},
            {"party_name": "PartyA", "vote_type": "Voted No", "member_count": 2},
            {"party_name": "PartyB", "vote_type": "Voted No", "member_count": 5},
        ]
    )
    fig = ve._party_chart(df)
    assert fig is not None
    assert len(fig.data) == 3  # Yes / No / Abstained traces


# ── render entry points (bare mode — st.* no-ops without a run context) ───────
def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


def test_render_division_panel_full(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(ve, "render_source_links", lambda *a, **k: None)
    vote_row = pd.Series(
        {
            "vote_id": "DAIL-123",
            "vote_outcome": "Carried",
            "vote_date": pd.Timestamp("2025-01-01"),
            "debate_title": "A Division",
            "yes_count": 70,
            "no_count": 60,
            "abstained_count": 1,
            "margin": 10,
        }
    )
    members = pd.DataFrame(
        [{"member_name": "Jane Doe", "party_name": "PartyA", "constituency": "Dublin",
          "vote_type": "Voted Yes", "member_id": "m1"}]
    )
    breakdown = pd.DataFrame(
        [{"party_name": "PartyA", "vote_type": "Voted Yes", "member_count": 70}]
    )
    sources = pd.DataFrame([{"source_url": "https://oireachtas.ie/z"}])
    # Must not raise.
    assert ve.render_division_panel(vote_row, members, sources, breakdown) is None


def test_render_division_panel_empty_branches(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(ve, "render_source_links", lambda *a, **k: None)
    vote_row = pd.Series({"vote_id": "", "vote_outcome": "", "debate_title": ""})
    empty = pd.DataFrame()
    assert ve.render_division_panel(vote_row, empty, empty, empty) is None


def test_render_member_votes_no_conn():
    _silence_streamlit()
    assert ve.render_member_votes(None, "m1") is None


def test_render_member_votes_with_duckdb():
    _silence_streamlit()
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE td_vote_summary AS SELECT * FROM (VALUES "
        "('m1','Jane Doe','PartyA','Dublin',10,2,1,13,76.9)) "
        "t(member_id, member_name, party_name, constituency, yes_count, no_count, "
        "abstained_count, division_count, yes_rate_pct)"
    )
    conn.execute(
        "CREATE TABLE v_vote_member_detail AS SELECT * FROM (VALUES "
        "('v1', DATE '2024-01-01','A Bill','Voted Yes','Carried','m1','http://x')) "
        "t(vote_id, vote_date, debate_title, vote_type, vote_outcome, member_id, oireachtas_url)"
    )
    conn.execute(
        "CREATE TABLE td_vote_year_summary AS SELECT * FROM (VALUES "
        "('m1', 2024, 8, 1, 1)) t(member_id, year, yes_count, no_count, abstained_count)"
    )
    assert ve.render_member_votes(conn, "m1", key_suffix="_t") is None


def test_render_member_votes_member_not_found():
    _silence_streamlit()
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE td_vote_summary AS SELECT * FROM (VALUES "
        "('m1','Jane','P','C',1,1,1,3,50.0)) "
        "t(member_id, member_name, party_name, constituency, yes_count, no_count, "
        "abstained_count, division_count, yes_rate_pct)"
    )
    # Unknown member id → empty td_df → "no vote data" branch, returns cleanly.
    assert ve.render_member_votes(conn, "nope") is None
