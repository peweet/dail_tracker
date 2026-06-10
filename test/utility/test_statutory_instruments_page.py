"""Unit + bare-mode render tests for pages_code/statutory_instruments.py.

The SI page was ~10% covered — a large pure-helper surface (token/ref
formatters, the filter engine, card/HTML builders) plus st.*-driven render
functions and the page entry point. Pattern mirrors
test_procurement_page_smoke.py: assert the pure helpers directly, then drive
the render functions and the page entry in Streamlit *bare mode*.

Run:  pytest test/utility/test_statutory_instruments_page.py -v
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Insert repo-root first so utility/ ends up ahead of it (config resolution); also
# registering repo-root keeps test_page_imports.py from re-inserting it at the front
# and inverting the order. See the dual-config note in test_page_imports.py.
_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import statutory_instruments as si  # noqa: E402


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


def _si_df() -> pd.DataFrame:
    """A small, fully-populated SI frame covering every column the page reads."""
    return pd.DataFrame(
        [
            {
                "si_id": "2025-332", "si_year": 2025, "si_number": 332,
                "si_title": "Fisheries (Amendment) Regulations 2025",
                "si_signed_date": pd.Timestamp("2025-12-15"),
                "si_policy_domain": "marine_fisheries", "si_operation": "amendment",
                "si_department_label": "Agriculture", "si_minister_name": "A Minister",
                "si_is_eu": True, "bill_id": "bill-1-of-2024", "bill_short_title": "Sea Act",
                "current_state": "revoked", "lrc_primary_subject": "Fisheries",
                "eisb_url": "https://www.irishstatutebook.ie/eli/2025/si/332/made/en/print",
            },
            {
                "si_id": "2018-10", "si_year": 2018, "si_number": 10,
                "si_title": "Health Order 2018",
                "si_signed_date": pd.Timestamp("2018-05-01"),
                "si_policy_domain": "health", "si_operation": "commencement",
                "si_department_label": "Health", "si_minister_name": "B Minister",
                "si_is_eu": False, "bill_id": None, "bill_short_title": "",
                "current_state": None, "lrc_primary_subject": "Health",
                "eisb_url": None,
            },
        ]
    )


# ── _safe ──────────────────────────────────────────────────────────────────────
def test_safe():
    assert si._safe(None) == ""
    assert si._safe(float("nan")) == ""
    assert si._safe("x") == "x"
    assert si._safe(5) == "5"


# ── _pretty_token ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", ""),
        ("fisheries", "Fisheries"),
        ("marine_fisheries", "Marine fisheries"),
        ("eu_derived", "EU derived"),
        ("eu_full_effect", "EU full effect"),
        ("Mixed Case Human", "Mixed Case Human"),
    ],
)
def test_pretty_token(raw, expected):
    assert si._pretty_token(raw) == expected


def test_pretty_token_non_string():
    assert si._pretty_token(None) == ""
    assert si._pretty_token(123) == ""


# ── _split_multi ─────────────────────────────────────────────────────────────────
def test_split_multi():
    assert si._split_multi("a|b| c ") == ["a", "b", "c"]
    assert si._split_multi("") == []
    assert si._split_multi(None) == []
    assert si._split_multi("x;y", sep=";") == ["x", "y"]


# ── _state_card_pill ─────────────────────────────────────────────────────────────
def test_state_card_pill():
    assert "Revoked" in si._state_card_pill("revoked")
    assert "si-pill-revoked" in si._state_card_pill("revoked")
    assert si._state_card_pill("in_force_as_made") == ""  # no positive pill
    assert si._state_card_pill("unknown_state") == ""
    assert si._state_card_pill("") == ""


# ── _fmt_si_ref / _si_ref_eli_url ────────────────────────────────────────────────
def test_fmt_si_ref():
    assert si._fmt_si_ref("332/2025") == "S.I. No. 332 of 2025"
    assert si._fmt_si_ref("not-a-ref") == "not-a-ref"
    assert si._fmt_si_ref("") == ""


def test_si_ref_eli_url():
    assert si._si_ref_eli_url("332/2025").endswith("/eli/2025/si/332/made/en/html")
    assert si._si_ref_eli_url("junk") == ""


# ── _affecting_list ──────────────────────────────────────────────────────────────
def test_affecting_list():
    assert si._affecting_list(None) == []
    assert si._affecting_list(np.nan) == []  # scalar NaN → TypeError → []
    assert si._affecting_list(["1/2020", "2/2021"]) == ["1/2020", "2/2021"]
    assert si._affecting_list(np.array(["3/2022"])) == ["3/2022"]


# ── _eisb_url ────────────────────────────────────────────────────────────────────
def test_eisb_url():
    # Prefers an explicit http eisb_url.
    assert si._eisb_url(pd.Series({"eisb_url": "https://example.com/x"})) == "https://example.com/x"
    # Falls back to the canonical ELI pattern from year/number.
    url = si._eisb_url(pd.Series({"eisb_url": None, "si_year": 2025, "si_number": 332}))
    assert url.endswith("/eli/2025/si/332/made/en/print")
    # Nothing usable → "".
    assert si._eisb_url(pd.Series({"eisb_url": None, "si_year": None, "si_number": None})) == ""


# ── _tab_label ───────────────────────────────────────────────────────────────────
def test_tab_label():
    assert si._tab_label("Department", None) == "Department"
    assert si._tab_label("Department", "Justice") == "Department: Justice"
    long = si._tab_label("Department", "A" * 30)
    assert long.endswith("…") and len(long) < len("Department: " + "A" * 30)


# ── _apply_filters ───────────────────────────────────────────────────────────────
def test_apply_filters_year_and_search():
    df = _si_df()
    assert len(si._apply_filters(df, years=[2025], domain="All", op="All", department="All",
                                 minister="All", eu_only=False, search="")) == 1
    assert len(si._apply_filters(df, years=[], domain="All", op="All", department="All",
                                 minister="All", eu_only=False, search="health")) == 1


def test_apply_filters_eu_state_subject_postcommittee():
    df = _si_df()
    # EU-only keeps the 2025 EU row.
    eu = si._apply_filters(df, years=[], domain="All", op="All", department="All",
                           minister="All", eu_only=True, search="")
    assert list(eu["si_year"]) == [2025]
    # state filter.
    rev = si._apply_filters(df, years=[], domain="All", op="All", department="All",
                            minister="All", eu_only=False, search="", state="revoked")
    assert list(rev["si_year"]) == [2025]
    # __unchecked__ keeps the null-state row.
    unchk = si._apply_filters(df, years=[], domain="All", op="All", department="All",
                              minister="All", eu_only=False, search="", state="__unchecked__")
    assert list(unchk["si_year"]) == [2018]
    # subject filter.
    subj = si._apply_filters(df, years=[], domain="All", op="All", department="All",
                             minister="All", eu_only=False, search="", subject="Health")
    assert list(subj["si_year"]) == [2018]
    # post_committee keeps only the Dec-2025 row.
    pc = si._apply_filters(df, years=[], domain="All", op="All", department="All",
                           minister="All", eu_only=False, search="", post_committee=True)
    assert list(pc["si_year"]) == [2025]


def test_apply_filters_domain_op_dept_minister():
    df = _si_df()
    assert len(si._apply_filters(df, years=[], domain="health", op="All", department="All",
                                 minister="All", eu_only=False, search="")) == 1
    assert len(si._apply_filters(df, years=[], domain="All", op="amendment", department="All",
                                 minister="All", eu_only=False, search="")) == 1
    assert len(si._apply_filters(df, years=[], domain="All", op="All", department="Health",
                                 minister="All", eu_only=False, search="")) == 1
    assert len(si._apply_filters(df, years=[], domain="All", op="All", department="All",
                                 minister="B Minister", eu_only=False, search="")) == 1


# ── _eu_scrutiny_stats ───────────────────────────────────────────────────────────
def test_eu_scrutiny_stats():
    stats = si._eu_scrutiny_stats(_si_df())
    assert stats["count"] == 1  # only the EU + post-Dec-2025 row
    assert "Agriculture" in stats["top_depts"]
    assert len(stats["eu_df"]) == 1


# ── _render_si_card ──────────────────────────────────────────────────────────────
def test_render_si_card():
    html = si._render_si_card(_si_df().iloc[0])
    assert "Fisheries (Amendment) Regulations 2025" in html
    assert "SI No. 2025-332" in html
    assert "Revoked" in html  # negative legal-state pill leads
    assert "EU-derived" in html
    assert "Made under Sea Act" in html


# ── load_si (monkeypatched fetch) ────────────────────────────────────────────────
def test_load_si_drops_mojibake(monkeypatch):
    raw = pd.DataFrame(
        [
            {"si_id": "1", "si_title": "Clean Title", "si_signed_date": "2025-01-01"},
            {"si_id": "2", "si_title": "Broken � Title", "si_signed_date": "2025-01-02"},
        ]
    )
    monkeypatch.setattr(si, "fetch_si_entity_index_classified", lambda *a, **k: raw)
    si.load_si.clear()
    out = si.load_si()
    assert len(out) == 1
    assert out.iloc[0]["si_title"] == "Clean Title"
    si.load_si.clear()


def test_load_si_falls_back_to_unclassified(monkeypatch):
    monkeypatch.setattr(si, "fetch_si_entity_index_classified", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(
        si, "fetch_si_entity_index",
        lambda *a, **k: pd.DataFrame([{"si_id": "9", "si_title": "Fallback", "si_signed_date": "2020-01-01"}]),
    )
    si.load_si.clear()
    out = si.load_si()
    assert list(out["si_title"]) == ["Fallback"]
    si.load_si.clear()


# ── render functions (bare mode) ─────────────────────────────────────────────────
def test_render_kpi_strip(monkeypatch):
    _silence_streamlit()
    assert si._render_kpi_strip(_si_df()) is None
    assert si._render_kpi_strip(pd.DataFrame()) is None  # empty → early return


def test_render_si_index(monkeypatch):
    _silence_streamlit()
    assert si._render_si_index(_si_df()) is None
    assert si._render_si_index(pd.DataFrame()) is None


@pytest.mark.parametrize("state", ["revoked", "amended", "in_force_as_made", "other_affected", None])
def test_render_legal_status(state):
    _silence_streamlit()
    row = pd.Series(
        {
            "current_state": state,
            "directory_updated_to": "2026-01-01",
            "state_source_url": "https://eisb/x",
            "affecting_sis": ["100/2026"] if state else None,
            "si_year": 2025, "si_number": 332,
            "how_affected_raw": "Revoked on 1 Jan 2026",
            "eisb_url": "https://eisb/orig",
        }
    )
    assert si._render_legal_status(row) is None


def test_render_lrc_classification():
    _silence_streamlit()
    assert si._render_lrc_classification(pd.Series({"lrc_primary_subject": ""})) is None  # silent
    row = pd.Series({"lrc_primary_subject": "Fisheries", "lrc_primary_leaf": "Sea fish",
                     "lrc_list_updated_to": "2026-01-01"})
    assert si._render_lrc_classification(row) is None


def test_render_amendments_made(monkeypatch):
    _silence_streamlit()
    # Empty → early return.
    monkeypatch.setattr(si, "fetch_si_amendments_made", lambda *a, **k: pd.DataFrame())
    assert si._render_amendments_made(pd.Series({"si_year": 2025, "si_number": 332})) is None
    # Bad year/number → early return without calling the fetch.
    assert si._render_amendments_made(pd.Series({"si_year": None, "si_number": None})) is None
    # Populated amendment graph.
    amd = pd.DataFrame(
        [{"effect": "Revokes", "affected_number": 5, "affected_year": 2010,
          "affected_title": "Old Reg", "affected_eli_url": "", "provision_note": "Reg. 2"}]
    )
    monkeypatch.setattr(si, "fetch_si_amendments_made", lambda *a, **k: amd)
    assert si._render_amendments_made(pd.Series({"si_year": 2025, "si_number": 332})) is None


def test_render_si_detail(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(si, "fetch_si_amendments_made", lambda *a, **k: pd.DataFrame())
    row = _si_df().iloc[0].copy()
    row["si_responsible_actor"] = "Minister for Agriculture"
    row["si_signatory_name"] = "The Signatory"
    row["si_minister_member_code"] = "TD-1"
    row["si_form"] = "regulations"
    row["si_eu_relationship"] = "eu_derived"
    row["si_parent_legislation"] = "Sea Act 1959|Fisheries Act 2003"
    row["si_operation_flags"] = "amendment|commencement"
    row["si_policy_domains_all"] = "marine_fisheries|environment"
    row["iris_source_pdf"] = "Iris-2025-99.pdf"
    assert si._render_si_detail(row) is None


def test_render_facets_and_eu_tab():
    _silence_streamlit()
    df = _si_df()
    assert si._render_facets(df) is None
    assert si._render_facets(pd.DataFrame()) is None
    assert si._render_eu_scrutiny_tab(df) is None


# ── page entry (bare mode, monkeypatched loader) ─────────────────────────────────
def test_statutory_instruments_page_index(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(si, "fetch_si_entity_index_classified", lambda *a, **k: _si_df())
    monkeypatch.setattr(si, "fetch_si_amendments_made", lambda *a, **k: pd.DataFrame())
    si.load_si.clear()
    assert si.statutory_instruments_page() is None
    si.load_si.clear()


def test_statutory_instruments_page_empty(monkeypatch):
    # A registered view that returns no rows still carries its schema (typed
    # empty frame). The index path must flow through to the empty-state, not crash.
    _silence_streamlit()
    empty_typed = _si_df().iloc[0:0]
    monkeypatch.setattr(si, "fetch_si_entity_index_classified", lambda *a, **k: empty_typed)
    monkeypatch.setattr(si, "fetch_si_entity_index", lambda *a, **k: empty_typed)
    si.load_si.clear()
    assert si.statutory_instruments_page() is None
    si.load_si.clear()
