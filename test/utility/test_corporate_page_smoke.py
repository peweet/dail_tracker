"""Page-layer tests for the Corporate page's graduated panels.

PR 1 moved the receiver-appointer ranking, operator-firm concentration and CBI
badge out of corporate.py into v_corporate_receiver_* + precomputed columns. The
enrichment/view side is covered by test/extractors/test_corporate_receiver_enrich.py;
THESE tests lock the PAGE side — that the render functions actually consume those
views and compute the headline stats correctly. They run the real functions in
Streamlit bare mode and capture the emitted HTML, so a broken view-wiring or a
wrong percentage fails the test (verified by mutation — see the asserts).

Run:  pytest test/utility/test_corporate_page_smoke.py -v
"""

import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Import by dotted path: a bare ``import corporate`` resolves to the root-level
# ``corporate/`` ETL package whenever that package is already in sys.modules (it is,
# once test/corporate/ has run — which is every full-suite/CI run, alphabetically),
# and the page's attributes then appear to be missing. Same trap as test_votes_page.py.
corporate = importlib.import_module("utility.pages_code.corporate")


@pytest.fixture
def capture_html(monkeypatch):
    """Capture every HTML string the page emits via st.markdown / st.html, and
    neutralise session_state so bare-mode render doesn't need a script context."""
    sink: list[str] = []

    def _grab(*args, **kwargs):
        if args:
            sink.append(str(args[0]))

    monkeypatch.setattr(corporate.st, "markdown", _grab)
    monkeypatch.setattr(corporate.st, "html", _grab)
    monkeypatch.setattr(corporate.st, "session_state", {})
    return sink


def _patch_receiver(monkeypatch, *, summary, appointers, bucket_mix, firms, year_counts):
    monkeypatch.setattr(corporate, "fetch_receiver_summary", lambda *a, **k: summary)
    monkeypatch.setattr(corporate, "fetch_receiver_appointers", lambda *a, **k: appointers)
    monkeypatch.setattr(corporate, "fetch_receiver_bucket_mix", lambda *a, **k: bucket_mix)
    monkeypatch.setattr(corporate, "fetch_receiver_firms", lambda *a, **k: firms)
    monkeypatch.setattr(corporate, "fetch_receiver_year_counts", lambda *a, **k: year_counts)


# ── _render_featured ──────────────────────────────────────────────────────────
def test_featured_renders_ranking_and_correct_coverage_pct(capture_html, monkeypatch):
    # n_recv=100, n_tagged=40 -> coverage 40%; n_spv=10 -> 10%.
    _patch_receiver(
        monkeypatch,
        summary=pd.DataFrame([{"n_recv": 100, "n_spv": 10, "n_tagged": 40, "n_any_tagged": 25}]),
        appointers=pd.DataFrame(
            [
                {"parent": "AIB", "n_notices": 30, "dominant_fund_type": "Irish bank", "type_bucket": "bank"},
                {"parent": "Cerberus", "n_notices": 10, "dominant_fund_type": "vulture fund", "type_bucket": "vulture"},
            ]
        ),
        bucket_mix=pd.DataFrame([{"type_bucket": "bank", "n": 30}, {"type_bucket": "vulture", "n": 10}]),
        firms=pd.DataFrame(columns=["firm", "n_notices", "is_big6"]),
        year_counts=pd.DataFrame([{"year": 2023, "n": 60}, {"year": 2024, "n": 40}]),
    )
    corporate._render_featured()
    html = " ".join(capture_html)

    assert "AIB" in html and "Cerberus" in html, "appointer ranking rows must render"
    assert ">100<" in html or "100" in html, "n_recv headline must render"
    assert "40%" in html, "coverage_pct = round(100*n_tagged/n_recv) = 40% must render"
    assert "10%" in html, "spv_pct = round(100*n_spv/n_recv) = 10% must render"
    # bucket mix: bank 30/40 = 75%, vulture 10/40 = 25%
    assert "75%" in html and "25%" in html, "type-mix percentages must render"


def test_featured_empty_appointers_takes_no_data_branch(capture_html, monkeypatch):
    _patch_receiver(
        monkeypatch,
        summary=pd.DataFrame([{"n_recv": 5, "n_spv": 0, "n_tagged": 0, "n_any_tagged": 0}]),
        appointers=pd.DataFrame(columns=["parent", "n_notices", "dominant_fund_type", "type_bucket"]),
        bucket_mix=pd.DataFrame(columns=["type_bucket", "n"]),
        firms=pd.DataFrame(columns=["firm", "n_notices", "is_big6"]),
        year_counts=pd.DataFrame(columns=["year", "n"]),
    )
    corporate._render_featured()
    html = " ".join(capture_html)
    assert "No known major loan-book buyer" in html
    assert "5 receivership notices" in html.replace(",", "")


def test_featured_returns_early_when_no_receiverships(capture_html, monkeypatch):
    _patch_receiver(
        monkeypatch,
        summary=pd.DataFrame([{"n_recv": 0, "n_spv": 0, "n_tagged": 0, "n_any_tagged": 0}]),
        appointers=pd.DataFrame(columns=["parent", "n_notices", "dominant_fund_type", "type_bucket"]),
        bucket_mix=pd.DataFrame(columns=["type_bucket", "n"]),
        firms=pd.DataFrame(columns=["firm", "n_notices", "is_big6"]),
        year_counts=pd.DataFrame(columns=["year", "n"]),
    )
    corporate._render_featured()
    assert capture_html == [], "n_recv==0 must short-circuit before emitting anything"


# ── _render_operator_strip ────────────────────────────────────────────────────
def test_operator_strip_big6_pct_and_firm_chips(capture_html, monkeypatch):
    # n_recv=100, n_any_tagged=50. Big6 (Deloitte 30 + KPMG 10) = 40 of 50 -> 80%.
    _patch_receiver(
        monkeypatch,
        summary=pd.DataFrame([{"n_recv": 100, "n_spv": 0, "n_tagged": 0, "n_any_tagged": 50}]),
        appointers=pd.DataFrame(columns=["parent", "n_notices", "dominant_fund_type", "type_bucket"]),
        bucket_mix=pd.DataFrame(columns=["type_bucket", "n"]),
        firms=pd.DataFrame(
            [
                {"firm": "Deloitte", "n_notices": 30, "is_big6": True},
                {"firm": "KPMG", "n_notices": 10, "is_big6": True},
                {"firm": "Friel Stafford", "n_notices": 8, "is_big6": False},
            ]
        ),
        year_counts=pd.DataFrame(columns=["year", "n"]),
    )
    corporate._render_operator_strip()
    html = " ".join(capture_html)
    assert "Deloitte" in html and "Friel Stafford" in html, "firm chips must render"
    assert "80%" in html, "Big-6 share = (30+10)/50 = 80% must render"
    assert "50%" in html, "coverage = n_any_tagged/n_recv = 50/100 = 50% must render"


# ── _row_cbi_badge (pure reader of the precomputed columns) ────────────────────
def test_row_cbi_badge_reads_precomputed_columns():
    hit = corporate._row_cbi_badge(pd.Series({"cbi_register": "Register of Schedule 2 Firms", "cbi_ref_no": "C123"}))
    assert hit == {"register": "Register of Schedule 2 Firms", "ref_no": "C123"}
    assert corporate._row_cbi_badge(pd.Series({"cbi_register": "", "cbi_ref_no": ""})) is None
    assert corporate._row_cbi_badge(pd.Series({"cbi_register": None, "cbi_ref_no": None})) is None


# ── firm_notices query (filters on the precomputed receiver_firms tag) ─────────
# The old page-side _firm_notice_mask graduated into dail_tracker_core.queries.
# corporate.firm_notices, where the matching runs in DuckDB. Same semantics pinned.
def _firm_notices_conn(rows_sql: str):
    import duckdb

    con = duckdb.connect()
    con.execute(f"CREATE VIEW v_corporate_notices AS SELECT * FROM (VALUES {rows_sql}) t(receiver_firms, raw_text)")
    return con


def test_firm_notices_uses_receiver_firms_column():
    from dail_tracker_core.queries import corporate as q

    con = _firm_notices_conn(
        "(['KPMG','Deloitte'], 'x'), (['Grant Thornton'], 'y'), (CAST([] AS VARCHAR[]), 'names KPMG in prose')"
    )
    res = q.firm_notices(con, "KPMG")
    assert res.ok
    assert res.data["raw_text"].tolist() == ["x"], "curated firm matched via the tag column, not raw_text"


def test_firm_notices_falls_back_to_raw_text_for_uncurated_firm():
    from dail_tracker_core.queries import corporate as q

    con = _firm_notices_conn(
        "(CAST([] AS VARCHAR[]), 'mentions Acme Receivers Ltd'), (CAST([] AS VARCHAR[]), 'nothing')"
    )
    res = q.firm_notices(con, "Acme Receivers")
    assert res.ok
    assert res.data["raw_text"].tolist() == ["mentions Acme Receivers Ltd"], (
        "uncurated firm falls back to word-boundary raw_text search"
    )


# ── integration: real page over live views ────────────────────────────────────
@pytest.mark.integration
def test_corporate_page_renders_without_exception():
    assert corporate.corporate_page() is None
