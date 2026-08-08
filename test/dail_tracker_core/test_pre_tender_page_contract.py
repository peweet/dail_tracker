"""Guards for the procurement "Before the tender" section's render contract.

Three regressions found in review on 2026-08-08, all of the same shape — the data layer knew
something the reader never saw:

  * seven published stage-date columns were plumbed parquet -> view -> query -> page frame and
    rendered NOWHERE, so the one signal that separates a live building site from a lead was
    invisible (the reason the promotion joins the snapshot table back in at all);
  * `snapshot_freshness` was banded only on the school lane, so 62 of 210 cards showed no age
    pill and the headline counted 55 stale rows when 84 were over a year old;
  * the headline asserted the projects "have not been advertised" while 9 rows were reported as
    in construction.

A column reaching the page frame is not the same as a column reaching the reader, and a test
that only checks the query cannot tell the two apart. These tests check the render path.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[2]
for _p in (_ROOT, _ROOT / "utility", _ROOT / "utility" / "pages_code"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

pytest.importorskip("streamlit")

from dail_tracker_core.queries.procurement import pre_tender as _q  # noqa: E402
from extractors.pre_tender_leads_promote import (  # noqa: E402
    FRESHNESS_BANDS,
    STAGE_DATE_COLUMNS,
)
from utility.pages_code.procurement import pre_tender as page  # noqa: E402

_VIEW_SQL = (_ROOT / "sql_views" / "procurement" / "procurement_pre_tender_leads.sql").read_text(encoding="utf-8")
_PAGE_SRC = Path(page.__file__).read_text(encoding="utf-8")


def _row(**overrides):
    """One lead row as the page receives it — an itertuples-style object, not a dict."""
    base = {
        "start_on_site_date": None,
        "start_on_site_note": None,
        "stage_1_start_date": None,
        "stage_2a_start_date": None,
        "stage_2b_start_date": None,
        "stage_3_start_date": None,
        "stage_4_start_date": None,
    }
    base.update(overrides)
    return next(pd.DataFrame([base]).itertuples())


# ── the columns must reach the READER, not just the frame ────────────────────────────────────
def test_every_published_stage_column_is_read_by_the_render_path():
    """The wiring-gap check. Each column the view exposes for this purpose must be named in the
    page's own source — selecting it and never rendering it is the defect this section shipped."""
    unread = [c for c in (*STAGE_DATE_COLUMNS, "start_on_site_note") if c not in _PAGE_SRC]
    assert not unread, (
        f"columns selected by the view but never read by the page: {unread}. "
        "A column that reaches the frame and not the reader is a dead column."
    )


def test_the_card_builder_actually_calls_the_stage_date_renderer():
    """Naming a column in a helper is not rendering it. The original defect was exactly this
    shape — the code to read the column can exist and simply never be reached — so assert the
    card loop invokes it, not merely that the helper is defined somewhere in the module."""
    import inspect

    render_src = inspect.getsource(page._render_pre_tender)
    assert "_site_pills(" in render_src, "the card loop never calls _site_pills — the columns are dead again"


def test_the_view_still_exposes_every_column_the_page_reads():
    """The other direction: the page must not read a column the view has stopped publishing,
    which would silently render nothing rather than fail."""
    missing = [c for c in (*STAGE_DATE_COLUMNS, "start_on_site_note") if c not in _VIEW_SQL]
    assert not missing, f"page reads column(s) the view no longer selects: {missing}"


# ── a past start-on-site date must be rendered as what it is ─────────────────────────────────
def test_a_past_start_on_site_date_is_marked_as_already_advertised():
    """The load-bearing case. A project whose main contract is let is not a lead, and the card
    has to say so rather than presenting it beside genuinely pre-tender work."""
    pills = page._site_pills(_row(start_on_site_date="2025-05-29"), pd.Timestamp("2026-08-08").date())
    assert pills, "a past start-on-site date must render something"
    assert "already advertised and awarded" in pills[0]


def test_a_future_start_on_site_date_is_not_marked_as_advertised():
    """The same column means the opposite thing on a future date — a planned start is still a
    lead, so it must not inherit the awarded wording."""
    pills = page._site_pills(_row(start_on_site_date="2027-01-04"), pd.Timestamp("2026-08-08").date())
    assert pills and "on site due" in pills[0]
    assert "already advertised" not in " ".join(pills)


def test_published_wording_survives_where_a_date_was_expected():
    """The Department writes "TBC" in some on-site cells. The promotion keeps that wording in
    start_on_site_note precisely so it is not read as an absent date; the card shows it."""
    pills = page._site_pills(_row(start_on_site_note="TBC"), pd.Timestamp("2026-08-08").date())
    assert any("TBC" in p for p in pills)


def test_only_the_furthest_approval_gate_is_shown():
    """Five gate pills on one card would bury the observation date, which is the fact that makes
    the rest of the card safe to read. The furthest gate reached stands for the rest."""
    pills = page._site_pills(
        _row(stage_1_start_date="2020-04-15", stage_2a_start_date="2022-04-11", stage_4_start_date="2025-05-29"),
        pd.Timestamp("2026-08-08").date(),
    )
    gates = [p for p in pills if "Stage" in p]
    assert len(gates) == 1
    assert "Stage 4" in gates[0]


def test_a_corpus_with_no_published_dates_renders_no_date_pills():
    """Most corpora publish no gate dates at all; absence must stay silent, never render an
    empty or "unknown" pill."""
    assert page._site_pills(_row(), pd.Timestamp("2026-08-08").date()) == []


def test_unparseable_stage_wording_never_renders_as_a_date():
    """A cell holding wording rather than a date must not reach the reader as a date pill."""
    assert page._site_pills(_row(start_on_site_date="not yet agreed"), pd.Timestamp("2026-08-08").date()) == []


# ── every value the data can hold must have a reader-facing label ────────────────────────────
def test_every_freshness_band_the_promoter_emits_has_a_label():
    """A band with no entry in the page's map renders NO pill — silently. That is how a new
    upstream vocabulary would remove the age signal from every affected card."""
    unlabelled = [b for b in FRESHNESS_BANDS if b not in page._FRESHNESS]
    assert not unlabelled, f"freshness band(s) with no reader-facing label: {unlabelled}"


def test_every_stage_in_the_live_corpus_has_a_reader_facing_label():
    """_stage_label falls back to the underscored key, so a missing label degrades to jargon
    rather than vanishing — but it should be caught here, not read by the public."""
    duckdb = pytest.importorskip("duckdb")
    parquet = _ROOT / "data" / "gold" / "parquet" / "pre_tender_leads.parquet"
    if not parquet.exists():
        pytest.skip("gold pre-tender parquet not built")
    conn = duckdb.connect()
    try:
        stages = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT normalized_stage FROM read_parquet(?) WHERE normalized_stage IS NOT NULL",
                [str(parquet)],
            ).fetchall()
        }
    finally:
        conn.close()
    unlabelled = sorted(stages - set(page._STAGE_LABEL))
    assert not unlabelled, f"normalized_stage value(s) with no label: {unlabelled}"


# ── the headline must not assert what the rows contradict ────────────────────────────────────
def test_the_headline_states_the_construction_count_instead_of_denying_it():
    """With in-construction rows present the blanket "have not been advertised" claim is false,
    so the caveat must carry the count. Rendered HTML is captured rather than trusted."""
    captured: list[str] = []
    original = page.st.html
    page.st.html = captured.append
    try:
        page._pre_tender_caveat(
            pd.Series(
                {
                    "n_leads": 210,
                    "n_areas": 59,
                    "n_sources": 27,
                    "n_status_verified": 0,
                    "n_stale": 84,
                    "n_undated": 6,
                    "n_past_tender": 13,
                    "newest_observation": pd.Timestamp("2026-05-28"),
                }
            )
        )
    finally:
        page.st.html = original

    html = captured[0]
    assert "but which have not been advertised" not in html, "blanket claim contradicted by 13 rows"
    assert "13 of these had already reached construction" in html
    assert "84" in html and "6 carry no usable date" in html


def test_the_headline_keeps_the_blanket_claim_when_it_is_true():
    """The qualifier is conditional on the data. With no past-tender rows the stronger, simpler
    sentence is the accurate one and must come back."""
    captured: list[str] = []
    original = page.st.html
    page.st.html = captured.append
    try:
        page._pre_tender_caveat(
            pd.Series(
                {
                    "n_leads": 40,
                    "n_areas": 9,
                    "n_sources": 4,
                    "n_status_verified": 0,
                    "n_stale": 0,
                    "n_undated": 0,
                    "n_past_tender": 0,
                    "newest_observation": pd.Timestamp("2026-05-28"),
                }
            )
        )
    finally:
        page.st.html = original

    assert "but which have not been advertised" in captured[0]
    assert "reached construction" not in captured[0]


def test_the_summary_query_supplies_every_count_the_headline_renders():
    """The caveat is built from the summary rather than asserted, so the query must actually
    carry each count. A missing key would render as 0 and quietly drop a qualifier."""
    sql = _q.pre_tender_summary.__doc__ or ""
    assert sql  # the contract is documented
    import inspect

    source = inspect.getsource(_q.pre_tender_summary)
    for field in ("n_leads", "n_areas", "n_sources", "n_status_verified", "n_stale", "n_undated", "n_past_tender"):
        assert field in source, f"pre_tender_summary does not supply {field}"
