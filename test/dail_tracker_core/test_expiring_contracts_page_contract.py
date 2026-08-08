"""Guard for the same defect shape fixed in the "Before the tender" section (see
test_pre_tender_page_contract.py): a field selected by the query is not the same as a field a
reader ever sees.

``is_multi_supplier_framework`` / ``n_winners`` (row grain) and ``n_frameworks`` (corpus grain)
were selected by ``dail_tracker_core.queries.procurement.ted`` and reached the page's own
dataframe, but the render function did not call anything that turned them into copy — the one
signal telling a smaller firm "several suppliers hold this, competed at each call-off" was
invisible. Fixed by extracting ``_framework_pill``/`_expiring_contracts_caveat` as named,
independently callable functions; these tests pin both the render call-site and the copy's
conditionality so the same field can't go dark again without a test failing.
"""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[2]
for _p in (_ROOT, _ROOT / "utility", _ROOT / "utility" / "pages_code"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

pytest.importorskip("streamlit")

from dail_tracker_core.queries.procurement import ted as _q  # noqa: E402
from utility.pages_code.procurement import tenders as page  # noqa: E402


def _row(**overrides):
    base = {"is_multi_supplier_framework": False, "n_winners": None}
    base.update(overrides)
    return next(pd.DataFrame([base]).itertuples())


# ── the field must reach the READER, not just the frame ──────────────────────────────────────
def test_the_render_loop_actually_calls_the_framework_pill():
    """Naming a column is not rendering it — the original defect was exactly this shape. Assert
    the card loop invokes the pill function, not merely that it is defined somewhere in the module."""
    render_src = inspect.getsource(page._render_expiring_contracts)
    assert "_framework_pill(" in render_src, "the card loop never calls _framework_pill — the flag is dead again"
    assert "_expiring_contracts_caveat(" in render_src, "the card loop never calls _expiring_contracts_caveat"


def test_the_query_still_supplies_every_field_the_page_reads():
    """The other direction: the page must not read a field the query has stopped selecting, which
    would silently render nothing rather than fail."""
    list_source = inspect.getsource(_q.expiring_contracts)
    stats_source = inspect.getsource(_q.expiring_contracts_stats)
    for field in ("is_multi_supplier_framework", "n_winners"):
        assert field in list_source, f"the page reads {field} but expiring_contracts no longer selects it"
    assert "n_frameworks" in stats_source, "the page reads n_frameworks but expiring_contracts_stats no longer supplies it"


# ── the pill must be right, and only appear when the flag is true ────────────────────────────
def test_a_multi_supplier_framework_is_marked_as_such():
    pill = page._framework_pill(_row(is_multi_supplier_framework=True, n_winners=4))
    assert pill is not None
    assert "multi-supplier framework" in pill
    assert "(4 suppliers)" in pill


def test_a_single_winner_contract_gets_no_framework_pill():
    assert page._framework_pill(_row(is_multi_supplier_framework=False)) is None


def test_a_framework_with_one_recorded_winner_omits_the_redundant_count():
    """n_winners=1 on a flagged framework is a notice quirk, not two suppliers — don't print '(1
    suppliers)'."""
    pill = page._framework_pill(_row(is_multi_supplier_framework=True, n_winners=1))
    assert pill is not None
    assert "suppliers)" not in pill


# ── the headline count must be conditional on the data, not asserted ─────────────────────────
def test_the_headline_states_the_framework_count_when_present():
    captured: list[str] = []
    original = page.st.html
    page.st.html = captured.append
    try:
        page._expiring_contracts_caveat(
            pd.Series({"n_with_estimate": 40, "n_explicit": 10, "n_frameworks": 6})
        )
    finally:
        page.st.html = original
    assert "6 of them are multi-supplier frameworks" in captured[0]


def test_the_headline_omits_the_framework_clause_when_there_are_none():
    captured: list[str] = []
    original = page.st.html
    page.st.html = captured.append
    try:
        page._expiring_contracts_caveat(
            pd.Series({"n_with_estimate": 40, "n_explicit": 10, "n_frameworks": 0})
        )
    finally:
        page.st.html = original
    assert "multi-supplier framework" not in captured[0]
