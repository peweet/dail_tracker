"""The ABP document-text privacy gate must be able to FAIL.

`assert_privacy_invariant` is correct by construction in the shipped pipeline: the tier is derived
from the same stripped body the gate tests, so no row the extractor builds can trip it. That is the
point of an invariant — it guards future edits, not today's data. But an invariant nothing ever
exercises is indistinguishable from one that cannot fire, which is this repo's recorded rule: prove
a gate can fail before committing it.

So these tests hand-build the rows the extractor cannot, and pin both directions.
"""

from __future__ import annotations

import polars as pl
import pytest

from pipeline_sandbox.new_sources.abp_doc_text_extract import (
    TIER_INDEX_ONLY,
    TIER_WITH_TEXT,
    PrivacyInvariantError,
    assert_privacy_invariant,
)

# The leak shape the corpus actually contains — an appellant's name and home address in body text.
# Real shape, not invented: ABP orders carry exactly this, which is why body text is tiered for
# review rather than published.
LEAK_TEXT = "Appeal by David Tobin of 3 Bay View, Possess Point, County Sligo"


def _frame(**over) -> pl.DataFrame:
    row = {
        "n_chars": 0,
        "text": None,
        "privacy_tier": TIER_INDEX_ONLY,
        "public_display": False,
    }
    row.update(over)
    return pl.DataFrame([row], schema={"n_chars": pl.Int64, "text": pl.String, "privacy_tier": pl.String, "public_display": pl.Boolean})


def test_text_bearing_row_tiered_public_is_refused():
    """The gate fires: body text tiered index-only is the leak it exists to stop."""
    df = _frame(n_chars=len(LEAK_TEXT), text=LEAK_TEXT, privacy_tier=TIER_INDEX_ONLY)
    with pytest.raises(PrivacyInvariantError):
        assert_privacy_invariant(df)


def test_same_text_tiered_for_review_is_allowed():
    """The inverse: correctly tiered body text writes. Without this the test above would pass on a
    gate that refuses everything."""
    df = _frame(n_chars=len(LEAK_TEXT), text=LEAK_TEXT, privacy_tier=TIER_WITH_TEXT)
    assert_privacy_invariant(df)


def test_n_chars_drift_alone_still_trips():
    """n_chars and text are tested independently, so a future edit that lets one drift from the
    other cannot slide through on whichever column happens to agree."""
    df = _frame(n_chars=3000, text=None, privacy_tier=TIER_INDEX_ONLY)
    with pytest.raises(PrivacyInvariantError):
        assert_privacy_invariant(df)

    df = _frame(n_chars=0, text=LEAK_TEXT, privacy_tier=TIER_INDEX_ONLY)
    with pytest.raises(PrivacyInvariantError):
        assert_privacy_invariant(df)


def test_whitespace_only_text_does_not_trip():
    """The regression this gate shipped with: a form-feed-joined image-only PDF ('\\f\\f') is not
    body text, and counting it as a leak discarded every successful row in the run."""
    df = _frame(n_chars=0, text="\f\f", privacy_tier=TIER_INDEX_ONLY)
    assert_privacy_invariant(df)


def test_review_tier_may_not_be_publicly_displayed():
    df = _frame(n_chars=len(LEAK_TEXT), text=LEAK_TEXT, privacy_tier=TIER_WITH_TEXT, public_display=True)
    with pytest.raises(PrivacyInvariantError):
        assert_privacy_invariant(df)
