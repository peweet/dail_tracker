"""Unit tests for dail_tracker_core.results.QueryResult.

The 3-state contract (rows / no-rows / unavailable) is the whole point of the
type — it exists so interfaces stop conflating "source down" with "no results".
"""

from __future__ import annotations

import pandas as pd
import pytest

from dail_tracker_core.results import QueryResult


def test_success_with_rows():
    df = pd.DataFrame({"a": [1, 2]})
    r = QueryResult.success(df)
    assert r.ok is True
    assert r.unavailable_reason is None
    assert r.is_empty is False
    assert r.data.equals(df)


def test_success_with_no_rows_is_still_ok():
    r = QueryResult.success(pd.DataFrame({"a": []}))
    assert r.ok is True
    assert r.is_empty is True
    assert r.unavailable_reason is None  # no-rows is NOT an error


def test_unavailable_carries_reason_and_empty_frame():
    r = QueryResult.unavailable("missing parquet")
    assert r.ok is False
    assert r.unavailable_reason == "missing parquet"
    assert r.is_empty is True
    assert r.data.empty


def test_ok_result_may_not_carry_reason():
    with pytest.raises(ValueError):
        QueryResult(data=pd.DataFrame(), ok=True, unavailable_reason="oops")


def test_unavailable_result_must_carry_reason():
    with pytest.raises(ValueError):
        QueryResult(data=pd.DataFrame(), ok=False, unavailable_reason=None)


def test_frozen_cannot_rebind():
    r = QueryResult.success(pd.DataFrame())
    with pytest.raises(Exception):  # FrozenInstanceError
        r.ok = False  # type: ignore[misc]
