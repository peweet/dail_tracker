"""Tests for dossiers.list_procurement_lobbying_overlap (procurement × lobbying).

The composer's whole job is to make the value-double-count trap impossible: the
raw view repeats a supplier's award total once per lobby-name match, so the
composer collapses to one row per supplier with the lobby entities nested. These
tests lock that invariant + the no-causation caveat.

Skips cleanly if the procurement/lobbying gold is absent.
"""

from __future__ import annotations

import pytest

from dail_tracker_core import dossiers


@pytest.fixture(scope="module")
def conn():
    try:
        from dail_tracker_core.connections import api_conn
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"api_conn unavailable: {exc}")
    c = api_conn()
    yield c
    c.close()


@pytest.fixture(scope="module")
def overlap(conn):
    out = dossiers.list_procurement_lobbying_overlap(conn)
    if "error" in out or not out.get("suppliers"):
        pytest.skip(f"overlap data not available: {out.get('error', 'empty')}")
    return out


def test_one_row_per_supplier_no_duplicates(overlap):
    norms = [s["supplier_norm"] for s in overlap["suppliers"]]
    assert len(norms) == len(set(norms)), "supplier appears more than once — dedupe failed"


def test_summary_count_matches_rows_when_unlimited(conn):
    out = dossiers.list_procurement_lobbying_overlap(conn, limit=0)
    assert out["summary"]["distinct_suppliers"] == len(out["suppliers"])


def test_caveat_disclaims_causation(overlap):
    cav = overlap["caveat"].lower()
    assert "not evidence" in cav and "never sum" in cav


def test_nested_lobby_entities_present(overlap):
    s = overlap["suppliers"][0]
    assert s["lobby_entities"], "expected at least one nested lobby entity"
    assert {"lobby_name", "lobby_side", "n_lobby_returns"} <= set(s["lobby_entities"][0])


def test_default_order_is_descending_by_value(overlap):
    vals = [s["awarded_value_safe_eur"] for s in overlap["suppliers"]]
    assert vals == sorted(vals, reverse=True)


def test_side_filter_is_a_subset(conn, overlap):
    reg = dossiers.list_procurement_lobbying_overlap(conn, limit=0, side="registrant")
    assert reg["summary"]["side_filter"] == "registrant"
    # filtering a role can only keep or drop suppliers, never add
    assert reg["summary"]["distinct_suppliers"] <= overlap["summary"]["distinct_suppliers"]
