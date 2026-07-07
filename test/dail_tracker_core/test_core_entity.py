"""Core tests for the organisation entity-crosswalk queries + dossier composition.

Skips cleanly if the spine gold/view isn't present on the machine running them.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dail_tracker_core import dossiers  # noqa: E402
from dail_tracker_core.db import connect_with_views  # noqa: E402
from dail_tracker_core.queries import entity as ent  # noqa: E402


@pytest.fixture(scope="module")
def conn():
    try:
        c = connect_with_views(["procurement_*.sql"], swallow_errors=False)
        c.execute("SELECT 1 FROM v_supplier_entity_xref LIMIT 1").fetchone()
    except Exception:
        pytest.skip("v_supplier_entity_xref not built / procurement views unavailable")
    yield c
    c.close()


def test_xref_summary_unknown_is_empty(conn):
    assert ent.xref_summary(conn, "__no_such_entity__").data.empty


def test_dossier_unknown_is_none(conn):
    assert dossiers.build_organisation_dossier(conn, "__no_such_entity__") is None


def test_dossier_shape_and_consistency(conn):
    top = ent.top_cross_register(conn, min_registers=1, limit=1).data
    if top.empty:
        pytest.skip("no cross-register entities in this gold")
    norm = str(top.iloc[0]["supplier_norm"])
    d = dossiers.build_organisation_dossier(conn, norm)
    assert d is not None
    assert set(d.keys()) == {"identity", "procurement", "cross_register", "caveat"}
    cr = d["cross_register"]
    # register_count must equal the sum of the four extra-register flags
    assert cr["register_count"] == (
        int(cr["on_lobbying_register"])
        + int(cr["has_corporate_notice"])
        + int(cr["is_charity"])
        + int(cr["has_epa_licence"])
    )
    assert isinstance(d["procurement"]["awarded_value_safe_eur"], float)
    assert "co-occurrence" in d["caveat"].lower()
