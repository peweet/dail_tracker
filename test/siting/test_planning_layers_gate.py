"""Unit tests for the two-axis geometry quarantine gate (the §13.6 defensive logic).

Axis 1 (topology, e.g. self-intersection) is REPAIRABLE via make_valid; axis 2
(out-of-bounds coordinates, the -9e12 case) is NOT — make_valid LAUNDERS it, so we
bounds-check before AND after the repair. These tests guard that contract.

    python -m pytest test/siting/test_planning_layers_gate.py -q
"""

from __future__ import annotations

import shapely
from shapely.geometry import Polygon

from extractors.planning_layers_ingest import gate


def _box(cx, cy, d=0.01):
    return Polygon([(cx - d, cy - d), (cx + d, cy - d), (cx + d, cy + d), (cx - d, cy + d)])


def test_valid_in_bounds_polygon_passes():
    g, reason = gate(_box(-9.05, 53.30))  # Galway
    assert g is not None and reason in ("ok", "ok_giant")


def test_empty_geometry_quarantined():
    g, reason = gate(Polygon())
    assert g is None and reason == "empty"


def test_none_geometry_quarantined():
    g, reason = gate(None)
    assert g is None and reason == "empty"


def test_out_of_bounds_polygon_quarantined_not_repaired():
    # the -9e12 trillion-degree corruption: must be detected, never kept
    g, reason = gate(Polygon([(-9e12, 53.0), (-9e12 + 1, 53.0), (-9e12 + 1, 53.1), (-9e12, 53.1)]))
    assert g is None and reason == "bounds_escape"


def test_self_intersecting_in_bounds_is_repaired():
    # a "bowtie" (figure-8) is invalid topology but in-bounds -> make_valid repairs it
    bowtie = Polygon([(-9.05, 53.30), (-9.03, 53.32), (-9.03, 53.30), (-9.05, 53.32)])
    assert not bowtie.is_valid
    g, reason = gate(bowtie)
    assert g is not None and reason in ("ok", "ok_giant")
    assert shapely.is_valid(g)


def test_make_valid_laundering_is_caught_by_post_bounds_check():
    # invalid topology AND an out-of-bounds vertex: make_valid would launder the bad coord
    # into a plausible band; the post-repair bounds check must still quarantine it.
    bad = Polygon([(-9.05, 53.30), (-9e12, 53.31), (-9.03, 53.30), (-9.04, 53.32)])
    g, reason = gate(bad)
    assert g is None and reason in ("bounds_escape", "bounds_escape_after_make_valid")
