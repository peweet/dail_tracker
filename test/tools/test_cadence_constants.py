"""The monitoring tools must reference ONE cadence constant, not copies.

This is the anti-drift ratchet for the constant-centralisation: it asserts each
tool's `GRACE` etc. IS the object in tools.cadence_constants (identity, not just
equal value), so if someone reintroduces a local `GRACE = 2.0` the test fails.
"""

from __future__ import annotations

from tools import cadence_constants as cc


def test_freshness_status_uses_shared_grace():
    from tools import freshness_status

    assert freshness_status.GRACE is cc.GRACE


def test_check_source_cadence_uses_shared_constants():
    from tools.migration import check_source_cadence

    assert check_source_cadence.GRACE is cc.GRACE
    assert check_source_cadence.TAKEN_DOWN_AFTER_DAYS is cc.TAKEN_DOWN_AFTER_DAYS


def test_freshness_report_uses_shared_max_age():
    from tools import freshness_report

    assert freshness_report._DEFAULT_MAX_AGE_DAYS is cc.FRESHNESS_MAX_AGE_DAYS


def test_build_source_cadence_uses_shared_map():
    from tools.migration import build_source_cadence

    assert build_source_cadence.CADENCE_DAYS is cc.CADENCE_DAYS


def test_grace_and_thresholds_have_expected_values():
    # Pin the values so a careless edit to the shared module is caught too.
    assert cc.GRACE == 2.0
    assert cc.TAKEN_DOWN_AFTER_DAYS == 14
    assert cc.FRESHNESS_MAX_AGE_DAYS == 14
    assert cc.CADENCE_DAYS["weekly"] == 7 and cc.CADENCE_DAYS["one_off"] == 0
