"""Tests for the merged cadence+health checker (tools/migration/check_source_cadence.py).

Covers the two axes: cadence (OK/DUE/OVERDUE/REVIEW/STATIC) and the health-duration
fold (BROKEN vs TAKEN_DOWN over time) that absorbed the former source_health_ledger.
"""

from __future__ import annotations

from datetime import date, timedelta

from tools.migration import check_source_cadence as csc

TODAY = date(2026, 7, 21)


def _row(sid="s", cadence_days="7", curated="yes", next_expected="", **kw):
    return {
        "source_id": sid,
        "cadence_days": cadence_days,
        "curated": curated,
        "next_expected": next_expected,
        "cadence": kw.get("cadence", ""),
        "poller": kw.get("poller", ""),
        "runner": kw.get("runner", ""),
    }


def _run(ledger, health, prior_state=None, today=TODAY):
    return csc.evaluate(today, ledger=ledger, health=health, prior_state=prior_state or {})


def _status(rollup, sid="s"):
    return next(r["status"] for r in rollup["sources"] if r["source_id"] == sid)


# ---- cadence axis (unchanged behaviour) -----------------------------------


def test_uncurated_is_review():
    r = _run([_row(curated="no")], {})
    assert _status(r) == "REVIEW"


def test_static_when_cadence_zero():
    r = _run([_row(cadence_days="0")], {"s": {"status": "ok", "days_old": 999}})
    assert _status(r) == "STATIC"


def test_ok_within_cadence():
    r = _run([_row(cadence_days="7")], {"s": {"status": "ok", "days_old": 3}})
    assert _status(r) == "OK"


def test_overdue_past_cadence_times_grace():
    r = _run([_row(cadence_days="7")], {"s": {"status": "ok", "days_old": 15}})
    assert _status(r) == "OVERDUE"  # 15 > 7 * 2.0


def test_due_when_window_open_and_past_one_cadence():
    r = _run([_row(cadence_days="7", next_expected="2026-07-01")], {"s": {"status": "ok", "days_old": 8}})
    assert _status(r) == "DUE"  # window open, 8 > 7 but < 14


def test_unknown_when_no_freshness_signal():
    r = _run([_row(cadence_days="7")], {"s": {"status": "skipped"}})
    assert _status(r) == "UNKNOWN"


# ---- health-duration fold (the former ledger) -----------------------------


def test_first_failure_is_broken_not_taken_down():
    r = _run([_row()], {"s": {"status": "failed"}}, prior_state={})
    assert _status(r) == "BROKEN"
    # state now remembers when it first went bad
    assert r["state"]["sources"]["s"]["first_broken_at"] == TODAY.isoformat()


def test_broken_escalates_to_taken_down_after_horizon():
    first = (TODAY - timedelta(days=csc.TAKEN_DOWN_AFTER_DAYS)).isoformat()
    r = _run([_row()], {"s": {"status": "failed"}}, prior_state={"s": {"first_broken_at": first}})
    assert _status(r) == "TAKEN_DOWN"


def test_broken_below_horizon_stays_broken():
    first = (TODAY - timedelta(days=3)).isoformat()
    r = _run([_row()], {"s": {"status": "failed"}}, prior_state={"s": {"first_broken_at": first}})
    assert _status(r) == "BROKEN"
    row = next(x for x in r["sources"] if x["source_id"] == "s")
    assert row["days_broken"] == 3


def test_recovery_clears_broken_state():
    prior = {"s": {"first_broken_at": (TODAY - timedelta(days=20)).isoformat()}}
    r = _run([_row(cadence_days="7")], {"s": {"status": "ok", "days_old": 2}}, prior_state=prior)
    assert _status(r) == "OK"
    assert "s" not in r["state"]["sources"]  # no longer failing -> forgotten


def test_health_failure_overrides_cadence_even_if_uncurated():
    """A broken source is flagged regardless of curation — the health axis needs no ledger."""
    r = _run([_row(curated="no")], {"s": {"status": "failed"}})
    assert _status(r) == "BROKEN"


# ---- rollup shape ---------------------------------------------------------


def test_problem_sources_include_broken_and_taken_down():
    first = (TODAY - timedelta(days=30)).isoformat()
    ledger = [_row("gone"), _row("bad"), _row("fine", cadence_days="7")]
    health = {
        "gone": {"status": "failed"},
        "bad": {"status": "failed"},
        "fine": {"status": "ok", "days_old": 1},
    }
    r = _run(ledger, health, prior_state={"gone": {"first_broken_at": first}})
    assert set(r["problem_sources"]) == {"gone", "bad"}
    assert _status(r, "gone") == "TAKEN_DOWN"
    assert _status(r, "bad") == "BROKEN"
    assert r["counts"]["TAKEN_DOWN"] == 1
    assert r["counts"]["BROKEN"] == 1


def test_worst_status_sorts_first():
    first = (TODAY - timedelta(days=30)).isoformat()
    ledger = [_row("ok1", cadence_days="7"), _row("gone")]
    health = {"ok1": {"status": "ok", "days_old": 1}, "gone": {"status": "failed"}}
    r = _run(ledger, health, prior_state={"gone": {"first_broken_at": first}})
    assert r["sources"][0]["source_id"] == "gone"  # TAKEN_DOWN sorts above OK
