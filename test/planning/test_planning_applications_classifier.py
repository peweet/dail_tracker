"""Unit tests for the Iris-style two-tier planning decision classifier.

Guards the ordering traps (REFUSE-before-GRANT, UNCONDITIONAL-before-CONDITIONAL,
Section-5/Part-8 before the generic grant catch), the ApplicationStatus residual
reducer, the no-inference rule (a finalised-but-blank row is never guessed), and the
back-compat decision_normalised contract that planning_decision_profiles.py depends on.

    python -m pytest pipeline_sandbox/test_planning_applications_classifier.py -q
"""

from __future__ import annotations

import polars as pl

from extractors.planning_applications_ingest import (
    _CATEGORY_TO_NORMALISED,
    _decision_category,
    _decision_subtype,
)


def _classify(decision: str | None, status: str | None = None) -> dict:
    """Apply the two-step classifier exactly as transform() does."""
    df = pl.DataFrame({"Decision": [decision], "ApplicationStatus": [status]})
    df = df.with_columns(
        _decision_category("Decision", "ApplicationStatus").alias("decision_category"),
    )
    df = df.with_columns(
        _decision_subtype("Decision", "ApplicationStatus").alias("decision_subtype"),
        pl.col("decision_category")
        .replace_strict(_CATEGORY_TO_NORMALISED, default="Other")
        .alias("decision_normalised"),
    )
    row = df.row(0, named=True)
    return {
        "category": row["decision_category"],
        "subtype": row["decision_subtype"],
        "normalised": row["decision_normalised"],
    }


# ── ordering traps (the bugs that motivated keyword ordering) ──────────────────


def test_refuse_permission_is_refused_not_granted():
    assert _classify("REFUSE PERMISSION")["category"] == "refused"


def test_unconditional_is_plain_grant_not_conditional():
    # "UNCONDITIONAL" literally contains "CONDITIONAL"
    r = _classify("UNCONDITIONAL")
    assert r["category"] == "granted"
    assert r["subtype"] == "unconditional"


def test_conditional_is_granted_conditional():
    r = _classify("CONDITIONAL")
    assert r["category"] == "granted_conditional"
    assert r["normalised"] == "Granted-Conditional"


def test_part8_approved_is_la_development_not_granted():
    # "PART 8 APPROVED BY COUNCIL" contains APPROVED -> must not fall to generic grant
    r = _classify("PART 8 APPROVED BY COUNCIL")
    assert r["category"] == "local_authority_development"
    assert r["subtype"] == "approved"


def test_part8_rejected_is_la_development_rejected():
    r = _classify("PART 8 REJECTED BY COUNCIL")
    assert r["category"] == "local_authority_development"
    assert r["subtype"] == "rejected"


def test_declared_not_exempt_is_section5_not_exempt():
    # "DECLARED NOT EXEMPT" contains EXEMPT (and NOT) -> section 5, subtype not_exempt
    r = _classify("DECLARED NOT EXEMPT")
    assert r["category"] == "section_5_exemption"
    assert r["subtype"] == "not_exempt"


def test_declared_exempt_is_section5_exempt():
    r = _classify("DECLARED EXEMPT")
    assert r["category"] == "section_5_exemption"
    assert r["subtype"] == "exempt"


def test_request_additional_information_is_in_progress_not_a_decision():
    r = _classify("REQUEST ADDITIONAL INFORMATION")
    assert r["category"] == "in_progress"
    assert r["normalised"] == "Undecided/None"


def test_split_decision_is_its_own_class():
    assert _classify("SPLIT DECISION(RETENTION")["category"] == "split_decision"


def test_quashed_is_court_action():
    assert _classify("DECISION QUASHED BY HIGH COURT")["category"] == "court_action"


def test_referral_to_other_body():
    assert _classify("Decision to be Made by Other Body")["category"] == "referral"


def test_grant_permission_for_retention():
    r = _classify("GRANT PERMISSION FOR RETENTION")
    assert r["category"] == "granted"
    assert r["subtype"] == "retention"


# ── residual reducer (Decision blank -> ApplicationStatus) ────────────────────


def test_blank_decision_withdrawn_status_rescued_to_withdrawn():
    for blank in (None, "", "N/A", "n/a"):
        r = _classify(blank, "WITHDRAWN")
        assert r["category"] == "withdrawn", blank
        assert r["normalised"] == "Withdrawn", blank


def test_blank_decision_deemed_withdrawn_subtype():
    r = _classify("", "DEEMED WITHDRAWN")
    assert r["category"] == "withdrawn"
    assert r["subtype"] == "deemed_withdrawn"


def test_blank_decision_incomplete_status():
    r = _classify("N/A", "INCOMPLETED APPLICATION")
    assert r["category"] == "incomplete"
    assert r["normalised"] == "Undecided/None"


def test_blank_decision_invalid_status_rescued():
    r = _classify("", "Invalid - Case Closed")
    assert r["category"] == "invalid"
    assert r["normalised"] == "Invalid"


def test_blank_decision_further_information_status_is_in_progress():
    assert _classify("", "FURTHER INFORMATION")["category"] == "in_progress"


# ── the no-inference rule (load-bearing) ──────────────────────────────────────


def test_finalised_but_blank_decision_is_never_guessed():
    # status says finalised/decided but there is NO outcome text -> do NOT infer grant/refuse
    for status in ("APPLICATION FINALISED", "Decision Made", "Decision Notice Issued"):
        r = _classify("", status)
        assert r["category"] == "no_decision", status
        assert r["subtype"] == "outcome_unstated", status
        assert r["normalised"] == "Undecided/None", status


def test_totally_blank_is_no_decision():
    r = _classify(None, None)
    assert r["category"] == "no_decision"


# ── back-compat: the decided whitelist is preserved exactly ───────────────────


def test_decided_whitelist_values_unchanged():
    # planning_decision_profiles.py keys on exactly these strings
    assert _classify("Grant Permission")["normalised"] == "Granted"
    assert _classify("CONDITIONAL")["normalised"] == "Granted-Conditional"
    assert _classify("REFUSED")["normalised"] == "Refused"


def test_new_classes_fold_to_other_for_back_compat():
    for d in ("DECLARED EXEMPT", "PART 8 APPROVED BY COUNCIL", "SPLIT DECISION"):
        assert _classify(d)["normalised"] == "Other", d
