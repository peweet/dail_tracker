"""Regression fixtures for the ABP inspector-report parser — the graduation gate.

Self-contained: the recommendation/inspector snippets are REAL wordings mined from the cached
corpus (2026-07-14), embedded here so the test is portable (no c:/tmp dependency, runs in CI).
Locks the two hard-won lessons:
  1. Verdict wording varies wildly — match the recommendation WINDOW then the operative decision
     VERB, earliest-verb-wins. A rigid sentence template silently drops ~half of verdicts.
  2. A null verdict is often CORRECT (condition-only appeals, s.5 referrals, AA cases) — the test
     asserts those stay null, so a future "helpful" change that forces a verdict gets caught.
"""
from __future__ import annotations

import pytest

from pipeline_sandbox.new_sources.abp_inspector_reports import parse_report

# (label, report-tail text, expected_verdict) — expected None = correctly no grant/refuse.
VERDICT_CASES = [
    ("cssi_315397_grant", "9.0 Recommendation\nThat retention permission and permission be granted.\n", "GRANT"),
    ("plain_refuse", "Recommendation\nI recommend that planning permission is refused in accordance with the reasons set out below.\n", "REFUSE"),
    ("grant_for_reasons", "Recommendation\nI recommend that permission for the above described development be granted for the following reasons and considerations subject to conditions.\n", "GRANT"),
    ("board_grant", "10.0 Recommendation\nIt is recommended that the Board grant planning permission for the proposed development for the following reasons and considerations and subject to the conditions set out below.\n", "GRANT"),
    ("should_be_refused", "Recommendation\nIt is considered that the proposed development should be refused for the reasons and considerations hereunder.\n", "REFUSE"),
    ("should_be_granted", "I recommend that planning permission should be granted subject to conditions, for the reasons and considerations as set out below.\n", "GRANT"),
    ("bare_grant", "Recommendation\nGrant permission subject to conditions\n9.0 Reasons and Considerations\n", "GRANT"),
    ("refuse_development", "I recommend that permission for the development be refused for the reasons and considerations as set out below.\n", "REFUSE"),
    # correctly NULL — no grant/refuse outcome exists for these
    ("condition_only_null", "Recommendation\nI recommend that Condition No. 2 be revised with subsection 2(a) omitted.\n", None),
    ("vacant_site_null", "7.1. I recommend that, in accordance with section 9(5) of the Urban Regeneration and Housing Act 2015, the Board should confirm that the site at Rosehill was a vacant site.\n", None),
]


@pytest.mark.parametrize("label,text,expected", VERDICT_CASES, ids=[c[0] for c in VERDICT_CASES])
def test_verdict(label, text, expected):
    assert parse_report(text)["recommendation_verdict"] == expected


# (label, signature-block text, expected_name) — the boilerplate under which ABP prints the name.
INSPECTOR_CASES = [
    ("moloney", "recommendation set out in my report in an improper or inappropriate way.\nKenneth Moloney\nSenior Planning Inspector\n19th March 2026\n", "Kenneth Moloney"),
    ("omahony", "improper or inappropriate way.\nSarah O’Mahony\nPlanning Inspector\n22nd April 2026\n", "Sarah O’Mahony"),
    ("underscore_sig", "improper or inappropriate way.\n_____________\nEmma Gosnell\nPlanning Inspector\n15th May 2026\n", "Emma Gosnell"),
    ("no_name_ok", "improper or inappropriate way.\nSenior Planning Inspector\n27th March 2026\n", None),  # some reports omit the name
]


@pytest.mark.parametrize("label,text,expected", INSPECTOR_CASES, ids=[c[0] for c in INSPECTOR_CASES])
def test_inspector(label, text, expected):
    assert parse_report(text)["inspector"] == expected


def test_section_flags_fire():
    """Section-presence flags key off the heading vocabulary — a smoke test that they detect."""
    text = (
        "Reasons for Refusal\n...\nAppropriate Assessment Screening\n...\n"
        "Flood Risk\n...\nTraffic and road safety\n...\nConditions\n1. ...\n"
    )
    p = parse_report(text)
    assert p["has_reasons_for_refusal"] and p["has_appropriate_assessment"]
    assert p["has_flood"] and p["has_traffic"] and p["has_conditions"]
