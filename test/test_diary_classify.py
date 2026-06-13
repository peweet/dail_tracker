"""Golden test for ministerial-diary entry classification.

``pipeline_sandbox/diary_entry_classify.py`` assigns ``entry_class`` to every
diary engagement via ordered keyword rules. The app FILTERS on this column but
never computes it (logic firewall), so the rule list is the contract — this
test pins it against hand-labelled real subjects (drawn from the DETE sandbox
corpus, 2026-06-12) and, crucially, against the precision edge cases that the
ordering was tuned to get right:

* "Pre-CC meeting with Tánaiste"  → govt_business, NOT external_meeting
  (CC = Cabinet Committee; the word "meeting" must not win first).
* "...Official Launch"            → external_meeting, NOT internal_dept
  (the adjective "official" must not match the civil-servant "officials").
* "Fáilte Ireland Clinic meeting" → external_meeting, NOT constituency
  (a business advisory clinic is not a TD constituency clinic).
* "DETE Divisional Update"        → internal_dept, NOT oireachtas
  ("divisional" must not match the \\bdivisions?\\b voting keyword).
* "press briefing"               → media, NOT internal_dept (media tested first).

If a rule edit re-breaks one of these, this test fails loudly rather than the
mistake surfacing as a silently mis-filtered page.
"""

from __future__ import annotations

import pytest

from pipeline_sandbox.diary_entry_classify import classify

# (subject, expected_class) — hand-labelled from the real DETE corpus.
GOLDEN: list[tuple[str, str]] = [
    # --- govt_business ---
    ("Government Meeting", "govt_business"),
    ("Pre-Government Meeting", "govt_business"),
    ("Cabinet Committee on Housing", "govt_business"),
    ("Cabinet Committee C", "govt_business"),
    ("Pre-CC meeting with Tánaiste", "govt_business"),  # CC=Cabinet Committee, beats "meeting"
    # --- oireachtas ---
    ("Leaders Questions", "oireachtas"),
    ("ALL MUST ATTEND - Leaders Questions", "oireachtas"),
    ("Topical Issues", "oireachtas"),
    ("Questions on Promised Legislation", "oireachtas"),
    ("Employment Permits Bill 2022 Seanad Committee Stage", "oireachtas"),
    ("Weekly Divisions", "oireachtas"),
    ("Votes", "oireachtas"),
    ("Voting Block", "oireachtas"),
    ("Commencement Matters", "oireachtas"),
    ("Subject Joint Committee on Environment and Climate Action", "oireachtas"),
    ("PP Meeting", "oireachtas"),
    ("FG PP Meeting", "oireachtas"),
    ("LQ & QPL", "oireachtas"),
    # --- media ---
    ("Shannonside Interview, Mercosur", "media"),
    ("Newstalk Business Breakfast (TBC)", "media"),  # newstalk beats "breakfast"
    ("News at One pre-rec", "media"),
    ("Video, IDA park", "media"),
    ("Local Enterprise Week Photocall", "media"),
    ("Minister on WLRfm - Right to disconnect", "media"),  # local radio *fm
    ("Press briefing on the Budget", "media"),  # media tested before internal_dept
    # --- internal_dept ---
    ("Briefing on Collective Bargaining", "internal_dept"),
    ("Pre-brief with officials", "internal_dept"),
    ("Pre Briefing for Ministerial Management Board Meeting", "internal_dept"),
    ("DETE Divisional Update", "internal_dept"),  # "divisional" must NOT hit votes/divisions
    ("Minister to be briefed by Dept. of Justice Officials", "internal_dept"),
    ("Sec Gen Meeting", "internal_dept"),
    # --- travel ---
    ("Travel to BnM The Accelerate Green Hub, Boora, Co. Offaly", "travel"),
    ("Flight to Dubai", "travel"),
    ("Flight to Brussels", "travel"),
    # --- constituency ---
    ("Constituency Day", "constituency"),
    # --- external_meeting (residual of interest) ---
    ("Meeting with Leo Clancy - Enterprise Ireland", "external_meeting"),
    ("Meeting with CCPC Chair Brian McHugh", "external_meeting"),
    ("France-Ireland Chamber of Commerce Lunch", "external_meeting"),
    ("Visit to the Book Centre", "external_meeting"),
    ("Official Launch of new services at Clones Courthouse", "external_meeting"),  # NOT internal
    ("Attend graduation of Neylons Academy for Growth", "external_meeting"),
    ("Credentials Ceremony", "external_meeting"),
    ("Tesco Jobs Announcement", "external_meeting"),
    ("Aviation Trade Mission to Kuwait", "external_meeting"),
    ("Phonecall with FSAI", "external_meeting"),
    ("Fáilte Ireland Clinic meeting", "external_meeting"),  # advisory clinic, NOT constituency
    # --- other (honest residual: no keyword, ambiguous fragment) ---
    ("Square CEO", "other"),
    ("No meetings scheduled", "other"),  # "scheduled" only; deliberately not forced
]


@pytest.mark.parametrize("subject,expected", GOLDEN)
def test_classify_golden(subject: str, expected: str) -> None:
    assert classify(subject) == expected, f"{subject!r} → {classify(subject)!r}, expected {expected!r}"


def test_classify_handles_empty() -> None:
    assert classify(None) == "other"
    assert classify("") == "other"
