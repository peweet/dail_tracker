"""Unit tests for the pure flatten transforms in legislation/legislation.py
and legislation/questions.py.

Both modules were refactored to expose a pure ``flatten_*`` helper (bronze
records → silver DataFrame) with the file read/write left in ``main()``. The
refactor was validated byte-for-byte against the real silver outputs; these
tests lock the project-specific derivations with tiny hand-built fixtures:
sponsor-resolution (member / office / unresolved), the member-code/url
derivations, the public debate url, and the question reference extraction.
"""

from __future__ import annotations

import pandas as pd

from legislation.legislation import flatten_bills
from legislation.questions import flatten_questions

# ── flatten_bills ────────────────────────────────────────────────────────────


def _bill():
    return {
        "contextDate": "2025-01-01",
        "billSort": {"billShortTitleEnSort": "health bill", "billYearSort": "2025"},
        "bill": {
            "billNo": "75",
            "billYear": "2025",
            "billType": "Public",
            "shortTitleEn": "Health Bill",
            "status": "Current",
            "sponsors": [
                {
                    "sponsor": {
                        "by": {
                            "showAs": "Mary Lou McDonald",
                            "uri": "https://data.oireachtas.ie/ie/oireachtas/member/id/MaryLouMcDonald.D.2011",
                        },
                        "as": {"showAs": None, "uri": None},
                        "isPrimary": True,
                    }
                },
                {
                    "sponsor": {
                        "by": {"showAs": None, "uri": None},
                        "as": {"showAs": "Minister for Health", "uri": "https://x/role/health"},
                        "isPrimary": True,
                    }
                },
            ],
            "debates": [
                {
                    "chamber": {"uri": "https://data.oireachtas.ie/akn/ie/house/dail"},
                    "date": "2025-02-01",
                    "debateSectionId": "dbsect_9",
                    "showAs": "Second Stage debate",
                }
            ],
            "stages": [],
            "events": [],
            "versions": [],
            "relatedDocs": [],
            "mostRecentStage": {"event": {"dates": []}},
        },
    }


def test_flatten_bills_returns_all_seven_frames():
    frames = flatten_bills([_bill()])
    assert set(frames) == {
        "sponsors",
        "stages",
        "debates",
        "events",
        "most_recent_stage_event_dates",
        "related_docs",
        "versions",
    }
    assert all(isinstance(v, pd.DataFrame) for v in frames.values())


def test_flatten_bills_sponsor_resolution_and_member_code():
    sponsors = flatten_bills([_bill()])["sponsors"]
    # The by.uri tail becomes the member code; office row stays null.
    member = sponsors[sponsors["sponsor_resolution"] == "member"].iloc[0]
    assert member["unique_member_code"] == "MaryLouMcDonald.D.2011"
    assert member["bill_url"] == "https://www.oireachtas.ie/en/bills/bill/2025/75"

    # Government/office sponsor → resolution "office", not "unresolved".
    assert (sponsors["sponsor_resolution"] == "office").sum() == 1
    assert set(sponsors["sponsor_resolution"]) == {"member", "office"}


def test_flatten_bills_debate_public_url_and_uri_drop():
    debates = flatten_bills([_bill()])["debates"]
    row = debates.iloc[0]
    # dbsect_ prefix stripped; chamber tail + date assembled into the public url.
    assert row["debate_url_web"] == "https://www.oireachtas.ie/en/debates/debate/dail/2025-02-01/9/"
    # internal chamber.uri consumed then dropped.
    assert "chamber.uri" not in debates.columns


# ── flatten_questions ────────────────────────────────────────────────────────


def _question(member_code, number, date, text, *, topic="Hospital Waiting Lists"):
    return {
        "contextDate": date,
        "question": {
            "by": {"memberCode": member_code, "showAs": "Mary Lou McDonald", "uri": "https://x/member"},
            "to": {"showAs": "Minister for Health", "roleCode": "HEALTH", "uri": "https://x/role"},
            "debateSection": {"showAs": topic, "debateSectionId": "dbsect_5", "uri": "https://x/sect"},
            "questionType": "Oral",
            "questionNumber": number,
            "date": date,
            "uri": "https://x/question",
            "showAs": text,
            "house": {"showAs": "Dáil Éireann", "houseNo": "34"},
        },
    }


def test_flatten_questions_renames_and_extracts_ref_and_year():
    df = flatten_questions([_question("Mary.D.2011", 42, "2026-01-15", "To ask the Minister … [31202/26]")])
    row = df.iloc[0]
    assert row["unique_member_code"] == "Mary.D.2011"
    assert row["topic"] == "Hospital Waiting Lists"
    assert row["question_ref"] == "31202/26"  # extracted from the [ref] token
    assert row["year"] == 2026
    assert row["question_text"].startswith("To ask the Minister")


def test_flatten_questions_drops_internal_uris():
    df = flatten_questions([_question("Mary.D.2011", 1, "2026-01-01", "No ref here")])
    for dropped in ("uri", "member_uri", "debate_section_uri", "ministry_role_code"):
        assert dropped not in df.columns
    # a question with no [ref] token → null question_ref, not an error.
    assert pd.isna(df.iloc[0]["question_ref"])


def test_flatten_questions_sorts_by_date_desc():
    df = flatten_questions(
        [
            _question("A.D.2011", 1, "2024-01-01", "older"),
            _question("B.D.2011", 2, "2026-01-01", "newer"),
        ]
    )
    # newest question first (descending question_date).
    assert list(df["question_text"]) == ["newer", "older"]
