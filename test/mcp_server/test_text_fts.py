"""Tests for mcp_server/text_fts.py — the concise/detailed hit shaping.

The BM25 search itself needs the real corpus + DuckDB fts extension (exercised via
test_mcp_server_smoke); these tests pin the pure response-shaping contract, which is
where the token-cost behaviour lives (response_format enum per the 2026-07-31
token-optimization adoption — see reference_token_optimization_literature_2026_07_31).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from mcp_server import text_fts  # noqa: E402

_Q_ROW = {
    "rid": 7,
    "question_date": "2025-03-04",
    "question_type": "written",
    "ministry": "Housing",
    "topic": "Planning Issues",
    "question_text": "x" * 400,
    "question_ref": "12345/25",
    "oireachtas_url": "https://www.oireachtas.ie/en/debates/question/2025-03-04/123/",
    "unique_member_code": "Some.TD",
    "score": 3.14159,
}


def test_shape_hit_concise_drops_urls_and_halves_snippet():
    rec = text_fts.shape_hit("questions", _Q_ROW, "concise")
    assert set(rec) == {"question_date", "ministry", "topic", "question_ref", "question_text", "score"}
    assert len(rec["question_text"]) == text_fts._SNIPPET_CONCISE + 1  # cap + ellipsis
    assert rec["score"] == 3.142


def test_shape_hit_detailed_keeps_provenance_columns():
    rec = text_fts.shape_hit("questions", _Q_ROW, "detailed")
    assert "rid" not in rec
    assert rec["oireachtas_url"].startswith("https://")
    assert rec["unique_member_code"] == "Some.TD"
    assert len(rec["question_text"]) == text_fts._SNIPPET + 1


def test_shape_hit_speeches_concise_keeps_speaker():
    row = {
        "rid": 1,
        "speech_date": "2024-11-20",
        "house": "Dáil",
        "business": "Order of Business",
        "speaker_raw": "An Taoiseach",
        "unique_member_code": "X",
        "speech_text": "short",
        "debate_url": "https://example",
        "score": 1.0,
    }
    rec = text_fts.shape_hit("speeches", row, "concise")
    assert set(rec) == {"speech_date", "speaker_raw", "speech_text", "score"}
    assert rec["speech_text"] == "short"


def test_search_rejects_bad_response_format_before_touching_data():
    out = text_fts.search("questions", "housing", cur=None, repo=REPO, response_format="terse")
    assert "error" in out and "response_format" in out["error"]


_CM_ROW = {
    "rid": 3,
    "council": "Kerry",
    "meeting": "mar26.pdf",
    "meeting_date": "2026-03-23",
    "doc_type": "plenary_minutes",
    "source_status": "ocr_winocr",
    "source_url": "https://example.ie/mar26.pdf",
    "chunk": 4,
    "body": "y" * 400,
    "score": 1.5,
}


def test_council_minutes_concise_shape() -> None:
    """council_minutes corpus (2026-08-01): concise keeps council/date/doc_type + halved
    snippet; detailed keeps provenance (source_status for the Extracted-band badge,
    source_url, chunk)."""
    concise = text_fts.shape_hit("council_minutes", dict(_CM_ROW), "concise")
    assert set(concise) <= {"council", "meeting_date", "doc_type", "body", "score"}
    assert len(concise["body"]) < 400
    detailed = text_fts.shape_hit("council_minutes", dict(_CM_ROW), "detailed")
    assert detailed["source_status"] == "ocr_winocr"
    assert detailed["source_url"].startswith("https://")
