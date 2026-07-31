"""Tests for mcp_server/text_fts.py::shape_feed — response shaping for the dossier
feed tools (get_member_questions / member_speeches response_format param).

Pure-dict shaping only: no DuckDB connection, no data. text_fts imports
resource_policy (duckdb), so the module importorskips duckdb — mirroring the
optional-extra guard convention of test_py_refs.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

pytest.importorskip("duckdb")

from mcp_server.text_fts import _SNIPPET_CONCISE, shape_feed  # noqa: E402


def _feed():
    return {
        "member": {"unique_member_code": "X123", "member_name": "Seán Ó Broin", "house": "Dáil"},
        "total_matched": 2,
        "returned": 2,
        "questions": [
            {
                "question_date": "2026-01-01",
                "ministry": "Housing",
                "question_text": "x" * 500,
                "oireachtas_url": "https://example.org/q/1",
                "unique_member_code": "X123",
            },
            {"question_date": "2026-01-02", "ministry": "Health", "question_text": "short"},
        ],
    }


def test_concise_truncates_long_text_and_drops_provenance():
    out = shape_feed(_feed(), "questions", "concise", "no match")
    q0 = out["questions"][0]
    assert q0["question_text"].endswith("…")
    assert len(q0["question_text"]) == _SNIPPET_CONCISE + 1  # cap + ellipsis
    assert "oireachtas_url" not in q0
    assert "unique_member_code" not in q0
    assert "note" in out


def test_concise_keeps_short_values_and_header_verbatim():
    src = _feed()
    out = shape_feed(src, "questions", "concise", "no match")
    assert out["questions"][1] == {"question_date": "2026-01-02", "ministry": "Health", "question_text": "short"}
    assert out["member"]["unique_member_code"] == "X123"  # header keeps the code
    assert out["total_matched"] == 2
    # shaping copies rows — the source feed is never mutated
    assert len(src["questions"][0]["question_text"]) == 500


def test_detailed_returns_feed_verbatim():
    src = _feed()
    assert shape_feed(src, "questions", "detailed", "no match") is src


def test_bad_format_and_no_match_return_errors():
    assert "error" in shape_feed(_feed(), "questions", "verbose", "no match")
    assert shape_feed(None, "questions", "concise", "no member matches 'X'") == {
        "error": "no member matches 'X'"
    }
