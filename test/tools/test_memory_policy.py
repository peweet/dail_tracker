from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from tools.memory_policy import select

NOW = datetime(2026, 10, 18, tzinfo=UTC)


def lead(**fields):
    return {"id": "lesson", "discovery": "Cached claim", "trigger": ["memory policy"], **fields}


@pytest.mark.parametrize("scope", [None, False, [], {}, 1, "typo"])
def test_bad_scope_does_not_prevent_another_eligible_finding(tmp_path, scope):
    records = select([lead(scope=scope), lead(id="good")], root=tmp_path, now=NOW)
    assert [row["id"] for row in records] == ["good"]


@pytest.mark.parametrize(
    "fields", [{"scope": "private"}, {"domain": "siting"}, {"repo_path": "planning/product/claude/memory/lesson.md"}]
)
def test_private_findings_require_private_context(tmp_path, fields):
    assert select([lead(**fields)], root=tmp_path, now=NOW) == []
    assert len(select([lead(**fields)], root=tmp_path, scope="private", now=NOW)) == 1


@pytest.mark.parametrize(
    "fields",
    [
        {"superseded_by": "replacement-not-yet-present"},
        {"status": "withdrawn"},
        {"valid_from": "2026-10-19T00:00:00Z"},
        {"revision": True},
        {"verified_at": "not-a-date"},
    ],
)
def test_explicit_invalidation_and_invalid_metadata_are_not_recalled(tmp_path, fields):
    assert select([lead(**fields)], root=tmp_path, now=NOW) == []


def test_expired_finding_returns_reverification_without_the_cached_claim(tmp_path):
    records = select([lead(expires_at="2026-10-17T00:00:00Z")], root=tmp_path, now=NOW)
    assert records[0]["_recall"]["state"] == "recheck"
    assert "Cached claim" not in records[0]["_recall"]["text"]


@pytest.mark.parametrize("path", ["../outside.md", "/outside.md", "C:/outside.md", "data/raw.jsonl"])
def test_source_references_cannot_open_outside_or_corpus_files(tmp_path, path):
    record = lead(verified_at="2026-10-17T00:00:00Z", source_refs=[{"path": path, "sha256": "0" * 64}])
    assert select([record], root=tmp_path, now=NOW) == []


def test_missing_evidence_is_a_recheck_and_not_a_source_match(tmp_path):
    record = lead(verified_at="2026-10-17T00:00:00Z", source_refs=[{"path": "missing.md", "sha256": "0" * 64}])
    result = select([record], root=tmp_path, now=NOW)[0]["_recall"]
    assert result["state"] == "recheck"
    assert result["sources"] == ["missing.md"]
    assert "Cached claim" not in result["text"]


def test_source_snapshot_match_does_not_promote_a_legacy_lead(tmp_path):
    source = tmp_path / "source.md"
    source.write_text("Actual evidence", encoding="utf-8")
    record = lead(
        verified_at="2026-10-17T00:00:00Z",
        source_refs=[{"path": "source.md", "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}],
    )
    matched, legacy = select([record, lead(id="legacy")], root=tmp_path, now=NOW)
    assert matched["_recall"]["state"] == "source-matched"
    assert legacy["_recall"]["state"] == "unverified"


def test_conflicting_duplicate_ids_are_quarantined(tmp_path):
    rows = [lead(), lead(discovery="Contradictory claim"), lead(id="other")]
    assert [row["id"] for row in select(rows, root=tmp_path, now=NOW)] == ["other"]


@pytest.mark.parametrize("field", ["source_refs", "superseded_by", "repo_path"])
@pytest.mark.parametrize("value", [None, False, []])
def test_present_invalid_optional_metadata_is_excluded(tmp_path, field, value):
    assert select([lead(**{field: value})], root=tmp_path, now=NOW) == []
