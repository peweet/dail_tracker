"""Deterministic coverage for tools/hooks/precompact_resume.py and resume_context.py.

A PreCompact hook writes logs/resume/<session>.md from the transcript tail and git; the
SessionStart hook injects it only when the start source is a compaction or a resume. Both
fail open. Fixtures are a synthetic transcript under tmp_path; git is left real but pointed
at a directory with no repository so the status sections degrade to their placeholder.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest

HOOKS = Path(__file__).resolve().parents[2] / "tools" / "hooks"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, HOOKS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pre():
    return _load("precompact_resume")


@pytest.fixture(scope="module")
def post():
    return _load("resume_context")


def _transcript(path: Path) -> None:
    rows = [
        {"type": "user", "message": {"content": "Please fix the vote join and run the tests"}},
        {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "tool_use", "name": "Edit", "input": {"file_path": "utility/data_access/votes.py"}},
                    {"type": "tool_use", "name": "Bash", "input": {"command": "pytest test/debates -q"}},
                ]
            },
        },
        {"type": "user", "message": {"content": [{"type": "tool_result", "content": "ok"}]}},
        {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "tool_use", "name": "Write", "input": {"file_path": "test/debates/test_votes.py"}},
                    {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}},
                ]
            },
        },
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def test_resume_file_lists_edits_checks_and_request(pre, tmp_path, monkeypatch):
    transcript = tmp_path / "s.jsonl"
    _transcript(transcript)
    monkeypatch.setattr(pre, "_RESUME_DIR", tmp_path / "resume")
    monkeypatch.setattr(pre, "_REPO", tmp_path / "norepo")
    payload = {
        "session_id": "abcdef123456789",
        "transcript_path": str(transcript),
        "cwd": str(tmp_path),
        "trigger": "auto",
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert pre.main() == 0
    out = (tmp_path / "resume" / "abcdef123456.md").read_text(encoding="utf-8")
    assert "utility/data_access/votes.py" in out
    assert "test/debates/test_votes.py" in out
    assert "pytest test/debates -q" in out
    assert "`ls`" not in out  # only check-shaped commands are kept
    assert "fix the vote join" in out
    assert "`auto` compaction" in out


def test_missing_transcript_still_writes_and_exits_zero(pre, tmp_path, monkeypatch):
    monkeypatch.setattr(pre, "_RESUME_DIR", tmp_path / "resume")
    monkeypatch.setattr(pre, "_REPO", tmp_path / "norepo")
    payload = {"session_id": "zzz", "transcript_path": str(tmp_path / "nope.jsonl"), "trigger": "manual"}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert pre.main() == 0
    assert "(none recorded)" in (tmp_path / "resume" / "zzz.md").read_text(encoding="utf-8")


def test_no_session_id_writes_nothing(pre, tmp_path, monkeypatch):
    monkeypatch.setattr(pre, "_RESUME_DIR", tmp_path / "resume")
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"trigger": "auto"})))
    assert pre.main() == 0
    assert not (tmp_path / "resume").exists()


def test_session_start_injects_only_after_compact(post, tmp_path, monkeypatch, capsys):
    resume = tmp_path / "resume"
    resume.mkdir()
    (resume / "abcdef123456.md").write_text("# Resume state\n- utility/data_access/votes.py\n", encoding="utf-8")
    monkeypatch.setattr(post, "_RESUME_DIR", resume)
    for source, expect in (("startup", False), ("clear", False), ("compact", True), ("resume", True)):
        payload = {"session_id": "abcdef123456789", "source": source}
        monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
        assert post.main() == 0
        out = capsys.readouterr().out.strip()
        assert bool(out) is expect, source
        if out:
            ctx = json.loads(out)["hookSpecificOutput"]["additionalContext"]
            assert ctx.startswith("[resume]")
            assert "votes.py" in ctx


def test_session_start_caps_and_ignores_missing(post, tmp_path, monkeypatch, capsys):
    resume = tmp_path / "resume"
    resume.mkdir()
    (resume / "big123456789.md").write_text("x" * 10_000, encoding="utf-8")
    monkeypatch.setattr(post, "_RESUME_DIR", resume)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"session_id": "big1234567890", "source": "compact"})))
    assert post.main() == 0
    ctx = json.loads(capsys.readouterr().out)["hookSpecificOutput"]["additionalContext"]
    assert "truncated" in ctx and len(ctx) < 5_000
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"session_id": "unknown", "source": "compact"})))
    assert post.main() == 0
    assert capsys.readouterr().out == ""
