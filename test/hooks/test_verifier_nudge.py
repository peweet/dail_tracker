"""Deterministic coverage for tools/hooks/verifier_nudge.py.

The hook keeps a per-session set of edited non-test code files and injects one reminder to
run the verifier agent the first time that set reaches the threshold. State lives in a temp
directory redirected to tmp_path here.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import uuid
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / "tools" / "hooks" / "verifier_nudge.py"


@pytest.fixture(scope="module")
def hook():
    spec = importlib.util.spec_from_file_location("verifier_nudge", HOOK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(hook, monkeypatch, capsys, path: str, session: str) -> str:
    payload = {"tool_name": "Edit", "tool_input": {"file_path": path}, "session_id": session}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert hook.main() == 0
    out = capsys.readouterr().out.strip()
    return json.loads(out)["hookSpecificOutput"]["additionalContext"] if out else ""


def test_is_code_excludes_tests_docs_and_config(hook):
    assert hook.is_code(Path("utility/data_access/votes.py"))
    assert hook.is_code(Path("planning/product/web/app/page.tsx"))
    assert not hook.is_code(Path("test/debates/test_votes.py"))
    assert not hook.is_code(Path("tools/foo_test.py"))
    assert not hook.is_code(Path("web/app/page.test.tsx"))
    assert not hook.is_code(Path(".claude/hooks/x.py"))
    assert not hook.is_code(Path("doc/PLAN.md"))
    assert not hook.is_code(Path("data/_meta/fact_cards.json"))


def test_nudges_once_at_threshold(hook, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(hook, "_STATE_DIR", tmp_path)
    session = f"s-{uuid.uuid4().hex[:8]}"
    assert _run(hook, monkeypatch, capsys, "a/one.py", session) == ""
    assert _run(hook, monkeypatch, capsys, "a/one.py", session) == ""  # same file, not a new one
    assert _run(hook, monkeypatch, capsys, "a/two.py", session) == ""
    assert _run(hook, monkeypatch, capsys, "test/test_two.py", session) == ""  # tests do not count
    msg = _run(hook, monkeypatch, capsys, "b/three.sql", session)
    assert msg.startswith("[verifier]") and "3 non-test code files" in msg and "verifier" in msg
    assert _run(hook, monkeypatch, capsys, "c/four.py", session) == ""  # fired once per session


def test_sessions_are_independent(hook, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(hook, "_STATE_DIR", tmp_path)
    a, b = f"a-{uuid.uuid4().hex[:6]}", f"b-{uuid.uuid4().hex[:6]}"
    for name in ("x.py", "y.py"):
        assert _run(hook, monkeypatch, capsys, name, a) == ""
        assert _run(hook, monkeypatch, capsys, name, b) == ""
    assert _run(hook, monkeypatch, capsys, "z.py", a) != ""
    assert _run(hook, monkeypatch, capsys, "w.py", b) != ""
