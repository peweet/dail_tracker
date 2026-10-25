"""Deterministic coverage for tools/hooks/assertion_guard.py.

The hook diffs an edited test or baseline file against HEAD and warns, once per distinct
state, when an assertion line is gone or a skip marker appeared. A throwaway git repository
under tmp_path supplies HEAD; nothing touches the real checkout.
"""

from __future__ import annotations

import importlib.util
import io
import json
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / "tools" / "hooks" / "assertion_guard.py"


@pytest.fixture(scope="module")
def hook():
    spec = importlib.util.spec_from_file_location("assertion_guard", HOOK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false", *args],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q")
    test_dir = tmp_path / "test"
    test_dir.mkdir()
    (test_dir / "test_votes.py").write_text(
        "def test_a():\n    assert 1 == 1\n\n\ndef test_b():\n    assert 2 == 2\n    assert 3 == 3\n",
        encoding="utf-8",
    )
    (tmp_path / "data" / "_meta").mkdir(parents=True)
    (tmp_path / "data" / "_meta" / "gold_quality_baseline.json").write_text('{"rows": 10}\n', encoding="utf-8")
    (tmp_path / "svc.py").write_text("def f():\n    assert True\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "seed")
    return tmp_path


def _run(hook, monkeypatch, capsys, path: Path, session: str | None = None) -> str:
    payload = {
        "tool_name": "Edit",
        "tool_input": {"file_path": str(path)},
        "session_id": session or f"t-{uuid.uuid4().hex[:8]}",
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert hook.main() == 0
    out = capsys.readouterr().out.strip()
    if not out:
        return ""
    return json.loads(out)["hookSpecificOutput"]["additionalContext"]


def test_analyse_counts_net_assertions_and_skips(hook):
    head = "def test_x():\n    assert a == b\n    with pytest.raises(ValueError):\n        f()\n    assert c\n"
    new = (
        "import pytest\n\n\n@pytest.mark.xfail(reason='x')\ndef test_x():\n    pytest.skip('later')\n    assert c\n"
        "    try:\n        f()\n    except Exception:\n        pass\n"
    )
    found = hook.analyse(head, new)
    assert found["removed_count"] == 2
    assert found["removed"] == ["assert a == b", "with pytest.raises(ValueError):"]
    assert found["skips"] == 2
    assert found["broad"] == 1
    moved = "import pytest\n\n\n" + head  # same assertions further down: a diff pairs them wrongly, totals do not
    assert hook.analyse(head, moved)["removed_count"] == 0
    assert hook.analyse(head, head.replace("a == b", "a >= 0"))["removed_count"] == 0  # reworded: not detected


def test_removed_assertion_warns_once_per_state(hook, repo, monkeypatch, capsys):
    target = repo / "test" / "test_votes.py"
    target.write_text("def test_a():\n    assert 1 == 1\n\n\ndef test_b():\n    assert 2 == 2\n", encoding="utf-8")
    session = f"s-{uuid.uuid4().hex[:8]}"
    msg = _run(hook, monkeypatch, capsys, target, session)
    assert msg.startswith("[assertion-guard]")
    assert "1 assertion line(s) removed" in msg
    assert "tests.md" in msg
    assert _run(hook, monkeypatch, capsys, target, session) == ""  # same state, same session: silent
    target.write_text("import pytest\n\n\n@pytest.mark.skip\ndef test_a():\n    assert 1 == 1\n", encoding="utf-8")
    msg2 = _run(hook, monkeypatch, capsys, target, session)
    assert "2 assertion line(s) removed" in msg2 and "1 skip/xfail marker(s) added" in msg2


def test_unchanged_or_non_test_files_are_silent(hook, repo, monkeypatch, capsys):
    assert _run(hook, monkeypatch, capsys, repo / "test" / "test_votes.py") == ""
    svc = repo / "svc.py"
    svc.write_text("def f():\n    return 1\n", encoding="utf-8")
    assert _run(hook, monkeypatch, capsys, svc) == ""


def test_baseline_change_names_the_rule(hook, repo, monkeypatch, capsys):
    baseline = repo / "data" / "_meta" / "gold_quality_baseline.json"
    baseline.write_text('{"rows": 9}\n', encoding="utf-8")
    msg = _run(hook, monkeypatch, capsys, baseline)
    assert "recorded baseline" in msg and "data-anchored" in msg


def test_untracked_test_file_is_silent(hook, repo, monkeypatch, capsys):
    new = repo / "test" / "test_new.py"
    new.write_text("def test_z():\n    assert 0 == 0\n", encoding="utf-8")
    assert _run(hook, monkeypatch, capsys, new) == ""
