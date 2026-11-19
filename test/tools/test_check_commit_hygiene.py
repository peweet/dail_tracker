"""The commit-msg guard: a commit that removes or rewrites an existing test assertion must say why."""

import subprocess
from pathlib import Path

import pytest

from tools import check_commit_hygiene as hygiene

INVERTED = (
    "diff --git a/test/t.py b/test/t.py\n--- a/test/t.py\n+++ b/test/t.py\n@@ -3 +3 @@\n"
    "-    assert verdict() == 1\n+    assert verdict() == 0\n"
)


def test_an_inverted_assertion_is_reported():
    assert hygiene.removed_assertions(INVERTED) == ["test/t.py: assert verdict() == 1"]


def test_an_assertion_moved_within_its_file_nets_out():
    assert hygiene.removed_assertions(INVERTED.replace("== 0", "== 1")) == []


def test_a_new_file_and_assert_lookalikes_are_not_removed_assertions():
    diff = (
        "diff --git a/test/n.py b/test/n.py\n--- /dev/null\n+++ b/test/n.py\n@@ -0,0 +1 @@\n+    assert x\n"
        "diff --git a/test/t.py b/test/t.py\n--- a/test/t.py\n+++ b/test/t.py\n@@ -3,2 +3 @@\n"
        "-    mock.assert_called_once()\n-    assert_frame_equal(a, b)\n+    pass\n"
    )
    assert hygiene.removed_assertions(diff) == []


def test_a_removed_unittest_assertion_is_reported():
    diff = INVERTED.replace("-    assert verdict() == 1", "-        self.assertEqual(verdict(), 1)")
    assert hygiene.removed_assertions(diff) == ["test/t.py: self.assertEqual(verdict(), 1)"]


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.invalid",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        check=True,
        capture_output=True,
    )


@pytest.fixture(params=["test_x.py", "test_é.py"])  # git quotes a non-ASCII path in diff headers unless told not to
def staged_inversion(tmp_path, monkeypatch, request):
    _git(tmp_path, "init", "-q")
    test_file = tmp_path / "test" / request.param
    test_file.parent.mkdir()
    test_file.write_text("def test_x():\n    assert run() == 1\n", encoding="utf-8")
    _git(tmp_path, "add", f"test/{request.param}")
    _git(tmp_path, "commit", "-q", "--no-verify", "-m", "Add the stale-run test")
    test_file.write_text("def test_x():\n    assert run() == 0\n", encoding="utf-8")
    _git(tmp_path, "add", f"test/{request.param}")
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.mark.parametrize(
    "message, expect",
    [
        ("Accept the stale ladder run\n", 1),
        ("Accept the stale ladder run\n\nAssertion-Change: test_x: the old expectation was wrong\n", 0),
    ],
)
def test_a_staged_assertion_change_needs_a_reason_in_the_message(staged_inversion, message, expect):
    msg = staged_inversion / "MSG"
    msg.write_text(message, encoding="utf-8")
    assert hygiene.check_commit_msg(str(msg)) == expect
