"""Regression coverage for the pre-push archive repository isolation.

The hook runs from linked worktrees, where Git exports local repository
environment variables to the hook.  The archive's temporary repository must
not let those variables redirect ``git init`` into the source repository.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
HOOK = REPO / ".githooks" / "pre-push"


def _clean_git_env() -> dict[str, str]:
    """Remove Git's local-repository variables before starting fixture Git commands."""
    probe_env = os.environ.copy()
    for name in tuple(probe_env):
        if name.startswith("GIT_"):
            probe_env.pop(name)
    names = subprocess.run(
        ["git", "rev-parse", "--local-env-vars"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
        env=probe_env,
    ).stdout.split()
    env = probe_env
    for name in names:
        env.pop(name, None)
    return env


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=True,
        env=_clean_git_env(),
    )
    return result.stdout.strip()


def _commit(cwd: Path, name: str) -> None:
    (cwd / name).write_text(name, encoding="utf-8")
    _git(cwd, "add", name)
    _git(cwd, "-c", "user.name=pre-push-test", "-c", "user.email=test@example.invalid", "commit", "-q", "-m", name)


def test_linked_worktree_push_does_not_mutate_shared_git_config(tmp_path: Path) -> None:
    bare = tmp_path / "origin.git"
    primary = tmp_path / "primary"
    linked = tmp_path / "linked"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(bare))
    _git(tmp_path, "init", "-q", "-b", "main", str(primary))
    _git(primary, "config", "user.name", "pre-push-test")
    _git(primary, "config", "user.email", "test@example.invalid")
    _git(primary, "remote", "add", "origin", str(bare))

    hooks = primary / ".githooks"
    hooks.mkdir()
    hook = hooks / "pre-push"
    hook.write_bytes(HOOK.read_bytes())
    hook.chmod(0o755)
    fake_python = primary / ".venv" / "Scripts" / "python"
    fake_python.parent.mkdir(parents=True)
    fake_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o755)
    _git(primary, "add", ".githooks/pre-push", ".venv/Scripts/python")
    _git(primary, "commit", "-q", "-m", "hook fixture")
    _git(primary, "push", "-q", "-u", "origin", "main")
    _git(primary, "config", "core.hooksPath", str(hooks))

    _git(primary, "worktree", "add", "-q", "-b", "feature", str(linked))
    _commit(linked, "feature.txt")
    config = Path(_git(primary, "rev-parse", "--git-path", "config"))
    if not config.is_absolute():
        config = primary / config
    config_before = config.read_bytes()

    pushed = subprocess.run(
        ["git", "-C", str(linked), "push", "-q", "origin", "feature"],
        capture_output=True,
        text=True,
        env=_clean_git_env(),
    )

    assert pushed.returncode == 0, pushed.stderr
    assert config.read_bytes() == config_before
    assert _git(primary, "config", "--get", "core.bare") == "false"
    assert _git(linked, "config", "--get", "core.bare") == "false"
    assert _git(primary, "status", "--porcelain") == ""
    assert _git(linked, "status", "--porcelain") == ""


@pytest.mark.parametrize("content_exit", [0, 1, 2])
def test_content_scan_receives_every_pushed_commit(tmp_path: Path, content_exit: int) -> None:
    root = tmp_path / "repo"
    _git(tmp_path, "init", "-q", "-b", "main", str(root))
    _commit(root, "one.md")
    first = _git(root, "rev-parse", "HEAD")
    _commit(root, "two.md")
    second = _git(root, "rev-parse", "HEAD")
    runner = root / ".venv" / "Scripts" / "python"
    runner.parent.mkdir(parents=True)
    runner.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_TRACE"\n'
        'case "$*" in *--content*) exit "$CONTENT_EXIT";; esac\nexit 0\n',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    trace = tmp_path / "calls.txt"
    env = _clean_git_env()
    env["CALL_TRACE"] = str(trace)
    env["CONTENT_EXIT"] = str(content_exit)
    env["DAIL_SKIP_PUSH_GATE"] = "1"  # fixture: exercise the IP gate, not lint in an empty project
    zero = "0" * 40
    lines = f"refs/heads/a {first} refs/heads/a {zero}\nrefs/heads/b {second} refs/heads/b {zero}\n"
    shell = shutil.which("sh")
    if shell is None:
        git = shutil.which("git")
        assert git is not None
        shell = str(Path(git).resolve().parents[1] / "bin" / "sh.exe")
    result = subprocess.run([shell, str(HOOK)], cwd=root, input=lines, text=True, capture_output=True, env=env)
    assert result.returncode == content_exit, result.stderr
    calls = trace.read_text(encoding="utf-8").splitlines()
    scans = [line for line in calls if "--content" in line]
    assert len(scans) == (1 if content_exit else 2)
    assert any(f"--ref {first}" in line for line in scans)
    if not content_exit:
        assert any(f"--ref {second}" in line for line in scans)


def test_content_scan_rejects_an_intermediate_outgoing_commit(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _git(tmp_path, "init", "-q", "-b", "main", str(root))
    _commit(root, "base.md")
    base = _git(root, "rev-parse", "HEAD")
    _commit(root, "intermediate.md")
    blocked = _git(root, "rev-parse", "HEAD")
    _commit(root, "clean-tip.md")
    tip = _git(root, "rev-parse", "HEAD")
    runner = root / ".venv" / "Scripts" / "python"
    runner.parent.mkdir(parents=True)
    runner.write_text(
        '#!/bin/sh\ncase "$*" in *"--content --ref $BLOCKED_COMMIT"*) exit 1;; esac\nexit 0\n',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    env = _clean_git_env()
    env.update(BLOCKED_COMMIT=blocked, DAIL_SKIP_PUSH_GATE="1")
    shell = shutil.which("sh")
    if shell is None:
        git = shutil.which("git")
        assert git is not None
        shell = str(Path(git).resolve().parents[1] / "bin" / "sh.exe")
    result = subprocess.run(
        [shell, str(HOOK)],
        cwd=root,
        input=f"refs/heads/main {tip} refs/heads/main {base}\n",
        text=True,
        capture_output=True,
        env=env,
    )
    assert result.returncode == 1, result.stderr
    assert "committed Markdown check blocked" in result.stderr
