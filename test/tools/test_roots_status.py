"""Tests for the multi-root git status reporter (tools/roots_status.py).

The two regressions that motivated this file both produced a FALSE ALL-CLEAR on
2026-08-14 — the failure mode that matters for a guard whose only job is to stop work
being lost:

  1. Commits sitting in a `git worktree` were invisible; only the primary checkout was
     ever inspected.
  2. A branch pushed with `push HEAD:main` left its own tracking ref behind, so the old
     ahead-of-@{u} count reported "not pushed" for commits that were already on the
     remote — noise that trains the reader to ignore the report.

Each test below drives real git repositories rather than mocking subprocess, because the
thing under test IS the git plumbing invocation.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location("roots_status", _REPO / "tools" / "roots_status.py")
rs = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
# Registered before exec: @dataclass resolves annotations through sys.modules[__module__],
# which is None for a spec-loaded module and raises on the first frozen dataclass.
sys.modules["roots_status"] = rs
_SPEC.loader.exec_module(rs)


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _commit(repo: Path, name: str) -> None:
    (repo / name).write_text(name, encoding="utf-8")
    _git(repo, "add", name)
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", name)


@pytest.fixture
def repo_with_remote(tmp_path: Path) -> Path:
    """A working repo whose `origin` is a real (bare) remote, with one pushed commit."""
    bare = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(bare)], check=True)

    repo = tmp_path / "work"
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    _git(repo, "remote", "add", "origin", str(bare))
    _commit(repo, "base.txt")
    _git(repo, "push", "-q", "-u", "origin", "main")
    return repo


# ── 1. the worktree blind spot ───────────────────────────────────────────────
def test_commits_in_a_worktree_are_reported(repo_with_remote: Path, tmp_path: Path) -> None:
    """The council-spine case: work committed in a worktree, invisible to the old check."""
    wt = tmp_path / "side"
    _git(repo_with_remote, "worktree", "add", "-q", "-b", "side", str(wt))
    _commit(wt, "in_worktree.txt")

    status = rs.check_root("r", repo_with_remote)
    by_path = {c.path.resolve(): c for c in status.checkouts}

    assert wt.resolve() in by_path, "worktree was not walked at all"
    assert by_path[wt.resolve()].unpublished == 1
    assert by_path[repo_with_remote.resolve()].unpublished == 0


def test_primary_checkout_is_still_reported(repo_with_remote: Path) -> None:
    _commit(repo_with_remote, "local_only.txt")
    status = rs.check_root("r", repo_with_remote)

    primary = status.checkouts[0]
    assert primary.is_primary
    assert primary.unpublished == 1


# ── 2. the false "not pushed" nag ────────────────────────────────────────────
def test_branch_pushed_to_main_is_not_reported_as_unpushed(repo_with_remote: Path) -> None:
    """`push HEAD:main` leaves the branch's OWN tracking ref behind.

    Reproduces the real 2026-08-14 shape exactly: the branch has an upstream
    (origin/feature) that it is genuinely ahead of, while every one of its commits is
    already on origin/main. The commits cannot be lost, so nothing must be flagged — the
    old ahead-of-upstream count reported "1 commit(s) not pushed" here.

    The upstream must exist and LAG for this to be a regression test; a branch with no
    upstream at all scored zero under the old code too and would pass either way.
    """
    _git(repo_with_remote, "checkout", "-q", "-b", "feature")
    _git(repo_with_remote, "push", "-q", "-u", "origin", "feature")
    _commit(repo_with_remote, "shipped.txt")
    _git(repo_with_remote, "push", "-q", "origin", "HEAD:main")
    _git(repo_with_remote, "fetch", "-q", "origin")

    # Precondition: the old metric would see this as unpushed work.
    ahead = _git(repo_with_remote, "rev-list", "--count", "@{u}..HEAD")
    assert ahead == "1", "test no longer reproduces the lagging-upstream shape"

    status = rs.check_root("r", repo_with_remote)
    assert status.checkouts[0].unpublished == 0


def test_detached_head_worktree_is_counted(repo_with_remote: Path, tmp_path: Path) -> None:
    """A detached HEAD has no upstream, so the old @{u} comparison silently scored zero."""
    _commit(repo_with_remote, "unpushed.txt")
    sha = _git(repo_with_remote, "rev-parse", "HEAD")
    wt = tmp_path / "detached"
    _git(repo_with_remote, "worktree", "add", "-q", "--detach", str(wt), sha)

    status = rs.check_root("r", repo_with_remote)
    detached = next(c for c in status.checkouts if c.path.resolve() == wt.resolve())

    assert detached.unpublished == 1
    assert detached.label.startswith("detached at ")


# ── 3. reporting mechanics ───────────────────────────────────────────────────
def test_dirty_worktree_is_reported(repo_with_remote: Path, tmp_path: Path) -> None:
    wt = tmp_path / "dirty"
    _git(repo_with_remote, "worktree", "add", "-q", "-b", "dirty", str(wt))
    (wt / "scratch.txt").write_text("uncommitted", encoding="utf-8")

    status = rs.check_root("r", repo_with_remote)
    dirty = next(c for c in status.checkouts if c.path.resolve() == wt.resolve())
    assert dirty.dirty_count == 1


def test_deleted_worktree_directory_is_flagged_not_crashed(repo_with_remote: Path, tmp_path: Path) -> None:
    """A worktree removed with `rm -rf` still lists until pruned; it must not raise."""
    import shutil

    wt = tmp_path / "gone"
    _git(repo_with_remote, "worktree", "add", "-q", "-b", "gone", str(wt))
    shutil.rmtree(wt)

    status = rs.check_root("r", repo_with_remote)
    missing = next(c for c in status.checkouts if c.path.resolve() == wt.resolve())
    assert not missing.exists
    assert rs._problems(missing) == 1


def test_clean_published_repo_has_no_problems(repo_with_remote: Path) -> None:
    status = rs.check_root("r", repo_with_remote)
    assert sum(rs._problems(c) for c in status.checkouts) == 0


def test_commit_checkout_stages_only_declared_paths(repo_with_remote: Path) -> None:
    intended = repo_with_remote / "intended.txt"
    unrelated = repo_with_remote / "unrelated.txt"
    intended.write_text("intended", encoding="utf-8")
    unrelated.write_text("unrelated", encoding="utf-8")
    checkout = rs.check_root("r", repo_with_remote).checkouts[0]

    assert rs.commit_checkout(checkout, "Commit intended", ["intended.txt"], dry_run=False) == 0

    assert _git(repo_with_remote, "show", "--format=", "--name-only", "HEAD") == "intended.txt"
    assert _git(repo_with_remote, "status", "--short") == "?? unrelated.txt"


def test_commit_checkout_refuses_unrelated_staged_paths(repo_with_remote: Path) -> None:
    intended = repo_with_remote / "intended.txt"
    unrelated = repo_with_remote / "unrelated.txt"
    intended.write_text("intended", encoding="utf-8")
    unrelated.write_text("unrelated", encoding="utf-8")
    _git(repo_with_remote, "add", "unrelated.txt")
    checkout = rs.check_root("r", repo_with_remote).checkouts[0]

    assert rs.commit_checkout(checkout, "Commit intended", ["intended.txt"], dry_run=False) == 1
    assert _git(repo_with_remote, "diff", "--cached", "--name-only") == "unrelated.txt"
    assert _git(repo_with_remote, "status", "--short", "--", "intended.txt") == "?? intended.txt"


def _run_cli(monkeypatch, capsys, root: Path, *args: str) -> tuple[int, str]:
    monkeypatch.setattr(rs, "ROOTS", (("public", "temporary public", root),))
    monkeypatch.setattr(sys, "argv", ["roots_status.py", *args])
    code = rs.main()
    return code, capsys.readouterr().out


def test_cli_refuses_a_checkout_outside_the_selected_root(
    repo_with_remote: Path, tmp_path: Path, monkeypatch, capsys
) -> None:
    outside = tmp_path / "outside"
    subprocess.run(["git", "init", "-q", "-b", "main", str(outside)], check=True)

    code, output = _run_cli(
        monkeypatch,
        capsys,
        repo_with_remote,
        "--repo",
        "public",
        "--checkout",
        str(outside),
        "--commit",
        "--path",
        "missing.txt",
        "-m",
        "must refuse",
    )

    assert code == 1
    assert "REFUSED:" in output
    assert "is not a checkout of public" in output


def test_cli_commit_targets_the_explicit_worktree(repo_with_remote: Path, tmp_path: Path, monkeypatch, capsys) -> None:
    wt = tmp_path / "side"
    _git(repo_with_remote, "worktree", "add", "-q", "-b", "side", str(wt))
    (wt / "intended.txt").write_text("intended", encoding="utf-8")
    (wt / "unrelated.txt").write_text("unrelated", encoding="utf-8")

    code, output = _run_cli(
        monkeypatch,
        capsys,
        repo_with_remote,
        "--repo",
        "public",
        "--checkout",
        str(wt),
        "--commit",
        "--path",
        "intended.txt",
        "-m",
        "Commit intended worktree path",
    )

    assert code == 0
    assert "Selected checkout action completed" in output
    assert _git(wt, "show", "--format=", "--name-only", "HEAD") == "intended.txt"
    assert _git(wt, "status", "--short") == "?? unrelated.txt"
    assert _git(repo_with_remote, "log", "-1", "--format=%s") == "base.txt"


@pytest.mark.parametrize("value", [".", "../outside", "C:/outside", "*.py"])
def test_commit_paths_must_be_bounded(value: str) -> None:
    with pytest.raises(ValueError, match="bounded relative"):
        rs._normalise_action_paths([value])
