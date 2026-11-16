"""Deterministic coverage for tools/code_context.py.

Each test builds a throwaway git repo under tmp_path, except the last, which validates the
ledger checked into this repository.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

import tools.code_context as cc

SOURCE = """import os


def alpha(x):
    # keep this small
    return max(x, 1) + 1


class Box:
    def put(self, item):
        return [item]
"""

ADR = "# ADR-0007: Keep alpha small\n\nStatus: `Accepted` for now\n\nGoverns `pkg/mod.py`.\n"
LEDGER_DOC = "| Id | Text |\n|---|---|\n| `alpha-twice` | mod.py:4 builds it twice |\n\nSee othermod.py too.\n"


def git(repo: Path, *args: str) -> None:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    cmd = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args]
    subprocess.run(cmd, check=True, capture_output=True, env=env, stdin=subprocess.DEVNULL)


def write(repo: Path, rel: str, text: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def commit(repo: Path) -> None:
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "snapshot")


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    write(root, "pkg/mod.py", SOURCE + "# ADR-0007 keeps this file small.\n")
    write(root, "doc/decisions/ADR-0007-keep-alpha-small.md", ADR)
    write(root, "doc/LEDGER.md", LEDGER_DOC)
    commit(root)
    return root.resolve()


def run(capsys, *argv: str) -> tuple[int, str]:
    code = cc.main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out + captured.err


def test_hash_ignores_comments_blank_lines_and_wrapping_but_not_logic():
    before = cc.symbol_hash(SOURCE, "alpha")
    reworded = SOURCE.replace("# keep this small", "# a different comment\n\n")
    wrapped = SOURCE.replace("max(x, 1)", "max(x,\n               1)")
    changed = SOURCE.replace("max(x, 1)", "max(x, 2)")
    assert before is not None
    assert cc.symbol_hash(reworded, "alpha") == before
    assert cc.symbol_hash(wrapped, "alpha") == before
    assert cc.symbol_hash(changed, "alpha") != before
    assert cc.symbol_hash(SOURCE, "Box.put") is not None
    assert cc.symbol_hash(SOURCE, "missing") is None


def test_add_then_show_marks_a_verdict_fresh_then_changed(repo, capsys):
    code, _ = run(capsys, "add", "pkg/mod.py", "alpha", "leave", "--why", "Flat and clear.", "--root", str(repo))
    assert code == 0
    rows, errors = cc.load_ledger(repo)
    assert errors == []
    assert [(r["symbol"], r["verdict"]) for r in rows] == [("alpha", "leave")]

    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "alpha: leave, fresh" in out

    write(repo, "pkg/mod.py", SOURCE.replace("max(x, 1)", "max(x, 2)"))
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "alpha: leave, changed since" in out


def test_show_lists_cited_adrs_and_the_doc_lines_that_name_the_file(repo, capsys):
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "cites ADR-0007: Keep alpha small [Accepted]" in out
    assert "    doc/decisions/ADR-0007-keep-alpha-small.md ADR-0007: Keep alpha small [Accepted]: L5\n" in out
    # LEDGER.md names mod.py on line 3; line 5 names othermod.py, a different file.
    assert "    doc/LEDGER.md: L3 `alpha-twice`\n" in out


def test_show_reads_records_at_a_revision(repo, capsys):
    run(capsys, "add", "pkg/mod.py", "Box.put", "candidate", "--why", "One-liner.", "--root", str(repo))
    commit(repo)
    (repo / cc.LEDGER).unlink()
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo), "--rev", "HEAD")
    assert "Box.put: candidate, fresh" in out
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "verdicts: none recorded" in out

    # A verdict recorded but not yet committed still shows beside the committed ones.
    run(capsys, "add", "pkg/mod.py", "alpha", "leave", "--why", "Flat.", "--root", str(repo))
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo), "--rev", "HEAD")
    assert "Box.put: candidate, fresh" in out
    assert "alpha: leave, fresh" in out


def test_a_newer_verdict_replaces_an_older_one(repo, capsys):
    for verdict, date in (("candidate", "2026-11-16"), ("done", "2026-11-17"), ("leave", "2026-11-10")):
        run(capsys, "add", "pkg/mod.py", "alpha", verdict, "--why", "x", "--date", date, "--root", str(repo))
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "alpha: done, fresh, 2026-11-17" in out
    assert "alpha: leave" not in out


def test_a_whole_directory_verdict_counts_commits_since(repo, capsys):
    code, _ = run(capsys, "add", "pkg", "-", "leave", "--why", "Low churn.", "--commit", "HEAD", "--root", str(repo))
    assert code == 0
    write(repo, "pkg/mod.py", SOURCE + "\nVALUE = 1\n")
    commit(repo)
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "pkg/ (whole): leave, 1 commit since" in out


def test_add_refuses_an_unknown_function_or_path(repo, capsys):
    code, out = run(capsys, "add", "pkg/mod.py", "nope", "leave", "--why", "x", "--root", str(repo))
    assert code == 2
    assert "no def or class 'nope'" in out
    code, out = run(capsys, "add", "pkg/missing.py", "-", "leave", "--why", "x", "--root", str(repo))
    assert code == 2
    assert not (repo / cc.LEDGER).exists()


def test_check_reports_malformed_rows_and_exits_1(repo, capsys):
    good = {"path": "pkg/mod.py", "symbol": "", "verdict": "leave", "why": "ok", "date": "2026-11-16", "commit": "HEAD"}
    bad = dict(good, verdict="maybe")
    write(repo, cc.LEDGER, "\n".join([json.dumps(good), json.dumps(bad), "{not json"]) + "\n")
    code, out = run(capsys, "check", "--root", str(repo))
    assert code == 1
    assert f"{cc.LEDGER}:2: verdict 'maybe'" in out
    assert f"{cc.LEDGER}:3: not JSON" in out


def test_typed_ledger_errors_are_reported_and_excluded_without_poisoning_show(repo, capsys):
    good = {"path": "pkg/mod.py", "symbol": "", "verdict": "leave", "why": "ok", "date": "2026-11-16", "commit": "HEAD"}
    bad = dict(good, path=7)
    write(repo, cc.LEDGER, "\n".join([json.dumps(good), json.dumps(bad)]) + "\n")

    code, out = run(capsys, "check", "--root", str(repo))
    assert code == 1
    assert f"{cc.LEDGER}:2: path must be a string" in out
    assert "1 row(s), 1 problem(s)" in out

    code, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert code == 0
    assert "leave, fresh" in out
    assert "path must be a string" not in out


def test_whole_file_and_directory_verdicts_notice_dirty_tracked_paths_but_rev_does_not(repo, capsys):
    run(capsys, "add", "pkg/mod.py", "-", "leave", "--why", "File.", "--commit", "HEAD", "--root", str(repo))
    run(capsys, "add", "pkg", "-", "candidate", "--why", "Directory.", "--commit", "HEAD", "--root", str(repo))
    write(repo, "pkg/mod.py", SOURCE.replace("max(x, 1)", "max(x, 2)"))

    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "(whole): leave, changed in working tree" in out
    assert "pkg/ (whole): candidate, changed in working tree" in out

    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo), "--rev", "HEAD")
    assert "(whole): leave, fresh" in out
    assert "pkg/ (whole): candidate, fresh" in out


@pytest.mark.parametrize("command, paths", [("show", ("pkg/mod.py",)), ("check", ())])
def test_unknown_revision_is_rejected_before_show_or_check(repo, capsys, command, paths):
    code, out = run(capsys, command, *paths, "--root", str(repo), "--rev", "missing-revision")
    assert code == 2
    assert "unknown revision 'missing-revision'" in out


@pytest.mark.parametrize("root_kind", ["outside", "subdirectory"])
@pytest.mark.parametrize(
    "command, tail",
    [("show", ("mod.py", "--rev", "HEAD")), ("check", ()), ("add", ("mod.py", "-", "leave", "--why", "x"))],
)
def test_root_must_be_a_git_top_level(repo, capsys, root_kind, command, tail):
    target = repo / "pkg" if root_kind == "subdirectory" else repo.parent / "outside"
    target.mkdir(exist_ok=True)
    code, out = run(capsys, command, *tail, "--root", str(target))
    assert code == 2
    assert "--root must be a Git repository top-level" in out
    assert not (target / cc.LEDGER).exists()


def test_paths_outside_root_are_rejected_without_reading_or_writing(repo, capsys):
    outside = repo.parent / "outside.py"
    write(repo.parent, outside.name, SOURCE)
    before = (repo / cc.LEDGER).read_bytes() if (repo / cc.LEDGER).exists() else None

    for raw in (str(outside), f"../{outside.name}"):
        code, out = run(capsys, "show", raw, "--root", str(repo))
        assert code == 2
        assert "outside repository root" in out
        code, out = run(capsys, "add", raw, "-", "leave", "--why", "No.", "--root", str(repo))
        assert code == 2
        assert "outside repository root" in out

    after = (repo / cc.LEDGER).read_bytes() if (repo / cc.LEDGER).exists() else None
    assert after == before

    code, out = run(capsys, "show", f"{repo.name}/pkg/mod.py", "--root", str(repo))
    assert code == 0
    assert "pkg/mod.py (" in out


def test_ledger_paths_outside_root_are_reported_and_excluded(repo, capsys):
    good = {"path": "pkg/mod.py", "symbol": "", "verdict": "leave", "why": "ok", "date": "2026-11-16", "commit": "HEAD"}
    bad = dict(good, path="../outside.py", verdict="done")
    write(repo, cc.LEDGER, "\n".join([json.dumps(good), json.dumps(bad)]) + "\n")

    code, out = run(capsys, "check", "--root", str(repo))
    assert code == 1
    assert f"{cc.LEDGER}:2: path outside repository root" in out
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "leave, fresh" in out
    assert "done" not in out


def test_invalid_dates_are_rejected_and_cannot_replace_a_valid_verdict(repo, capsys):
    good = {"path": "pkg/mod.py", "symbol": "", "verdict": "leave", "why": "ok", "date": "2026-11-16", "commit": "HEAD"}
    bad = dict(good, verdict="done", date="not-a-date")
    write(repo, cc.LEDGER, "\n".join([json.dumps(good), json.dumps(bad)]) + "\n")

    code, out = run(capsys, "check", "--root", str(repo))
    assert code == 1
    assert f"{cc.LEDGER}:2: date must be YYYY-MM-DD" in out
    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo))
    assert "leave, fresh" in out
    assert "done" not in out


def test_revision_show_labels_uncommitted_overlay_rows(repo, capsys):
    run(capsys, "add", "pkg/mod.py", "alpha", "leave", "--why", "Committed.", "--root", str(repo))
    commit(repo)
    run(capsys, "add", "pkg/mod.py", "Box.put", "candidate", "--why", "Overlay.", "--root", str(repo))

    _, out = run(capsys, "show", "pkg/mod.py", "--root", str(repo), "--rev", "HEAD")
    assert "working-tree verdict; code at requested rev HEAD" in out


def test_show_applies_limit_to_verdicts_and_adrs(repo, capsys, monkeypatch):
    alpha_hash = cc.symbol_hash(SOURCE, "alpha")
    box_hash = cc.symbol_hash(SOURCE, "Box.put")
    rows = [
        {
            "path": "pkg/mod.py",
            "symbol": "",
            "verdict": "leave",
            "why": "whole",
            "date": "2026-11-14",
            "commit": "HEAD",
        },
        {
            "path": "pkg/mod.py",
            "symbol": "alpha",
            "verdict": "candidate",
            "why": "alpha",
            "date": "2026-11-15",
            "commit": "HEAD",
            "hash": alpha_hash,
        },
        {
            "path": "pkg/mod.py",
            "symbol": "Box.put",
            "verdict": "done",
            "why": "box",
            "date": "2026-11-16",
            "commit": "HEAD",
            "hash": box_hash,
        },
    ]
    write(repo, cc.LEDGER, "\n".join(json.dumps(row) for row in rows) + "\n")
    monkeypatch.setattr(cc, "cited_adrs", lambda *args: ["ADR one", "ADR two"])

    _, out = run(capsys, "show", "pkg/mod.py", "--limit", "1", "--root", str(repo))
    assert "verdicts (tools/refactor_ledger.jsonl), first 1:" in out
    assert "(+2 more)" in out
    assert "cites ADR one" in out
    assert "cites ADR two" not in out


def test_the_checked_in_ledger_is_valid():
    assert (cc.REPO / cc.LEDGER).exists()
    rows, errors = cc.load_ledger(cc.REPO)
    assert errors == []
    assert rows
