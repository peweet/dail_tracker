"""Deterministic coverage for tools/check_code_hygiene.py.

Every test builds a throwaway git repo under tmp_path; nothing reads this checkout's git state.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

import tools.check_code_hygiene as hygiene

SIMPLE = """def f(a):
    b = a + 1
    c = b + 2
    d = c + 3
    e = d + 4
    g = e + 5
    h = g + 6
    return h
"""


def complex_source(name: str, branches: int) -> str:
    lines = [f"def {name}(a):", "    x = 0"]
    for i in range(branches):
        lines += [f"    if a == {i}:", f"        x += {i + 1}"]
    lines.append("    return x")
    return "\n".join(lines) + "\n"


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
def repo(tmp_path, monkeypatch):
    monkeypatch.setattr(hygiene, "CACHE_DIR", tmp_path / "cache")
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    return root.resolve()


def regression(result: dict, key: str) -> dict | None:
    return next((r for r in result["regressions"] if r["key"] == key), None)


def test_measure_source_counts_and_weights():
    metrics = hygiene.measure_source(complex_source("g", 11), "pkg/mod.py")
    assert metrics["C901"] == {"count": 1, "weight": 2}  # complexity 12, threshold 10
    assert "PLR0913" not in metrics


def test_complexity_increase_in_edited_function_fails(repo):
    write(repo, "pkg/mod.py", complex_source("g", 11))
    commit(repo)
    write(repo, "pkg/mod.py", complex_source("g", 14))
    result = hygiene.gate([repo / "pkg" / "mod.py"], root=repo)
    reg = regression(result, "C901")
    assert reg is not None
    assert (reg["base"], reg["head"]) == (2, 5)
    assert reg["findings"][0]["path"] == "pkg/mod.py"
    assert "C901 complexity weight 2 -> 5 (+3)" in hygiene.regression_lines(result)


def test_extracting_part_of_a_complex_function_is_an_improvement(repo):
    # Weight is the excess over the threshold: one function at complexity 20 (excess 10) split
    # into one at 11 and one at 12 (excess 1 + 2) must improve, never read as 20 -> 23.
    write(repo, "pkg/mod.py", complex_source("g", 19))
    commit(repo)
    write(repo, "pkg/mod.py", complex_source("g", 10) + "\n\n" + complex_source("h", 11))
    result = hygiene.gate([repo / "pkg" / "mod.py"], root=repo)
    assert result["regressions"] == []
    improved = {item["key"]: (item["base"], item["head"]) for item in result["improved"]}
    assert improved["C901"] == (10, 3)


def test_unchanged_file_passes(repo):
    write(repo, "pkg/mod.py", complex_source("g", 11))
    commit(repo)
    result = hygiene.gate([repo / "pkg" / "mod.py"], root=repo)
    assert result["regressions"] == []
    assert result["checked_files"] == ["pkg/mod.py"]


def test_new_file_with_complex_function_fails(repo):
    write(repo, "pkg/old.py", "X = 1\n")
    commit(repo)
    write(repo, "pkg/new.py", complex_source("g", 11))
    result = hygiene.gate([repo / "pkg" / "new.py"], root=repo)
    reg = regression(result, "C901")
    assert reg is not None and (reg["base"], reg["head"]) == (0, 2)


def test_moving_complex_function_between_changed_files_passes(repo):
    write(repo, "pkg/a.py", complex_source("g", 11))
    write(repo, "pkg/b.py", "X = 1\n")
    commit(repo)
    write(repo, "pkg/a.py", "X = 2\n")
    write(repo, "pkg/b.py", complex_source("g", 11))
    result = hygiene.gate([repo / "pkg" / "a.py", repo / "pkg" / "b.py"], root=repo)
    assert result["regressions"] == []


def test_new_copy_of_existing_function_is_a_duplicate(repo):
    write(repo, "pkg/a.py", SIMPLE)
    commit(repo)
    write(repo, "pkg/b.py", SIMPLE.replace("def f", "def copy_of_f"))
    result = hygiene.gate([repo / "pkg" / "b.py"], root=repo)
    reg = regression(result, "DUP")
    assert reg is not None and reg["head"] == 1
    finding = reg["findings"][0]
    assert (finding["path"], finding["other_path"]) == ("pkg/b.py", "pkg/a.py")
    assert (finding["name"], finding["other_name"]) == ("copy_of_f", "f")
    line = hygiene.format_result(result)[1]
    assert line == "  pkg/b.py:1 copy_of_f duplicates pkg/a.py:1 f"


def test_moved_function_is_not_a_duplicate(repo):
    write(repo, "pkg/a.py", SIMPLE)
    write(repo, "pkg/b.py", "X = 1\n")
    commit(repo)
    write(repo, "pkg/a.py", "X = 2\n")
    write(repo, "pkg/b.py", SIMPLE)
    result = hygiene.gate([repo / "pkg" / "a.py", repo / "pkg" / "b.py"], root=repo)
    assert regression(result, "DUP") is None


def test_function_below_statement_floor_is_not_indexed(repo):
    short = "def f(a):\n    b = a + 1\n    return b\n"
    write(repo, "pkg/a.py", short)
    commit(repo)
    write(repo, "pkg/b.py", short)
    assert hygiene.gate([repo / "pkg" / "b.py"], root=repo)["regressions"] == []


def test_dup_budget_exhausted_is_a_note_not_a_failure(repo):
    write(repo, "pkg/a.py", SIMPLE)
    commit(repo)
    write(repo, "pkg/b.py", SIMPLE)
    result = hygiene.gate([repo / "pkg" / "b.py"], root=repo, dup_budget_seconds=0)
    assert result["regressions"] == []
    assert "DUP not checked (budget)" in result["notes"]


def test_explicit_gate_fails_closed_when_duplicate_budget_is_incomplete(repo):
    write(repo, "pkg/a.py", SIMPLE)
    commit(repo)
    write(repo, "pkg/b.py", SIMPLE)
    assert hygiene.main(["gate", "--files", str(repo / "pkg" / "b.py"), "--dup-budget-seconds", "0"]) == 1


def test_dup_index_is_reused_when_files_are_unchanged(repo):
    write(repo, "pkg/a.py", SIMPLE)
    commit(repo)
    cold = hygiene.build_index(repo, [], 60)
    warm = hygiene.build_index(repo, [], 60)
    assert (cold["parsed"], warm["parsed"]) == (1, 0)
    assert warm["entries"] == cold["entries"]


def test_waiver_raises_the_allowance(repo):
    write(repo, "pkg/mod.py", complex_source("g", 11))
    commit(repo)
    write(repo, "pkg/mod.py", complex_source("g", 14))
    target = [repo / "pkg" / "mod.py"]
    assert regression(hygiene.gate(target, root=repo), "C901") is not None
    waivers = [{"path": "pkg/mod.py", "key": "C901", "allow": 3, "reason": "fixture"}]
    write(repo, "tools/code_hygiene_waivers.json", json.dumps(waivers))
    assert regression(hygiene.gate(target, root=repo), "C901") is None


def test_improvement_is_reported(repo):
    write(repo, "pkg/mod.py", complex_source("g", 14))
    commit(repo)
    write(repo, "pkg/mod.py", complex_source("g", 11))
    result = hygiene.gate([repo / "pkg" / "mod.py"], root=repo)
    assert result["regressions"] == []
    assert {"key": "C901", "base": 5, "head": 2, "root": str(repo)} in result["improved"]


def test_syntax_error_in_head_is_a_regression(repo):
    write(repo, "pkg/mod.py", "X = 1\n")
    commit(repo)
    write(repo, "pkg/mod.py", "def broken(:\n")
    reg = regression(hygiene.gate([repo / "pkg" / "mod.py"], root=repo), "unparseable")
    assert reg is not None and reg["head"] == 1


def test_test_files_and_non_python_files_are_ignored(repo):
    write(repo, "pkg/old.py", "X = 1\n")
    commit(repo)
    write(repo, "test/test_big.py", complex_source("g", 14))
    write(repo, "pkg/test_big.py", complex_source("g", 14))
    write(repo, "notes.txt", complex_source("g", 14))
    files = [repo / "test" / "test_big.py", repo / "pkg" / "test_big.py", repo / "notes.txt"]
    result = hygiene.gate(files, root=repo)
    assert result["checked_files"] == [] and result["regressions"] == []


def test_default_changed_set_covers_modified_files_only(repo):
    write(repo, "pkg/mod.py", "X = 1\n")
    write(repo, "pkg/other.py", "Y = 1\n")
    commit(repo)
    write(repo, "pkg/mod.py", complex_source("g", 11))
    write(repo, "pkg/fresh.py", "Z = 1\n")
    write(repo, "test/test_x.py", "A = 1\n")
    result = hygiene.gate(root=repo)
    assert result["checked_files"] == ["pkg/mod.py"]
    assert regression(result, "C901") is not None


def test_untracked_complex_file_is_ignored_by_default_and_caught_on_request(repo, capsys):
    write(repo, "pkg/old.py", "X = 1\n")
    commit(repo)
    write(repo, "scratch/big.py", complex_source("g", 11))
    assert hygiene.gate(root=repo)["regressions"] == []
    result = hygiene.gate(root=repo, include_untracked=True)
    assert regression(result, "C901") is not None
    assert hygiene.main(["gate", "--root", str(repo)]) == 0
    assert hygiene.main(["gate", "--root", str(repo), "--include-untracked"]) == 1
    capsys.readouterr()


def test_each_file_resolves_to_its_own_git_root(repo, tmp_path):
    write(repo, "pkg/mod.py", "X = 1\n")
    commit(repo)
    other = (tmp_path / "other").resolve()
    other.mkdir()
    git(other, "init", "-q")
    write(other, "lib/x.py", "X = 1\n")
    commit(other)
    write(other, "lib/x.py", complex_source("g", 11))
    result = hygiene.gate([repo / "pkg" / "mod.py", other / "lib" / "x.py"])
    assert sorted(result["checked_files"]) == ["lib/x.py", "pkg/mod.py"]
    assert [r["root"] for r in result["regressions"]] == [str(other)]


def test_explicit_base_overrides_head(repo):
    write(repo, "pkg/mod.py", complex_source("g", 11))
    commit(repo)
    git(repo, "tag", "first")
    write(repo, "pkg/mod.py", complex_source("g", 14))
    commit(repo)
    target = [repo / "pkg" / "mod.py"]
    assert hygiene.gate(target, root=repo)["regressions"] == []
    assert regression(hygiene.gate(target, root=repo, base="first"), "C901") is not None


def test_cli_exit_codes_and_ascii_output(repo, capsys):
    write(repo, "pkg/mod.py", complex_source("g", 11))
    commit(repo)
    target = str(repo / "pkg" / "mod.py")
    assert hygiene.main(["gate", "--files", target]) == 0
    assert capsys.readouterr().out.startswith("clean: 1 file(s) checked")
    write(repo, "pkg/mod.py", complex_source("g", 14))
    assert hygiene.main(["gate", "--files", target]) == 1
    out = capsys.readouterr().out
    assert out.startswith("C901 complexity weight 2 -> 5 (+3)")
    assert out.isascii()
    assert hygiene.main(["gate", "--root", str(repo.parent / "missing-dir"), "--base", "nope"]) == 2


def test_report_totals_match_a_hand_countable_fixture(repo, capsys):
    write(repo, "pkg/wide.py", "def f(a, b, c, d, e, g, h):\n    return a\n")
    write(repo, "pkg/imp.py", "import os\n")
    write(repo, "test/test_skip.py", "import os\n")
    commit(repo)
    rep = hygiene.report(root=repo, top=5)
    assert rep["files"] == 2
    assert rep["totals"]["PLR0913"] == {"count": 1, "weight": 2}  # 7 arguments, threshold 5
    assert rep["totals"]["F401"] == {"count": 1, "weight": 1}
    assert rep["dup_groups"] == 0
    assert [t["path"] for t in rep["top"]] == ["pkg/wide.py", "pkg/imp.py"]
    assert hygiene.main(["report", "--root", str(repo), "--top", "5"]) == 0
    out = capsys.readouterr().out
    assert "PLR0913 argument-count count 1 weight 2" in out and out.isascii()


def two_functions() -> str:
    return complex_source("g1", 11) + "\n\n" + complex_source("g2", 12)


def test_git_mv_of_a_complex_file_passes_and_names_the_old_path(repo, capsys):
    write(repo, "pkg/old.py", complex_source("g", 11))
    commit(repo)
    git(repo, "mv", "pkg/old.py", "pkg/new.py")
    result = hygiene.gate(root=repo)
    assert result["regressions"] == []
    assert result["checked_files"] == ["pkg/new.py"]
    assert result["notes"] == ["credited 1 deleted file(s) at base: pkg/old.py"]
    assert hygiene.main(["gate", "--root", str(repo)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("clean: 1 file(s) checked") and "pkg/old.py" in out


def test_rename_checked_through_explicit_files_passes(repo):
    write(repo, "pkg/old.py", complex_source("g", 11))
    commit(repo)
    git(repo, "mv", "pkg/old.py", "pkg/new.py")
    assert hygiene.gate([repo / "pkg" / "new.py"], root=repo)["regressions"] == []


def test_split_of_one_complex_file_into_two_passes(repo):
    write(repo, "pkg/foo.py", two_functions())
    commit(repo)
    (repo / "pkg" / "foo.py").unlink()
    write(repo, "pkg/foo/__init__.py", "")
    write(repo, "pkg/foo/a.py", complex_source("g1", 11))
    write(repo, "pkg/foo/b.py", complex_source("g2", 12))
    files = [repo / "pkg" / "foo" / n for n in ("__init__.py", "a.py", "b.py")]
    result = hygiene.gate(files, root=repo)
    assert result["regressions"] == []
    assert result["notes"] == ["credited 1 deleted file(s) at base: pkg/foo.py"]


def test_new_complex_file_with_nothing_deleted_still_fails(repo):
    write(repo, "pkg/old.py", "X = 1\n")
    commit(repo)
    write(repo, "pkg/new.py", complex_source("g", 11))
    result = hygiene.gate([repo / "pkg" / "new.py"], root=repo)
    assert regression(result, "C901") is not None and result["notes"] == []


def test_moving_a_complex_file_while_adding_another_still_fails(repo):
    write(repo, "pkg/old.py", complex_source("g", 11))
    commit(repo)
    git(repo, "mv", "pkg/old.py", "pkg/new.py")
    write(repo, "pkg/extra.py", complex_source("h", 11))
    result = hygiene.gate([repo / "pkg" / "new.py", repo / "pkg" / "extra.py"], root=repo)
    reg = regression(result, "C901")
    assert reg is not None and (reg["base"], reg["head"]) == (2, 4)


def test_no_credit_without_a_new_file(repo):
    write(repo, "pkg/mod.py", complex_source("g", 11))
    write(repo, "pkg/gone.py", complex_source("h", 11))
    commit(repo)
    (repo / "pkg" / "gone.py").unlink()
    write(repo, "pkg/mod.py", complex_source("g", 14))
    result = hygiene.gate(root=repo)
    assert regression(result, "C901") is not None
    assert result["notes"] == []


def test_function_moved_out_of_a_deleted_file_is_not_a_duplicate(repo):
    write(repo, "pkg/old.py", SIMPLE)
    commit(repo)
    git(repo, "mv", "pkg/old.py", "pkg/new.py")
    assert regression(hygiene.gate(root=repo), "DUP") is None
    git(repo, "commit", "-q", "-m", "moved")
    write(repo, "pkg/old2.py", SIMPLE)
    (repo / "pkg" / "new.py").unlink()
    result = hygiene.gate([repo / "pkg" / "old2.py"], root=repo)
    assert regression(result, "DUP") is None
