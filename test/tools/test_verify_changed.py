"""Tests for the changed-file verification planner, fingerprint, and receipts."""

from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path

import pytest

from tools import verify_changed as vc


def _keys(checks: tuple[vc.CheckSpec, ...]) -> list[str]:
    return [check.key for check in checks]


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def _git_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    _git(repo, "init")
    _git(repo, "config", "user.email", "verify@example.test")
    _git(repo, "config", "user.name", "Verify Tests")
    (repo / "tracked.txt").write_text("base\n", encoding="utf-8")
    (repo / "logs").mkdir()
    (repo / "logs" / "memory_pressure.jsonl").write_text('{"base":true}\n', encoding="utf-8")
    _git(repo, "add", "tracked.txt", "logs/memory_pressure.jsonl")
    _git(repo, "commit", "-m", "base")
    return repo, _git(repo, "rev-parse", "HEAD")


def test_normalize_path_and_change_set_union_are_stable() -> None:
    changes = vc.ChangeSet(
        committed=("b.py",),
        staged=("a.py", "b.py"),
        unstaged=("docs\\guide.md",),
        untracked=("./new.py",),
    )
    assert vc.normalize_path("./docs\\guide.md") == "docs/guide.md"
    assert changes.all_files == ("a.py", "b.py", "docs/guide.md", "new.py")


def test_append_only_logs_and_verification_cache_are_not_source_state() -> None:
    paths = [
        "source.py",
        "logs/memory_pressure.jsonl",
        "logs/style_lint_log.jsonl",
        "logs/grep_vs_index_trial.jsonl",
        "logs/session_token_ledger.jsonl",
        "logs/closeout_reviews.jsonl",
        ".cache/verify-changed/logs/run/check.log",
        ".cache/verify-changed/receipts/key.json",
    ]
    assert vc.filter_verification_paths(paths) == ("source.py",)
    changes = vc.ChangeSet(staged=tuple(paths))
    assert changes.all_files == ("source.py",)
    assert changes.as_dict()["staged"] == ("source.py",)


def test_extractor_change_gets_focused_lint_tests_and_ratchets() -> None:
    checks = vc.build_checks(["extractors/new_feed.py"], python_executable="python")
    assert _keys(checks) == [
        "ruff-check",
        "ruff-format",
        "conventions",
        "dependency-declarations",
        "pytest-focused",
    ]
    pytest_check = checks[-1]
    assert "test/extractors" in pytest_check.argv
    assert "pytest-fast" not in _keys(checks)


def test_ui_change_selects_firewall_conventions_and_utility_tests() -> None:
    checks = vc.build_checks(["utility/pages_code/member.py"], python_executable="python")
    keys = _keys(checks)
    assert "firewall" in keys
    assert "conventions" in keys
    assert checks[keys.index("firewall")].argv[-1] == "utility/pages_code/member.py"
    assert "test/utility" in checks[keys.index("pytest-focused")].argv


def test_sql_docs_and_mcp_each_select_their_special_gate() -> None:
    sql = vc.build_checks(["sql_views/member/member_votes.sql"], python_executable="python")
    assert _keys(sql) == ["pytest-sql"]
    assert dict(sql[0].env)["DAIL_INTEGRATION_TESTS"] == "1"

    docs = vc.build_checks(["doc/DATA_GRAINS.md"], python_executable="python")
    assert _keys(docs) == ["doc-index"]
    assert vc.build_checks(["README.MD"], python_executable="python") == ()

    mcp = vc.build_checks(["mcp_server/server.py"], python_executable="python")
    assert "mcp-catalog" in _keys(mcp)
    assert "test/mcp_server" in mcp[_keys(mcp).index("pytest-focused")].argv


def test_agent_guides_and_prompts_select_the_portable_context_contract() -> None:
    checks = vc.build_checks(
        [
            "AGENTS.md",
            "CLAUDE.md",
            ".gitignore",
            ".mcp.json",
            ".vscode/mcp.json",
            "CONTRIBUTING.md",
            ".codex/config.toml",
            ".codex/agents/reviewer.toml",
            ".github/prompts/build-page.prompt.md",
            "dail_tracker_bold_ui_contract_pack_v5/prompts/11_design_critique_after_implementation.prompt.md",
            "utility/pages_code/AGENTS.md",
            "extractors/AGENTS.md",
            "sql_views/AGENTS.md",
            "mcp_server/AGENTS.md",
            "dail_tracker_core/AGENTS.md",
        ],
        python_executable="python",
    )
    assert _keys(checks) == ["pytest-agent-context", "mcp-catalog"]
    assert checks[0].argv[-1] == "test/tools/test_agent_context.py"


def test_agent_context_checker_changes_select_focused_contract_tests() -> None:
    checks = vc.build_checks(
        [
            "tools/check_agent_context.py",
            "dail_tracker_bold_ui_contract_pack_v5/tools/check_prompt_context_budget.py",
        ],
        python_executable="python",
    )
    assert _keys(checks) == [
        "ruff-check",
        "ruff-format",
        "dependency-declarations",
        "pytest-agent-context",
        "pytest-focused",
    ]
    assert checks[-1].argv[-1] == "test/tools/test_agent_context.py"


def test_dependency_manifest_expands_to_global_checks_and_fast_tests() -> None:
    checks = vc.build_checks(["pyproject.toml"], python_executable="python")
    keys = _keys(checks)
    assert checks[0].argv[-1] == "."
    assert keys == [
        "ruff-check",
        "ruff-format",
        "dependency-declarations",
        "dependency-state",
        "typecheck",
        "expected-failures",
        "pytest-fast",
    ]


def test_unknown_python_and_deleted_test_fail_safe() -> None:
    unknown = vc.build_checks(["new_domain/widget.py"], python_executable="python")
    assert "pytest-fast" in _keys(unknown)
    assert "pytest-focused" not in _keys(unknown)

    deleted_test = vc.build_checks(
        ["test/services/test_removed.py"],
        existing_paths=[],
        python_executable="python",
    )
    assert "pytest-fast" in _keys(deleted_test)
    assert all("test_removed.py" not in check.argv for check in deleted_test)


def test_full_selects_every_deterministic_lane() -> None:
    checks = vc.build_checks([], python_executable="python", full=True)
    assert _keys(checks) == [
        "ruff-check",
        "ruff-format",
        "firewall",
        "conventions",
        "dependency-declarations",
        "dependency-state",
        "doc-index",
        "mcp-catalog",
        "agent-context",
        "typecheck",
        "expected-failures",
        "pytest-fast",
        "pytest-sql",
    ]


def test_cli_parser_exposes_plan_base_cache_and_full_modes() -> None:
    args = vc.parse_args(["--plan", "--base", "upstream/main", "--no-cache", "--full"])
    assert args.plan and args.no_cache and args.full
    assert args.base == "upstream/main"


def test_resolve_base_defaults_to_origin_main_and_falls_back_to_head(tmp_path: Path) -> None:
    with_origin, base = _git_repo(tmp_path / "with-origin")
    _git(with_origin, "update-ref", "refs/remotes/origin/main", base)
    (with_origin / "later.txt").write_text("later\n", encoding="utf-8")
    _git(with_origin, "add", "later.txt")
    _git(with_origin, "commit", "-m", "later")

    resolved = vc.resolve_base(with_origin)
    assert resolved.label == "origin/main"
    assert resolved.merge_base == base
    assert not resolved.used_fallback

    without_origin, head = _git_repo(tmp_path / "without-origin")
    fallback = vc.resolve_base(without_origin)
    assert fallback.label == "HEAD"
    assert fallback.merge_base == head
    assert fallback.used_fallback

    with pytest.raises(vc.GitError, match="does not resolve"):
        vc.resolve_base(without_origin, "missing/ref")


def test_discover_changes_includes_all_four_git_surfaces(tmp_path: Path) -> None:
    repo, base = _git_repo(tmp_path)
    _git(repo, "update-ref", "refs/remotes/origin/main", base)

    (repo / "committed.py").write_text("COMMITTED = True\n", encoding="utf-8")
    _git(repo, "add", "committed.py")
    _git(repo, "commit", "-m", "committed change")

    (repo / "staged.py").write_text("STAGED = True\n", encoding="utf-8")
    _git(repo, "add", "staged.py")
    (repo / "tracked.txt").write_text("unstaged\n", encoding="utf-8")
    (repo / "untracked.py").write_text("UNTRACKED = True\n", encoding="utf-8")
    (repo / "logs" / "memory_pressure.jsonl").write_text('{"turn":2}\n', encoding="utf-8")
    (repo / "logs" / "session_token_ledger.jsonl").write_text('{"tokens":10}\n', encoding="utf-8")

    changes = vc.discover_changes(repo, vc.resolve_base(repo).merge_base)
    assert changes.committed == ("committed.py",)
    assert changes.staged == ("staged.py",)
    assert changes.unstaged == ("tracked.txt",)
    assert changes.untracked == ("untracked.py",)
    assert changes.all_files == ("committed.py", "staged.py", "tracked.txt", "untracked.py")


def test_collect_file_states_records_contents_and_deleted_files(tmp_path: Path) -> None:
    (tmp_path / "present.py").write_text("x = 1\n", encoding="utf-8")
    present, missing = vc.collect_file_states(tmp_path, ["present.py", "gone.py"])
    assert present.path == "gone.py" and not present.exists  # alphabetical order
    assert missing.path == "present.py" and missing.exists
    assert missing.size == 7 and len(missing.sha256) == 64


def test_fingerprint_covers_state_commands_runtime_and_policy() -> None:
    changes = vc.ChangeSet(unstaged=("x.py",))
    state = (vc.FileState("x.py", True, 3, "aaa", 0o644),)
    checks = (vc.CheckSpec("ruff", ("python", "-m", "ruff"), "lint"),)

    def fingerprint(**overrides: object) -> str:
        values: dict[str, object] = {
            "head": "head",
            "base_commit": "base",
            "changes": changes,
            "file_states": state,
            "checks": checks,
            "interpreter": "python",
            "python_version": "3.12",
            "platform_name": "test-os",
            "policy_identity": "policy-a",
            "full": False,
        }
        values.update(overrides)
        return vc.compute_fingerprint(**values)  # type: ignore[arg-type]

    baseline = fingerprint()
    assert fingerprint(file_states=(vc.FileState("x.py", True, 4, "bbb", 0o644),)) != baseline
    assert fingerprint(checks=(vc.CheckSpec("pytest", ("python", "-m", "pytest"), "tests"),)) != baseline
    assert fingerprint(interpreter="other-python") != baseline
    assert fingerprint(platform_name="other-os") != baseline
    assert fingerprint(policy_identity="policy-b") != baseline
    assert fingerprint(full=True) != baseline
    two_files = (*state, vc.FileState("y.py", True, 2, "yyy", 0o644))
    assert fingerprint(file_states=two_files) == fingerprint(file_states=tuple(reversed(two_files)))
    telemetry = vc.FileState("logs/memory_pressure.jsonl", True, 10, "telemetry", 0o644)
    assert (
        fingerprint(
            changes=vc.ChangeSet(unstaged=("x.py", "logs/memory_pressure.jsonl")),
            file_states=(*state, telemetry),
        )
        == baseline
    )


def test_receipt_matching_rejects_non_success_or_changed_commands() -> None:
    checks = (vc.CheckSpec("one", ("python", "one.py"), "reason"),)
    receipt = {
        "status": "success",
        "fingerprint": "abc",
        "policy_version": vc.POLICY_VERSION,
        "checks": [checks[0].cache_payload()],
    }
    assert vc.receipt_matches(receipt, "abc", checks)
    assert not vc.receipt_matches({**receipt, "status": "failed"}, "abc", checks)
    assert not vc.receipt_matches(receipt, "different", checks)
    changed = (vc.CheckSpec("two", ("python", "two.py"), "reason"),)
    assert not vc.receipt_matches(receipt, "abc", changed)


def test_execute_checks_writes_raw_logs_and_success_receipt(tmp_path: Path) -> None:
    checks = (vc.CheckSpec("demo", ("python", "demo.py"), "test"),)
    receipt = tmp_path / ".cache" / "verify-changed" / "receipts" / "ok.json"

    def success(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, "all good\n", "small warning\n")

    ok, results = vc.execute_checks(
        repo=tmp_path,
        checks=checks,
        fingerprint="abc123",
        cache_root=tmp_path / ".cache" / "verify-changed",
        receipt_path=receipt,
        runner=success,
    )
    assert ok and len(results) == 1
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    assert vc.receipt_matches(payload, "abc123", checks)
    raw = (tmp_path / results[0].log_path).read_text(encoding="utf-8")
    assert "all good" in raw and "small warning" in raw and "exit=0" in raw


def test_execute_checks_never_leaves_receipt_after_failure(tmp_path: Path) -> None:
    checks = (
        vc.CheckSpec("bad", ("python", "bad.py"), "test"),
        vc.CheckSpec("never", ("python", "never.py"), "test"),
    )
    receipt = tmp_path / ".cache" / "verify-changed" / "receipts" / "bad.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text('{"status":"success"}', encoding="utf-8")
    calls: list[list[str]] = []

    def failure(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 1, "", "broken\n")

    ok, results = vc.execute_checks(
        repo=tmp_path,
        checks=checks,
        fingerprint="failed-state",
        cache_root=tmp_path / ".cache" / "verify-changed",
        receipt_path=receipt,
        runner=failure,
    )
    assert not ok
    assert not receipt.exists()
    assert len(results) == 1 and len(calls) == 1  # fail fast; no false receipt
    assert "broken" in (tmp_path / results[0].log_path).read_text(encoding="utf-8")


def test_execute_checks_escapes_unencodable_child_output_on_a_legacy_console(tmp_path: Path, monkeypatch) -> None:
    output = io.BytesIO()
    stream = io.TextIOWrapper(output, encoding="cp1252", errors="strict")
    monkeypatch.setattr(vc.sys, "stdout", stream)
    vc._configure_console_output()

    checks = (vc.CheckSpec("demo", ("python", "demo.py"), "test"),)
    receipt = tmp_path / ".cache" / "verify-changed" / "receipts" / "ok.json"

    def success(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, f"child {chr(0x2192)} output\n", "")

    ok, _ = vc.execute_checks(
        repo=tmp_path,
        checks=checks,
        fingerprint="legacy-console",
        cache_root=tmp_path / ".cache" / "verify-changed",
        receipt_path=receipt,
        runner=success,
    )

    stream.flush()
    assert ok
    assert "child \\u2192 output" in output.getvalue().decode("cp1252")


def test_compact_output_keeps_head_tail_and_bounds_size() -> None:
    text = "\n".join(f"line-{index}-" + "x" * 30 for index in range(50))
    compact = vc.compact_output(text, max_lines=8, max_chars=300)
    assert "line-0" in compact and "line-49" in compact
    assert "truncated" in compact
    assert len(compact) <= 330  # truncation marker adds a small fixed overhead
