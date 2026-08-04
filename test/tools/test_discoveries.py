from __future__ import annotations

from pathlib import Path

from tools.discoveries import default_memory_roots, load, resolve_memory


def test_default_memory_roots_put_shared_cards_before_personal_compatibility(tmp_path):
    repo = tmp_path / "repo"
    home = tmp_path / "home"

    roots = default_memory_roots(root=repo, home=home)

    assert roots[:2] == [
        repo / "memory",
        repo / "planning" / "product" / "claude" / "memory",
    ]


def test_resolve_memory_prefers_a_repo_resolvable_detail(tmp_path):
    tracked = tmp_path / "memory"
    tracked.mkdir()
    detail = tracked / "known-trap.md"
    detail.write_text("# Evidence\n", encoding="utf-8")
    assert resolve_memory("known-trap", [tracked]) == detail


def test_resolve_memory_omits_a_dead_workstation_only_pointer(tmp_path):
    assert resolve_memory("missing-detail", [tmp_path]) is None


def test_cli_testing_lesson_has_a_portable_detail_card():
    root = Path(__file__).resolve().parents[2]
    row = next(item for item in load() if item.get("id") == "cli-testing-profile-console")

    assert row["memory"] == "feedback_cli_testing_profile_and_console"
    assert resolve_memory(row["memory"], [root / "memory"]) == (
        root / "memory" / "feedback_cli_testing_profile_and_console.md"
    )
