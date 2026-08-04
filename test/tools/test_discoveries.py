from __future__ import annotations

from tools.discoveries import resolve_memory


def test_resolve_memory_prefers_a_repo_resolvable_detail(tmp_path):
    tracked = tmp_path / "memory"
    tracked.mkdir()
    detail = tracked / "known-trap.md"
    detail.write_text("# Evidence\n", encoding="utf-8")
    assert resolve_memory("known-trap", [tracked]) == detail


def test_resolve_memory_omits_a_dead_workstation_only_pointer(tmp_path):
    assert resolve_memory("missing-detail", [tmp_path]) is None
