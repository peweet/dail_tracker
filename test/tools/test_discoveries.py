from __future__ import annotations

import hashlib
import json
from pathlib import Path

from tools import discoveries
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


def test_mcp_session_pileup_lesson_has_a_portable_detail_card():
    root = Path(__file__).resolve().parents[2]
    row = next(item for item in load() if item.get("id") == "windows-mcp-session-pileup")

    assert row["memory"] == "windows_mcp_session_pileup_2026_08_05"
    assert resolve_memory(row["memory"], [root / "memory"]) == (
        root / "memory" / "windows_mcp_session_pileup_2026_08_05.md"
    )


def test_cross_session_sidecar_lesson_has_a_portable_detail_card():
    root = Path(__file__).resolve().parents[2]
    row = next(item for item in load() if item.get("id") == "codex-cross-session-sidecar-handoff")

    assert row["memory"] == "project_codex_sidecar_handoff_2026_08_23"
    assert {"massive session", "add workers", "parralelize", "share findings"} <= set(row["trigger"])
    assert resolve_memory(row["memory"], [root / "memory"]) == (
        root / "memory" / "project_codex_sidecar_handoff_2026_08_23.md"
    )


def test_cli_recall_excludes_private_and_superseded_findings(tmp_path, monkeypatch, capsys):
    rows = [
        {"id": "private", "scope": "private", "discovery": "PRIVATE FINDING", "trigger": ["recall policy"]},
        {"id": "old", "superseded_by": "new", "discovery": "OBSOLETE FINDING", "trigger": ["recall policy"]},
        {"id": "new", "scope": "public", "discovery": "Current lead", "trigger": ["recall policy"]},
    ]
    path = tmp_path / "discoveries.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    monkeypatch.setattr(discoveries, "DATA", path)

    discoveries.main(["recall policy"])

    output = capsys.readouterr().out
    assert "Current lead" in output
    assert "PRIVATE FINDING" not in output
    assert "OBSOLETE FINDING" not in output
    assert "unverified" in output


def test_cli_recall_replaces_changed_evidence_with_a_recheck_lead(tmp_path, monkeypatch, capsys):
    source = tmp_path / "evidence.md"
    source.write_text("Current repository boundary\n", encoding="utf-8")
    row = {
        "id": "boundary",
        "scope": "public",
        "revision": 1,
        "discovery": "CACHED BOUNDARY CLAIM",
        "trigger": ["boundary"],
        "verified_at": "2026-10-18T00:00:00Z",
        "source_refs": [{"path": "evidence.md", "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}],
    }
    path = tmp_path / "discoveries.jsonl"
    path.write_text(json.dumps(row), encoding="utf-8")
    monkeypatch.setattr(discoveries, "DATA", path)
    monkeypatch.setattr(discoveries, "ROOT", tmp_path)

    discoveries.main(["boundary"])
    first = capsys.readouterr().out
    assert "source-matched" in first and "CACHED BOUNDARY CLAIM" in first

    source.write_text("Changed repository boundary\n", encoding="utf-8")
    discoveries.main(["boundary"])
    second = capsys.readouterr().out
    assert "recheck" in second and "evidence.md" in second
    assert "CACHED BOUNDARY CLAIM" not in second


def test_resolve_memory_rejects_parent_traversal(tmp_path):
    source = tmp_path / "outside.md"
    source.write_text("outside", encoding="utf-8")
    cards = tmp_path / "cards"
    cards.mkdir()
    assert resolve_memory("../outside", [cards]) is None


def test_cli_detail_resolution_requires_explicit_private_scope(tmp_path, monkeypatch, capsys):
    private = tmp_path / "planning/product/claude/memory/private-detail.md"
    private.parent.mkdir(parents=True)
    private.write_text("private evidence", encoding="utf-8")
    path = tmp_path / "discoveries.jsonl"
    path.write_text(
        json.dumps({"id": "legacy", "discovery": "Public lead", "trigger": ["recall"], "memory": "private-detail"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(discoveries, "ROOT", tmp_path)
    monkeypatch.setattr(discoveries, "DATA", path)
    monkeypatch.setattr(discoveries, "default_memory_roots", lambda: [tmp_path / "memory", private.parent])

    discoveries.main(["recall"])
    assert "private-detail" not in capsys.readouterr().out
    discoveries.main(["--scope", "private", "recall"])
    assert "private-detail.md" in capsys.readouterr().out


def test_cli_public_finding_keeps_public_detail_scope_in_private_context(tmp_path, monkeypatch, capsys):
    private = tmp_path / "planning/product/claude/memory/same.md"
    private.parent.mkdir(parents=True)
    private.write_text("private evidence", encoding="utf-8")
    path = tmp_path / "discoveries.jsonl"
    path.write_text(
        json.dumps(
            {"id": "public-row", "scope": "public", "discovery": "Public lead", "trigger": ["recall"], "memory": "same"}
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(discoveries, "ROOT", tmp_path)
    monkeypatch.setattr(discoveries, "DATA", path)
    discoveries.main(["--scope", "private", "recall"])
    assert "same.md" not in capsys.readouterr().out


def test_cli_private_finding_resolves_private_card_when_public_slug_collides(tmp_path, monkeypatch, capsys):
    public = tmp_path / "memory/same.md"
    private = tmp_path / "planning/product/claude/memory/same.md"
    public.parent.mkdir(parents=True)
    private.parent.mkdir(parents=True)
    public.write_text("public evidence", encoding="utf-8")
    private.write_text("private evidence", encoding="utf-8")
    path = tmp_path / "discoveries.jsonl"
    path.write_text(
        json.dumps(
            {
                "id": "private-row",
                "scope": "private",
                "discovery": "Private lead",
                "trigger": ["recall"],
                "memory": "same",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(discoveries, "ROOT", tmp_path)
    monkeypatch.setattr(discoveries, "DATA", path)
    discoveries.main(["--scope", "private", "recall"])
    assert "planning\\product\\claude\\memory\\same.md" in capsys.readouterr().out
