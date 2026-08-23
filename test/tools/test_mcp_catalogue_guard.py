"""Tests for tools/hooks/guard_mcp_catalogue.py.

Two things need proving, and the second is the one that would actually bite:
  1. the guard fires on an unreviewed tool and stays silent on a reviewed one;
  2. the catalogue matches the REAL tool surface. A catalogue missing a live tool would
     block legitimate calls -- a guard that fails into "everything is a violation" is the
     shape feedback_empty_string_collapses_identity_guard records. The corpus test below
     cross-checks every mcp__dail-tracker__ call in the local transcripts against it.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TRANSCRIPTS = Path.home() / ".claude" / "projects" / "c--Users-pglyn-PycharmProjects-dail-extractor"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / "tools" / "hooks" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def guard():
    return _load("guard_mcp_catalogue")


def _run(guard, monkeypatch, payload: dict) -> int:
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    return guard.main()


# ── the guard must be able to fire ───────────────────────────────────────────


def test_blocks_an_unreviewed_dail_tracker_tool(guard, monkeypatch, capsys):
    rc = _run(guard, monkeypatch, {"tool_name": "mcp__dail-tracker__delete_everything"})
    assert rc == 2
    err = capsys.readouterr().err
    assert "Unreviewed MCP tool" in err and "mcp_reviewed_tools.json" in err


def test_block_message_refuses_to_infer_from_the_name(guard, monkeypatch, capsys):
    _run(guard, monkeypatch, {"tool_name": "mcp__dail-tracker__brand_new_tool"})
    assert "do not infer from the name" in capsys.readouterr().err


# ── and must stay silent on everything that is fine today ────────────────────


@pytest.mark.parametrize(
    "tool",
    [
        "mcp__dail-tracker__describe_dataset",
        "mcp__dail-tracker__search_project",
        "mcp__dail-tracker__view_deps",
        "mcp__dail-tracker__who_was_minister",
    ],
)
def test_allows_reviewed_tools(guard, monkeypatch, tool):
    assert _run(guard, monkeypatch, {"tool_name": tool}) == 0


@pytest.mark.parametrize(
    "tool",
    [
        "mcp__siting-private__siting_check",
        "mcp__next-devtools__browser_eval",
        "Read",
        "Bash",
    ],
)
def test_ignores_other_servers_and_native_tools(guard, monkeypatch, tool):
    """Only dail-tracker carries a wildcard grant; the rest already prompt per call."""
    assert _run(guard, monkeypatch, {"tool_name": tool}) == 0


# ── fail-open contract ───────────────────────────────────────────────────────


@pytest.mark.parametrize("payload", ["", "not json", "[]", "{}"])
def test_fails_open_on_broken_payload(guard, monkeypatch, payload):
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    assert guard.main() == 0


def test_unreadable_catalogue_fails_open_not_closed(guard, monkeypatch):
    """A broken catalogue must never sever access to the data layer."""
    monkeypatch.setattr(guard, "CATALOGUE", str(REPO / "no_such_catalogue.json"))
    assert guard.reviewed_tools() == set()
    assert _run(guard, monkeypatch, {"tool_name": "mcp__dail-tracker__anything"}) == 0


def test_escape_hatch_disables_the_guard(guard, monkeypatch):
    monkeypatch.setenv("DAIL_SKIP_MCP_CATALOGUE", "1")
    assert _run(guard, monkeypatch, {"tool_name": "mcp__dail-tracker__unreviewed"}) == 0


# ── the catalogue must match the real surface ────────────────────────────────


def test_catalogue_is_well_formed(guard):
    names = guard.reviewed_tools()
    assert len(names) >= 70, "catalogue looks truncated"
    assert not any(n.startswith("mcp__") for n in names), "store bare names, not prefixed"


@pytest.mark.skipif(not TRANSCRIPTS.exists(), reason="no local transcripts to cross-check")
def test_every_tool_actually_called_locally_is_in_the_catalogue(guard):
    """A tool used in anger but missing from the catalogue would be blocked wrongly."""
    known = guard.reviewed_tools()
    seen: set[str] = set()
    for fp in TRANSCRIPTS.glob("*.jsonl"):
        try:
            with fp.open(encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if "mcp__dail-tracker__" not in line:
                        continue
                    try:
                        o = json.loads(line)
                    except Exception:
                        continue
                    for b in (o.get("message") or {}).get("content") or []:
                        if isinstance(b, dict) and b.get("type") == "tool_use":
                            n = b.get("name") or ""
                            if n.startswith("mcp__dail-tracker__"):
                                seen.add(n[len("mcp__dail-tracker__") :])
        except Exception:
            continue
    missing = sorted(seen - known)
    assert not missing, f"tools called locally but absent from the catalogue: {missing}"
