"""Tests for tools/hooks/closeout_gate.py (Stop hook), shipped 2026-07-31.

Escalates the advisory session-closeout nudges (session_context.py's pending count,
context_tripwire.py's 400k-token note) into a one-time block: pin the exit-code
contract (0 = allow, 2 = block), the once-per-session firing, and the
already-recorded short-circuit. Same importlib/monkeypatch harness as
test_efficiency_hooks.py.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / "tools" / "hooks" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run(mod, payload: dict, monkeypatch) -> int:
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    return mod.main()


def test_silent_below_turns_min(monkeypatch, capsys):
    cg = _load("closeout_gate")
    sid = uuid.uuid4().hex
    for _ in range(cg.TURNS_MIN - 1):
        assert _run(cg, {"session_id": sid}, monkeypatch) == 0
    assert capsys.readouterr().err == ""


def test_blocks_once_at_turns_min_then_stays_silent(tmp_path, monkeypatch, capsys):
    cg = _load("closeout_gate")
    monkeypatch.setattr(cg, "REVIEWS", tmp_path / "closeout_reviews.jsonl")
    sid = uuid.uuid4().hex

    for _ in range(cg.TURNS_MIN - 1):
        assert _run(cg, {"session_id": sid}, monkeypatch) == 0

    rc = _run(cg, {"session_id": sid}, monkeypatch)
    err = capsys.readouterr().err
    assert rc == 2
    assert "closeout" in err.lower()
    assert sid in err
    assert "Stop invocations" in err
    assert "1/38" not in err
    assert len(err) <= 1000

    # ignoring it and trying to stop again does not re-block
    assert _run(cg, {"session_id": sid}, monkeypatch) == 0
    assert capsys.readouterr().err == ""


def test_stop_hook_active_never_blocks_or_counts(monkeypatch, capsys):
    cg = _load("closeout_gate")
    sid = uuid.uuid4().hex
    for _ in range(cg.TURNS_MIN + 5):
        assert _run(cg, {"session_id": sid, "stop_hook_active": True}, monkeypatch) == 0
    assert capsys.readouterr().err == ""


def test_already_recorded_session_never_blocks(tmp_path, monkeypatch, capsys):
    cg = _load("closeout_gate")
    reviews = tmp_path / "closeout_reviews.jsonl"
    sid = uuid.uuid4().hex
    reviews.write_text(
        json.dumps(
            {
                "session": sid[:12],
                "outcome": "no-durable-delta",
                "note": "checked the session for durable deltas and repeat questions",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cg, "REVIEWS", reviews)

    for _ in range(cg.TURNS_MIN + 5):
        assert _run(cg, {"session_id": sid}, monkeypatch) == 0
    assert capsys.readouterr().err == ""


def test_no_session_id_is_noop(monkeypatch, capsys):
    cg = _load("closeout_gate")
    assert _run(cg, {}, monkeypatch) == 0
    assert capsys.readouterr().err == ""


def test_fails_open_on_garbage(monkeypatch, capsys):
    cg = _load("closeout_gate")
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json{{"))
    assert cg.main() == 0
    assert capsys.readouterr().err == ""


def test_gated_session_does_not_write_counter_again(tmp_path, monkeypatch, capsys):
    cg = _load("closeout_gate")
    sid = uuid.uuid4().hex
    counter = tmp_path / "counter"
    gated = tmp_path / "gated"
    counter.write_text("500", encoding="utf-8")
    gated.write_text("1", encoding="utf-8")
    monkeypatch.setattr(cg, "_counter_path", lambda _sid: str(counter))
    monkeypatch.setattr(cg, "_gated_path", lambda _sid: str(gated))

    assert _run(cg, {"session_id": sid}, monkeypatch) == 0
    assert counter.read_text(encoding="utf-8") == "500"
    assert capsys.readouterr().err == ""


def test_gate_markers_use_full_session_and_repository_identity(tmp_path, monkeypatch):
    cg = _load("closeout_gate")
    prefix = "a" * 64
    sid_a = prefix + "A"
    sid_b = prefix + "B"
    assert cg._counter_path(sid_a) != cg._counter_path(sid_b)

    monkeypatch.setattr(cg, "REPO", str(tmp_path / "repo-a"))
    path_a = cg._counter_path("same-session")
    monkeypatch.setattr(cg, "REPO", str(tmp_path / "repo-b"))
    path_b = cg._counter_path("same-session")
    assert path_a != path_b
