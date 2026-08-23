"""Tests for tools/hooks/verification_gate.py.

Loaded via importlib with stdin monkeypatched, matching test_efficiency_hooks.py, so the
exit-code contract (0 = allow, 2 = block with reason on stderr) is pinned without spawning
subprocesses.

The block cases come first and are the point of the file: per
feedback_prove_a_gate_can_fail_before_committing, a guard whose failure path was never
demonstrated is not evidence of anything. All three verdicts -- NO_RUN, FAILED, STALE --
are proven to fire, and the two parser bugs the 509-transcript replay exposed carry named
regression pins ("Found 0 errors" must read as a pass; a re-run after an edit must clear
STALE even when its output is unparseable).
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / "tools" / "hooks" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def vg():
    return _load("verification_gate")


# ── transcript builders ──────────────────────────────────────────────────────


def _run_cmd(tid: str, cmd: str) -> dict:
    return {
        "type": "assistant",
        "message": {"content": [{"type": "tool_use", "id": tid, "name": "Bash", "input": {"command": cmd}}]},
    }


def _run_result(tid: str, out: str, is_error: bool = False) -> dict:
    block = {"type": "tool_result", "tool_use_id": tid, "content": out}
    if is_error:
        block["is_error"] = True
    return {"type": "user", "message": {"content": [block]}}


def _edit(path: str) -> dict:
    return {
        "type": "assistant",
        "message": {
            "content": [{"type": "tool_use", "id": uuid.uuid4().hex, "name": "Edit", "input": {"file_path": path}}]
        },
    }


def _transcript(tmp_path: Path, *rows: dict) -> str:
    fp = tmp_path / f"t_{uuid.uuid4().hex}.jsonl"
    fp.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    return str(fp)


def _invoke(vg, monkeypatch, message: str, transcript: str) -> int:
    payload = {
        "last_assistant_message": message,
        "transcript_path": transcript,
        "session_id": uuid.uuid4().hex,  # unique so the once-per-turn marker never collides
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    return vg.main()


# ── the gate must be able to FAIL: all three verdicts ────────────────────────


def test_blocks_when_no_run_exists(vg, tmp_path, monkeypatch, capsys):
    t = _transcript(tmp_path, _edit("engine.py"))
    assert _invoke(vg, monkeypatch, "All 45 tests pass.", t) == 2
    assert "NO_RUN" in capsys.readouterr().err


def test_blocks_when_every_run_failed(vg, tmp_path, monkeypatch, capsys):
    t = _transcript(tmp_path, _run_cmd("t1", "pytest test/"), _run_result("t1", "3 failed, 2 passed"))
    assert _invoke(vg, monkeypatch, "My three new tests pass.", t) == 2
    assert "FAILED" in capsys.readouterr().err


def test_blocks_when_source_edited_after_last_run(vg, tmp_path, monkeypatch, capsys):
    t = _transcript(
        tmp_path,
        _run_cmd("t1", "pytest test/"),
        _run_result("t1", "45 passed"),
        _edit("dail_tracker_core/engine.py"),
    )
    assert _invoke(vg, monkeypatch, "All 45 tests pass.", t) == 2
    err = capsys.readouterr().err
    assert "STALE" in err and "engine.py" in err


def test_block_message_offers_the_downband_route(vg, tmp_path, monkeypatch, capsys):
    t = _transcript(tmp_path, _edit("engine.py"))
    _invoke(vg, monkeypatch, "All 45 tests pass.", t)
    err = capsys.readouterr().err
    assert "[Reported" in err and "[Indicative" in err


# ── and must stay quiet when the evidence is there ───────────────────────────


def test_allows_when_run_passed_and_is_current(vg, tmp_path, monkeypatch):
    t = _transcript(tmp_path, _run_cmd("t1", "pytest test/"), _run_result("t1", "45 passed"))
    assert _invoke(vg, monkeypatch, "All 45 tests pass.", t) == 0


def test_allows_when_reply_makes_no_run_claim(vg, tmp_path, monkeypatch):
    t = _transcript(tmp_path, _edit("engine.py"))
    assert _invoke(vg, monkeypatch, "I refactored the loader and moved the guard.", t) == 0


def test_markdown_edit_does_not_make_a_green_run_stale(vg, tmp_path, monkeypatch):
    t = _transcript(tmp_path, _run_cmd("t1", "pytest test/"), _run_result("t1", "45 passed"), _edit("doc/NOTES.md"))
    assert _invoke(vg, monkeypatch, "All 45 tests pass.", t) == 0


# ── regression pins for the two bugs the 509-transcript replay exposed ───────


def test_zero_errors_reads_as_a_pass_not_a_failure(vg, tmp_path, monkeypatch):
    """v1's bare `\\d+\\s+errors?` matched mypy's SUCCESS line and scored clean runs red."""
    assert vg.run_failed("Found 0 errors in 5 source files") is False
    t = _transcript(tmp_path, _run_cmd("t1", "mypy ."), _run_result("t1", "Found 0 errors in 5 source files"))
    assert _invoke(vg, monkeypatch, "Typecheck is clean.", t) == 0


def test_nonzero_errors_still_reads_as_a_failure(vg):
    assert vg.run_failed("Found 3 errors in 2 source files") is True
    assert vg.run_failed("2 failed, 2586 passed") is True
    assert vg.run_failed("0 failed, 12 passed") is False


def test_rerun_after_edit_clears_stale_even_if_output_unparseable(vg, tmp_path, monkeypatch):
    """Claude Code truncates long tool results; an unread summary must not score as red."""
    t = _transcript(
        tmp_path,
        _run_cmd("t1", "pytest test/"),
        _run_result("t1", "45 passed"),
        _edit("engine.py"),
        _run_cmd("t2", "pytest test/"),
        _run_result("t2", "...output truncated..."),
    )
    assert _invoke(vg, monkeypatch, "All 45 tests pass.", t) == 0


# ── claim detection: the measured false-positive classes stay out ────────────


def test_verified_band_tag_does_not_discharge_the_claim(vg, tmp_path, monkeypatch):
    """The design call: `Verified` means reproduced THIS SESSION, so citing is not proof."""
    t = _transcript(tmp_path, _edit("engine.py"))
    assert _invoke(vg, monkeypatch, "All 45 tests pass [Verified - pytest run this session].", t) == 2


@pytest.mark.parametrize(
    "message",
    [
        "The tests pass [Reported - builder's pasted output].",
        "All 45 tests pass [Indicative - not run this session].",
        "The suite is green [Extracted - from the CI log].",
    ],
)
def test_honest_downband_discharges_the_claim(vg, tmp_path, monkeypatch, message):
    t = _transcript(tmp_path, _edit("engine.py"))
    assert _invoke(vg, monkeypatch, message, t) == 0


@pytest.mark.parametrize(
    "message",
    [
        "Now let's run the full GIS suite again to confirm the new tests pass.",
        "Let me run the suite; the tests pass only after the loader fix.",
        "If the tests pass we can ship.",
        "My traceability tests passed because I exempted the node from the sweep.",
        "My stash test passed by luck on a single run.",
        "A hand-fed fake store can make a test pass while the field names never match.",
    ],
)
def test_discussion_and_hypotheticals_are_not_claims(vg, message):
    assert vg.find_claim(message) == ""


@pytest.mark.parametrize(
    "message",
    ["All 45 tests pass.", "The suite is green.", "Typecheck is clean.", "46/46 tests pass."],
)
def test_real_claims_are_detected(vg, message):
    assert vg.find_claim(message) != ""


# ── harness contract: fail open, never nag ───────────────────────────────────


def test_blocks_at_most_once_per_turn(vg, tmp_path, monkeypatch, capsys):
    t = _transcript(tmp_path, _edit("engine.py"))
    sid = uuid.uuid4().hex
    payload = {"last_assistant_message": "All 45 tests pass.", "transcript_path": t, "session_id": sid}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert vg.main() == 2
    capsys.readouterr()
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert vg.main() == 0  # marker set -> silent on the forced continuation


def test_stop_hook_active_is_silent(vg, tmp_path, monkeypatch):
    t = _transcript(tmp_path, _edit("engine.py"))
    payload = {
        "last_assistant_message": "All 45 tests pass.",
        "transcript_path": t,
        "session_id": uuid.uuid4().hex,
        "stop_hook_active": True,
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert vg.main() == 0


def test_escape_hatch_disables_the_gate(vg, tmp_path, monkeypatch):
    monkeypatch.setenv("DAIL_SKIP_VERIFICATION_GATE", "1")
    t = _transcript(tmp_path, _edit("engine.py"))
    assert _invoke(vg, monkeypatch, "All 45 tests pass.", t) == 0


@pytest.mark.parametrize("payload", ["", "not json", "[]", "{}", '{"last_assistant_message": null}'])
def test_fails_open_on_broken_payload(vg, monkeypatch, payload):
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    assert vg.main() == 0


def test_missing_transcript_fails_open(vg, monkeypatch):
    payload = {
        "last_assistant_message": "All 45 tests pass.",
        "transcript_path": str(REPO / "does_not_exist.jsonl"),
        "session_id": uuid.uuid4().hex,
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    assert vg.main() == 0
