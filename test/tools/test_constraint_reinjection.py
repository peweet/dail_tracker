"""Contract tests for the constraint-reinjection UserPromptSubmit hook.

Exercises the real stdin/stdout contract via subprocess, matching test_style_lint.py's
approach -- that is what Claude Code actually consumes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

HOOK = Path(__file__).resolve().parents[2] / "tools" / "hooks" / "constraint_reinjection.py"


def _write_transcript(tmp_path: Path, context_tokens: int) -> Path:
    path = tmp_path / "transcript.jsonl"
    row = {
        "type": "assistant",
        "message": {"usage": {"cache_read_input_tokens": context_tokens, "cache_creation_input_tokens": 0, "input_tokens": 0}},
    }
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    return path


def run(transcript_path: Path, session_id: str | None = None) -> subprocess.CompletedProcess:
    payload = {"transcript_path": str(transcript_path), "session_id": session_id or str(uuid.uuid4())}
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=30,
    )


def test_silent_below_first_threshold(tmp_path):
    transcript = _write_transcript(tmp_path, 10_000)
    r = run(transcript)
    assert r.returncode == 0
    assert r.stdout.strip() == ""


def test_fires_once_past_first_threshold(tmp_path):
    transcript = _write_transcript(tmp_path, 45_000)
    sid = "reinject" + uuid.uuid4().hex
    r = run(transcript, session_id=sid)
    assert r.returncode == 0
    out = json.loads(r.stdout)
    assert "constraint-refresh" in out["hookSpecificOutput"]["additionalContext"]


def test_does_not_refire_within_the_same_band(tmp_path):
    transcript = _write_transcript(tmp_path, 45_000)
    sid = "sameband" + uuid.uuid4().hex
    first = run(transcript, session_id=sid)
    second = run(transcript, session_id=sid)
    assert first.stdout.strip() != ""
    assert second.stdout.strip() == "", "must not repeat inside the same repeat-band"


def test_fires_again_in_a_later_band(tmp_path):
    sid = "laterband" + uuid.uuid4().hex
    first = run(_write_transcript(tmp_path, 45_000), session_id=sid)  # band 1
    second = run(_write_transcript(tmp_path, 130_000), session_id=sid)  # band 2
    assert first.stdout.strip() != ""
    assert second.stdout.strip() != "", "a later repeat-band must fire again"


def test_missing_transcript_is_silent():
    r = run(Path("does/not/exist.jsonl"))
    assert r.returncode == 0
    assert r.stdout.strip() == ""


def test_fails_open_on_bad_input():
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    r = subprocess.run(
        [sys.executable, str(HOOK)],
        input="not json",
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=30,
    )
    assert r.returncode == 0
