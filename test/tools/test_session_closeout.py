"""Tests for tools/session_closeout.py's --record CLI, incl. the 2026-07-31 --note gate.

Before 2026-07-31, only 'promoted' required --note; a bare
`--record <s> no-durable-delta` cost nothing to type and proved nothing was assessed.
Now every outcome requires a 20+ char note naming what was checked.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location("session_closeout", REPO / "tools" / "session_closeout.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_record(mod, monkeypatch, argv, reviews_path):
    monkeypatch.setattr(mod, "REVIEWS", reviews_path)
    monkeypatch.setattr("sys.argv", ["session_closeout.py", *argv])
    return mod.main()


def test_no_durable_delta_without_note_is_rejected(tmp_path, monkeypatch, capsys):
    sc = _load()
    reviews = tmp_path / "reviews.jsonl"
    rc = _run_record(sc, monkeypatch, ["--record", "abc123", "no-durable-delta"], reviews)
    assert rc == 1
    assert "--note required" in capsys.readouterr().out
    assert not reviews.exists()


def test_no_durable_delta_with_short_note_is_rejected(tmp_path, monkeypatch, capsys):
    sc = _load()
    reviews = tmp_path / "reviews.jsonl"
    rc = _run_record(sc, monkeypatch, ["--record", "abc123", "no-durable-delta", "--note", "nothing"], reviews)
    assert rc == 1
    assert not reviews.exists()


def test_no_durable_delta_with_real_note_is_recorded(tmp_path, monkeypatch, capsys):
    sc = _load()
    reviews = tmp_path / "reviews.jsonl"
    note = "checked for wiring gaps and repeat-question patterns, found none new this session"
    rc = _run_record(sc, monkeypatch, ["--record", "abc123", "no-durable-delta", "--note", note], reviews)
    assert rc == 0
    row = json.loads(reviews.read_text(encoding="utf-8").splitlines()[0])
    assert row == {"session": "abc123", "outcome": "no-durable-delta", "note": note, "ts": row["ts"]}


def test_promoted_without_note_is_rejected(tmp_path, monkeypatch, capsys):
    sc = _load()
    reviews = tmp_path / "reviews.jsonl"
    rc = _run_record(sc, monkeypatch, ["--record", "abc123", "promoted"], reviews)
    assert rc == 1
    assert not reviews.exists()


def test_unknown_outcome_rejected_before_note_check(tmp_path, monkeypatch, capsys):
    sc = _load()
    reviews = tmp_path / "reviews.jsonl"
    rc = _run_record(sc, monkeypatch, ["--record", "abc123", "bogus-outcome"], reviews)
    assert rc == 1
    assert "outcome must be one of" in capsys.readouterr().out
