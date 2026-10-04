"""Tests for tools/session_closeout.py's --record CLI, incl. the 2026-07-31 --note gate.

Before 2026-07-31, only 'promoted' required --note; a bare
`--record <s> no-durable-delta` cost nothing to type and proved nothing was assessed.
Now every outcome requires a 20+ char note naming what was checked.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location("session_closeout", REPO / "tools" / "session_closeout.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_module(filename: str, name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / "tools" / "hooks" / filename)
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


def test_pending_deduplicates_by_highest_turns_then_latest_record(tmp_path, monkeypatch):
    sc = _load()
    ledger = tmp_path / "ledger.jsonl"
    reviews = tmp_path / "reviews.jsonl"
    sid = "full-session-1234567890"
    ledger.write_text(
        "\n".join(
            [
                "[]",
                json.dumps({"session": sid, "turns": {"bad": 1}, "ts": "2026-08-01"}),
                json.dumps({"session": sid, "turns": 600, "ts": "2026-08-01", "prompt": "older"}),
                json.dumps({"session": sid, "turns": 700, "ts": "2026-08-02", "prompt": "lower"}),
                json.dumps({"session": sid, "turns": 700, "ts": "2026-08-03", "prompt": "latest"}),
                "not json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sc, "LEDGER", ledger)
    monkeypatch.setattr(sc, "REVIEWS", reviews)

    rows = sc.pending()

    assert len(rows) == 1
    assert rows[0]["turns"] == 700 and rows[0]["prompt"] == "latest"


def test_pending_legacy_id_requires_unique_full_id(tmp_path, monkeypatch):
    sc = _load()
    ledger = tmp_path / "ledger.jsonl"
    reviews = tmp_path / "reviews.jsonl"
    legacy = "123456789abc"
    full_a = legacy + "A" * 20
    full_b = legacy + "B" * 20
    valid = {"outcome": "no-durable-delta", "note": "checked durable lessons and repeat questions"}

    ledger.write_text(
        "\n".join(json.dumps({"session": sid, "turns": 600, "ts": "2026-08-01"}) for sid in (full_a, full_b)) + "\n",
        encoding="utf-8",
    )
    reviews.write_text(json.dumps({"session": legacy, **valid}) + "\n", encoding="utf-8")
    monkeypatch.setattr(sc, "LEDGER", ledger)
    monkeypatch.setattr(sc, "REVIEWS", reviews)
    assert {row["session"] for row in sc.pending()} == {full_a, full_b}

    ledger.write_text(json.dumps({"session": full_a, "turns": 600, "ts": "2026-08-01"}) + "\n", encoding="utf-8")
    assert sc.pending() == []


def test_record_is_idempotent_but_allows_a_new_lesson(tmp_path, monkeypatch, capsys):
    sc = _load()
    reviews = tmp_path / "reviews.jsonl"
    note = "checked durable lessons and repeat questions"

    assert _run_record(sc, monkeypatch, ["--record", "full-session", "promoted", "--note", note], reviews) == 0
    assert (
        _run_record(
            sc,
            monkeypatch,
            ["--record", "full-session", "promoted", "--note", "  checked   durable lessons and repeat questions  "],
            reviews,
        )
        == 0
    )
    assert "already recorded" in capsys.readouterr().out
    assert (
        _run_record(
            sc,
            monkeypatch,
            ["--record", "full-session", "already-captured", "--note", "checked a different substantive lesson"],
            reviews,
        )
        == 0
    )
    assert len(reviews.read_text(encoding="utf-8").splitlines()) == 2


def test_actual_ledger_writer_preserves_full_ids_with_shared_legacy_prefix(tmp_path, monkeypatch):
    ledger_hook = _load_module("session_token_ledger.py", "session_token_ledger_full_ids")
    sc = _load()
    ledger = tmp_path / "ledger.jsonl"
    monkeypatch.setattr(ledger_hook, "LEDGER", ledger)
    full_ids = ["123456789abc" + suffix * 20 for suffix in ("A", "B")]
    for index, session in enumerate(full_ids):
        transcript = tmp_path / f"transcript-{index}.jsonl"
        transcript.write_text(
            json.dumps({"type": "assistant", "message": {"usage": {"output_tokens": 1}, "content": []}}),
            encoding="utf-8",
        )
        monkeypatch.setattr(
            sys, "stdin", io.StringIO(json.dumps({"session_id": session, "transcript_path": str(transcript)}))
        )
        assert ledger_hook.main() == 0

    reviews = tmp_path / "reviews.jsonl"
    monkeypatch.setattr(sc, "LEDGER", ledger)
    monkeypatch.setattr(sc, "REVIEWS", reviews)
    monkeypatch.setattr(sc, "TURNS_MIN", 1)
    assert {row["session"] for row in sc.pending()} == set(full_ids)


def test_pending_ignores_invalid_timestamps_and_non_integral_turns(tmp_path, monkeypatch):
    sc = _load()
    ledger = tmp_path / "ledger.jsonl"
    reviews = tmp_path / "reviews.jsonl"
    rows = [
        {"session": "bad-date", "turns": 700, "ts": "not-a-date"},
        {"session": "float-turns", "turns": 700.5, "ts": "2026-08-01"},
        {"session": "valid-ledger", "turns": 700, "ts": "2026-08-01"},
    ]
    ledger.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(sc, "LEDGER", ledger)
    monkeypatch.setattr(sc, "REVIEWS", reviews)

    assert [row["session"] for row in sc.pending()] == ["valid-ledger"]


def test_ambiguous_legacy_ledger_is_not_closed_by_full_review(tmp_path, monkeypatch):
    sc = _load()
    legacy = "123456789abc"
    ledger = tmp_path / "ledger.jsonl"
    reviews = tmp_path / "reviews.jsonl"
    ledger.write_text(json.dumps({"session": legacy, "turns": 600, "ts": "2026-10-04"}) + "\n")
    reviews.write_text(
        "\n".join(
            json.dumps(
                {"session": legacy + suffix, "outcome": "promoted", "note": "saved distinct lessons with evidence"}
            )
            for suffix in ("full-session-a", "full-session-b")
        )
        + "\n"
    )
    monkeypatch.setattr(sc, "LEDGER", ledger)
    monkeypatch.setattr(sc, "REVIEWS", reviews)
    assert len(sc.pending()) == 1
