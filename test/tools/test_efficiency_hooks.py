"""Tests for the context-efficiency hooks (tools/hooks/) shipped 2026-07-23.

The hooks are plain scripts, not a package — loaded via importlib with stdin/stdout
monkeypatched, so the tests pin the exact exit-code contract the harness relies on
(0 = allow/silent, 2 = block with reason on stderr) without spawning subprocesses.
Covered: flood_warn (threshold, once-per-session rate limit), guard_data_reads
(64 KB whole-read block vs bounded allow), session_token_ledger (row written from a
synthetic transcript). All hooks must FAIL OPEN — a broken payload never raises.
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


# ── flood_warn ───────────────────────────────────────────────────────────────


def test_flood_warn_quiet_result_is_silent(monkeypatch, capsys):
    fw = _load("flood_warn")
    rc = _run(fw, {"tool_name": "Bash", "session_id": uuid.uuid4().hex, "tool_response": {"stdout": "ok"}}, monkeypatch)
    assert rc == 0 and capsys.readouterr().out == ""


def test_flood_warn_fires_once_then_rate_limits(monkeypatch, capsys):
    fw = _load("flood_warn")
    sid = uuid.uuid4().hex  # unique per test run so the temp-dir marker never collides
    fat = {"tool_name": "Grep", "session_id": sid, "tool_response": {"stdout": "x" * 9_000}}

    assert _run(fw, fat, monkeypatch) == 0
    out = json.loads(capsys.readouterr().out)
    ctx = out["hookSpecificOutput"]["additionalContext"]
    assert "context-efficiency" in ctx and "head_limit" in ctx

    # same session + tool again → marker exists → silent
    assert _run(fw, fat, monkeypatch) == 0
    assert capsys.readouterr().out == ""


def test_flood_warn_fails_open_on_garbage(monkeypatch, capsys):
    fw = _load("flood_warn")
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json{{"))
    assert fw.main() == 0


# ── guard_data_reads (64 KB whole-file threshold) ────────────────────────────


def test_guard_blocks_unbounded_read_over_64k(tmp_path, monkeypatch, capsys):
    g = _load("guard_data_reads")
    big = tmp_path / "big_module.py"
    big.write_bytes(b"# filler\n" * 8_000)  # ~72 KB > LARGE_FILE_BYTES
    assert big.stat().st_size > g.LARGE_FILE_BYTES

    rc = _run(g, {"tool_input": {"file_path": str(big)}}, monkeypatch)
    assert rc == 2
    assert "offset=/limit=" in capsys.readouterr().err


def test_guard_allows_bounded_read_of_same_file(tmp_path, monkeypatch):
    g = _load("guard_data_reads")
    big = tmp_path / "big_module.py"
    big.write_bytes(b"# filler\n" * 8_000)
    assert _run(g, {"tool_input": {"file_path": str(big), "limit": 50}}, monkeypatch) == 0


def test_guard_still_blocks_parquet_even_bounded(monkeypatch, capsys):
    g = _load("guard_data_reads")
    rc = _run(g, {"tool_input": {"file_path": "data/silver/parquet/x.parquet", "limit": 5}}, monkeypatch)
    assert rc == 2 and "describe_dataset" in capsys.readouterr().err


# ── session_token_ledger ─────────────────────────────────────────────────────


def _transcript_line(**usage) -> str:
    return json.dumps({"type": "assistant", "message": {"usage": usage, "content": []}})


def test_ledger_writes_one_row_per_session(tmp_path, monkeypatch):
    led = _load("session_token_ledger")
    monkeypatch.setattr(led, "LEDGER", tmp_path / "ledger.jsonl")

    transcript = tmp_path / "t.jsonl"
    transcript.write_text(
        "\n".join(
            [
                json.dumps({"type": "user", "message": {"content": "audit the widgets"}}),
                _transcript_line(output_tokens=100, input_tokens=10, cache_read_input_tokens=5),
                _transcript_line(output_tokens=250, cache_creation_input_tokens=40),
            ]
        ),
        encoding="utf-8",
    )
    rc = _run(led, {"session_id": "sess-abc", "transcript_path": str(transcript), "reason": "exit"}, monkeypatch)
    assert rc == 0

    row = json.loads((tmp_path / "ledger.jsonl").read_text(encoding="utf-8"))
    assert (row["out"], row["turns"], row["fresh_in"], row["cache_read"]) == (350, 2, 50, 5)
    assert row["prompt"] == "audit the widgets" and row["reason"] == "exit"


def test_ledger_skips_empty_transcript_and_missing_file(tmp_path, monkeypatch):
    led = _load("session_token_ledger")
    monkeypatch.setattr(led, "LEDGER", tmp_path / "ledger.jsonl")

    empty = tmp_path / "empty.jsonl"
    empty.write_text(json.dumps({"type": "user", "message": {"content": "hi"}}), encoding="utf-8")
    assert _run(led, {"transcript_path": str(empty)}, monkeypatch) == 0
    assert _run(led, {"transcript_path": str(tmp_path / "nope.jsonl")}, monkeypatch) == 0
    assert not (tmp_path / "ledger.jsonl").exists(), "no assistant turns -> no row"


# ── context_tripwire (200k/400k /clear nudge) ────────────────────────────────


def _tripwire_transcript(tmp_path, cache_read: int):
    t = tmp_path / "t.jsonl"
    t.write_text(
        "\n".join(
            [
                json.dumps({"type": "user", "message": {"content": "build the thing"}}),
                _transcript_line(output_tokens=10, cache_read_input_tokens=cache_read),
            ]
        ),
        encoding="utf-8",
    )
    return t


def test_tripwire_silent_below_200k(tmp_path, monkeypatch, capsys):
    tw = _load("context_tripwire")
    t = _tripwire_transcript(tmp_path, 150_000)
    rc = _run(tw, {"session_id": uuid.uuid4().hex, "transcript_path": str(t)}, monkeypatch)
    assert rc == 0 and capsys.readouterr().out == ""


def test_tripwire_fires_at_200k_once(tmp_path, monkeypatch, capsys):
    tw = _load("context_tripwire")
    sid = uuid.uuid4().hex
    t = _tripwire_transcript(tmp_path, 250_000)

    assert _run(tw, {"session_id": sid, "transcript_path": str(t)}, monkeypatch) == 0
    ctx = json.loads(capsys.readouterr().out)["hookSpecificOutput"]["additionalContext"]
    # /clear is offered only for unrelated work; continuing the same thread is not discouraged
    assert "context-tripwire" in ctx and "/clear" in ctx
    assert "250,000" in ctx and "UNRELATED" in ctx and "keep going" in ctx

    # same session, still in the 200k band -> marker exists -> silent
    assert _run(tw, {"session_id": sid, "transcript_path": str(t)}, monkeypatch) == 0
    assert capsys.readouterr().out == ""


def test_tripwire_escalates_to_400k_handoff(tmp_path, monkeypatch, capsys):
    tw = _load("context_tripwire")
    sid = uuid.uuid4().hex

    # session that jumps straight past 400k gets the handoff nudge (level 1 never fired)
    t = _tripwire_transcript(tmp_path, 450_000)
    assert _run(tw, {"session_id": sid, "transcript_path": str(t)}, monkeypatch) == 0
    ctx = json.loads(capsys.readouterr().out)["hookSpecificOutput"]["additionalContext"]
    # the level-2 nudge reports the measured size and offers /compact before any handoff
    assert "450,000" in ctx and "closeout" in ctx and "/compact" in ctx

    # after the 400k nudge the session stays silent
    assert _run(tw, {"session_id": sid, "transcript_path": str(t)}, monkeypatch) == 0
    assert capsys.readouterr().out == ""


def test_tripwire_reads_only_last_assistant_turn(tmp_path, monkeypatch, capsys):
    tw = _load("context_tripwire")
    t = tmp_path / "t.jsonl"
    t.write_text(
        "\n".join(
            [
                _transcript_line(output_tokens=10, cache_read_input_tokens=500_000),
                _transcript_line(output_tokens=10, cache_read_input_tokens=90_000),
            ]
        ),
        encoding="utf-8",
    )
    # last turn is back under threshold (e.g. after a compaction) -> silent
    rc = _run(tw, {"session_id": uuid.uuid4().hex, "transcript_path": str(t)}, monkeypatch)
    assert rc == 0 and capsys.readouterr().out == ""


def test_tripwire_fails_open(tmp_path, monkeypatch, capsys):
    tw = _load("context_tripwire")
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json{{"))
    assert tw.main() == 0
    assert _run(tw, {"session_id": "s", "transcript_path": str(tmp_path / "nope.jsonl")}, monkeypatch) == 0
    assert capsys.readouterr().out == ""


# ── discovery_hint (provider-neutral UserPromptSubmit context) ───────────────


def test_discovery_hint_accepts_codex_payload_and_uses_real_repo_detail(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    rows = tmp_path / "discoveries.jsonl"
    rows.write_text(
        json.dumps(
            {
                "id": "afs-trap",
                "domain": "afs",
                "trigger": ["audited financial statements"],
                "discovery": "Keep gross and net measures distinct.",
                "cost_band": "high",
                "memory": "afs-detail",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    detail = REPO / "memory" / "afs-detail.md"
    monkeypatch.setattr(hint, "DATA", str(rows))
    monkeypatch.setattr(hint, "resolve_detail", lambda _row, **_kwargs: detail)

    rc = _run(
        hint,
        {
            "hook_event_name": "UserPromptSubmit",
            "prompt": "Review these audited financial statements before editing.",
            "session_id": uuid.uuid4().hex,
        },
        monkeypatch,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    context = payload["hookSpecificOutput"]["additionalContext"]
    assert payload["hookSpecificOutput"]["hookEventName"] == "UserPromptSubmit"
    assert "Keep gross and net measures distinct" in context
    assert "detail: memory/afs-detail.md" in context


def test_discovery_hint_does_not_expose_personal_absolute_memory_path(tmp_path, monkeypatch):
    hint = _load("discovery_hint")
    personal = tmp_path / "private" / "lesson.md"
    monkeypatch.setattr(hint, "resolve_detail", lambda _row, **_kwargs: personal)

    label = hint._detail_label("lesson")

    assert str(personal) not in label
    assert label == "external memory slug: lesson; resolve with tools/discoveries.py"


def test_discovery_hint_caps_rows_and_fails_open(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    rows = tmp_path / "discoveries.jsonl"
    rows.write_text(
        "[]\n"
        + "\n".join(
            json.dumps(
                {
                    "id": f"lesson-{index}",
                    "trigger": ["planning evidence"],
                    "discovery": f"lesson body {index}",
                    "cost_band": "high",
                }
            )
            for index in range(3)
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(hint, "DATA", str(rows))

    assert (
        _run(
            hint,
            {"prompt": "Review the planning evidence for this proposal.", "session_id": uuid.uuid4().hex},
            monkeypatch,
        )
        == 0
    )
    context = json.loads(capsys.readouterr().out)["hookSpecificOutput"]["additionalContext"]
    assert context.count("\n-") == hint.MAX_ROWS
    assert "lesson-2" not in context

    assert (
        _run(
            hint,
            {"prompt": "Review the planning evidence for this proposal.", "session_id": uuid.uuid4().hex},
            monkeypatch,
        )
        == 0
    )
    # A repeated prompt in the original session is suppressed by its marker.
    sid = uuid.uuid4().hex
    payload = {"prompt": "Review the planning evidence for this proposal.", "session_id": sid}
    assert _run(hint, payload, monkeypatch) == 0
    capsys.readouterr()
    assert _run(hint, payload, monkeypatch) == 0
    assert "lesson-2" in capsys.readouterr().out
    assert _run(hint, payload, monkeypatch) == 0
    assert capsys.readouterr().out == ""

    monkeypatch.setattr(sys, "stdin", io.StringIO("not json{{"))
    assert hint.main() == 0
    assert capsys.readouterr().out == ""


def test_discovery_hint_rejects_malformed_payload_and_bounds_full_context(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    rows = tmp_path / "discoveries.jsonl"
    rows.write_text(
        "\n".join(
            json.dumps(
                {
                    "id": "long-" + ("x" * 120),
                    "trigger": ["alpha beta"],
                    "discovery": "lesson " + ("y" * 500),
                    "memory": "z" * 400,
                }
            )
            for _ in range(3)
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(hint, "DATA", str(rows))
    sid_a = "a" * 12 + "A" * 20
    sid_b = "a" * 12 + "B" * 20

    assert hint._seen_path(sid_a) != hint._seen_path(sid_b)
    marker_dir = tmp_path / "markers"
    marker_dir.mkdir()
    monkeypatch.setattr(hint, "_seen_path", lambda session: str(marker_dir / f"{session}.json"))
    assert _run(hint, [], monkeypatch) == 0
    assert capsys.readouterr().out == ""
    assert _run(hint, {"prompt": {}, "session_id": sid_a}, monkeypatch) == 0
    assert capsys.readouterr().out == ""
    assert _run(hint, {"prompt": "alpha beta needs review", "session_id": 42}, monkeypatch) == 0
    assert capsys.readouterr().out == ""

    assert _run(hint, {"prompt": "alpha beta needs review", "session_id": sid_a}, monkeypatch) == 0
    payload = json.loads(capsys.readouterr().out)
    context = payload["hookSpecificOutput"]["additionalContext"]
    assert len(context) <= hint.MAX_CONTEXT_CHARS
    assert all(len(line) <= hint.MAX_LINE_CHARS + 150 for line in context.splitlines()[1:])


def test_discovery_hint_skips_malformed_trigger_and_reaches_next_valid_row(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    rows = tmp_path / "discoveries.jsonl"
    rows.write_text(
        "\n".join(
            [
                json.dumps({"id": "trigger123", "trigger": 123, "discovery": "bad row"}),
                json.dumps({"id": "valid-row", "trigger": ["alpha beta"], "discovery": "valid finding"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(hint, "DATA", str(rows))

    assert _run(hint, {"prompt": "alpha beta needs review", "session_id": uuid.uuid4().hex}, monkeypatch) == 0
    output = capsys.readouterr().out
    assert "valid-row" in output and "trigger123" not in output


def test_discovery_hint_does_not_mark_overflow_rows_seen(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    rows = tmp_path / "discoveries.jsonl"
    rows.write_text(
        "\n".join(
            [
                json.dumps({"id": "x" * 950, "trigger": ["alpha beta"], "discovery": "f" * 320, "memory": "first"}),
                json.dumps({"id": "second-row", "trigger": ["alpha beta"], "discovery": "s" * 320, "memory": "second"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(hint, "DATA", str(rows))
    monkeypatch.setattr(hint, "_detail_label", lambda _slug, **_kwargs: "d" * hint.MAX_DETAIL_CHARS)
    marker_dir = tmp_path / "markers"
    marker_dir.mkdir()
    monkeypatch.setattr(hint, "_seen_path", lambda session: str(marker_dir / f"{session}.json"))
    payload = {"prompt": "alpha beta needs review", "session_id": uuid.uuid4().hex}

    assert _run(hint, payload, monkeypatch) == 0
    capsys.readouterr()
    assert _run(hint, payload, monkeypatch) == 0
    assert "second-row" in capsys.readouterr().out


def test_discovery_hint_skips_invalid_cost_bands(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    path = tmp_path / "discoveries.jsonl"
    rows = [
        {"id": f"invalid-{i}", "trigger": ["alpha beta"], "discovery": "malformed band", "cost_band": band}
        for i, band in enumerate(([], {}, 42, True))
    ]
    rows.append({"id": "valid-row", "trigger": ["alpha beta"], "discovery": "valid lesson", "cost_band": None})
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(hint, "DATA", str(path))
    assert _run(hint, {"prompt": "alpha beta needs review", "session_id": uuid.uuid4().hex}, monkeypatch) == 0
    output = capsys.readouterr().out
    assert "valid-row" in output and "invalid-" not in output


def test_discovery_hint_filters_scope_and_supersession_before_ranking(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    path = tmp_path / "discoveries.jsonl"
    rows = [
        {"id": "private", "scope": "private", "trigger": ["memory policy"], "discovery": "PRIVATE CLAIM"},
        {"id": "old", "superseded_by": "new", "trigger": ["memory policy"], "discovery": "OBSOLETE CLAIM"},
        {"id": "new", "scope": "public", "trigger": ["memory policy"], "discovery": "Current lead"},
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    monkeypatch.setattr(hint, "DATA", str(path))
    payload = {
        "prompt": "Review the memory policy before proceeding.",
        "session_id": uuid.uuid4().hex,
        "cwd": str(REPO),
    }

    assert _run(hint, payload, monkeypatch) == 0
    output = capsys.readouterr().out
    assert "Current lead" in output and "unverified" in output
    assert "PRIVATE CLAIM" not in output and "OBSOLETE CLAIM" not in output


def test_discovery_hint_emits_a_corrected_revision_in_the_same_session(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    path = tmp_path / "discoveries.jsonl"
    row = {"id": "lesson", "revision": 1, "trigger": ["memory policy"], "discovery": "Initial lead"}
    path.write_text(json.dumps(row), encoding="utf-8")
    monkeypatch.setattr(hint, "DATA", str(path))
    monkeypatch.setattr(hint, "_seen_path", lambda _session: str(tmp_path / "seen.json"))
    payload = {"prompt": "Review the memory policy before proceeding.", "session_id": "revision-test"}

    assert _run(hint, payload, monkeypatch) == 0
    assert "Initial lead" in capsys.readouterr().out
    assert _run(hint, payload, monkeypatch) == 0
    assert capsys.readouterr().out == ""

    row.update(revision=2, discovery="Corrected lead")
    path.write_text(json.dumps(row), encoding="utf-8")
    assert _run(hint, payload, monkeypatch) == 0
    assert "Corrected lead" in capsys.readouterr().out


def test_discovery_hint_reports_changed_evidence_without_repeating_stale_claim(tmp_path, monkeypatch, capsys):
    import hashlib

    hint = _load("discovery_hint")
    source = tmp_path / "evidence.md"
    source.write_text("original evidence", encoding="utf-8")
    row = {
        "id": "lesson",
        "revision": 1,
        "scope": "public",
        "trigger": ["memory policy"],
        "discovery": "STALE CLAIM",
        "verified_at": "2026-10-18T00:00:00Z",
        "source_refs": [{"path": "evidence.md", "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}],
    }
    path = tmp_path / "discoveries.jsonl"
    path.write_text(json.dumps(row), encoding="utf-8")
    monkeypatch.setattr(hint, "DATA", str(path))
    monkeypatch.setattr(hint, "REPO", str(tmp_path))
    monkeypatch.setattr(hint, "_seen_path", lambda _session: str(tmp_path / "seen.json"))
    payload = {"prompt": "Review the memory policy before proceeding.", "session_id": "evidence-test"}
    assert _run(hint, payload, monkeypatch) == 0
    assert "STALE CLAIM" in capsys.readouterr().out

    source.write_text("changed evidence", encoding="utf-8")
    assert _run(hint, payload, monkeypatch) == 0
    output = capsys.readouterr().out
    assert "recheck" in output and "evidence.md" in output
    assert "STALE CLAIM" not in output


def test_discovery_hint_quarantines_duplicate_ids_even_if_only_one_trigger_matches(tmp_path, monkeypatch, capsys):
    hint = _load("discovery_hint")
    path = tmp_path / "discoveries.jsonl"
    rows = [
        {"id": "conflict", "trigger": ["memory policy"], "discovery": "FIRST CLAIM"},
        {"id": "conflict", "trigger": ["different topic"], "discovery": "OTHER CLAIM"},
        {"id": "good", "trigger": ["memory policy"], "discovery": "Useful lead"},
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    monkeypatch.setattr(hint, "DATA", str(path))
    payload = {"prompt": "Review the memory policy before proceeding.", "session_id": uuid.uuid4().hex}
    assert _run(hint, payload, monkeypatch) == 0
    output = capsys.readouterr().out
    assert "Useful lead" in output
    assert "FIRST CLAIM" not in output and "OTHER CLAIM" not in output


def test_discovery_hint_never_resolves_a_private_detail_in_public_context(tmp_path, monkeypatch, capsys):
    from tools import discoveries

    hint = _load("discovery_hint")
    private = tmp_path / "planning/product/claude/memory/private-detail.md"
    private.parent.mkdir(parents=True)
    private.write_text("private evidence", encoding="utf-8")
    path = tmp_path / "discoveries.jsonl"
    path.write_text(
        json.dumps(
            {"id": "legacy", "discovery": "Public lead", "trigger": ["memory policy"], "memory": "private-detail"}
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(discoveries, "ROOT", tmp_path)
    monkeypatch.setattr(discoveries, "default_memory_roots", lambda: [tmp_path / "memory", private.parent])
    monkeypatch.setattr(hint, "REPO", str(tmp_path))
    monkeypatch.setattr(hint, "DATA", str(path))
    payload = {
        "prompt": "Review the memory policy before proceeding.",
        "session_id": uuid.uuid4().hex,
        "cwd": str(tmp_path),
    }
    assert _run(hint, payload, monkeypatch) == 0
    assert "private-detail" not in capsys.readouterr().out


def test_discovery_hint_public_finding_does_not_gain_private_provenance(tmp_path, monkeypatch, capsys):
    from tools import discoveries

    hint = _load("discovery_hint")
    private = tmp_path / "planning/product/claude/memory/same.md"
    private.parent.mkdir(parents=True)
    private.write_text("private evidence", encoding="utf-8")
    path = tmp_path / "discoveries.jsonl"
    path.write_text(
        json.dumps(
            {
                "id": "public-row",
                "scope": "public",
                "discovery": "Public lead",
                "trigger": ["memory policy"],
                "memory": "same",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(discoveries, "ROOT", tmp_path)
    monkeypatch.setattr(hint, "REPO", str(tmp_path))
    monkeypatch.setattr(hint, "DATA", str(path))
    payload = {
        "prompt": "Review the memory policy before proceeding.",
        "session_id": uuid.uuid4().hex,
        "cwd": str(tmp_path / "planning/product"),
    }
    assert _run(hint, payload, monkeypatch) == 0
    assert "same.md" not in capsys.readouterr().out
