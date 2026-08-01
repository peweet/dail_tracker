"""Tests for tools/hooks/grep_routing_hint.py (PostToolUse, matcher Grep).

Pins the two contracts the harness relies on: (1) the nudge fires once per
session+category and stays classification-gated (the 07-31 shapes were only
smoke-tested ad hoc — nothing persisted, so a regression would go unnoticed);
(2) the auto-append to logs/grep_vs_index_jsonl added 08-01 — the trial
telemetry replaced manual self-logging that produced zero rows, so if the
write silently breaks the adoption question becomes unanswerable again.
Same importlib + stdin-monkeypatch pattern as test_efficiency_hooks.py; the
log path is monkeypatched into tmp_path so tests never touch logs/.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location(
        "grep_routing_hint", REPO / "tools" / "hooks" / "grep_routing_hint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run(mod, payload: dict, monkeypatch) -> int:
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    return mod.main()


def _grep(pattern: str, sid: str, **ti) -> dict:
    return {"tool_name": "Grep", "session_id": sid, "tool_input": {"pattern": pattern, **ti}}


# ── classification ───────────────────────────────────────────────────────────


def test_classify_symbol_repo_wide_is_py_refs():
    mod = _load()
    assert mod._classify({"pattern": "save_parquet"}) == "py_refs"


def test_classify_sql_views_path_is_sql_lineage():
    mod = _load()
    assert mod._classify({"pattern": "total_paid", "path": "sql_views/payments"}) == "sql_lineage"


def test_classify_import_statement_is_py_deps():
    mod = _load()
    assert mod._classify({"pattern": "from services.parquet_io import"}) == "py_deps"


def test_classify_unscoped_phrase_files_mode_is_search_project():
    mod = _load()
    assert (
        mod._classify({"pattern": "flood risk assessment", "output_mode": "files_with_matches"})
        == "search_project"
    )


def test_classify_scoped_or_regex_greps_stay_silent():
    mod = _load()
    # scoped to a file → not a routing-table shape
    assert mod._classify({"pattern": "save_parquet", "path": "services/parquet_io.py"}) is None
    # regex metachars → literal-text search, Grep is the right tool
    assert mod._classify({"pattern": r"def \w+_parquet"}) is None
    # short bare word without symbol shape → too weak a signal
    assert mod._classify({"pattern": "main"}) is None


# ── nudge + trial log ────────────────────────────────────────────────────────


def test_nudge_fires_once_and_both_calls_are_logged(monkeypatch, capsys, tmp_path):
    mod = _load()
    log = tmp_path / "trial.jsonl"
    monkeypatch.setattr(mod, "_TRIAL_LOG", str(log))
    sid = uuid.uuid4().hex

    assert _run(mod, _grep("save_parquet", sid), monkeypatch) == 0
    out = json.loads(capsys.readouterr().out)
    assert "py_refs" in out["hookSpecificOutput"]["additionalContext"]

    # same session + category again → silent, but still logged
    assert _run(mod, _grep("save_parquet", sid), monkeypatch) == 0
    assert capsys.readouterr().out == ""

    rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert [r["nudged"] for r in rows] == [True, False]
    assert all(r["category"] == "py_refs" and r["session"] == sid[:12] for r in rows)


def test_non_matching_grep_writes_no_log_row(monkeypatch, capsys, tmp_path):
    mod = _load()
    log = tmp_path / "trial.jsonl"
    monkeypatch.setattr(mod, "_TRIAL_LOG", str(log))

    assert _run(mod, _grep("main", uuid.uuid4().hex), monkeypatch) == 0
    assert capsys.readouterr().out == ""
    assert not log.exists()


def test_fails_open_on_garbage_payload(monkeypatch, capsys):
    mod = _load()
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json{{"))
    assert mod.main() == 0
    assert capsys.readouterr().out == ""


def test_unwritable_log_never_breaks_the_nudge(monkeypatch, capsys):
    mod = _load()
    # a directory path can't be opened for append — the log write must swallow it
    monkeypatch.setattr(mod, "_TRIAL_LOG", str(REPO / "tools"))
    sid = uuid.uuid4().hex
    assert _run(mod, _grep("save_parquet", sid), monkeypatch) == 0
    out = json.loads(capsys.readouterr().out)
    assert "py_refs" in out["hookSpecificOutput"]["additionalContext"]
