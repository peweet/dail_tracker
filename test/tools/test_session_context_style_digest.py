"""Contract test for session_context._style_digest_note's warning-kind bucketing.

Regression: before 2026-08-24 every non-jargon warning -- including "N-word reply" (rule 1)
-- fell into the "long-sentence" bucket because nothing distinguished them. Also covers the
rule 5 (bullet density) bucket added the same day.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location(
        "session_context_digest_test", ROOT / "tools" / "hooks" / "session_context.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_style_digest_buckets_each_warning_kind_correctly(tmp_path, monkeypatch):
    module = _load()
    monkeypatch.setattr(module, "REPO", tmp_path)
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    rows = [
        {"ts": "x", "session": "a", "warns": ['jargon: "utilize" (rule 3 -- use the plain word)']},
        {"ts": "x", "session": "b", "warns": ["50-word sentence (rule 4 -- one idea per sentence)"]},
        {"ts": "x", "session": "c", "warns": ["420-word reply (rule 1 -- answer at the size of the question)"]},
        {"ts": "x", "session": "d", "warns": ["8 bullet lines (rule 5 -- prose by default)"]},
    ]
    (logs_dir / "style_lint_log.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")

    note = module._style_digest_note()

    assert "1 jargon" in note
    assert "1 long-sentence" in note
    assert "1 long-reply" in note
    assert "1 bullets" in note


def test_style_digest_empty_when_no_log(tmp_path, monkeypatch):
    module = _load()
    monkeypatch.setattr(module, "REPO", tmp_path)
    assert module._style_digest_note() == ""
