"""Tests for the ministerial-diaries transform orchestrator (ministerial_diaries_refresh.py).

The chain's load-bearing safety property is the SANDBOX GUARD: the diary working
table (data/sandbox/enrichment/ministerial_diary_entries.parquet) is gitignored
and produced by a manual WAF/OCR-gated extract, so on every cloud / fresh-checkout
run it is ABSENT. The chain must then no-op with exit 0 — a non-zero exit would
mark the chain failed and block the gated daily publish. When the table IS present
(a local box post-extract) it must run the five transform steps in order.

These tests stub the subprocess + logging so nothing is fetched or written.
"""

from __future__ import annotations

import ministerial_diaries_refresh as mdr


def _stub_logging(monkeypatch):
    monkeypatch.setattr("services.logging_setup.setup_standalone_logging", lambda *_a, **_k: None)


def _no_argv(monkeypatch):
    # main() calls parse_args() with no args; keep it from seeing pytest's argv.
    monkeypatch.setattr("sys.argv", ["ministerial_diaries_refresh"])


def test_skips_with_exit_0_when_sandbox_absent(monkeypatch, tmp_path):
    _no_argv(monkeypatch)
    _stub_logging(monkeypatch)
    monkeypatch.setattr(mdr, "_SANDBOX_ENTRIES", tmp_path / "does_not_exist.parquet")

    calls: list[str] = []
    monkeypatch.setattr(mdr, "_subprocess", lambda script: calls.append(script) or True)

    assert mdr.main() == 0
    assert calls == []  # no transform step ran — clean no-op, publish not blocked


def test_runs_five_steps_in_order_when_sandbox_present(monkeypatch, tmp_path):
    _no_argv(monkeypatch)
    _stub_logging(monkeypatch)
    entries = tmp_path / "ministerial_diary_entries.parquet"
    entries.write_bytes(b"")  # presence is all the guard checks
    monkeypatch.setattr(mdr, "_SANDBOX_ENTRIES", entries)

    calls: list[str] = []
    monkeypatch.setattr(mdr, "_subprocess", lambda script: calls.append(script) or True)

    assert mdr.main() == 0
    assert calls == [
        "extractors/diary_entry_classify.py",
        "extractors/diary_org_match.py",
        "extractors/diary_lobbying_overlap.py",
        "extractors/diary_promote_gold.py",
        "extractors/diary_company_influence.py",
    ]
    # company_influence (reads procurement gold) runs LAST, after promote_gold.
    assert calls.index("extractors/diary_promote_gold.py") < calls.index("extractors/diary_company_influence.py")


def test_nonzero_exit_when_a_step_fails(monkeypatch, tmp_path):
    _no_argv(monkeypatch)
    _stub_logging(monkeypatch)
    entries = tmp_path / "ministerial_diary_entries.parquet"
    entries.write_bytes(b"")
    monkeypatch.setattr(mdr, "_SANDBOX_ENTRIES", entries)

    # org_match fails; the chain presses on through the rest but returns 1.
    def fake(script: str) -> bool:
        return script != "extractors/diary_org_match.py"

    calls: list[str] = []
    monkeypatch.setattr(mdr, "_subprocess", lambda script: calls.append(script) or fake(script))

    assert mdr.main() == 1
    assert len(calls) == 5  # all steps still attempted (one bad step doesn't mask the rest)
