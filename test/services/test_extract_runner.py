"""Tests for services/extract_runner.py — the standard extractor __main__ harness.

What this catches:
  - Exit-code contract: success → 0, unhandled exception → 1 (pipeline.py and
    schedulers key off this), int return passes through, Ctrl+C → 130,
    an explicit sys.exit() propagates untouched.
  - The failure path logs the traceback before exiting (silent failures were
    the reason ad-hoc __main__ blocks kept growing try/except boilerplate).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services import extract_runner
from services.extract_runner import run_extractor


@pytest.fixture(autouse=True)
def _no_real_logging_setup(monkeypatch):
    """Keep the harness from touching the real logs/ dir in tests."""
    monkeypatch.setattr(extract_runner, "setup_standalone_logging", lambda *a, **k: None)


def test_success_exits_zero():
    with pytest.raises(SystemExit) as e:
        run_extractor(lambda: None, name="t")
    assert e.value.code == 0


def test_int_return_becomes_exit_code():
    with pytest.raises(SystemExit) as e:
        run_extractor(lambda: 3, name="t")
    assert e.value.code == 3


def test_unhandled_exception_exits_one_and_logs(caplog):
    def boom():
        raise ValueError("parse failed")

    with pytest.raises(SystemExit) as e:
        run_extractor(boom, name="t_fail")
    assert e.value.code == 1
    assert any("extractor failed" in r.message for r in caplog.records)


def test_keyboard_interrupt_exits_130():
    def interrupted():
        raise KeyboardInterrupt

    with pytest.raises(SystemExit) as e:
        run_extractor(interrupted, name="t")
    assert e.value.code == 130


def test_explicit_sys_exit_propagates_untouched():
    def main():
        sys.exit(7)

    with pytest.raises(SystemExit) as e:
        run_extractor(main, name="t")
    assert e.value.code == 7


def test_name_defaults_to_script_stem(monkeypatch):
    captured: list[str] = []
    monkeypatch.setattr(extract_runner, "setup_standalone_logging", lambda name, **k: captured.append(name))
    monkeypatch.setattr(sys, "argv", ["extractors/lpt_laf_extract.py"])
    with pytest.raises(SystemExit):
        run_extractor(lambda: None)
    assert captured == ["lpt_laf_extract"]
