"""Tests for ui.components.dt_page — the single page-bootstrap decorator.

What this catches:
  - The boot order contract: inject_css + hide_sidebar run BEFORE the page body.
  - The error boundary is composed outermost: a raising page (or a raising
    boot) never propagates — it renders the calm fallback and returns None.
    This is the guarantee that closed the 12-pages-without-a-boundary gap.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))

import shared_css  # noqa: E402
import ui.components as components  # noqa: E402
from ui.components import dt_page  # noqa: E402


def _spy_boot(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(shared_css, "inject_css", lambda: calls.append("css"))
    monkeypatch.setattr(components, "hide_sidebar", lambda: calls.append("sidebar"))
    return calls


def test_boot_runs_before_page_body(monkeypatch):
    calls = _spy_boot(monkeypatch)

    @dt_page
    def my_page() -> None:
        calls.append("body")

    assert my_page() is None
    assert calls == ["css", "sidebar", "body"]


def test_return_value_passes_through(monkeypatch):
    _spy_boot(monkeypatch)

    @dt_page
    def my_page() -> str:
        return "rendered"

    assert my_page() == "rendered"


def test_raising_page_is_caught_returns_none(monkeypatch):
    _spy_boot(monkeypatch)

    @dt_page
    def broken_page() -> None:
        raise RuntimeError("view missing")

    assert broken_page() is None  # boundary swallows, renders fallback


def test_raising_boot_is_also_caught(monkeypatch):
    """The boundary wraps the boot itself — a hide_sidebar crash must not
    escape as a red traceback either."""
    monkeypatch.setattr(shared_css, "inject_css", lambda: None)

    def broken_sidebar():
        raise RuntimeError("boot crash")

    monkeypatch.setattr(components, "hide_sidebar", broken_sidebar)

    @dt_page
    def fine_page() -> str:
        return "never reached"

    assert fine_page() is None


def test_wraps_preserves_page_identity(monkeypatch):
    _spy_boot(monkeypatch)

    @dt_page
    def procurement_page() -> None: ...

    assert procurement_page.__name__ == "procurement_page"
