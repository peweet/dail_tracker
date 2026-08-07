"""Accessibility contracts for the app-wide Streamlit shell."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))

import shared_css  # noqa: E402


def _render_shared_shell(monkeypatch) -> list[str]:
    rendered: list[str] = []
    fake_streamlit = SimpleNamespace(
        session_state={},
        markdown=lambda body, **_kwargs: rendered.append(body),
        html=lambda body: rendered.append(body),
    )
    monkeypatch.setattr(shared_css, "st", fake_streamlit)

    shared_css.inject_css()
    shared_css.inject_css()
    return rendered


def test_accessibility_layer_is_injected_once(monkeypatch) -> None:
    rendered = _render_shared_shell(monkeypatch)

    assert sum(body.count("@media (prefers-reduced-motion: reduce)") for body in rendered) == 1


def test_shared_shell_exposes_keyboard_focus(monkeypatch) -> None:
    shell = "\n".join(_render_shared_shell(monkeypatch))

    assert '[data-testid="stTopNavLink"]:focus-visible' in shell
    assert '[data-testid="stSidebarNav"] a:focus-visible' in shell
    assert '[data-testid="stExpandSidebarButton"]:focus-visible' in shell
    assert '[data-testid="stButton"] button:focus-visible' in shell
    assert '[data-testid="stDownloadButton"] button:focus-visible' in shell


def test_shared_shell_respects_reduced_motion_and_mobile_targets(monkeypatch) -> None:
    shell = "\n".join(_render_shared_shell(monkeypatch))

    assert "@media (prefers-reduced-motion: reduce)" in shell
    assert "animation-duration: 0.01ms" in shell
    assert "min-height: 2.75rem" in shell
    assert '[data-testid="stSidebar"][aria-expanded="false"]' in shell


def test_mobile_navigation_buttons_have_accessible_names() -> None:
    component = (Path(__file__).resolve().parents[2] / "utility" / "ui" / "spa_links" / "index.html").read_text(
        encoding="utf-8"
    )

    assert 'setAttribute("aria-label", "Open navigation menu")' in component
    assert 'setAttribute("aria-label", "Close navigation menu")' in component


def test_shared_stylesheet_is_an_external_reusable_asset() -> None:
    css = shared_css._load_css()

    assert shared_css._CSS_PATH.name == "dailtracker.css"
    assert ":root" in css
    assert ".site-banner" in css
    assert all(line.strip() not in {"<style>", "</style>"} for line in css.splitlines())
