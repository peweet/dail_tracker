"""Focused UI contracts for the Your Council landing and dossier."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _path in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import your_council as yc  # noqa: E402


def test_council_picker_navigates_as_soon_as_a_council_is_chosen(monkeypatch):
    navigated = []
    monkeypatch.setattr(yc.st, "selectbox", lambda *args, **kwargs: "Cork City Council")
    monkeypatch.setattr(yc, "_go", lambda council=None, **kwargs: navigated.append(council))

    yc._render_council_picker(["Cork City Council"])

    assert navigated == ["Cork City Council"]


def test_return_to_all_councils_clears_the_picker_state(monkeypatch):
    query_params = {"council": "Cork City Council"}
    state = {"yc_index_council": "Cork City Council"}
    reruns = []
    monkeypatch.setattr(yc.st, "query_params", query_params)
    monkeypatch.setattr(yc.st, "session_state", state)
    monkeypatch.setattr(yc.st, "rerun", lambda: reruns.append(True))

    yc._go()

    assert query_params == {}
    assert "yc_index_council" not in state
    assert reruns == [True]


def test_index_shows_council_choices_before_optional_orientation():
    source = (Path(__file__).resolve().parents[2] / "utility" / "pages_code" / "your_council.py").read_text(
        encoding="utf-8"
    )
    start = source.index("def _render_index")
    end = source.index("\n\n# ── the three sections", start)
    index = source[start:end]

    assert index.index("_render_council_picker(") < index.index("con-card-grid")
    assert index.index("con-card-grid") < index.index("How does local council power work?")
    assert index.index("How does local council power work?") < index.index("Find a council on the map")


def test_glance_cards_are_summaries_not_duplicate_navigation():
    html = yc._glance_card(
        "Spending",
        "€1m paid",
        "Published payments",
        "#3d719c",
    )

    assert html.startswith('<div class="yc-glance-card')
    assert "href=" not in html
    assert "yc-glance-arrow" not in html
