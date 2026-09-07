"""Focused UI contracts for the What They Own page."""

from __future__ import annotations

import sys
from contextlib import nullcontext
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _path in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import what_they_own as wto  # noqa: E402


def test_interest_filters_use_compact_selectors_with_visible_labels(monkeypatch):
    calls = []
    monkeypatch.setattr(wto.st, "columns", lambda *args, **kwargs: (nullcontext(), nullcontext()))

    def selectbox(label, options, **kwargs):
        calls.append((label, list(options), kwargs))
        return options[0]

    monkeypatch.setattr(wto.st, "selectbox", selectbox)

    category, year = wto._render_interest_filters([2025, 2024])

    assert (category, year) == ("Everyone", "Most recent on file")
    assert calls == [
        ("Show members with", list(wto._CATEGORIES), {"index": 0, "key": "wto_category"}),
        (
            "Declaration year",
            ["Most recent on file", "2025", "2024"],
            {"index": 0, "key": "wto_year"},
        ),
    ]
