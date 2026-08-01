"""Guards on ui.format.distinguish_truncations.

A fixed truncation made two Cork City budget amendments render as identical cards carrying
OPPOSITE votes — the strings diverged past the display limit (04/12/2024, found in the render
2026-08-01). Two different motions must never produce the same display string; two identical
ones must still look identical.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "utility"))

from ui.format import distinguish_truncations  # noqa: E402

PREFIX = "'The % proportion of wages/superannuation to rates & services be equal to 59.75% i.e., 2020 Budget standard. An immediate staff level review with the aim of reducing "


def test_short_texts_are_untouched():
    assert distinguish_truncations(["short one", "short two"]) == ["short one", "short two"]


def test_a_lone_long_text_is_simply_truncated():
    text = "x" * 400
    out = distinguish_truncations([text], limit=170)
    assert out[0].startswith("x" * 170)
    assert out[0].endswith("…")


def test_colliding_prefixes_render_differently():
    a, b = PREFIX + "costs by 5% in 2025.'", PREFIX + "costs by 10% in 2026.'"
    out = distinguish_truncations([a, b], limit=170)
    assert out[0] != out[1], "two different motions must never display identically"
    assert "5%" in out[0] or "2025" in out[0]
    assert "10%" in out[1] or "2026" in out[1]


def test_identical_texts_still_look_identical():
    """The same motion voted once is not a collision — it must not be 'disambiguated'."""
    same = PREFIX + "costs.'"
    out = distinguish_truncations([same, same], limit=170)
    assert out[0] == out[1]


def test_three_way_collision_all_distinct():
    texts = [PREFIX + f"costs by {n}% this year.'" for n in (5, 10, 15)]
    out = distinguish_truncations(texts, limit=170)
    assert len(set(out)) == 3


def test_head_is_preserved_so_the_reader_still_sees_the_subject():
    a, b = PREFIX + "costs by 5%.'", PREFIX + "costs by 10%.'"
    out = distinguish_truncations([a, b], limit=170)
    for shown in out:
        assert shown.startswith("'The % proportion of wages/superannuation")


def test_mixed_list_leaves_non_colliding_items_alone():
    a, b = PREFIX + "costs by 5%.'", PREFIX + "costs by 10%.'"
    other = "A completely different motion about burial grounds."
    out = distinguish_truncations([a, other, b], limit=170)
    assert out[1] == other
    assert out[0] != out[2]
