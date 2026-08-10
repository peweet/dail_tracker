"""Focused parser contracts for the per-council AFS capital appendix."""

from __future__ import annotations

from extractors.la_afs_capital_extract import _parse_geom, _reconciles


class _Page:
    def __init__(self, words: list[tuple]):
        self.words = words

    def get_text(self, kind: str):
        assert kind == "words"
        return self.words


def _word(x: float, y: float, value: str) -> tuple:
    return (x, y, x + 8, y + 4, value, 0, 0, 0)


def _numbers(y: float, values: list[int]) -> list[tuple]:
    return [_word(120 + i * 45, y, f"{value:,}") for i, value in enumerate(values)]


def _page(*, split_agriculture: bool) -> _Page:
    labels = [
        "Housing",
        "Road",
        "Water",
        "Development",
        "Environment",
        "Recreation",
        "Agriculture",
        "Miscellaneous",
    ]
    words: list[tuple] = []
    totals = [0] * 10
    for index, label in enumerate(labels, start=1):
        values = [
            index * 10,
            index * 100,
            index * 90,
            index * 5,
            index * 5,
            index * 100,
            0,
            0,
            0,
            index * 10,
        ]
        totals = [a + b for a, b in zip(totals, values, strict=True)]
        y = 100 + index * 20
        words.append(_word(10, y, label))
        if split_agriculture and label == "Agriculture":
            words.append(_word(110, y, "07"))
            words.extend(_numbers(y + 5, values))
        else:
            words.extend(_numbers(y, values))
    words.extend(_numbers(285, totals))
    return _Page(words)


def test_geom_recovers_unlabelled_printed_total_below_divisions():
    parsed = _parse_geom(_page(split_agriculture=False))
    assert parsed is not None
    matrix, _columns, printed_total = parsed
    assert len(matrix) == 8
    assert _reconciles(matrix, printed_total, 1)


def test_geom_attaches_nearby_numeric_continuation_to_split_division_label():
    parsed = _parse_geom(_page(split_agriculture=True))
    assert parsed is not None
    matrix, _columns, printed_total = parsed
    assert matrix["Agriculture, Education, Health & Welfare"][1] == 700
    assert _reconciles(matrix, printed_total, 1)
