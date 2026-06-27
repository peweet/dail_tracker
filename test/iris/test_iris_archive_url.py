"""URL-builder test for iris/iris_archive_backfill.month_index_url.

A wrong month slug silently pulls the wrong month's Iris Oifigiúil index (or 404s).
Pure function — pin the slug format.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from iris.iris_archive_backfill import month_index_url  # noqa: E402


def test_month_index_url_uses_lowercase_month_name():
    assert month_index_url(2026, 6) == "https://www.irisoifigiuil.ie/archive/2026/june/"


def test_month_index_url_january_and_december():
    assert month_index_url(2025, 1) == "https://www.irisoifigiuil.ie/archive/2025/january/"
    assert month_index_url(2024, 12) == "https://www.irisoifigiuil.ie/archive/2024/december/"
