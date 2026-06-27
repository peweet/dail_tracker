"""URL-builder tests for extractors/si_legislation_directory_extract.

A wrong ELI link silently points a citizen (and the 'confirm' link) at the WRONG
statutory instrument, and a mis-parsed citation attaches the wrong affecting SI. These
builders are pure, so they're cheap to pin exactly.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from extractors.si_legislation_directory_extract import affecting_sis, affecting_urls, eli_url  # noqa: E402


def test_eli_url_format():
    assert eli_url(2025, 332) == "https://www.irishstatutebook.ie/eli/2025/si/332/made/en/html"


def test_affecting_urls_maps_num_slash_year_refs():
    assert affecting_urls(["332/2025"]) == ["https://www.irishstatutebook.ie/eli/2025/si/332/made/en/html"]


def test_affecting_sis_extracts_dedups_and_sorts_by_year_then_number():
    text = "Revoked by S.I. No. 332 of 2025; amended by S.I. No. 16 of 2024; again S.I. No. 332 of 2025."
    assert affecting_sis(text) == ["16/2024", "332/2025"]


def test_affecting_sis_empty_when_no_citation():
    assert affecting_sis("no statutory instrument cited here") == []
