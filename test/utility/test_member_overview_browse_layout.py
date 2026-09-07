"""Source-level UI hierarchy contract for the Member Overview browse page."""

from __future__ import annotations

from pathlib import Path

_PAGE = Path(__file__).resolve().parents[2] / "utility" / "pages_code" / "member_overview.py"


def test_browse_places_the_title_and_search_before_secondary_filters():
    source = _PAGE.read_text(encoding="utf-8")
    start = source.index("def _render_browse")
    end = source.index("\ndef _render_constituency_context", start)
    browse = source[start:end]

    assert browse.index('<div class="dt-hero">') < browse.index("st.segmented_control(")
    assert 'with st.expander("Refine the list"' in browse
    assert "glossary_strip(" not in browse
