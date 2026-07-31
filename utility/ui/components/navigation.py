"""Main-panel navigation: TD jump/search, filter bars, breadcrumbs, pill chips.

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.

DEVIATION: ``field_label`` and ``PILL_VARIANTS``/``pill`` are not named in the
original brief's grouping for this module, but they are contiguous with
(``field_label``) or naturally paired with (``pill``) the functions listed
here in the source file, so they are kept here rather than invented a new
single-function module.
"""

from __future__ import annotations

from contextlib import contextmanager
from html import escape as _h

import streamlit as st

from .layout import render_notable_chips


def main_member_jump(
    members: list[str],
    *,
    key_prefix: str,
    label: str = "Find a TD",
    placeholder: str = "Type a name…",
) -> str | None:
    """Prominent main-panel search — type a name, pick from the dropdown.

    Returns the selected member name when chosen (caller is responsible for
    setting the relevant ``selected_td`` session-state key + ``st.rerun()``).
    Mirrors ``sidebar_member_filter`` but is sized and labelled for the
    main column as a primary call-to-action under the hero.

    Audit fix (2026-05-26, interests P1-4): the previous version was a
    ``text_input + selectbox`` pair where the text input filtered the
    selectbox options. Streamlit's red-border ``Press Enter to apply``
    hint led users to think Enter would commit the filter; in reality
    only clicking a dropdown option did anything. Worse, ``st.selectbox``
    has its own built-in type-to-search, so the text input was doubly
    redundant. Now a single placeholder-leading ``st.selectbox`` — one
    affordance, one click target, no Enter trap.
    """
    st.html(f'<p class="dt-main-search-kicker">{_h(label)}</p>')
    options = [placeholder] + list(members)
    chosen = st.selectbox(
        label,
        options,
        index=0,
        key=f"{key_prefix}_main_select",
        label_visibility="collapsed",
    )
    return chosen if chosen and chosen != placeholder else None


def field_label(text: str) -> None:
    """Small-caps micro-label for a control inside a main-panel filter bar.

    Same typographic token as the old sidebar ``.sidebar-label`` but named
    for the main panel so the sidebar/main-panel split stays legible. Place
    immediately above the widget inside a :func:`filter_bar` column.
    """
    st.html(f'<p class="dt-field-label">{_h(text)}</p>')


def member_jump_panel(
    members: list[str],
    *,
    search_key_prefix: str,
    session_key: str,
    label: str = "Find a TD",
    placeholder: str = "Type a name…",
    notable: list[str] | None = None,
    chip_key_prefix: str | None = None,
    chip_cols: int = 6,
) -> str | None:
    """Main-panel member jump: searchable selectbox + optional notable-chip row.

    Replaces the sidebar ``member_picker`` + ``notable_chips`` slots that
    the (since-removed) sidebar shell used to carry. Returns the picked member name (from
    the search or a clicked chip), or ``None``. The caller owns the post-pick
    action (set session + rerun, or navigate to the canonical profile).

    ``chip_cols`` defaults to 6 because the main panel is far wider than the
    old sidebar, where 2 columns made each chip span half the page.
    """
    picked = main_member_jump(members, key_prefix=search_key_prefix, label=label, placeholder=placeholder)
    if (
        notable
        and chip_key_prefix
        and render_notable_chips(notable, members, chip_key_prefix, session_key, cols=chip_cols)
    ):
        picked = st.session_state.get(session_key)
    return picked


@contextmanager
def filter_bar(weights: list[int]):
    """Horizontal main-panel filter bar that sits directly under a page hero.

    Replaces the per-page sidebar filter stack. Yields the column list so the
    caller renders each control with a :func:`field_label` above it::

        with filter_bar([3, 2, 4]) as cols:
            with cols[0]:
                field_label("Status")
                status = st.selectbox(...)
            with cols[1]:
                field_label("Introduced")
                dates = st.date_input(...)

    Inline + hairline-rule treatment (no container box) per the ink-on-paper
    register: a height:0 marker is dropped inside the first column so a
    ``[data-testid="stHorizontalBlock"]:has(.dt-filterbar-marker)`` rule in
    shared_css.py can scope the row (same ``:has()`` convention the card rows
    use), and a closing ``.dt-filterbar-rule`` is drawn on exit. Streamlit
    columns stack vertically on narrow viewports, so the bar is responsive
    for free; the ≤640px CSS guard removes overflow.
    """
    cols = st.columns(weights, gap="medium")
    with cols[0]:
        st.html('<div class="dt-filterbar-marker"></div>')
    try:
        yield cols
    finally:
        st.html('<hr class="dt-filterbar-rule">')


def breadcrumb(labels: list[str], *, key_prefix: str) -> int | None:
    """Horizontal breadcrumb trail with ``›`` separators.

    ``labels`` — ordered path from root to the current page. The LAST label is
    rendered as plain bold text (the current page); every preceding label is
    rendered as a clickable link-style button.

    Returns the index of the clicked segment (in ``labels``), or ``None`` when
    nothing was clicked this run. Caller is responsible for navigation +
    ``st.rerun()`` based on the returned index.

    Button keys are auto-prefixed with ``dt_crumb_`` so a single CSS rule in
    shared_css.py styles every breadcrumb consistently across pages.

    Usage::

        clicked = breadcrumb(
            ["Lobbying", "Revolving Door", "Mary Smith"],
            key_prefix="rd_indiv",
        )
        if clicked == 0:
            _clear_all(); st.rerun()
        elif clicked == 1:
            _open_rd_index(); st.rerun()
    """
    if not labels:
        return None
    n = len(labels)
    weights: list[int] = []
    for i, lbl in enumerate(labels):
        weights.append(max(2, min(8, len(lbl) // 2)))
        if i < n - 1:
            weights.append(1)
    cols = st.columns(weights, gap="small")
    clicked: int | None = None
    st.html('<div class="dt-crumb-row-marker"></div>')
    for i, lbl in enumerate(labels):
        col = cols[i * 2]
        if i == n - 1:
            col.html(f'<div class="dt-crumb-current">{_h(lbl)}</div>')
        else:
            if col.button(lbl, key=f"dt_crumb_{key_prefix}_{i}"):
                clicked = i
            cols[i * 2 + 1].html('<span class="dt-crumb-sep">›</span>')
    return clicked


PILL_VARIANTS: dict[str, str] = {
    "default": "int-stat-pill",
    "accent": "int-stat-pill int-stat-pill-accent",
    "decl": "int-stat-pill int-pill-decl",
    "company": "int-stat-pill int-pill-company",
    "prop": "int-stat-pill int-pill-prop",
    "shares": "int-stat-pill int-pill-shares",
    "owner": "int-stat-pill int-pill-owner",
}


def pill(text: str, variant: str = "default", *, icon: str = "") -> str:
    """Single stat-pill <span>, the canonical chip used on cards and profile headers.

    variant — key in PILL_VARIANTS; unknown values fall back to the neutral chip.
    icon    — optional emoji prefix (passed through unescaped).
    """
    classes = PILL_VARIANTS.get(variant, PILL_VARIANTS["default"])
    body = f"{icon} {_h(text)}" if icon else _h(text)
    return f'<span class="{classes}">{body}</span>'
