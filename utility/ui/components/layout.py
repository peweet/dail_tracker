"""Page-layout primitives: year selector, notable-member chips, info cards.

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.
"""

from __future__ import annotations

import datetime
from html import escape as _h

import streamlit as st


def year_selector(
    options: list[str],
    key: str,
    default: str | None = None,
    skip_current: bool = True,
    include_all: bool = False,
    all_label: str = "All years",
) -> int | None:
    """Year pill selector — the single year-filter control for the app.

    Two modes:
    - ``include_all=False`` (default): a year is always selected; defaults to
      the most recent *completed* year when ``skip_current=True``. Returns int.
    - ``include_all=True``: prepends an "All years" pill (the default), and
      returns ``None`` when it is selected — callers pass that straight to
      SQL as "no year filter".

    Always ``st.pills`` — year navigation is pills everywhere, never a
    dropdown or segmented control (segmented controls are for scope/mode).
    """
    if include_all:
        selected = st.pills(
            "Year",
            options=[all_label] + list(options),
            default=default or all_label,
            key=key,
            label_visibility="collapsed",
        )
        if not selected or selected == all_label:
            return None
        return int(selected)
    if skip_current and default is None:
        today_year = datetime.date.today().year
        default = next((y for y in options if int(y) < today_year), options[0])
    # Seed the pill's state once, then create the widget WITHOUT a default.
    # This keeps the same initial selection for every caller while letting
    # external code (e.g. the clickable "All years" rows in payments_panel)
    # set st.session_state[key] before this call to drive the pill — passing
    # both ``default=`` and a programmatic session_state value would warn.
    if key not in st.session_state:
        st.session_state[key] = default or options[0]
    selected = st.pills(
        "Year",
        options=options,
        key=key,
        label_visibility="collapsed",
    )
    return int(selected) if selected else int(options[0])


def render_notable_chips(
    names: list[str],
    available: list[str],
    key_prefix: str,
    session_key: str,
    cols: int = 2,
) -> bool:
    """Render quick-select chips for notable members. Returns True if any chip was clicked.

    names       — ordered list of notable member names to show
    available   — members actually in the dataset (filters names to this set)
    key_prefix  — unique prefix for button keys
    session_key — st.session_state key to write the selected name into

    Audit fix (2026-05-26, interests P1-3 / attendance P1-6): when two
    chips share a surname (e.g. "Michael Healy-Rae" and "Danny Healy-Rae"),
    the chip label was ambiguous — both rendered as "Healy-Rae" with only
    a hover tooltip distinguishing them. Hover doesn't work on mobile, so
    citizens couldn't tell them apart. Now: when a surname collides among
    visible chips, prepend the first initial (D. Healy-Rae, M. Healy-Rae).
    """
    visible = [n for n in names if n in available]
    if not visible:
        # No label without chips: an orphan "Notable members" heading captions
        # whatever control the page renders next (seen on TD Payments, where it
        # sat over the year pills in the 2026-07-17 visual audit).
        return False
    st.markdown('<p class="sidebar-label">Notable members</p>', unsafe_allow_html=True)
    # Count surnames to detect collisions among visible chips only.
    surname_counts: dict[str, int] = {}
    for n in visible:
        last = n.split()[-1] if n else n
        surname_counts[last] = surname_counts.get(last, 0) + 1  # logic_firewall: display_only

    def _chip_label(name: str) -> str:
        parts = name.split()
        if not parts:
            return name
        last = parts[-1]
        if surname_counts.get(last, 0) > 1 and len(parts) >= 2:
            first_initial = parts[0][:1].upper()
            return f"{first_initial}. {last}"
        return last

    chip_cols = st.columns(cols)
    for i, name in enumerate(visible):
        if chip_cols[i % cols].button(
            _chip_label(name), key=f"{key_prefix}_{name}", use_container_width=True, help=name
        ):
            st.session_state[session_key] = name
            return True
    return False


def info_card(
    html: str,
    *,
    min_height: str = "auto",
    padding: str = "0.55rem 0.9rem",
    border_radius: str = "6px",
    border_left_color: str = "rgba(0,0,0,0.14)",
    bg: str = "#ffffff",
) -> None:
    """Render a styled content card. No click behaviour.

    All visual properties are Python-level overrides — no CSS editing needed:
        border_radius      e.g. "12px" for rounder, "2px" for tight
        border_left_color  accent colour of the left border stripe
        padding            inner spacing, e.g. "0.3rem 0.7rem" for compact
        min_height         e.g. "4rem" to force a taller card
        bg                 background colour; default is pure white

    Use card_row() to add an adjacent → navigation button.
    """
    style = (
        f"min-height:{min_height};"
        f"padding:{padding};"
        f"border-radius:{border_radius};"
        f"border:1px solid rgba(0,0,0,0.08);"
        f"border-left:3px solid {border_left_color};"
        f"background:{bg};"
        f"box-shadow:0 1px 3px rgba(0,0,0,0.05);"
        f"box-sizing:border-box;width:100%;"
    )
    st.markdown(
        f'<div class="dt-info-card" style="{style}">{html}</div>',
        unsafe_allow_html=True,
    )


def card_row(
    html: str,
    *,
    btn_key: str,
    btn_label: str = "→",
    btn_help: str = "",
    col_ratio: tuple[int, int] = (14, 1),
    min_height: str = "auto",
    padding: str = "0.55rem 0.9rem",
    border_radius: str = "6px",
    border_left_color: str = "rgba(0,0,0,0.14)",
    bg: str = "#ffffff",
) -> bool:
    """Card + adjacent navigation button in a row. Returns True when button clicked.

    All info_card style params are forwarded. col_ratio controls the
    card-column vs button-column width split (default 14:1).

    Usage:
        if card_row(build_html(row), btn_key=f"row_{i}", btn_help=row["name"]):
            st.session_state["selected"] = row["name"]
            st.rerun()
    """
    card_col, btn_col = st.columns(col_ratio)
    with card_col:
        info_card(
            html,
            min_height=min_height,
            padding=padding,
            border_radius=border_radius,
            border_left_color=border_left_color,
            bg=bg,
        )
    btn_col.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
    return btn_col.button(btn_label, key=btn_key, help=btn_help)


def hero_banner(kicker: str, title: str, dek: str = "", badges: list[str] | None = None) -> None:
    dek_html = f'<p class="dt-dek">{_h(dek)}</p>' if dek else ""
    badge_html = ""
    if badges:
        badge_html = '<div style="display:flex;flex-wrap:wrap;gap:0.4rem;margin-top:0.65rem">'
        for b in badges:
            badge_html += f'<span class="dt-badge">{_h(b)}</span>'
        badge_html += "</div>"
    st.html(
        f'<div class="dt-hero">'
        f'<p class="dt-kicker">{_h(kicker)}</p>'
        f'<h1 style="margin:0.1rem 0 0.25rem;font-size:1.65rem;font-weight:700">{_h(title)}</h1>'
        f"{dek_html}"
        f"{badge_html}"
        f"</div>"
    )
