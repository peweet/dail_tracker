"""Stat fragments, profile header, sidebar pickers, pagination controls.

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.

DEVIATION: the brief names only ``stat_item, render_stat_strip, paginate,
pagination_controls, interest_declaration_item`` for this module.
``member_profile_header``, ``sidebar_date_range``, ``sidebar_member_filter``,
``clickable_card_link``, ``nav_button`` and ``_page_window`` sit in the same
contiguous span of the source file (between ``render_stat_strip`` and the
"Support page" section banner) and have no other natural home in the brief's
groupings, so they stay here as one contiguous cut rather than being split
into a module the brief didn't ask for. None of these functions call into
any other new submodule, so this keeps the module free of cross-module
imports.
"""

from __future__ import annotations

import datetime
from html import escape as _h

import streamlit as st


def stat_item(num, label: str) -> str:
    """Single stat HTML fragment — combine several inside render_stat_strip()."""
    return f'<div><div class="stat-num">{num}</div><div class="stat-lbl">{label}</div></div>'


def render_stat_strip(*items: str) -> None:
    """Render a .stat-strip row from stat_item() fragments."""
    st.markdown(f'<div class="stat-strip">{"".join(items)}</div>', unsafe_allow_html=True)


def member_profile_header(
    name: str,
    meta: str,
    badges_html: str = "",
    *,
    avatar_url: str | None = None,
    avatar_initials: str | None = None,
    avatar_credit_html: str | None = None,
) -> None:
    """Standard member name + meta header used on all profile views.

    avatar_url        — data URL or HTTP URL for the portrait. None falls back
                        to an initials chip.
    avatar_initials   — 1–2 letter fallback. Required when avatar_url is None.
    avatar_credit_html — inline attribution caption for CC BY / CC BY-SA.
                        Shown under the photo. None when no photo.
    """
    badges = f'<p style="margin:0.3rem 0 0.6rem;">{badges_html}</p>' if badges_html else ""

    if avatar_url:
        avatar_block = f'<img class="dt-profile-avatar" src="{_h(avatar_url)}" alt="" loading="lazy">'
        caption = f'<p class="dt-profile-avatar-credit">{avatar_credit_html}</p>' if avatar_credit_html else ""
    else:
        initials = _h(avatar_initials or "?")
        avatar_block = f'<span class="dt-profile-initials" aria-hidden="true">{initials}</span>'
        caption = '<p class="dt-profile-avatar-empty">No photo available</p>'

    st.markdown(
        f'<div class="dt-profile-header">'
        f'  <div class="dt-profile-avatar-col">{avatar_block}{caption}</div>'
        f'  <div class="dt-profile-meta-col">'
        f'    <p class="td-name">{name}</p>'
        f'    <p class="td-meta">{meta}</p>'
        f"    {badges}"
        f"  </div>"
        f"</div>",
        unsafe_allow_html=True,
    )


def sidebar_date_range(
    label: str,
    key: str,
    default_start: datetime.date | None = None,
    *,
    empty_default: bool = False,
) -> tuple[str | None, str | None]:
    """Date range picker for the sidebar. Returns (start_str, end_str) or (None, None).

    empty_default=True renders an empty input on first load (no pre-filled
    range) so the user is not committed to a date filter until they pick one.
    """
    if empty_default:
        value: tuple = ()
    else:
        start = default_start or datetime.date(2020, 1, 1)
        today = datetime.date.today()
        value = (start, today)
    st.markdown(f'<p class="sidebar-label">{label}</p>', unsafe_allow_html=True)
    date_val = st.date_input(
        label,
        value=value,
        label_visibility="collapsed",
        key=key,
    )
    if isinstance(date_val, (list, tuple)) and len(date_val) == 2:
        return str(date_val[0]), str(date_val[1])
    return None, None


def sidebar_member_filter(
    label: str,
    members: list[str],
    key_search: str,
    key_select: str,
    placeholder: str = "Search a member…",
) -> str | None:
    """Searchable member-picker for the sidebar. Returns selected name or None.

    Audit fix (2026-05-26, sidebar P1-2): collapsed from a
    ``st.text_input + st.selectbox`` pair to a single ``st.selectbox``.
    Streamlit's red-border "Press Enter to apply" hint on the text input
    led citizens to think Enter would commit a filter; in reality Enter
    only re-filtered the selectbox below and the user still had to click
    an option. Same fix as ``main_member_jump`` (interests P1-4).
    Streamlit's selectbox has built-in type-to-search, so the text input
    was doubly redundant.

    ``key_search`` is accepted for backwards compatibility but no longer
    creates a widget — the single ``key_select`` widget handles both
    typing and selection.
    """
    _ = key_search  # accepted for backwards compatibility; no widget
    st.html(f'<p class="sidebar-label">{_h(label)}</p>')
    options = [placeholder] + list(members)
    chosen = st.selectbox(
        label,
        options,
        index=0,
        key=key_select,
        label_visibility="collapsed",
    )
    return chosen if chosen and chosen != placeholder else None


def clickable_card_link(
    *,
    href: str,
    inner_html: str,
    aria_label: str,
    target: str = "_self",
    show_arrow: bool = True,
) -> str:
    """Wrap a card in a full-card-clickable link with an optional arrow.

    Uses the **stretched-link** pattern: the inner HTML is *not* nested
    inside the ``<a>``. Instead an empty ``<a>`` is absolute-positioned to
    cover the wrapper, so the whole card becomes the click target while
    inner interactive elements (e.g. an "Oireachtas ↗" link inside the
    card) remain independently clickable. The CSS in ``shared_css.py``
    (``.dt-card-link-wrap`` / ``.dt-card-link`` / ``.dt-card-arrow``)
    handles layering, hover lift + accent, and the arrow slide.

    Use when the page navigates via URL (e.g. ``?member=…`` query params)
    rather than session state + rerun. Returns an HTML string — collect
    several into a list and emit with ``st.html("\\n".join(...))``.

    Args:
        href:        URL the card navigates to.
        inner_html:  Card HTML (e.g. from ``member_card_html()`` /
                     ``committee_row_html()`` / a custom builder). Inner
                     ``<a>`` / ``<button>`` elements automatically sit
                     above the stretched link via the shared CSS.
        aria_label:  Spoken description of the link target.
        target:      ``"_self"`` (same tab, default) or ``"_blank"``.
        show_arrow:  Render the decorative right-edge arrow. Default True.
    """
    arrow = '<span class="dt-card-arrow" aria-hidden="true">→</span>' if show_arrow else ""
    return (
        f'<div class="dt-card-link-wrap">'
        f'<a class="dt-card-link" href="{_h(href)}" target="{_h(target)}" '
        f'aria-label="{_h(aria_label)}"></a>'
        f"{inner_html}"
        f"{arrow}"
        f"</div>"
    )


def nav_button(
    *,
    key: str,
    help: str | None = None,
    label: str = "→",
) -> bool:
    """Standard square arrow button used beside list cards.

    Renders a marker div + button. The CSS in ``shared_css.py`` (``.dt-nav-btn``
    rules) forces a uniform 2.1rem × 2.1rem square and centers the button
    vertically inside its column, so it lines up against multi-line cards
    regardless of card height.

    Place inside the second column of a ``[N, 1]`` columns row whose first
    column holds the card. Returns ``True`` when clicked.
    """
    st.html('<div class="dt-nav-btn"></div>')
    return st.button(label, key=key, help=help)


def _page_window(current: int, total: int) -> list[int | str]:
    """Stable page set with leading/trailing ellipses for a 1-indexed pager.

    For ``total > 7`` always returns **exactly 7 elements** (page 1, an
    optional "…", three inner pages around current, an optional "…", and
    the last page) so the pager's column count is constant — chips don't
    shift left/right when the user clicks between pages.

    Always shows page 1 and the last page. For ``total <= 7`` returns
    every page (no truncation needed).
    """
    if total <= 7:
        return list(range(1, total + 1))
    if current <= 4:
        # Near the start: [1, 2, 3, 4, 5, …, total]
        return [1, 2, 3, 4, 5, "…", total]
    if current >= total - 3:
        # Near the end: [1, …, total-4, total-3, total-2, total-1, total]
        return [1, "…", total - 4, total - 3, total - 2, total - 1, total]
    # Middle: [1, …, current-1, current, current+1, …, total]
    return [1, "…", current - 1, current, current + 1, "…", total]


def paginate(
    total: int,
    *,
    key_prefix: str,
    page_size: int,
) -> int:
    """Resolve the current 0-indexed page from session state, without rendering.

    Use this when you want pagination controls rendered *below* the content:
    call ``paginate()`` to get the page index, slice and render your data,
    then call :func:`pagination_controls` with the same ``key_prefix`` and a
    matching ``page_sizes``/``default_page_size`` to draw the controls.

    The ``page_size`` passed here must match the size used by the eventual
    :func:`pagination_controls` call so the two agree on ``total_pages``.

    Returns the 0-indexed page; slice with
    ``df.iloc[page_idx*page_size : (page_idx+1)*page_size]``.
    """
    size_key = f"{key_prefix}_size"
    page_key = f"{key_prefix}_page"

    # Seed size so pagination_controls (called later) sees the same value.
    if size_key not in st.session_state:
        st.session_state[size_key] = int(page_size)

    total_pages = max(1, (total + page_size - 1) // page_size)
    cur = int(st.session_state.get(page_key, 1))
    if cur > total_pages:
        cur = 1
        st.session_state[page_key] = 1
    return cur - 1


def pagination_controls(
    total: int,
    *,
    key_prefix: str,
    page_sizes: tuple[int, ...] = (25, 50, 100),
    default_page_size: int = 25,
    label: str = "results",
    show_caption: bool = True,
) -> tuple[int, int]:
    """Reusable pagination row: page chips + "Showing X–Y of Z" caption + size selector.

    Args:
        total: total number of items across all pages.
        key_prefix: namespace for session-state keys; pass a stable, unique
            string (often including a record id, e.g. ``f"td_hist_{member_id}"``).
        page_sizes: options for the per-page selector.
        default_page_size: initial selection.
        label: noun used in the "Showing X–Y of Z {label}" caption,
            e.g. ``"votes"``, ``"members"``, ``"declarations"``. Pass the plural.
        show_caption: set False to suppress the "Showing X–Y of Z {label}" line
            (useful when the caller already shows a count above).

    Returns:
        ``(page_size, page_idx)`` where ``page_idx`` is **0-indexed**.
        Slice the dataframe with ``df.iloc[page_idx*size : (page_idx+1)*size]``.

    To render the controls *below* the content, pair this with :func:`paginate`:
    call ``paginate()`` first to get the page index, render your rows, then
    call ``pagination_controls()`` with matching ``key_prefix`` / sizes.
    """
    size_key = f"{key_prefix}_size"
    page_key = f"{key_prefix}_page"

    if size_key not in st.session_state:
        st.session_state[size_key] = default_page_size

    page_size = int(st.session_state[size_key])
    total_pages = max(1, (total + page_size - 1) // page_size)

    cur = int(st.session_state.get(page_key, 1))
    if cur > total_pages:
        cur = 1
        st.session_state[page_key] = 1

    start = (cur - 1) * page_size + 1 if total else 0
    end = min(cur * page_size, total)

    show_size_picker = len(page_sizes) > 1
    if show_size_picker:
        nav_col, size_col = st.columns([3, 1])
    else:
        # Always wrap in a column so the .dt-pager CSS selector
        # ([data-testid="stColumn"]:has(> div .dt-pager)) matches and the
        # page chips collapse to tight spacing instead of equal-width columns.
        (nav_col,) = st.columns([1])
        size_col = None

    with nav_col:
        # Marker element so .dt-pager CSS can target buttons in this column via :has().
        st.html('<div class="dt-pager"></div>')
        if total_pages > 1:
            window = _page_window(cur, total_pages)
            btn_cols = st.columns(len(window) + 2, gap="small")
            if btn_cols[0].button(
                "‹",
                key=f"{key_prefix}_prev",
                disabled=(cur <= 1),
                help="Previous page",
            ):
                st.session_state[page_key] = cur - 1
                st.rerun()
            for i, p in enumerate(window, start=1):
                if p == "…":
                    with btn_cols[i]:
                        st.html('<div class="dt-pager-ellipsis">…</div>')
                    continue
                is_cur = p == cur
                if is_cur:
                    with btn_cols[i]:
                        st.html(f'<div class="dt-pager-current">{p}</div>')
                else:
                    if btn_cols[i].button(str(p), key=f"{key_prefix}_p_{p}"):
                        st.session_state[page_key] = int(p)
                        st.rerun()
            if btn_cols[-1].button(
                "›",
                key=f"{key_prefix}_next",
                disabled=(cur >= total_pages),
                help="Next page",
            ):
                st.session_state[page_key] = cur + 1
                st.rerun()

        if total > 0 and show_caption:
            st.html(
                f'<div class="dt-pager-caption">'
                f"Showing <strong>{start:,}–{end:,}</strong> of "
                f"<strong>{total:,}</strong> {_h(label)}"
                f"</div>"
            )

    if size_col is not None:
        with size_col:
            st.html('<div class="dt-pager-size-label">Per page</div>')
            new_size = st.segmented_control(
                "Per page",
                options=list(page_sizes),
                default=page_size,
                key=f"{key_prefix}_size_widget",
                label_visibility="collapsed",
            )
            if new_size and int(new_size) != page_size:
                st.session_state[size_key] = int(new_size)
                st.session_state[page_key] = 1
                st.rerun()

    return page_size, max(0, cur - 1)


def interest_declaration_item(text: str, status: str = "unchanged") -> None:
    """Render one interest declaration row with year-on-year diff styling.

    status: 'new' | 'removed' | 'unchanged'
    """
    if status == "new":
        wrap = "background:#f0fdf4;border-left:3px solid #16a34a;"
        badge = '<span class="int-diff-badge-new">NEW</span> '
        body = _h(text)
    elif status == "removed":
        wrap = "background:#fef2f2;border-left:3px solid #dc2626;opacity:0.82;"
        badge = '<span class="int-diff-badge-removed">REMOVED</span> '
        body = f"<s>{_h(text)}</s>"
    else:
        wrap = "border-bottom:1px solid var(--dt-border);"
        badge = ""
        body = _h(text)
    st.html(
        f'<div style="{wrap}padding:0.4rem 0.65rem;margin:0.1rem 0;'
        f'border-radius:0 4px 4px 0;font-size:0.9rem;line-height:1.55;">'
        f"{badge}{body}</div>"
    )
