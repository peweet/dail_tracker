"""Shared UI components for Dáil Tracker Streamlit pages (v5)."""
from __future__ import annotations
import datetime
from html import escape as _h
import streamlit as st


def clean_meta(*parts: str) -> str:
    """Join non-empty, non-NaN string parts with ' · '."""
    return " · ".join(p for p in parts if p and p.lower() not in ("nan", ""))


def sidebar_page_header(title: str, kicker: str = "Dáil Tracker") -> None:
    """Standardised sidebar kicker + page title block. title may contain <br>."""
    st.markdown(f'<p class="page-kicker">{kicker}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="page-title">{title}</p>', unsafe_allow_html=True)


def year_selector(
    options: list[str],
    key: str,
    default: str | None = None,
    skip_current: bool = True,
) -> int:
    """Year pill selector. Defaults to most recent completed year when skip_current=True.

    Returns the selected year as int.
    """
    if skip_current and default is None:
        today_year = datetime.date.today().year
        default = next((y for y in options if int(y) < today_year), options[0])
    selected = st.pills(
        "Year",
        options=options,
        default=default or options[0],
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
    """
    st.markdown('<p class="sidebar-label">Notable members</p>', unsafe_allow_html=True)
    visible = [n for n in names if n in available]
    chip_cols = st.columns(cols)
    for i, name in enumerate(visible):
        if chip_cols[i % cols].button(name.split()[-1], key=f"{key_prefix}_{name}", use_container_width=True, help=name):
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


def stat_strip(stats: list[tuple[str, str, str]]) -> None:
    """Render evidence stats. Each stat is (value, label, colour). Reuses .stat-strip CSS."""
    items = ""
    for value, label, colour in stats:
        items += (
            f'<div><div class="stat-num" style="color:{colour}">{value}</div>'
            f'<div class="stat-lbl">{label}</div></div>'
        )
    st.html(f'<div class="stat-strip">{items}</div>')


def outcome_badge(outcome: str) -> str:
    s = _h(outcome)
    if outcome == "Carried":
        return f'<span class="dt-outcome-carried">{s}</span>'
    if outcome == "Lost":
        return f'<span class="dt-outcome-lost">{s}</span>'
    return f'<span class="dt-outcome-unknown">{s or "—"}</span>'


def evidence_heading(text: str) -> None:
    st.markdown(f'<p class="section-heading">{text}</p>', unsafe_allow_html=True)


def todo_callout(message: str) -> None:
    st.markdown(
        f'<div class="dt-callout"><strong>Not yet available.</strong><br>'
        f'<code>TODO_PIPELINE_VIEW_REQUIRED</code>: {message}</div>',
        unsafe_allow_html=True,
    )


def empty_state(heading: str, body: str) -> None:
    st.html(
        f'<div class="dt-callout"><strong>{_h(heading)}</strong><br>'
        f'<span style="color:var(--text-meta)">{_h(body)}</span></div>'
    )


def back_button(label: str, key: str, *, help: str | None = None) -> bool:
    """Pill-shaped, dark-navy back button that stands out against the beige page bg.

    Pass any unique key — it is auto-prefixed with `dt_back_` so the single CSS
    rule in shared_css.py styles every back button consistently.
    """
    return st.button(label, key=f"dt_back_{key}", help=help)


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


def member_card_html(
    name: str,
    meta: str = "",
    rank: int | None = None,
    pills_html: str = "",
    badge_html: str = "",
    avatar_url: str | None = None,
) -> str:
    """Canonical member name card HTML string.

    Avatar slot is always rendered at fixed width (2.25 rem).  When avatar_url
    is None the slot shows the rank number.  Wiring in Wikidata photos later
    is a one-line change — zero layout rework across pages.

    pills_html  — raw HTML for pill <span> elements (use int-stat-pill class)
    badge_html  — optional right-side metric; use dt-name-card-badge-metric
                  sub-class for the standard blue days/amount style
    """
    if avatar_url:
        left_inner = f'<img class="dt-name-card-avatar" src="{_h(avatar_url)}" alt="">'
    elif rank is not None:
        rank_cls   = "dt-name-card-rank dt-name-card-rank-top" if rank <= 3 else "dt-name-card-rank"
        left_inner = f'<span class="{rank_cls}">#{rank}</span>'
    else:
        left_inner = ""
    meta_html    = f'<div class="dt-name-card-meta">{_h(meta)}</div>' if meta else ""
    pills_sec    = f'<div class="dt-name-card-pills">{pills_html}</div>' if pills_html else ""
    badge_sec    = f'<div class="dt-name-card-badge">{badge_html}</div>' if badge_html else ""
    return (
        f'<div class="dt-name-card">'
        f'<div class="dt-name-card-left">{left_inner}</div>'
        f'<div class="dt-name-card-body">'
        f'<div class="dt-name-card-name">{_h(name)}</div>'
        f'{meta_html}{pills_sec}'
        f'</div>'
        f'{badge_sec}'
        f'</div>'
    )


def rank_card_row(
    name: str,
    meta: str,
    pills: list[str],
    btn_key: str,
    rank: int | None = None,
    quote: str = "",
    btn_help: str = "",
    col_ratio: tuple[int, int] = (14, 1),
) -> bool:
    """Name card + navigation arrow. Returns True when the arrow is clicked.

    rank  — pass an int to show the #N badge (gold for top 3); omit for unranked lists.
    quote — optional italic snippet shown below the pills (e.g. top declaration text).
    Caller is responsible for navigation + st.rerun() on True.
    """
    card_col, btn_col = st.columns(col_ratio)
    pills_html = "".join(f'<span class="int-stat-pill">{p}</span>' for p in pills)
    if quote:
        pills_html += f'<p class="int-highlight-quote">{quote}</p>'
    card_col.markdown(
        member_card_html(name=name, meta=meta, rank=rank, pills_html=pills_html),
        unsafe_allow_html=True,
    )
    btn_col.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
    return btn_col.button("→", key=btn_key, help=btn_help or f"View {name}")


def stat_item(num, label: str) -> str:
    """Single stat HTML fragment — combine several inside render_stat_strip()."""
    return f'<div><div class="stat-num">{num}</div><div class="stat-lbl">{label}</div></div>'


def render_stat_strip(*items: str) -> None:
    """Render a .stat-strip row from stat_item() fragments."""
    st.markdown(f'<div class="stat-strip">{"".join(items)}</div>', unsafe_allow_html=True)


def member_profile_header(name: str, meta: str, badges_html: str = "") -> None:
    """Standard member name + meta header used on all profile views."""
    badges = f'<p style="margin:0.3rem 0 0.6rem;">{badges_html}</p>' if badges_html else ""
    st.markdown(
        f'<p class="td-name">{name}</p>'
        f'<p class="td-meta">{meta}</p>'
        f'{badges}',
        unsafe_allow_html=True,
    )


def sidebar_date_range(
    label: str,
    key: str,
    default_start: datetime.date | None = None,
) -> tuple[str | None, str | None]:
    """Date range picker for the sidebar. Returns (start_str, end_str) or (None, None)."""
    start = default_start or datetime.date(2020, 1, 1)
    today = datetime.date.today()
    st.markdown(f'<p class="sidebar-label">{label}</p>', unsafe_allow_html=True)
    date_val = st.date_input(
        label,
        value=(start, today),
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
    """Search input + selectbox for choosing a member. Returns selected name or None."""
    st.markdown(f'<p class="sidebar-label">{label}</p>', unsafe_allow_html=True)
    search = st.text_input(
        label,
        placeholder=placeholder,
        key=key_search,
        label_visibility="collapsed",
    )
    sq = search.strip().lower()
    filtered = [m for m in members if sq in m.lower()] if sq else members
    chosen = st.selectbox(
        label,
        ["— select —"] + filtered,
        key=key_select,
        label_visibility="collapsed",
    )
    return chosen if chosen and chosen != "— select —" else None


def _page_window(current: int, total: int) -> list[int | str]:
    """Compact page set with leading/trailing ellipses for a 1-indexed pager.

    Always shows page 1 and the last page. Shows current ±1.
    Inserts "…" when there is a gap.
    """
    if total <= 7:
        return list(range(1, total + 1))
    pages: list[int | str] = [1]
    left  = max(2, current - 1)
    right = min(total - 1, current + 1)
    if left > 2:
        pages.append("…")
    for p in range(left, right + 1):
        pages.append(p)
    if right < total - 1:
        pages.append("…")
    pages.append(total)
    return pages


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
    """
    size_key = f"{key_prefix}_size"
    page_key = f"{key_prefix}_page"

    if size_key not in st.session_state:
        st.session_state[size_key] = default_page_size

    page_size   = int(st.session_state[size_key])
    total_pages = max(1, (total + page_size - 1) // page_size)

    cur = int(st.session_state.get(page_key, 1))
    if cur > total_pages:
        cur = 1
        st.session_state[page_key] = 1

    start = (cur - 1) * page_size + 1 if total else 0
    end   = min(cur * page_size, total)

    nav_col, size_col = st.columns([3, 1])

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
                is_cur = (p == cur)
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
                f'Showing <strong>{start:,}–{end:,}</strong> of '
                f'<strong>{total:,}</strong> {_h(label)}'
                f'</div>'
            )

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
        wrap  = "background:#f0fdf4;border-left:3px solid #16a34a;"
        badge = '<span class="int-diff-badge-new">NEW</span> '
        body  = _h(text)
    elif status == "removed":
        wrap  = "background:#fef2f2;border-left:3px solid #dc2626;opacity:0.82;"
        badge = '<span class="int-diff-badge-removed">REMOVED</span> '
        body  = f"<s>{_h(text)}</s>"
    else:
        wrap  = "border-bottom:1px solid var(--dt-border);"
        badge = ""
        body  = _h(text)
    st.html(
        f'<div style="{wrap}padding:0.4rem 0.65rem;margin:0.1rem 0;'
        f'border-radius:0 4px 4px 0;font-size:0.9rem;line-height:1.55;">'
        f"{badge}{body}</div>"
    )
