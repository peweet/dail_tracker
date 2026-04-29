"""Shared UI components for Dáil Tracker Streamlit pages (v5)."""
from __future__ import annotations
import datetime
import streamlit as st


def scroll_to_top() -> None:
    """Scroll the app viewport to the top. Call at the start of any detail-view render."""
    st.markdown(
        '<script>window.parent.document.querySelector('
        '"[data-testid=stAppViewContainer]").scrollTo(0,0);</script>',
        unsafe_allow_html=True,
    )


def clickable_card(html: str, key: str, help: str = "") -> bool:
    """Render an HTML card where the entire surface is clickable.

    Usage pattern (copy for any new page):
        1. Build card HTML with class="dt-clickable-card" on the outer div.
        2. Call clickable_card(html, unique_key) inside a loop.
        3. Handle the True return: set session state + st.rerun().

    Mechanism: a transparent full-coverage Streamlit button is overlaid on the
    card via CSS (shared_css.py). Clicking anywhere on the card surface is a
    genuine button click — no JS needed. The CSS selector that makes this work:
    [data-testid="stVerticalBlock"]:has(.dt-clickable-card):not(:has([data-testid="stVerticalBlock"]))
    targets the innermost stVerticalBlock (the st.container() wrapper) without
    depending on Streamlit's internal intermediate div class names.
    """
    with st.container():
        st.markdown(html, unsafe_allow_html=True)
        return st.button(" ", key=key, help=help)


def hero_banner(kicker: str, title: str, dek: str, badges: list[str] | None = None) -> None:
    badge_html = ""
    if badges:
        badge_html = '<div style="display:flex;flex-wrap:wrap;gap:0.4rem;margin-top:0.65rem">'
        for b in badges:
            badge_html += f'<span class="dt-badge">{b}</span>'
        badge_html += "</div>"
    st.markdown(
        f'<div class="dt-hero">'
        f'<p class="dt-kicker">{kicker}</p>'
        f'<h1 style="margin:0.1rem 0 0.25rem;font-size:1.65rem;font-weight:700">{title}</h1>'
        f'<p class="dt-dek">{dek}</p>'
        f"{badge_html}"
        f"</div>",
        unsafe_allow_html=True,
    )


def stat_strip(stats: list[tuple[str, str, str]]) -> None:
    """Render evidence stats. Each stat is (value, label, colour). Reuses .stat-strip CSS."""
    items = ""
    for value, label, colour in stats:
        items += (
            f'<div><div class="stat-num" style="color:{colour}">{value}</div>'
            f'<div class="stat-lbl">{label}</div></div>'
        )
    st.markdown(f'<div class="stat-strip">{items}</div>', unsafe_allow_html=True)


def outcome_badge(outcome: str) -> str:
    if outcome == "Carried":
        return f'<span class="dt-outcome-carried">{outcome}</span>'
    if outcome == "Lost":
        return f'<span class="dt-outcome-lost">{outcome}</span>'
    return f'<span class="dt-outcome-unknown">{outcome or "—"}</span>'


def evidence_heading(text: str) -> None:
    st.markdown(f'<p class="section-heading">{text}</p>', unsafe_allow_html=True)


def todo_callout(message: str) -> None:
    st.markdown(
        f'<div class="dt-callout"><strong>Not yet available.</strong><br>'
        f'<code>TODO_PIPELINE_VIEW_REQUIRED</code>: {message}</div>',
        unsafe_allow_html=True,
    )


def empty_state(heading: str, body: str) -> None:
    st.markdown(
        f'<div class="dt-callout"><strong>{heading}</strong><br>'
        f'<span style="color:var(--text-meta)">{body}</span></div>',
        unsafe_allow_html=True,
    )


def rank_card_row(
    name: str,
    meta: str,
    pills: list[str],
    btn_key: str,
    rank: int | None = None,
    quote: str = "",
    btn_help: str = "",
    col_ratio: tuple[int, int] = (5, 1),
) -> bool:
    """Name card + navigation arrow. Returns True when the arrow is clicked.

    rank  — pass an int to show the #N badge (gold for top 3); omit for unranked lists.
    quote — optional italic snippet shown below the pills (e.g. top declaration text).
    Caller is responsible for navigation + st.rerun() on True.
    """
    card_col, btn_col = st.columns(col_ratio)
    pills_html = "".join(f'<span class="int-stat-pill">{p}</span>' for p in pills)
    if rank is not None:
        rank_cls  = "int-rank-num int-rank-num-top" if rank <= 3 else "int-rank-num"
        rank_html = f'<div class="{rank_cls}">#{rank}</div>'
    else:
        rank_html = ""
    quote_html = f'<p class="int-highlight-quote">{quote}</p>' if quote else ""
    card_col.markdown(
        f'<div class="int-rank-card">'
        f'{rank_html}'
        f'<div class="int-rank-body">'
        f'<p class="int-rank-name">{name}</p>'
        f'<p class="int-rank-meta">{meta}</p>'
        f'<div class="int-rank-stats">{pills_html}</div>'
        f'{quote_html}'
        f'</div></div>',
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


def interest_declaration_item(text: str, status: str = "unchanged") -> None:
    """Render one interest declaration row with year-on-year diff styling.

    status: 'new' | 'removed' | 'unchanged'
    """
    if status == "new":
        wrap  = "background:#f0fdf4;border-left:3px solid #16a34a;"
        badge = '<span class="int-diff-badge-new">NEW</span> '
        body  = text
    elif status == "removed":
        wrap  = "background:#fef2f2;border-left:3px solid #dc2626;opacity:0.82;"
        badge = '<span class="int-diff-badge-removed">REMOVED</span> '
        body  = f"<s>{text}</s>"
    else:
        wrap  = "border-bottom:1px solid var(--dt-border);"
        badge = ""
        body  = text
    st.markdown(
        f'<div style="{wrap}padding:0.4rem 0.65rem;margin:0.1rem 0;'
        f'border-radius:0 4px 4px 0;font-size:0.9rem;line-height:1.55;">'
        f"{badge}{body}</div>",
        unsafe_allow_html=True,
    )
