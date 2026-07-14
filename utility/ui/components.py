
# ── SECTION MAP ── ─────────────────────────────────────────
# ⚠️  DO NOT READ WHOLE — ~17,878 tokens (1,838 lines after this header).
#     Read this map, then jump:  Read(file, offset=<start>, limit=<n>)
#
#      78-85     clean_meta
#      86-109    search_normalise
#     110-121    search_matches
#     122-139    text_search_mask
#     140-173    period_year_pills
#     174-192    fmt_civic_date
#     193-231    page_error_boundary
#     232-281    year_selector
#     282-330    render_notable_chips
#     331-366    info_card
#     367-403    card_row
#     404-421    hero_banner
#     422-448    finding_lede
#     449-461    _bn_eur
#     462-500    render_national_finance_context
#     501-516    card_sources_html
#     517-535    glossary_strip
#     536-568    totals_strip
#     569-588    stat_strip
#     589-597    outcome_badge
#     598-615    evidence_heading
#     616-627    subsection_heading
#     628-684    todo_callout
#     685-691    empty_state
#     692-770    member_moved_callout
#     771-779    back_button
#     780-814    main_member_jump
#     815-824    field_label
#     825-852    hide_sidebar
#     853-884    member_jump_panel
#     885-915    filter_bar
#     916-973    breadcrumb
#     974-984    pill
#     985-1042   member_card_html
#    1043-1089   ranked_member_card
#    1090-1142   rank_card_row
#    1143-1148   party_colour
#    1149-1201   party_stripe_html
#    1202-1242   proportion_stripe_html
#    1243-1295   committee_row_html
#    1296-1371   committee_identity_strip
#    1372-1401   find_a_td_search
#    1402-1461   find_a_td_filter
#    1462-1466   stat_item
#    1467-1471   render_stat_strip
#    1472-1511   member_profile_header
#    1512-1541   sidebar_date_range
#    1542-1576   sidebar_member_filter
#    1577-1619   clickable_card_link
#    1620-1639   nav_button
#    1640-1662   _page_window
#    1663-1696   paginate
#    1697-1815   pagination_controls
#    1816-1838   interest_declaration_item
# ── END SECTION MAP ── ─────────────────────────────────
"""Shared UI components for Dáil Tracker Streamlit pages (v5)."""

from __future__ import annotations

import datetime
import functools
import logging
import re
import traceback
from contextlib import contextmanager
from html import escape as _h

import streamlit as st

_log = logging.getLogger(__name__)


def clean_meta(*parts: str) -> str:
    """Join non-empty, non-NaN string parts with ' · '."""
    return " · ".join(p for p in parts if p and p.lower() not in ("nan", ""))


_SEARCH_WS_RE = re.compile(r"\s+")


def search_normalise(value):
    """Normalise text for tolerant, crash-proof substring search.

    Lowercases, turns hyphens into spaces, and collapses runs of whitespace,
    so a plain term like "Dublin South West" matches stored "Dublin
    South-West" (and vice versa), and a stray double space never blocks a
    match. Accepts a ``str`` or a pandas ``Series`` and returns the same type.

    Always pair the result with ``str.contains(..., regex=False)`` (or use
    :func:`text_search_mask`): user input is treated as a literal, so a term
    containing regex metacharacters ("(", "*", "+") never raises.
    """
    if isinstance(value, str):
        return _SEARCH_WS_RE.sub(" ", value.replace("-", " ")).strip().lower()
    # pandas Series — normalise element-wise without importing pandas here.
    return (
        value.astype(str)
        .str.replace("-", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.lower()
    )


def search_matches(query: str, *fields: str) -> bool:
    """True if the normalised ``query`` is a substring of any normalised field.

    Scalar counterpart to :func:`text_search_mask` for list-comprehension
    filters. Blank/whitespace query matches everything.
    """
    q = search_normalise(query or "")
    if not q:
        return True
    return any(q in search_normalise(f or "") for f in fields)


def text_search_mask(df, query, columns):
    """Boolean mask selecting rows where any of ``columns`` contains ``query``.

    Tolerant (hyphen/space/case-insensitive) and regex-safe — see
    :func:`search_normalise`. Returns an all-True mask when ``query`` is blank,
    so callers can apply it unconditionally.
    """
    import pandas as pd

    q = search_normalise(query or "")
    if not q:
        return pd.Series(True, index=df.index)
    mask = pd.Series(False, index=df.index)
    for col in columns:
        mask |= search_normalise(df[col]).str.contains(q, na=False, regex=False)
    return mask


def period_year_pills(df, key: str) -> tuple[str | None, str | None]:
    """Year filter pills above a lobbying-style returns table.

    Reads unique years from ``df["period_start_date"]`` (datetime-like or
    string) and renders ``st.pills`` with "All years" + each year (pills, not
    a segmented control — year navigation is pills app-wide).
    Returns a SQL-ready ``(start_iso, end_iso)`` tuple, or ``(None, None)``
    when "All years" is selected or when no years can be derived. Selection is
    pushed back to SQL via the returned tuple — callers do no pandas row
    masking on the year here.

    Used to be byte-equivalent ``_year_pills`` / ``_year_selector`` in
    lobbying_2 and lobbying_3.
    """
    import pandas as pd

    if df.empty or "period_start_date" not in df.columns:
        return None, None
    try:
        years = sorted(
            pd.to_datetime(df["period_start_date"], errors="coerce").dropna().dt.year.unique().tolist(),
            reverse=True,
        )
    except Exception:
        return None, None
    if not years:
        return None, None
    options = ["All years"] + [str(y) for y in years]
    chosen = st.pills("Year", options, default=options[0], key=key, label_visibility="collapsed") or options[0]
    if chosen == "All years":
        return None, None
    return f"{chosen}-01-01", f"{chosen}-12-31"


def fmt_civic_date(val) -> str:
    """Format a date for civic display as ``"7 Jul 2024"`` (no leading zero on
    the day). Used across legislation, statutory instruments, votes, and
    lobbying for a consistent ink-on-paper date shape.

    Returns ``"—"`` for None / NaN; passes through unparseable values as
    ``str(val)`` so SQL strings or already-formatted labels don't blow up.
    """
    import pandas as pd  # local import keeps pages that never call this off the pandas import path

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        ts = pd.Timestamp(val)
        return f"{ts.day} {ts.strftime('%b %Y')}"
    except Exception:
        return str(val)


def page_error_boundary(page_fn):
    """Decorator: catch any unhandled exception in a page entry point and
    show a calm civic-voice empty_state instead of Streamlit's red traceback.

    Logs full traceback for debugging; exposes a brief technical summary in
    a collapsed expander so journalists/devs can paste it into a GitHub issue.
    Only catches Exception (not BaseException), so st.stop() and Ctrl+C work.
    """

    @functools.wraps(page_fn)
    def wrapper(*args, **kwargs):
        try:
            return page_fn(*args, **kwargs)
        except Exception as exc:
            tb = traceback.format_exc()
            _log.exception("page entry crashed: %s", page_fn.__name__)
            try:
                from shared_css import inject_css

                inject_css()
            except Exception:
                pass
            st.html(
                '<div class="dt-callout">'
                "<strong>Something went wrong rendering this page.</strong><br>"
                '<span style="color:var(--text-meta)">'
                "Try refreshing. If it persists, the underlying view may be "
                "missing or the data file may be stale. "
                f"({_h(type(exc).__name__)})"
                "</span>"
                "</div>"
            )
            with st.expander("Technical details", expanded=False):
                st.code(tb, language="text")
            return None

    return wrapper


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
    st.markdown('<p class="sidebar-label">Notable members</p>', unsafe_allow_html=True)
    visible = [n for n in names if n in available]
    # Count surnames to detect collisions among visible chips only.
    surname_counts: dict[str, int] = {}
    for n in visible:
        last = n.split()[-1] if n else n
        surname_counts[last] = surname_counts.get(last, 0) + 1

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


def finding_lede(sentences: list[str], *, source_html: str = "") -> None:
    """The page's opening findings — the app-wide replacement for stat strips.

    Renders 1–3 plain-English sentences under the hero, each stating a fact the
    page's data supports ("Deloitte Ireland has won more public contracts than
    any other firm — <strong>329</strong> since 2013, from <strong>54</strong>
    public bodies."). Numbers go inside ``<strong>`` for the tabular-figure
    emphasis treatment; everything else reads as prose. This is the
    findings-not-filters pattern from doc/archive/APP_REDESIGN_SWEEP_2026_06_10.md:
    the page opens by answering its own headline question, and the controls
    come after the first facts.

    DISPLAY-ONLY: every figure must arrive pre-computed from a registered view
    via ``dail_tracker_core/queries``; this helper renders, it never derives.
    Sentences are already-built HTML — escape free-text tokens with ``_h()``
    at the call site before interpolating.

    ``source_html``: optional pre-built anchor(s) from ``source_link_html()``;
    rendered as a quiet trailing source attribution on the last line.
    """
    if not sentences:
        return
    body = "".join(f"<p>{s}</p>" for s in sentences if s)
    src = f'<span class="dt-lede-source">{source_html}</span>' if source_html else ""
    st.html(f'<div class="dt-finding-lede">{body}{src}</div>')


def _bn_eur(val) -> str:
    """Compact national-scale euro: €133.8bn / €149.0bn."""
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if abs(n) >= 1_000_000_000:
        return f"€{n / 1_000_000_000:.1f}bn"
    if abs(n) >= 1_000_000:
        return f"€{n / 1_000_000:.0f}m"
    return f"€{n:,.0f}"


def render_national_finance_context(*, year: int | None = None, note: str = "") -> None:
    """Reusable national-scale anchor from the (previously orphaned) ``v_gov_finance_annual``
    view: the State's total general-government revenue / expenditure / balance for one year,
    as a denominator a reader can eyeball big public-money figures against.

    DELIBERATELY NOT a computed "% of total spend": general-government expenditure is a
    whole-economy national-accounts measure, NOT a clean superset of any single register here
    (published over-€20k payments, contract awards, etc. mix bases and tiers). Stating it as a
    share would be the "never mix registers" trap. So this renders the denominator as context
    only, with that caveat. Silently no-ops if the view is unavailable. ``note`` appends a
    page-specific framing sentence.
    """
    # Lazy import keeps ui/ free of a hard data_access dependency at module load.
    from data_access.publicfinance_data import fetch_gov_finance_annual_result

    res = fetch_gov_finance_annual_result()
    if not res.ok or res.data.empty:
        return
    df = res.data  # newest-first
    row = df[df["year"] == year] if year is not None else df.head(1)
    if row.empty:
        row = df.head(1)
    r = row.iloc[0]
    yr = int(r["year"])
    rev, exp, bal = r.get("revenue_eur"), r.get("expenditure_eur"), r.get("surplus_deficit_eur")
    balance_word = "surplus" if (bal is not None and float(bal) >= 0) else "deficit"
    extra = f" {_h(note)}" if note else ""
    st.html(
        '<div class="dt-natfin">'
        f'<span class="dt-natfin-k">National scale · {yr}</span>'
        f'<span class="dt-natfin-v">Total government spending <strong>{_bn_eur(exp)}</strong> · '
        f"revenue <strong>{_bn_eur(rev)}</strong> · "
        f"<strong>{_bn_eur(abs(float(bal)) if bal is not None else None)}</strong> {balance_word}</span>"
        '<span class="dt-natfin-c">A whole-economy national-accounts measure (CSO) — context for the '
        f"figures here, not a total they sum into.{extra}</span>"
        "</div>"
    )


def card_sources_html(links: list[str]) -> str:
    """Quiet conduit row for a card footer — splice into card HTML.

    Pass pre-built anchors from ``source_link_html()`` (which no-ops to ``""``
    on missing/non-http URLs); empties are dropped here, and the whole row
    collapses to ``""`` when nothing survives, so callers can interpolate the
    result unconditionally. One consistent placement app-wide: the conduit
    principle says every card that represents an official record links to that
    record at its official source.
    """
    kept = [x for x in links if x]
    if not kept:
        return ""
    return f'<div class="dt-card-sources">{"".join(kept)}</div>'


def glossary_strip(terms: list[tuple[str, str]]) -> None:
    """Render a one-line glossary of acronyms under the hero.

    Each entry is (acronym, expansion). The strip is small, secondary,
    designed for first-time citizen readers who don't know "TD" or "DPO".
    Journalists ignore it; citizens don't have to Google.

    Usage:
        glossary_strip([
            ("TD", "Teachta Dála (member of the Dáil)"),
            ("DPO", "Designated Public Official"),
        ])
    """
    if not terms:
        return
    items = "".join(f'<span class="dt-glossary-term"><b>{_h(a)}</b> {_h(d)}</span>' for a, d in terms)
    st.html(f'<div class="dt-glossary-strip">{items}</div>')


def totals_strip(items: list[tuple[str, str]]) -> None:
    """Compact horizontal strip of value / label pairs, with thin dividers
    between cells. Replaces ``st.metric`` triplets / quadruplets on Stage 2
    views that previously read as a fintech-dashboard hero block. CSS
    classes (``.dt-totals-*``) live in ``shared_css.py``.

    Each tuple is ``(value, label)``; value is rendered escaped, label is
    rendered escaped + UPPERCASED via CSS.

    Use this rather than ``st.columns(N)`` + ``st.metric`` on:
    - payments Rankings view (since-2020 summary)
    - lobbying org Stage 2 (returns / politicians / periods / span)
    - lobbying topic Stage 2 (returns / orgs / areas / period)
    - lobbying DPO Stage 2b individual (firms / clients / politicians / returns)

    The year-view of payments has historically used the older ``pay-totals-*``
    markup directly; that call site should migrate to this helper in the same
    pass and the ``pay-totals-*`` classes can be retired.
    """
    if not items:
        return
    cells: list[str] = []
    for value, label in items:
        cells.append(
            f'<div class="dt-totals-item">'
            f'<span class="dt-totals-num">{_h(str(value))}</span>'
            f'<span class="dt-totals-lbl">{_h(str(label))}</span>'
            f"</div>"
        )
    inner = '<div class="dt-totals-divider"></div>'.join(cells)
    st.html(f'<div class="dt-totals-strip">{inner}</div>')


def stat_strip(stats: list[tuple[str, str, str]] | list[tuple[str, str, str, str]]) -> None:
    """Render evidence stats. Each stat is (value, label, colour) or
    (value, label, colour, sub_label) where sub_label adds comparative
    context like "rank 87 of 174" below the label. Reuses .stat-strip CSS."""
    items = ""
    for stat in stats:
        if len(stat) == 4:
            value, label, colour, sub = stat
        else:
            value, label, colour = stat  # type: ignore[misc]
            sub = ""
        sub_html = f'<div class="stat-sub">{_h(sub)}</div>' if sub else ""
        items += (
            f'<div><div class="stat-num" style="color:{colour}">{_h(value)}</div>'
            f'<div class="stat-lbl">{_h(label)}</div>'
            f"{sub_html}</div>"
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
    """Cross-page section heading.

    Tier-2 audit fix (2026-05-26): emits a real ``<h2>`` rather than
    ``<p class="section-heading">``. Screen readers can now navigate by
    heading level between the page ``<h1>`` (in `hero_banner`) and
    section content. Visual styling is unchanged — same class is kept
    so the existing CSS rule still applies; only the tag changes.
    Resolves: votes Appendix #4, interests Part 3 H4, legislation P2-3,
    attendance P2-6.

    2026-06-05: switched from st.markdown(unsafe_allow_html=True) to st.html
    to comply with the page contracts' `no_unsafe_allow_html` rule (the input
    is already escaped, so this is a like-for-like swap).
    """
    st.html(f'<h2 class="section-heading">{_h(text)}</h2>')


def subsection_heading(text: str) -> None:
    """Sub-section heading nested one level below `evidence_heading`.

    Emits a real ``<h3>`` so screen readers see proper h2 → h3 nesting when a
    section (h2) contains several labelled sub-sections (e.g. the Member
    Overview "Legislation" section's "Legislation sponsored" / "Ministerial
    roles" / "Statutory Instruments signed" blocks). Reuses the
    `.section-heading` class for visual parity; only the tag level differs.
    """
    st.html(f'<h3 class="section-heading section-subheading">{_h(text)}</h3>')


def todo_callout(message: str) -> None:
    """Citizen-facing "Coming soon" callout.

    Round-3 audit fix (P1-A): previously rendered the project-internal
    `TODO_PIPELINE_VIEW_REQUIRED` token and the full developer message
    verbatim — leaked SQL view names and yaml refs to end users. Now
    strips the internal scaffolding and shows a clean "Coming soon"
    headline; the rest of the message is rendered but with the
    `TODO_PIPELINE_VIEW_REQUIRED:` prefix stripped if callers included it
    for grep-ability.

    For richer pipeline diagnostics in dev, set DT_SHOW_TODO_DETAIL=1 in
    the environment — the original developer-facing detail is then shown
    in a small monospace block under the headline.
    """
    import os
    import re

    # Strip the internal tag from the message if present so the citizen
    # sees only the human-readable trailer. Source strings can still
    # include the tag for grep-ability ("TODO_PIPELINE_VIEW_REQUIRED:
    # v_member_interests_index — Coming soon, ranked leaderboard").
    cleaned = re.sub(
        r"^\s*TODO_PIPELINE_VIEW_REQUIRED\s*:\s*",
        "",
        message,
        flags=re.IGNORECASE,
    ).strip()

    # Pull out the citizen sentence: take everything AFTER the first em-dash
    # or the first sentence ending. If neither, just show "Coming soon".
    parts = re.split(r"\s+[—–-]\s+|\.\s+", cleaned, maxsplit=1)
    citizen_msg = parts[1].strip() if len(parts) > 1 else ""
    if not citizen_msg:
        citizen_msg = "More data coming soon."
    # Audit fix (2026-05-26, interests P1-1 / committees P1-1): callers
    # often write the citizen sentence in lowercase because the developer
    # prefix before the em-dash naturally flows into it. Capitalise the
    # first character so the rendered sentence reads as a complete
    # standalone statement ("A ranked leaderboard..." not "a ranked
    # leaderboard...").
    citizen_msg = citizen_msg[0].upper() + citizen_msg[1:] if citizen_msg else citizen_msg

    show_detail = os.getenv("DT_SHOW_TODO_DETAIL") == "1"
    detail_html = (
        f'<div style="margin-top:0.4rem;font-family:monospace;font-size:0.72rem;'
        f'color:var(--text-meta);">{_h(cleaned)}</div>'
        if show_detail and cleaned
        else ""
    )
    st.html(
        f'<div class="dt-callout"><strong>Coming soon.</strong><br>'
        f'<span style="color:var(--text-meta)">{_h(citizen_msg)}</span>'
        f"{detail_html}</div>"
    )


def empty_state(heading: str, body: str) -> None:
    st.html(
        f'<div class="dt-callout"><strong>{_h(heading)}</strong><br>'
        f'<span style="color:var(--text-meta)">{_h(body)}</span></div>'
    )


def member_moved_callout(
    name: str,
    section: str,
    *,
    section_label: str = "this section",
    legacy_param: str | None = None,
    state_keys: tuple[str, ...] = (),
) -> None:
    """Render a "Member profiles have moved" callout and stop the page.

    Round-3 audit fix for two issues that were duplicated across 5 pages:
    (1) every dimension page's redirect callout was producing a broken
    target href because it used the deprecated ``name_join_key()``; this
    helper looks up the actual ``unique_member_code`` via
    :func:`data_access.identity_resolver.resolve_member_code`. (2) every
    redirect callout fell through to render the full page body underneath;
    this helper calls ``st.stop()`` so the user only sees the moved
    notice + a working link.

    Args:
        name: the TD name from the legacy URL / sidebar selection.
        section: the section-anchor id (``"interests"``, ``"payments"``,
            ``"attendance"``, ``"committees"``, etc.) — appended as
            ``#<section>`` on the target URL.
        section_label: human label for the callout copy
            (``"the Interests section"`` / ``"per-TD attendance"``).
        legacy_param: query-param key to scrub from the URL (``"member"``,
            ``"att_td"``, ``"lob_pol"``) so a refresh doesn't re-stick the
            callout.
        state_keys: session-state keys to clear (e.g. ``("selected_td_pay",)``)
            so sidebar selectboxes don't immediately re-trigger the callout.

    The page stops after rendering. Callers should put this BEFORE any
    other rendering they don't want shown when the redirect fires.
    """
    from data_access.identity_resolver import resolve_member_code
    from ui.entity_links import member_profile_url

    code = resolve_member_code(name)
    if code:
        target = member_profile_url(code, section=section)
        # Audit 2026-05-27 P2-5: button-styled CTA (was a plain underlined
        # text-link) so the redirect action carries the visual weight of an
        # affordance, not an afterthought. .dt-moved-cta lives in shared_css.
        link_html = (
            f'<a class="dt-moved-cta" href="{_h(target)}" target="_self">'
            f'Open {_h(name)}\'s profile <span aria-hidden="true">&rarr;</span></a>'
        )
    else:
        link_html = (
            f'<span class="dt-moved-fallback">'
            f"Couldn't find {_h(name)} in the member registry. Try the "
            f'<a class="dt-member-link" href="/member-overview">'
            f"All TDs browse</a>.</span>"
        )

    # Sentence-case the section label while preserving the canonical acronym
    # casing for TD / TAA / PRA / EU / US / SI. `str.capitalize()` lowercases
    # everything after the first letter — turning "Per-TD attendance" into the
    # ugly "Per-td attendance". Instead, uppercase the first letter only.
    label_display = section_label[:1].upper() + section_label[1:] if section_label else section_label

    st.html(
        f'<div class="dt-callout dt-moved-callout">'
        f"<strong>Member profiles have moved.</strong><br>"
        f'<span class="dt-moved-body">{_h(label_display)} '
        f"now lives on the canonical member-overview page.</span><br>"
        f"{link_html}"
        f"</div>"
    )

    if legacy_param:
        st.query_params.pop(legacy_param, None)
    for k in state_keys:
        st.session_state.pop(k, None)

    st.stop()


def back_button(label: str, key: str, *, help: str | None = None) -> bool:
    """Pill-shaped, dark-navy back button that stands out against the beige page bg.

    Pass any unique key — it is auto-prefixed with `dt_back_` so the single CSS
    rule in shared_css.py styles every back button consistently.
    """
    return st.button(label, key=f"dt_back_{key}", help=help)


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


def hide_sidebar() -> None:
    """Hide the (empty) sidebar rail on a page whose filters have moved into a
    main-panel :func:`filter_bar`.

    Hides the rail and its collapse/expand controls, and reverts the dark
    brand band's 22rem sidebar-clearing gutter to a normal main gutter. Every
    content page now calls this, so the sidebar is effectively gone app-wide;
    ``app.py`` also sets ``initial_sidebar_state="collapsed"`` so it never
    flashes on first paint.

    Desktop-only (min-width 768px): below Streamlit's md breakpoint the
    top-nav widget is not rendered at all and st.navigation falls back to the
    sidebar — hiding the sidebar + expand button there removed ALL cross-page
    navigation on phones, trapping users on the landing page.
    """
    st.markdown(
        "<style>"
        "@media (min-width: 768px){"
        '[data-testid="stSidebar"],'
        '[data-testid="stSidebarCollapsedControl"],'
        '[data-testid="stExpandSidebarButton"]{display:none !important;}'
        "}"
        ".site-banner-inner{padding-left:2rem !important;}"
        "</style>",
        unsafe_allow_html=True,
    )


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


def member_card_html(
    name: str,
    meta: str = "",
    rank: int | None = None,
    pills_html: str = "",
    badge_html: str = "",
    avatar_url: str | None = None,
    avatar_initials: str | None = None,
    meta_prefix_html: str = "",
) -> str:
    """Canonical member name card HTML string.

    Avatar slot priority: photo → rank chip → initials chip → empty. The
    slot is always 2.25rem wide so layout is stable across all states.

    pills_html       — raw HTML for pill <span> elements (use int-stat-pill class)
    badge_html       — optional right-side metric; use dt-name-card-badge-metric
                       sub-class for the standard blue days/amount style
    avatar_initials  — 1–2 letter fallback when neither photo nor rank fits
                       the page (e.g. profile-context cards).
    """
    # Audit fix (2026-05-26, interests P1-2): the previous priority was
    # photo → rank → initials, which meant rank was INVISIBLE on every
    # card that had a member photo (~80% of Dáil members). Critical for a
    # leaderboard. Now: when a photo is present, the rank renders as a
    # small overlay chip on the avatar; rank-only and initials-only paths
    # are unchanged. The overlay slot is positioned by
    # ``.dt-name-card-rank-overlay`` in shared_css.py.
    if avatar_url:
        rank_overlay = ""
        if rank is not None:
            rank_overlay_cls = (
                "dt-name-card-rank-overlay dt-name-card-rank-overlay-top" if rank <= 3 else "dt-name-card-rank-overlay"
            )
            rank_overlay = f'<span class="{rank_overlay_cls}">#{rank}</span>'
        left_inner = f'<img class="dt-name-card-avatar" src="{_h(avatar_url)}" alt="" loading="lazy">{rank_overlay}'
    elif rank is not None:
        rank_cls = "dt-name-card-rank dt-name-card-rank-top" if rank <= 3 else "dt-name-card-rank"
        left_inner = f'<span class="{rank_cls}">#{rank}</span>'
    elif avatar_initials:
        left_inner = f'<span class="dt-name-card-initials" aria-hidden="true">{_h(avatar_initials)}</span>'
    else:
        left_inner = ""
    meta_html = f'<div class="dt-name-card-meta">{meta_prefix_html}{_h(meta)}</div>' if meta or meta_prefix_html else ""
    pills_sec = f'<div class="dt-name-card-pills">{pills_html}</div>' if pills_html else ""
    badge_sec = f'<div class="dt-name-card-badge">{badge_html}</div>' if badge_html else ""
    return (
        f'<div class="dt-name-card">'
        f'<div class="dt-name-card-left">{left_inner}</div>'
        f'<div class="dt-name-card-body">'
        f'<div class="dt-name-card-name">{_h(name)}</div>'
        f"{meta_html}{pills_sec}"
        f"</div>"
        f"{badge_sec}"
        f"</div>"
    )


def ranked_member_card(
    name: str,
    meta: str,
    *,
    rank: int | None = None,
    pills_html: str = "",
    badge_html: str = "",
    profile_href: str = "",
    avatar_url: str | None = None,
    avatar_initials: str | None = None,
) -> str:
    """Canonical ranked-list member card — derives avatar from the member name
    and routes pills/badge through ``member_card_html``.

    Use ``pills_html`` (pre-built string) so callers can mix the canonical
    :func:`pill` helper with domain-specific CSS classes (e.g. ``pay-taa-pill``).
    When ``profile_href`` is provided, a small "Profile ↗" pill is appended to
    the pill row (the link goes to the canonical /member-overview profile).

    Replaces the byte-similar ``_pay_card_html`` / ``_int_member_card_html`` /
    ``_lob_card_html`` / ``_ranked_card_html`` closing boilerplate that used
    to live in 4 page files.
    """
    if avatar_url is None or avatar_initials is None:
        from ui.avatars import avatar_data_url
        from ui.avatars import initials as _initials_fn

        if avatar_url is None:
            avatar_url = avatar_data_url(name)
        if avatar_initials is None:
            avatar_initials = _initials_fn(name)
    if profile_href:
        pills_html = pills_html + (
            f'<a class="dt-member-link int-stat-pill-link" href="{_h(profile_href)}" '
            f'target="_self" aria-label="View profile of {_h(name)}">Profile ↗</a>'
        )
    return member_card_html(
        name=name,
        meta=meta,
        rank=rank,
        pills_html=pills_html,
        badge_html=badge_html,
        avatar_url=avatar_url,
        avatar_initials=avatar_initials,
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
    profile_href: str = "",
) -> bool:
    """Name card + navigation arrow. Returns True when the arrow is clicked.

    rank          — pass an int to show the #N badge (gold for top 3); omit for unranked lists.
    quote         — optional italic snippet shown below the pills (e.g. top declaration text).
    profile_href  — optional cross-page profile URL. When provided, appends a small
                    "Profile ↗" anchor pill. Build with utility/ui/entity_links.member_profile_url.
    Caller is responsible for navigation + st.rerun() on True.
    """
    card_col, btn_col = st.columns(col_ratio)
    pills_html = "".join(pill(p) for p in pills)
    if profile_href:
        pills_html += (
            f'<a class="dt-member-link int-stat-pill-link" href="{_h(profile_href)}" '
            f'target="_self" aria-label="View profile of {_h(name)}">Profile ↗</a>'
        )
    if quote:
        pills_html += f'<p class="int-highlight-quote">{quote}</p>'
    card_col.markdown(
        member_card_html(name=name, meta=meta, rank=rank, pills_html=pills_html),
        unsafe_allow_html=True,
    )
    btn_col.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
    return btn_col.button("→", key=btn_key, help=btn_help or f"View {name}")


PARTY_COLOURS: dict[str, str] = {
    "Fianna Fáil": "#66bb6a",
    "Fine Gael": "#1e88e5",
    "Sinn Féin": "#2e7d32",
    "Labour": "#e53935",
    "Social Democrats": "#8e24aa",
    "Green Party": "#43a047",
    "People Before Profit": "#d81b60",
    "Solidarity": "#c2185b",
    "Aontú": "#3949ab",
    "Independent": "#9e9e9e",
    "Independent Ireland": "#ff7043",
    "Right To Change": "#7b1fa2",
    "Unknown": "#bdbdbd",
}


def party_colour(party: str) -> str:
    if not party:
        return PARTY_COLOURS["Unknown"]
    return PARTY_COLOURS.get(party.strip(), PARTY_COLOURS["Unknown"])


def party_stripe_html(parties: list[tuple[str, int]], *, show_legend: bool = True) -> str:
    """Inline horizontal stacked stripe of party seat shares.

    parties — ordered list of (party_name, seat_count). Caller controls order
              (e.g. descending by seats). Zero-count entries are skipped.
    show_legend — render the dot-and-count legend below the stripe.
    """
    cleaned = [(p, int(c)) for p, c in parties if c and int(c) > 0]
    if not cleaned:
        return ""
    total = sum(c for _, c in cleaned) or 1
    segs = "".join(
        f'<div class="cmt-stripe-seg" style="width:{(c / total) * 100:.2f}%;'
        f'background:{party_colour(p)}" title="{_h(p)}: {c}"></div>'
        for p, c in cleaned
    )
    legend = ""
    if show_legend:
        chips = "".join(
            f'<span><span class="cmt-stripe-legend-dot" style="background:{party_colour(p)}"></span>'
            f"<strong>{_h(p)}</strong> {c}</span>"
            for p, c in cleaned
        )
        legend = f'<div class="cmt-stripe-legend">{chips}</div>'
    return f'<div class="cmt-stripe">{segs}</div>{legend}'


# Non-party distribution palettes. Sequential = single-hue light→dark for ORDERED
# scales (e.g. time-on-list, so the long-wait tail reads as "heavy"). Categorical =
# distinct neutral hues for nominal dimensions (tenure/employment/…). Deliberately
# NOT party colours and NOT red/green (no good/bad encoding).
_SEQ_RAMP = [
    "#e9eff5",
    "#cfe0ec",
    "#aecbdf",
    "#86afcd",
    "#5d8fb6",
    "#3d719c",
    "#275680",
    "#173e5e",
]
_CAT_PALETTE = [
    "#4c78a8",
    "#72b7b2",
    "#dba43c",
    "#b07aa1",
    "#9c755f",
    "#83b26f",
    "#a3acb9",
    "#5b9bd5",
]


def proportion_stripe_html(
    segments: list[tuple[str, float]],
    *,
    palette: str = "categorical",
    show_legend: bool = True,
    unit: str = "",
) -> str:
    """Generic stacked proportion stripe — the non-party sibling of party_stripe_html.

    segments — ordered list of (label, value). Caller controls order.
    palette  — 'sequential' (ordered single-hue ramp) | 'categorical' (distinct hues).
    Reuses the .cmt-stripe* CSS. Zero/None values are skipped; legend shows % shares.
    """
    cleaned = [(str(lbl), float(v)) for lbl, v in segments if v and float(v) > 0]
    if not cleaned:
        return ""
    total = sum(v for _, v in cleaned) or 1.0
    ramp = _SEQ_RAMP if palette == "sequential" else _CAT_PALETTE
    n = len(cleaned)

    def colour(i: int) -> str:
        if palette == "sequential":
            return ramp[round(i * (len(ramp) - 1) / max(n - 1, 1))]
        return ramp[i % len(ramp)]

    segs = "".join(
        f'<div class="cmt-stripe-seg" style="width:{(v / total) * 100:.2f}%;'
        f'background:{colour(i)}" title="{_h(lbl)}: {v:,.0f}{_h(unit)} ({v / total * 100:.0f}%)"></div>'
        for i, (lbl, v) in enumerate(cleaned)
    )
    legend = ""
    if show_legend:
        chips = "".join(
            f'<span><span class="cmt-stripe-legend-dot" style="background:{colour(i)}"></span>'
            f"<strong>{_h(lbl)}</strong> {v / total * 100:.0f}%</span>"
            for i, (lbl, v) in enumerate(cleaned)
        )
        legend = f'<div class="cmt-stripe-legend">{chips}</div>'
    return f'<div class="cmt-stripe">{segs}</div>{legend}'


def committee_row_html(
    name: str,
    *,
    rank: int | None = None,
    chair: str | None = None,
    chair_party: str | None = None,
    members: int = 0,
    type_: str = "",
    status: str = "",
    party_seats: list[tuple[str, int]] | None = None,
    oireachtas_url: str | None = None,
) -> str:
    """Single committee register row — card with chair, type, status, party stripe, link.

    Use the adjacent `→` button column (st.columns) for navigation; the CSS
    rule on stHorizontalBlock:has(.cmt-row) collapses the row so the button
    sits next to the fit-content card.
    """
    rank_html = f'<div class="cmt-row-rank">#{int(rank)}</div>' if rank is not None else ""
    status_cls = "cmt-row-status-active" if status == "Active" else "cmt-row-status-ended"
    status_html = f'<span class="cmt-row-status {status_cls}">{_h(status)}</span>' if status else ""
    meta_parts: list[str] = []
    if chair:
        chair_meta = f"Chair: <strong>{_h(chair)}</strong>"
        if chair_party:
            chair_meta += f" ({_h(chair_party)})"
        meta_parts.append(chair_meta)
    if type_:
        meta_parts.append(f"Type: <strong>{_h(type_)}</strong>")
    if members:
        meta_parts.append(f"<strong>{int(members)}</strong> member{'s' if members != 1 else ''}")
    meta_html = f'<div class="cmt-row-meta">{" · ".join(meta_parts)}</div>' if meta_parts else ""
    stripe_html = party_stripe_html(party_seats, show_legend=True) if party_seats else ""
    # P2-5 audit fix: previously each register card carried its own
    # "Oireachtas.ie ↗" link — five identical accent-coloured external
    # links per page created a vertical column of click-bait that
    # competed with the actual card click target. The committee detail
    # identity strip already surfaces this link in context (one link,
    # the right time). The `oireachtas_url` argument is kept on the
    # signature so callers don't need editing; it's just not rendered
    # on the register row any more.
    return (
        f'<div class="cmt-row">'
        f"{rank_html}"
        f'<div class="cmt-row-body">'
        f'<div class="cmt-row-head"><span class="cmt-row-name">{_h(name)}</span>{status_html}</div>'
        f"{meta_html}"
        f"{stripe_html}"
        f"</div>"
        f"</div>"
    )


def committee_identity_strip(
    name: str,
    *,
    type_: str = "",
    status: str = "",
    chair: str | None = None,
    chair_party: str | None = None,
    chair_html: str | None = None,
    member_count: int = 0,
    oireachtas_url: str | None = None,
    source_document_url: str | None = None,
) -> None:
    """Stage-2 identity strip for a single committee.

    Pass ``chair_html`` to render the chair name as an already-safe HTML
    fragment (e.g. a ``member_link_html`` anchor) instead of the default
    escaped ``chair`` text — the caller is then responsible for escaping.
    """
    # P2-3 audit fix: Active / Ended was rendered as inline text inside the
    # meta line, despite the register cards rendering the same value as a
    # coloured chip. Lift status out of the meta line and emit it with the
    # same chip CSS so the detail page is visually consistent with the
    # register.
    status_html = ""
    if status:
        status_cls = "cmt-row-status-active" if status == "Active" else "cmt-row-status-ended"
        status_html = f'<span class="cmt-row-status {status_cls}">{_h(status)}</span>'
    # Plain parts are escaped here; the chair part may carry already-safe HTML
    # (a member-profile anchor) so it is assembled separately and spliced in
    # unescaped after the others are escaped.
    meta_parts: list[str] = []
    if type_:
        meta_parts.append(type_)
    if member_count:
        meta_parts.append(f"{member_count} members")
    safe_parts = [_h(p) for p in meta_parts]
    if chair_html:
        party_suffix = f" ({_h(chair_party)})" if chair_party else ""
        safe_parts.append(f"Chair: {chair_html}{party_suffix}")
    elif chair:
        chair_text = chair if not chair_party else f"{chair} ({chair_party})"
        safe_parts.append(_h(f"Chair: {chair_text}"))
    meta_html = " · ".join(safe_parts)
    links: list[str] = []
    if oireachtas_url or source_document_url:
        from ui.entity_links import source_link_html  # local — avoids any future circular risk
    if oireachtas_url:
        links.append(
            source_link_html(
                oireachtas_url,
                "Oireachtas.ie",
                aria_label=f"Open {name} on oireachtas.ie",
            )
        )
    if source_document_url:
        links.append(
            source_link_html(
                source_document_url,
                "Source document",
                aria_label=f"Open the source document for {name}",
            )
        )
    links_html = f'<div class="cmt-identity-links">{"".join(links)}</div>' if links else ""
    st.markdown(
        f'<div class="cmt-identity">'
        f'<div class="cmt-identity-head">'
        f'<p class="cmt-identity-name">{_h(name)}</p>'
        f"{status_html}"
        f"</div>"
        f'<p class="cmt-identity-meta">{meta_html}</p>'
        f"{links_html}"
        f"</div>",
        unsafe_allow_html=True,
    )


def find_a_td_search(
    members: list[str],
    *,
    key_prefix: str,
    placeholder: str = "Type a TD name…",
) -> str | None:
    """Inline search-and-select for a TD. Returns the selected name or None.

    Designed for the command bar in the committee register's primary view.
    Same shape as `main_member_jump` but no kicker label — fits the bar layout.
    """
    cols = st.columns([3, 2])
    with cols[0]:
        search = st.text_input(
            "Find a TD",
            placeholder=placeholder,
            key=f"{key_prefix}_td_search",
            label_visibility="collapsed",
        )
    filtered = [m for m in members if search_matches(search, m)]
    with cols[1]:
        chosen = st.selectbox(
            "Find a TD",
            ["— pick a TD —"] + filtered,
            key=f"{key_prefix}_td_select",
            label_visibility="collapsed",
        )
    return chosen if chosen and chosen != "— pick a TD —" else None


def find_a_td_filter(
    members: list[str],
    *,
    key_prefix: str,
    label: str = "Find a TD",
    placeholder: str = "Search by name, party or constituency…",
    select_placeholder: str = "— select —",
    show_label: bool = True,
    show_picker: bool = True,
    width_ratio: tuple[int, int, int] = (3, 2, 4),
) -> tuple[str, str | None]:
    """Compact Find-a-TD filter: search input + helper dropdown side-by-side.

    Use this anywhere in the app where the user needs to find a TD by name,
    party, or constituency. The component is deliberately narrower than the
    full content column (last ratio slot is an empty spacer) so the filter
    doesn't dominate the page.

    ``show_picker=False`` drops the helper dropdown and renders the search
    input alone. Use it when the results below are themselves clickable (a
    card grid): the dropdown duplicates that affordance, and users mistake
    its combobox for the search box — text typed or deleted there filters
    only the option list, never the page, which reads as a broken filter.

    Returns ``(query, picked)``:
        query   raw search text — caller filters the list/grid below by this
                across whatever fields are relevant (name, party, constituency).
        picked  name selected from the helper dropdown, or None (always None
                with ``show_picker=False``). Caller should treat
                ``picked is not None`` as a navigation event (set the
                relevant session-state key + ``st.rerun()``).
    """
    if show_label:
        st.html(f'<p class="dt-main-search-kicker">{_h(label)}</p>')
    cols = st.columns(width_ratio)
    with cols[0]:
        query = (
            st.text_input(
                label,
                placeholder=placeholder,
                key=f"{key_prefix}_filter_query",
                label_visibility="collapsed",
                icon=":material/search:",
            )
            or ""
        )
    if not show_picker:
        return query, None
    filtered = [m for m in members if search_matches(query, m)]
    with cols[1]:
        chosen = st.selectbox(
            label,
            [select_placeholder] + filtered,
            key=f"{key_prefix}_filter_pick",
            label_visibility="collapsed",
        )
    picked = chosen if chosen and chosen != select_placeholder else None
    return query, picked


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
