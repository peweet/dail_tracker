from __future__ import annotations

from html import escape as _h

import pandas as pd
import streamlit as st

from ui.components import (
    empty_state,
    field_label,
    filter_bar,
    paginate,
    pagination_controls,
    stat_strip,
    subsection_heading,
    year_selector,
)
from ui.entity_links import (
    bill_detail_url,
    si_detail_url,
    source_link_html,
)
from ui.export_controls import export_button
from data_access.committees_data import fetch_committee_assignments, fetch_office_holders
from pages_code.committees import render_member_committees

from ._shared import (
    _legislation,
    _member_speeches,
    _ministerial_roles,
    _q_feed,
    _q_focus_shift,
    _q_ministries,
    _q_profile,
    _q_top_topics,
    _q_years,
    _si_signed,
    _speech_business,
    _speech_summary,
    _speech_years,
)

# ── Profile section renderers ──────────────────────────────────────────────────


def _section_legislation(conn, join_key: str, member_name: str) -> None:
    subsection_heading("Legislation sponsored")

    df = _legislation(conn, join_key)
    if df.empty:
        empty_state(
            "No bills found",
            f"No bills sponsored by {member_name} in v_legislation_index.",
        )
        return

    n = len(df)
    st.caption(f"{n} bill{'s' if n != 1 else ''} sponsored")

    for _, row in df.iterrows():
        title = str(row.get("bill_title", "—"))
        status = str(row.get("bill_status", "—"))
        year = str(row.get("bill_year", "—"))
        url = str(row.get("oireachtas_url", "") or "")

        sl = status.lower()
        status_css = (
            "leg-status-enacted"
            if ("enact" in sl or "sign" in sl)
            else "leg-status-lapsed"
            if sl in ("lapsed", "withdrawn", "defeated")
            else "leg-status-active"
        )
        if url in ("nan", "None"):
            url = ""
        bill_id = str(row.get("bill_id", "") or "")
        url_html = source_link_html(
            url,
            "Oireachtas.ie",
            aria_label="Open this bill on oireachtas.ie",
        )
        # Cross-page jump into the bill detail panel — adds the legislation
        # page's stages, amendment intensity and SIs made under it. Mirrors the
        # SI section below. NOTE: the reciprocal bill->sponsor(member) edge does
        # NOT yet exist (verified absent 2026-06-20), so this is currently a
        # one-way edge; closing the loop needs defect #8 (bill sponsor -> member).
        bill_page_html = (
            f'<a class="dt-source-link" href="{_h(bill_detail_url(bill_id))}" '
            f'target="_self" aria-label="Open this bill on /rankings-legislation">'
            f"Full bill detail</a>"
            if bill_id and bill_id not in ("nan", "None")
            else ""
        )
        links_html = " &nbsp;·&nbsp; ".join(p for p in (url_html, bill_page_html) if p)
        st.html(
            f'<div class="leg-bill-card mo-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(year)}</span>'
            f'<span class="signal {status_css}">{_h(status)}</span>'
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(title)}</div>'
            f'<div class="mo-bill-card-link-row">{links_html}</div>'
            f"</div>"
        )

    export_button(
        df,
        label="Export legislation (CSV)",
        filename=f"legislation_{member_name.replace(' ', '_')}.csv",
        key="mo_leg_export",
    )


def _fmt_tenure_days(days) -> str:
    """Humanise a tenure length in days → '2 yrs 10 mths'. Presentation only."""
    if days is None or pd.isna(days):
        return ""
    days = int(days)
    yrs, rem = divmod(days, 365)
    mths = rem // 30
    parts: list[str] = []
    if yrs:
        parts.append(f"{yrs} yr{'s' if yrs != 1 else ''}")
    if mths:
        parts.append(f"{mths} mth{'s' if mths != 1 else ''}")
    return " ".join(parts) or "< 1 mth"


def _section_ministerial_roles(conn, join_key: str) -> None:
    """Ministerial posts the member has held (Wikidata-sourced tenure spine).
    Conditional: rendered only when the member held office, so non-ministers see
    nothing. Wider than the SIs-signed section below — it covers earlier
    governments back to 2011, not just the current one."""
    df = _ministerial_roles(conn, join_key)
    if df.empty:
        return

    st.divider()
    subsection_heading("Ministerial roles")

    n = len(df)
    current_n = int(df["is_current"].fillna(False).astype(bool).sum())
    current_str = f", {current_n} held now" if current_n else ""
    st.caption(
        f"{n} ministerial post{'s' if n != 1 else ''} held{current_str} — "
        "departmental office history sourced from Wikidata. Dates are the "
        "appointment and departure recorded for each post."
    )

    for _, row in df.iterrows():
        start = row.get("start_date")
        end = row.get("end_date")
        is_current = bool(row.get("is_current"))
        start_txt = start.strftime("%b %Y") if pd.notna(start) else "—"
        if is_current or pd.isna(end):
            date_range = f"since {start_txt}"
            pill = '<span class="signal leg-status-active">Current</span>'
        else:
            date_range = f"{start_txt} – {end.strftime('%b %Y')}"
            pill = ""
        duration = _fmt_tenure_days(row.get("tenure_days"))
        dur_html = f"In post {_h(duration)}" if duration else ""
        st.html(
            f'<div class="leg-bill-card mo-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(date_range)}</span>'
            f"{pill}"
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(str(row.get("department_label", "—")))}</div>'
            f'<div class="mo-bill-card-link-row">{dur_html}</div>'
            f"</div>"
        )


def _section_statutory_instruments(conn, join_key: str) -> None:
    """SIs the member signed as a minister — secondary legislation made by
    ministerial order. Conditional: rendered only when at least one SI
    resolves to this member, so non-ministers see nothing. Resolution covers
    the current government only (the limit of the ministerial-tenure data)."""
    df = _si_signed(conn, join_key)
    if df.empty:
        return

    st.divider()
    subsection_heading("Statutory Instruments signed")

    n = len(df)
    depts = [d for d in df["si_department_label"].dropna().unique().tolist()]
    dept_str = ", ".join(depts) if depts else "—"
    eu_n = int(df["si_is_eu"].fillna(False).astype(bool).sum())
    st.caption(
        f"{n} statutory instrument{'s' if n != 1 else ''} signed as a minister "
        f"({dept_str}) — secondary legislation made by ministerial order, "
        f"{eu_n} of it EU-derived. Covers the current government only."
    )

    for _, row in df.head(50).iterrows():
        op = _h(str(row.get("si_operation", "") or "").replace("_", " ")) or "—"
        url = str(row.get("eisb_url", "") or "")
        si_id = str(row.get("si_id", "") or "")
        # Round-3 audit P3-3: was inline-style amber hex; now uses the
        # tokenised .signal-eu class so the EU palette lives in one place.
        eu_badge = '<span class="signal-eu">EU</span>' if bool(row.get("si_is_eu")) else ""
        eisb_html = (
            source_link_html(
                url,
                "irishstatutebook.ie",
                aria_label="Open this SI on irishstatutebook.ie",
            )
            if url.startswith("http")
            else ""
        )
        # Cross-page jump into the SI detail panel — adds the SI page's
        # taxonomy, parent legislation, and EU-relationship context that
        # don't fit in this sub-section card.
        si_page_html = (
            f'<a class="dt-source-link" href="{_h(si_detail_url(si_id))}" '
            f'target="_self" aria-label="Open SI {_h(si_id)} on /rankings-statutory-instruments">'
            f"Full SI detail</a>"
            if si_id
            else ""
        )
        links_html = " &nbsp;·&nbsp; ".join(p for p in (eisb_html, si_page_html) if p)
        st.html(
            f'<div class="leg-bill-card mo-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">SI {_h(si_id or "—")}</span>'
            f'<span class="signal leg-status-active">{op}</span>'
            f"{eu_badge}"
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(str(row.get("si_title", "—")))}</div>'
            f'<div class="mo-bill-card-link-row">{links_html}</div>'
            f"</div>"
        )

    export_button(
        df,
        label="Export statutory instruments (CSV)",
        filename=f"si_signed_{join_key}.csv",
        key="mo_si_export",
    )


def _section_questions(conn, join_key: str, member_name: str) -> None:
    """Parliamentary questions section. Three bands:
      1. Header strip with concentration % + total + top topics + shift subtitle.
      2. Filter bar: year pills, type segmented control, ministry selectbox.
      3. Paginated feed of question cards (date desc).
    Built on the post-cap-fix full history (264k rows, 2020-present).
    """
    profile = _q_profile(conn, join_key)
    total_qs = int(profile.get("total_qs", 0) or 0)

    if total_qs == 0:
        empty_state(
            "No parliamentary questions on file",
            f"{member_name} does not appear in the questions register (2020 onwards).",
        )
        return

    # ── Build header strip ───────────────────────────────────────────────────
    # Three columns: concentration / total / top topics, plus an optional
    # inline shift subtitle spanning the full width below.
    top_min = str(profile.get("top_ministry") or "").strip()
    top_count = int(profile.get("top_count", 0) or 0)
    top_pct = profile.get("top_pct")
    distinct_min = int(profile.get("distinct_ministries", 0) or 0)

    # Concentration cell. Suppress the percentage when total < 100 (the
    # ratio is unstable below that and would mislead).
    if total_qs >= 100 and top_pct is not None and not pd.isna(top_pct):
        conc_html = (
            f'<div class="q-strip-cell-label">Most-questioned ministry</div>'
            f'<div class="q-conc-pct">{float(top_pct):.1f}%</div>'
            f'<div class="q-conc-ministry">{_h(top_min)}</div>'
            f'<div class="q-conc-detail">{top_count:,} of {total_qs:,} questions</div>'
        )
    elif distinct_min >= 15:
        conc_html = (
            '<div class="q-strip-cell-label">Pattern</div>'
            f'<div class="q-conc-sparse">Questions across {distinct_min} ministries</div>'
            '<div class="q-conc-detail">Constituency generalist</div>'
        )
    else:
        conc_html = (
            '<div class="q-strip-cell-label">Recently elected</div>'
            f'<div class="q-conc-sparse">{total_qs} questions on record</div>'
        )

    # Middle panel: distinct ministries with cabinet-denominator sub-line.
    # Replaces the redundant "on file / total_qs" panel from v1 — total was
    # already in the concentration sub-line. Distinct ministries is the
    # genuine second-axis signal (specialist vs generalist).
    if distinct_min > 0:
        total_html = (
            '<div class="q-strip-cell-label">Ministries engaged</div>'
            f'<div class="q-total-num">{distinct_min}</div>'
            '<div class="q-total-sub">Out of 26 ministries on record</div>'
        )
    else:
        total_html = (
            '<div class="q-strip-cell-label">Activity</div>'
            f'<div class="q-total-num">{total_qs:,}</div>'
            '<div class="q-total-sub">Questions, 2020 to present</div>'
        )

    # Top topics: small clickable chips that apply a topic filter to the feed.
    # Click handler is via st.query_params (?mo_q_topic=...) read at the top of
    # this section. Matches the feedback_css_card_pattern URL handler pattern.
    # Each chip has a trailing ▾ glyph + aria-label so the click-to-filter
    # affordance is recognisable. Cell label says "click to filter".
    topics_df = _q_top_topics(conn, join_key)
    if topics_df.empty:
        topics_inner = (
            '<div class="q-strip-cell-label">Top topics</div>'
            '<div class="q-conc-detail">No topic taxonomy on file.</div>'
        )
    else:
        chip_html_parts = []
        for _, row in topics_df.iterrows():
            t = str(row["topic"])
            n = int(row["n"])
            chip_html_parts.append(
                f'<a class="q-topic-chip" href="?member={_h(join_key)}&mo_q_topic={_h(t)}" '
                f'target="_self" aria-label="Filter feed to questions on {_h(t)} ({n} questions)">'
                f'{_h(t)}<span class="q-topic-chip-count">{n}</span>'
                '<span class="q-topic-chip-action" aria-hidden="true">▾</span>'
                "</a>"
            )
        topics_inner = (
            '<div class="q-strip-cell-label">Top topics <span class="q-strip-cell-hint">— click to filter</span></div>'
            '<div class="q-topic-list">' + "".join(chip_html_parts) + "</div>"
        )

    # Focus shift subtitle (only when present).
    shift = _q_focus_shift(conn, join_key)
    shift_html = ""
    if shift:
        shift_html = (
            '<div class="q-shift-subtitle">'
            f"Most-questioned ministry shifted from "
            f"<strong>{_h(str(shift['past_top']))}</strong> "
            f"({int(shift['past_year_min'])}–{int(shift['past_year_max'])}, "
            f"{int(shift['past_n'])} questions) to "
            f"<strong>{_h(str(shift['recent_top']))}</strong> "
            f"({int(shift['recent_year_min'])}–{int(shift['recent_year_max'])}, "
            f"{int(shift['recent_n'])} questions)."
            "</div>"
        )

    st.html(
        '<div class="q-header-strip">'
        f"<div>{conc_html}</div>"
        f"<div>{total_html}</div>"
        f"<div>{topics_inner}</div>"
        f"{shift_html}"
        "</div>"
    )

    # ── Filter bar ───────────────────────────────────────────────────────────
    # Topic comes from the chip URL handler; year, type, ministry from
    # controls; free-text search from a text input above the row.
    topic_filter = st.query_params.get("mo_q_topic")
    if topic_filter:
        # Render an active-filter chip (× removes the filter via URL).
        clear_href = f"?member={_h(join_key)}"
        st.html(
            '<div class="q-active-filter-bar">'
            '<span class="q-active-filter-label">Topic filter:</span>'
            f'<a class="q-active-chip" href="{_h(clear_href)}" target="_self" '
            f'aria-label="Clear topic filter {_h(topic_filter)}">'
            f"{_h(topic_filter)} "
            '<span class="q-active-chip-x" aria-hidden="true">×</span>'
            "</a></div>"
        )

    # Free-text search of question_text. Empty input matches everything.
    search_text = st.text_input(
        "Search question text",
        key=f"mo_q_search_{join_key}",
        placeholder="Search question text (e.g. 'cardiac services', 'endometriosis')",
        label_visibility="collapsed",
    )

    years = _q_years(conn, join_key)
    ministries = _q_ministries(conn, join_key)

    # Year pills (shared year_selector — same control as every other year filter)
    year_val: int | None = year_selector([str(y) for y in years], key=f"mo_q_year_{join_key}", include_all=True)

    # Type + ministry side by side — filter_bar + field_label keeps the
    # mixed-height widgets (segmented control vs selectbox) on one baseline.
    with filter_bar([1, 2]) as cols:
        with cols[0]:
            field_label("Type")
            selected_type = st.segmented_control(
                "Question type",
                options=["All types", "Written", "Oral"],
                default="All types",
                key=f"mo_q_type_{join_key}",
                label_visibility="collapsed",
            )
        with cols[1]:
            field_label("Ministry")
            selected_ministry = st.selectbox(
                "Ministry",
                options=["All ministries"] + ministries,
                index=0,
                key=f"mo_q_min_{join_key}",
                label_visibility="collapsed",
            )

    qtype_val = None if not selected_type or selected_type == "All types" else selected_type.lower()
    ministry_val = None if not selected_ministry or selected_ministry == "All ministries" else selected_ministry
    search_val = (search_text or "").strip() or None

    # ── Feed ─────────────────────────────────────────────────────────────────
    df = _q_feed(conn, join_key, year_val, qtype_val, ministry_val, topic_filter, search_val)
    if df.empty:
        empty_state(
            "No questions match these filters",
            "Try clearing the search box, the ministry, the year pill, or the topic filter.",
        )
        return

    total = len(df)
    PAGE_SIZE = 10
    filter_sig = (
        f"{year_val or 'all'}_{qtype_val or 'all'}_{ministry_val or 'all'}"
        f"_{topic_filter or 'all'}_{hash(search_val) if search_val else 'all'}"
    )
    pager_key = f"mo_q_{join_key}_{filter_sig}"
    page_idx = paginate(total, key_prefix=pager_key, page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    start = page_idx * PAGE_SIZE + 1
    end = min((page_idx + 1) * PAGE_SIZE, total)
    st.caption(f"Showing {start:,}–{end:,} of {total:,} question{'s' if total != 1 else ''}")

    # Render each card. The body uses <details> for "Read full text" expand
    # so toggling stays client-side (no Streamlit rerun per card).
    TRUNC = 280
    for _, row in visible.iterrows():
        raw_date = row.get("question_date")
        try:
            date_disp = pd.to_datetime(raw_date).strftime("%d %b %Y")
        except Exception:
            date_disp = str(raw_date or "")
        qtype = str(row.get("question_type", "") or "").lower()
        ministry = str(row.get("ministry", "") or "").strip()
        topic = str(row.get("topic", "") or "").strip()
        text = str(row.get("question_text", "") or "").strip()
        ref = str(row.get("question_ref", "") or "").strip()
        url = str(row.get("oireachtas_url", "") or "").strip()

        type_cls = "q-card-type-oral" if qtype == "oral" else "q-card-type-written"
        type_label = "Oral" if qtype == "oral" else "Written"

        # Build the head row as a series of flex children so the .q-card-head
        # flex gap rule actually spaces them. (Nesting separators inside the
        # kicker span squashes them visually.)
        # Ministry kicker is dropped when topic starts with the ministry word
        # (Oireachtas taxonomy regularly does this — "Health" + "Health
        # Services Waiting Lists" reads as "Health Health Services" otherwise).
        head_parts = [f'<span class="q-card-date">{_h(date_disp)}</span>']
        topic_dupes_ministry = bool(ministry and topic and topic.lower().startswith(ministry.lower()))
        if ministry and not topic_dupes_ministry:
            head_parts.append('<span class="q-card-sep">·</span>')
            head_parts.append(f'<span class="q-card-kicker">{_h(ministry)}</span>')
        if topic:
            head_parts.append('<span class="q-card-sep">·</span>')
            head_parts.append(f'<span class="q-card-kicker">{_h(topic)}</span>')
        head_parts.append(f'<span class="q-card-type {type_cls}">{type_label}</span>')

        # Body: truncate beyond TRUNC chars with <details> expand.
        if len(text) > TRUNC:
            short = text[:TRUNC].rstrip()
            body_html = (
                "<details>"
                f'<summary><span class="q-card-truncated">{_h(short)}…</span></summary>'
                f'<div class="q-card-fulltext">{_h(text)}</div>'
                "</details>"
            )
        else:
            body_html = _h(text)

        link_html = ""
        if url.startswith("http"):
            link_html = source_link_html(
                url,
                "Open on Oireachtas.ie",
                aria_label="Open this question on oireachtas.ie",
            )
        ref_html = f'<span class="q-card-ref">[{_h(ref)}]</span>' if ref else ""

        st.html(
            '<div class="q-card">'
            '<div class="q-card-head">' + "".join(head_parts) + "</div>"
            f'<div class="q-card-body">{body_html}</div>'
            '<div class="q-card-foot">'
            f"{link_html}"
            f"{ref_html}"
            "</div>"
            "</div>"
        )

    pagination_controls(
        total=total,
        key_prefix=pager_key,
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="questions",
        show_caption=False,
    )

    # Export
    export_button(
        df,
        label="Export filtered questions (CSV)",
        filename=f"questions_{member_name.replace(' ', '_')}.csv",
        key=f"mo_q_export_{join_key}",
    )

    st.caption("Source: oireachtas.ie/en/debates/questions/ · 2020 to present · complete history per TD.")


_SPEECH_EXCERPT_CHARS = 360


def _render_speech_card(row) -> None:
    """One floor-contribution 'transcript' card: date + badges, topic, spoken
    excerpt, word count + source. Full text follows in an expander when clamped."""
    date_raw = str(row.get("speech_date", "") or "")
    try:
        date_disp = pd.to_datetime(date_raw).strftime("%d %b %Y")
    except Exception:
        date_disp = date_raw
    chamber = str(row.get("house", "") or "").strip() or "—"
    business = str(row.get("business", "") or "").strip()
    topic = str(row.get("section_heading", "") or "").strip()
    ctype = str(row.get("contribution_type", "") or "")
    words = int(row.get("word_count", 0) or 0)
    text = str(row.get("speech_text", "") or "").strip()
    url = str(row.get("debate_url", "") or "")
    if url in ("nan", "None"):
        url = ""

    title = topic or business or "—"
    crumb = business if business and business != topic else ""

    badges = f'<span class="signal leg-status-active">{_h(chamber)}</span>'
    if bool(row.get("is_irish")):
        badges += '<span class="signal signal-gaeilge">As Gaeilge</span>'
    if ctype == "question":
        badges += '<span class="signal signal-neutral">Oral question</span>'

    clamped = len(text) > _SPEECH_EXCERPT_CHARS
    excerpt = (text[:_SPEECH_EXCERPT_CHARS].rsplit(" ", 1)[0] + "…") if clamped else text

    url_html = source_link_html(url, "Oireachtas.ie", aria_label="Open this debate on oireachtas.ie") if url else ""
    crumb_html = f'<div class="mo-speech-crumb">{_h(crumb)}</div>' if crumb else ""
    meta_tail = ("&nbsp;·&nbsp;" + url_html) if url_html else ""

    # Full text expands inline via <details> (same client-side pattern as the
    # Questions cards) — the old full-width st.expander below each 600px card
    # read as a separate, broken element.
    if clamped:
        excerpt_html = (
            "<details>"
            f'<summary><span class="mo-speech-excerpt mo-speech-truncated">{_h(excerpt)}</span> '
            '<span class="mo-speech-read-more">Read full contribution</span></summary>'
            f'<div class="mo-speech-excerpt">{_h(text)}</div>'
            "</details>"
        )
    else:
        excerpt_html = f'<div class="mo-speech-excerpt">{_h(text)}</div>'

    st.html(
        f'<div class="leg-bill-card mo-bill-card mo-speech-card">'
        f'<div class="leg-bill-card-header">'
        f'<span class="leg-bill-card-date">{_h(date_disp)}</span>'
        f'<span class="mo-speech-badges">{badges}</span>'
        f"</div>"
        f"{crumb_html}"
        f'<div class="leg-bill-card-title">{_h(title)}</div>'
        f"{excerpt_html}"
        f'<div class="mo-debate-card-meta">{words:,} word{"s" if words != 1 else ""}{meta_tail}</div>'
        f"</div>"
    )


def _section_debates(conn, join_key: str, member_name: str) -> None:
    """Floor contributions (speeches + oral questions) from the AKN debate
    transcript — the member's actual spoken words, with an As-Gaeilge flag and
    full-text search. Replaces the former question-derived debate-section proxy.
    """
    subsection_heading("Debates")

    summary = _speech_summary(conn, join_key)
    total = int(summary.get("total_contributions", 0) or 0)
    if total == 0:
        empty_state(
            "No floor contributions on record",
            f"{member_name} has no speeches or oral questions in the available debate transcript record.",
        )
        return

    house = str(summary.get("house") or "Dáil")
    role = "Senator" if house == "Seanad" else "TD"
    words = int(summary.get("total_words", 0) or 0)
    irish = int(summary.get("irish_count", 0) or 0)
    distinct_business = int(summary.get("distinct_business", 0) or 0)
    commencement = int(summary.get("commencement_count", 0) or 0)

    st.caption(
        f"What this {role} actually said on the floor — speeches and oral "
        "questions from the Oireachtas debate record (oireachtas.ie AKN "
        "transcripts). Contributions delivered in Irish are flagged."
    )

    # ── Header strip (reuses stat_strip) ─────────────────────────────────────
    stats: list[tuple[str, str, str, str]] = [
        (f"{total:,}", "Contributions", "var(--ink-strong)", f"≈{words:,} words spoken"),
    ]
    if irish > 0:
        stats.append((f"{irish:,}", "As Gaeilge", "var(--accent)", "delivered in Irish"))
    if commencement > 0:
        stats.append((f"{commencement:,}", "Commencement Matters", "var(--ink-strong)", "issues raised"))
    else:
        stats.append((f"{distinct_business}", "Items of business", "var(--ink-strong)", "distinct debates"))
    stat_strip(stats)

    # ── Filter bar ───────────────────────────────────────────────────────────
    years = _speech_years(conn, join_key)
    year_val = year_selector([str(y) for y in years], key="mo_speech_year", include_all=True)

    with filter_bar([3, 1]) as fcols:
        with fcols[0]:
            field_label("Type")
            type_label = (
                st.segmented_control(
                    "Type",
                    options=["All types", "Speeches", "Questions"],
                    default="All types",
                    key="mo_speech_type",
                    label_visibility="collapsed",
                )
                or "All types"
            )
        with fcols[1]:
            field_label("Language")
            irish_only = st.toggle(
                "As Gaeilge",
                key="mo_speech_irish",
                help="Show only contributions identified as delivered in Irish.",
            )
    ctype = {"All types": None, "Speeches": "speech", "Questions": "question"}.get(type_label)

    business_opts = _speech_business(conn, join_key)
    selected_business = (
        st.selectbox(
            "Item of business",
            options=["All business"] + business_opts,
            index=0,
            key="mo_speech_business",
            label_visibility="collapsed",
        )
        or "All business"
    )
    business_val = None if selected_business == "All business" else selected_business

    search = (
        st.text_input(
            "Search what they said",
            key="mo_speech_search",
            placeholder="Search the words they spoke…",
            label_visibility="collapsed",
        ).strip()
        or None
    )

    df = _member_speeches(conn, join_key, year_val, ctype, business_val, bool(irish_only), search)
    if df.empty:
        if irish_only:
            empty_state(
                "No contributions in Irish match",
                f"No contributions by {member_name} were identified as delivered in Irish under these filters.",
            )
        else:
            empty_state(
                "No contributions match these filters",
                "Try a different year, type, item of business, or search term.",
            )
        return

    total_rows = len(df)
    PAGE_SIZE = 8
    filter_sig = f"{year_val or 'all'}_{ctype or 'all'}_{business_val or 'all'}_{int(bool(irish_only))}_{search or ''}"
    pager_key = f"mo_speech_{join_key}_{filter_sig}"
    page_idx = paginate(total_rows, key_prefix=pager_key, page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    start = page_idx * PAGE_SIZE + 1
    end = min((page_idx + 1) * PAGE_SIZE, total_rows)
    st.caption(f"Showing {start:,}–{end:,} of {total_rows:,} contribution{'s' if total_rows != 1 else ''}")

    for _, row in visible.iterrows():
        _render_speech_card(row)

    pagination_controls(
        total=total_rows,
        key_prefix=pager_key,
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="contributions",
        show_caption=False,
    )


def _section_committees(member_name: str, join_key: str) -> None:
    """Phase 8 lift: per-TD committee profile body.

    Backed by the v_committee_* analytical views via data_access.committees_data
    (same fetchers committees.py uses for its register and per-committee pages).
    """
    df_long = fetch_committee_assignments("Dáil")
    offices = fetch_office_holders("Dáil")
    if df_long.empty:
        st.html(
            '<div class="dt-callout">No committee data available — '
            "the committees pipeline scaffold returned no rows.</div>"
        )
        return
    render_member_committees(
        member_name,
        df_long,
        offices,
        chamber="Dáil",
        show_member_header=False,
        status_filter_key=f"mo_comm_status_{join_key}",
        export_key_suffix="_mo",
    )
