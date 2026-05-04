import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import BILL_STATUS_CSS
from data_access.legislation_data import (
    fetch_all_statuses,
    fetch_bill_debates,
    fetch_bill_detail,
    fetch_bill_timeline,
    fetch_legislation_index_filtered,
)
from shared_css import inject_css
from ui.components import back_button, clickable_card_link, empty_state, evidence_heading, hero_banner, paginate, pagination_controls, render_stat_strip, sidebar_date_range, sidebar_page_header, stat_item
from ui.entity_links import bill_detail_url, source_link_html
from ui.export_controls import export_button
from ui.source_pdfs import provenance_expander

# ── Helpers ────────────────────────────────────────────────────────────────────

def _status_badge_class(status: str) -> str:
    key = status.lower() if status else ""
    for k, cls in BILL_STATUS_CSS.items():
        if k in key:
            return f"signal {cls}"
    return "signal leg-status-active"


def _fmt_date(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        ts = pd.Timestamp(val)
        return f"{ts.day} {ts.strftime('%b %Y')}"
    except Exception:
        return str(val)


# ── Stage 1 — legislation index ────────────────────────────────────────────────

def _render_legislation_index(
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    title_search: str | None = None,
) -> None:
    # ── Hero ──────────────────────────────────────────────────────────────────
    hero_banner(
        kicker="Bills · Oireachtas · Dáil Tracker",
        title="Bills Before the Oireachtas",
        dek=(
            "Track where each Private Members' Bill stands in the legislative journey — "
            "from First Reading in the Dáil through the Seanad to Presidential signature. "
            "Select a phase below, then click any bill to open the full record."
        ),
    )

    # ── Fetch ─────────────────────────────────────────────────────────────────
    df = fetch_legislation_index_filtered(
        start_date=start_date,
        end_date=end_date,
        status=status,
        title_search=title_search,
    )

    if df.empty:
        st.html('<p class="section-heading">0 bills</p>')
        empty_state(
            "No bills found",
            "No bills match the current filters. Try widening the date range or clearing the status filter.",
        )
        return

    # ── Phase grouping ────────────────────────────────────────────────────────
    dail_df    = df[df["bill_phase"] == "dail"]
    seanad_df  = df[df["bill_phase"] == "seanad"]
    enacted_df = df[df["bill_phase"] == "enacted"]

    # ── Pipeline strip ────────────────────────────────────────────────────────
    st.html(
        f"""
        <div class="leg-pipeline-strip">
            <div class="leg-pipeline-card">
                <div class="leg-pipeline-num">{len(dail_df)}</div>
                <div class="leg-pipeline-label">Dáil Stages</div>
                <div class="leg-pipeline-sub">stages 1–5 · before the Dáil</div>
            </div>
            <div class="leg-pipeline-sep">→</div>
            <div class="leg-pipeline-card">
                <div class="leg-pipeline-num">{len(seanad_df)}</div>
                <div class="leg-pipeline-label">Seanad Stages</div>
                <div class="leg-pipeline-sub">stages 6–10 · before the Seanad</div>
            </div>
            <div class="leg-pipeline-sep">→</div>
            <div class="leg-pipeline-card">
                <div class="leg-pipeline-num">{len(enacted_df)}</div>
                <div class="leg-pipeline-label">Enacted</div>
                <div class="leg-pipeline-sub">stage 11 · signed into law</div>
            </div>
        </div>
        """
    )

    # ── Government Bills notice ───────────────────────────────────────────────
    st.html(
        '<div class="leg-todo-callout">'
        '<span class="leg-todo-label">Pipeline todo</span> '
        'Government Bills are not yet indexed — the pipeline is currently scoped to '
        "Private Members' Bills only. Government Bills will appear here once the "
        'pipeline scope is extended.'
        '</div>'
    )

    # ── Phase selector ────────────────────────────────────────────────────────
    phase_opts = {
        f"All ({len(df)})": df,
        f"Dáil Stages ({len(dail_df)})": dail_df,
        f"Seanad Stages ({len(seanad_df)})": seanad_df,
        f"Enacted ({len(enacted_df)})": enacted_df,
    }
    _phase_keys = list(phase_opts.keys())
    phase_sel = st.segmented_control(
        "Filter by phase",
        _phase_keys,
        default=_phase_keys[0],
        label_visibility="collapsed",
        key="leg_phase_radio",
    ) or _phase_keys[0]
    view_df = phase_opts[phase_sel]

    if view_df.empty:
        empty_state("No bills in this phase", "No bills in this phase match the current filters.")
        return

    total = len(view_df)
    st.html(f'<p class="section-heading">{total:,} bill{"s" if total != 1 else ""}</p>')

    # ── Pagination state (controls rendered below the cards) ──────────────────
    LEG_PAGE_SIZE = 10
    page_idx = paginate(total, key_prefix="leg_index", page_size=LEG_PAGE_SIZE)
    visible_df = view_df.iloc[page_idx * LEG_PAGE_SIZE : (page_idx + 1) * LEG_PAGE_SIZE]

    # ── Bill card list — entire card is the click target via
    # clickable_card_link (URL-based navigation: ?bill=<bill_id>). The
    # inner Oireachtas ↗ link remains independently clickable thanks to
    # the stretched-link CSS pattern in shared_css.py. ──────────────────
    cards: list[str] = []
    for _, row in visible_df.iterrows():
        status      = row.get("bill_status", "—") or "—"
        status_cls  = _status_badge_class(status)
        date_str    = _fmt_date(row.get("introduced_date"))
        title       = row.get("bill_title", "—") or "—"
        sponsor     = row.get("sponsor", "—") or "—"
        stage       = row.get("current_stage", "—") or "—"
        url         = row.get("oireachtas_url") or ""
        link_html   = source_link_html(
            url, "Oireachtas",
            aria_label="Open this bill on oireachtas.ie",
        )

        card_html = (
            f'<div class="leg-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="{status_cls}">{html.escape(status)}</span>'
            f'<span class="leg-bill-card-date">{html.escape(date_str)}</span>'
            f'</div>'
            f'<div class="leg-bill-card-title">{html.escape(title)}</div>'
            f'<div class="leg-bill-card-footer">'
            f'<span class="leg-bill-card-meta">{html.escape(sponsor)} · {html.escape(stage)}</span>'
            f'{link_html}'
            f'</div>'
            f'</div>'
        )
        cards.append(
            clickable_card_link(
                href=bill_detail_url(str(row["bill_id"])),
                inner_html=card_html,
                aria_label=f"Open {title}",
            )
        )
    st.html("\n".join(cards))

    # ── Pagination controls (below the cards) ─────────────────────────────────
    pagination_controls(
        total,
        key_prefix="leg_index",
        page_sizes=(LEG_PAGE_SIZE,),
        default_page_size=LEG_PAGE_SIZE,
        label="bills",
    )

    # ── Export ────────────────────────────────────────────────────────────────
    export_cols = [c for c in ["bill_title", "sponsor", "bill_type", "bill_status",
                               "introduced_date", "current_stage", "oireachtas_url"]
                   if c in view_df.columns]
    export_button(view_df[export_cols], "Export current view as CSV", "legislation_filtered.csv", "leg_csv_export")

    # ── Provenance ────────────────────────────────────────────────────────────
    provenance_expander(
        sections=[
            "**Source:** [Houses of the Oireachtas Open Data API](https://api.oireachtas.ie)\n\n"
            "**Dataset:** Private Members' Bills introduced to the Dáil. Government Bills not yet included.\n\n"
            "**Bill phases:** Dáil stages (1–5) → Seanad stages (6–10) → Enacted (stage 11). "
            "[How a Bill Becomes Law](https://www.oireachtas.ie/en/how-parliament-works/legislation/how-a-bill-becomes-law/)\n\n"
            "**Stage information** reflects the most recent stage recorded in the API at time of extract.",
        ],
        source_caption="Data: Houses of the Oireachtas Open Data API",
    )


# ── Stage 2 — bill detail ──────────────────────────────────────────────────────

def _render_stage_timeline(timeline_df: pd.DataFrame) -> None:
    if timeline_df.empty:
        empty_state("Timeline not available", "Stage timeline data is not available for this bill.")
        return

    # Stage group thresholds — immutable Oireachtas legislative procedure.
    # Source: Houses of the Oireachtas, "How a Bill becomes Law"
    # https://www.oireachtas.ie/en/how-parliament-works/legislation/how-a-bill-becomes-law/
    _STAGE_GROUPS = {
        2:  "Dáil stages",            # stages 2–5: Second Stage through Report/Final Stage
        6:  "Seanad stages",          # stages 6–10: same progression in the Seanad
        11: "Presidential signature",
    }
    _seen_groups: set[int] = set()

    rows_html = ""
    for _, r in timeline_df.iterrows():
        num = r.get("stage_number")
        num_val = int(num) if pd.notna(num) else None

        if num_val is not None:
            for threshold, group_label in sorted(_STAGE_GROUPS.items()):
                if num_val >= threshold and threshold not in _seen_groups:
                    rows_html += f'<div class="leg-stage-group">{html.escape(group_label)}</div>'
                    _seen_groups.add(threshold)

        current_cls = "leg-stage-current" if r.get("is_current_stage") else ""
        date_str = _fmt_date(r.get("stage_date"))
        num_str  = str(num_val) if num_val is not None else "·"
        chamber  = r.get("chamber") or ""
        label    = html.escape(r.get("stage_name", "—") or "—")
        if chamber:
            label = f"{label} <span class='leg-stage-chamber'>· {html.escape(chamber)}</span>"

        rows_html += (
            f'<div class="leg-stage-row {current_cls}">'
            f'<span class="leg-stage-num">{html.escape(num_str)}</span>'
            f'<span class="leg-stage-label">{label}</span>'
            f'<span class="leg-stage-date">{html.escape(date_str)}</span>'
            f"</div>"
        )

    st.html(f'<div class="leg-stage-list">{rows_html}</div>')


def _render_debates(debates_df: pd.DataFrame) -> None:
    if debates_df.empty:
        empty_state("No debates found", "No debate records found for this bill.")
        return

    rows_html = ""
    for _, r in debates_df.iterrows():
        date_str = _fmt_date(r.get("debate_date"))
        title    = r.get("debate_title", "—") or "—"
        url      = r.get("debate_url") or ""
        chamber  = r.get("chamber") or ""

        title_html = (
            f'<a class="leg-debate-title" href="{html.escape(url, quote=True)}" target="_blank">{html.escape(title)}</a>'
            if url else
            f'<span class="leg-debate-title-plain">{html.escape(title)}</span>'
        )
        rows_html += (
            f'<div class="leg-debate-row">'
            f'<span class="leg-debate-date">{html.escape(date_str)}</span>'
            f'{title_html}'
            f'<span class="leg-debate-chamber">{html.escape(chamber)}</span>'
            f'</div>'
        )

    st.html(f'<div class="leg-debate-list">{rows_html}</div>')


def _render_bill_detail(bill_id: str) -> None:
    # ── Back navigation ───────────────────────────────────────────────────────
    if back_button("← Back to Legislation Index", key="leg"):
        st.session_state.pop("leg_selected_bill_id", None)
        st.query_params.clear()
        st.rerun()

    # ── Load data ─────────────────────────────────────────────────────────────
    detail_df   = fetch_bill_detail(bill_id)
    timeline_df = fetch_bill_timeline(bill_id)
    debates_df  = fetch_bill_debates(bill_id)

    if detail_df.empty:
        st.warning(f"Bill '{bill_id}' not found. It may have been removed or filtered out.")
        return

    row = detail_df.iloc[0]

    # ── Bill identity strip ───────────────────────────────────────────────────
    status        = row.get("bill_status", "—") or "—"
    bill_type     = row.get("bill_type", "—") or "—"
    current_house = row.get("current_house") or ""
    status_cls    = _status_badge_class(status)

    badges_html = f'<span class="{status_cls}">{html.escape(status)}</span>'
    if bill_type and bill_type != "—":
        badges_html += f' <span class="signal signal-neutral">{html.escape(bill_type)}</span>'
    if current_house:
        badges_html += f' <span class="signal signal-dark">{html.escape(current_house)}</span>'

    bill_title     = row.get("bill_title", "—") or "—"
    bill_no        = row.get("bill_no", "")
    bill_year      = row.get("bill_year", "")
    oireachtas_url = row.get("oireachtas_url") or ""
    ref = (
        f"Bill {html.escape(str(bill_no))} of {html.escape(str(bill_year))}"
        if bill_no and bill_year else ""
    )

    link_html = source_link_html(
        oireachtas_url, "View Bill on Oireachtas.ie",
        aria_label="Open this bill on oireachtas.ie",
    )

    long_title = row.get("long_title") or ""
    long_title_html = (
        f'<p class="leg-long-title" style="margin:0.45rem 0 0.35rem">{html.escape(long_title)}</p>'
        if long_title.strip() else ""
    )

    st.html(
        f"""
        <div class="leg-bill-identity">
            <div class="leg-bill-badges">{badges_html}</div>
            <div class="leg-bill-title">{html.escape(bill_title)}</div>
            {f'<div class="leg-bill-ref">{ref}</div>' if ref else ''}
            {long_title_html}
            {link_html}
        </div>
        """
    )

    # ── Stat strip ────────────────────────────────────────────────────────────
    introduced    = _fmt_date(row.get("introduced_date"))
    sponsor       = row.get("sponsor", "—") or "—"
    current_stage = row.get("current_stage") or "—"
    method        = row.get("method") or "—"

    render_stat_strip(
        stat_item(introduced,    "Introduced"),
        stat_item(sponsor,       "Sponsor"),
        stat_item(current_stage, "Current stage"),
        stat_item(method,        "Method"),
    )

    # ── Two-column detail ─────────────────────────────────────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        evidence_heading("Stage Timeline")
        _render_stage_timeline(timeline_df)

    with col_right:
        debate_label = (
            f"Debates ({len(debates_df)})"
            if not debates_df.empty else "Debates"
        )
        evidence_heading(debate_label)
        _render_debates(debates_df)

    # ── CSV export of this bill's timeline ────────────────────────────────────
    if not timeline_df.empty:
        export_button(timeline_df, "Export stage timeline as CSV", f"bill_{bill_no}_{bill_year}_timeline.csv", "leg_timeline_csv")

    # ── Provenance ────────────────────────────────────────────────────────────
    last_updated = row.get("last_updated") or "—"
    source       = row.get("source") or "—"
    provenance_expander(
        sections=[
            f"**Bill ID:** {bill_id}",
            f"**Source:** {source}  ·  **Last updated (API):** {last_updated}",
            "**Data origin:** Houses of the Oireachtas Open Data API.",
        ]
    )


# ── Entry point ────────────────────────────────────────────────────────────────

def legislation_page() -> None:
    inject_css()

    # URL-driven entry: ?bill=<bill_id> opens the detail view (mirrors the
    # member_overview ?member=… pattern). Session state is the source of
    # truth thereafter so back-button + reruns stay consistent.
    url_bill = st.query_params.get("bill")
    if url_bill:
        st.session_state["leg_selected_bill_id"] = url_bill

    selected_bill_id: str | None = st.session_state.get("leg_selected_bill_id")

    # ── Sidebar ───────────────────────────────────────────────────────────────
    start_date: str | None   = None
    end_date: str | None     = None
    status_param: str | None = None
    search_param: str | None = None

    with st.sidebar:
        sidebar_page_header("Legislation")

        if selected_bill_id:
            st.html('<div class="page-subtitle">Bill detail</div>')
        else:
            st.html('<div class="page-subtitle">Source: Oireachtas Open Data API</div>')
            st.divider()

            start_date, end_date = sidebar_date_range(
                "Introduced between",
                key="leg_date_range",
                empty_default=True,
            )

            st.html('<p class="sidebar-label">Status</p>')
            statuses = fetch_all_statuses()
            status_sel = st.selectbox(
                "Status",
                ["All"] + statuses,
                label_visibility="collapsed",
                key="leg_status_filter",
            )
            status_param = status_sel if status_sel != "All" else None

            st.html('<p class="sidebar-label">Search title</p>')
            title_search = st.text_input(
                "Search title",
                placeholder="e.g. Housing, Health, Education…",
                label_visibility="collapsed",
                key="leg_title_search",
            )
            search_param = title_search.strip() or None

    # ── Main content ──────────────────────────────────────────────────────────
    if selected_bill_id:
        _render_bill_detail(selected_bill_id)
    else:
        _render_legislation_index(
            start_date=start_date,
            end_date=end_date,
            status=status_param,
            title_search=search_param,
        )
