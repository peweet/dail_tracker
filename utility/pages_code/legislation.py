import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.legislation_data import (
    fetch_all_statuses,
    fetch_bill_detail,
    fetch_bill_sources,
    fetch_bill_timeline,
    fetch_legislation_index_filtered,
)
from shared_css import inject_css
from ui.components import render_stat_strip, stat_item

# ── Helpers ────────────────────────────────────────────────────────────────────

_STATUS_CLASS = {
    "enacted": "leg-status-enacted",
    "signed": "leg-status-enacted",
    "lapsed": "leg-status-lapsed",
    "withdrawn": "leg-status-withdrawn",
    "defeated": "leg-status-lapsed",
}


def _status_badge_class(status: str) -> str:
    key = status.lower() if status else ""
    for k, cls in _STATUS_CLASS.items():
        if k in key:
            return f"signal {cls}"
    return "signal leg-status-active"


def _fmt_date(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        return pd.Timestamp(val).strftime("%-d %b %Y")
    except Exception:
        return str(val)


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# ── Stage 1 — legislation index ────────────────────────────────────────────────

def _render_legislation_index() -> None:
    # ── Hero ──────────────────────────────────────────────────────────────────
    st.markdown(
        """
        <div class="dt-hero">
            <div class="dt-kicker">Bills · Oireachtas · Dáil Tracker</div>
            <h2 style="font-family:'Zilla Slab',Georgia,serif;font-size:1.85rem;
                       font-weight:700;margin:0.2rem 0 0.4rem;letter-spacing:-0.02em;">
                Bills Before the Oireachtas
            </h2>
            <p class="dt-dek">
                Browse Private Members' Bills introduced to the Dáil.
                Filter by date, status, or title — click any row to open the full bill record,
                stage history, and official sources.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Filter bar ────────────────────────────────────────────────────────────
    col_date, col_status, col_search = st.columns([3, 2, 3])

    with col_date:
        st.markdown('<p class="sidebar-label">Introduced between</p>', unsafe_allow_html=True)
        date_val = st.date_input(
            "Introduced between",
            value=(pd.Timestamp("2020-01-01").date(), pd.Timestamp.today().date()),
            label_visibility="collapsed",
            key="leg_date_range",
        )

    with col_status:
        st.markdown('<p class="sidebar-label">Status</p>', unsafe_allow_html=True)
        statuses = fetch_all_statuses()
        status_opts = ["All"] + statuses
        status_sel = st.selectbox(
            "Status",
            status_opts,
            label_visibility="collapsed",
            key="leg_status_filter",
        )

    with col_search:
        st.markdown('<p class="sidebar-label">Search title</p>', unsafe_allow_html=True)
        title_search = st.text_input(
            "Search title",
            placeholder="e.g. Housing, Health, Education…",
            label_visibility="collapsed",
            key="leg_title_search",
        )

    # ── Resolve filter values ─────────────────────────────────────────────────
    start_date: str | None = None
    end_date: str | None = None
    if isinstance(date_val, (list, tuple)) and len(date_val) == 2:
        start_date = str(date_val[0])
        end_date = str(date_val[1])

    status_param = status_sel if status_sel != "All" else None
    search_param = title_search.strip() if title_search.strip() else None

    # ── Fetch ─────────────────────────────────────────────────────────────────
    df = fetch_legislation_index_filtered(
        start_date=start_date,
        end_date=end_date,
        status=status_param,
        title_search=search_param,
    )

    # ── Count heading ─────────────────────────────────────────────────────────
    if df.empty:
        st.markdown(
            '<p class="section-heading">0 bills</p>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="dt-callout">No bills match the current filters. '
            "Try widening the date range or clearing the status filter.</div>",
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f'<p class="section-heading">{len(df):,} bills</p>',
        unsafe_allow_html=True,
    )

    # ── Index table ───────────────────────────────────────────────────────────
    display_cols = [
        c for c in
        ["bill_title", "sponsor", "bill_type", "bill_status", "introduced_date", "current_stage"]
        if c in df.columns
    ]
    display_df = df[display_cols].copy()

    col_cfg: dict = {
        "bill_title": st.column_config.TextColumn("Bill", width="large"),
        "sponsor": st.column_config.TextColumn("Sponsor"),
        "bill_type": st.column_config.TextColumn("Type", width="small"),
        "bill_status": st.column_config.TextColumn("Status", width="small"),
        "introduced_date": st.column_config.DateColumn("Introduced", format="DD MMM YYYY", width="small"),
        "current_stage": st.column_config.TextColumn("Stage"),
    }

    event = st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        column_config=col_cfg,
        selection_mode="single-row",
        on_select="rerun",
        key="leg_index_table",
    )

    # ── Handle row selection ───────────────────────────────────────────────────
    if event.selection and event.selection.rows:
        idx = event.selection.rows[0]
        bill_id = df.iloc[idx]["bill_id"]
        st.session_state["leg_selected_bill_id"] = bill_id
        st.rerun()

    # ── CSV export ────────────────────────────────────────────────────────────
    export_df = df[display_cols].copy()
    st.download_button(
        label="Export current view as CSV",
        data=_to_csv_bytes(export_df),
        file_name="legislation_filtered.csv",
        mime="text/csv",
        key="leg_csv_export",
    )

    # ── Provenance ────────────────────────────────────────────────────────────
    with st.expander("About & data provenance", expanded=False):
        st.markdown(
            """
            **Source:** [Houses of the Oireachtas Open Data API](https://api.oireachtas.ie)

            **Dataset:** Private Members' Bills introduced to the Dáil.

            **Note:** Government Bills are not yet included in this dataset.
            TODO_PIPELINE_VIEW_REQUIRED: Government Bills — pipeline query scoped to Private Members only.

            **Bill status values:** Current · Enacted · Lapsed · Withdrawn · Defeated.

            **Stage information** reflects the most recent stage recorded in the API at time of extract.

            **Last updated:** data/silver/parquet/sponsors.parquet (check file timestamp).
            """
        )


# ── Stage 2 — bill detail ──────────────────────────────────────────────────────

def _render_stage_timeline(timeline_df: pd.DataFrame) -> None:
    if timeline_df.empty:
        st.markdown(
            '<div class="dt-callout">'
            "Stage timeline data is not available for this bill.<br>"
            "<em>TODO_PIPELINE_VIEW_REQUIRED: stage_date — "
            "event.dates STRUCT array may need flattening in stages.parquet</em>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    rows_html = ""
    for _, r in timeline_df.iterrows():
        current_cls = "leg-stage-current" if r.get("is_current_stage") else ""
        stage_date = r.get("stage_date")
        date_str = _fmt_date(stage_date)
        num = r.get("stage_number", "")
        num_str = str(int(num)) if pd.notna(num) else "·"
        chamber = r.get("chamber") or ""
        label = r.get("stage_name", "—")
        if chamber:
            label = f"{label} <span style='font-weight:400;color:var(--text-meta);font-size:0.78rem;'>· {chamber}</span>"
        rows_html += (
            f'<div class="leg-stage-row {current_cls}">'
            f'<span class="leg-stage-num">{num_str}</span>'
            f'<span class="leg-stage-label">{label}</span>'
            f'<span class="leg-stage-date">{date_str}</span>'
            f"</div>"
        )

    st.markdown(
        f'<div class="leg-stage-list">{rows_html}</div>',
        unsafe_allow_html=True,
    )


def _render_sources(row: pd.Series, sources_df: pd.DataFrame) -> None:
    oireachtas_url = row.get("oireachtas_url") or (
        sources_df.iloc[0].get("oireachtas_url") if not sources_df.empty else None
    )

    if oireachtas_url:
        st.markdown(
            '<div class="leg-source-card">'
            '<div class="leg-source-label">Official record</div>',
            unsafe_allow_html=True,
        )
        st.link_button("View on Oireachtas.ie ↗", oireachtas_url)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.markdown(
            '<div class="dt-callout">Official URL not available for this bill.</div>',
            unsafe_allow_html=True,
        )

    st.caption(
        "TODO_PIPELINE_VIEW_REQUIRED: official_pdf_url — bill text PDF (from versions.parquet)\n\n"
        "TODO_PIPELINE_VIEW_REQUIRED: source_document_url — Explanatory Memo (from related_docs.parquet)"
    )


def _render_bill_detail(bill_id: str) -> None:
    # ── Back navigation ───────────────────────────────────────────────────────
    if st.button("← Back to Legislation Index", key="leg_back"):
        st.session_state.pop("leg_selected_bill_id", None)
        st.rerun()

    # ── Load data ─────────────────────────────────────────────────────────────
    detail_df = fetch_bill_detail(bill_id)
    timeline_df = fetch_bill_timeline(bill_id)
    sources_df = fetch_bill_sources(bill_id)

    if detail_df.empty:
        st.warning(f"Bill '{bill_id}' not found. It may have been removed or filtered out.")
        return

    row = detail_df.iloc[0]

    # ── Bill identity strip ───────────────────────────────────────────────────
    status = row.get("bill_status", "—") or "—"
    bill_type = row.get("bill_type", "—") or "—"
    current_house = row.get("current_house") or ""
    status_cls = _status_badge_class(status)

    badges_html = f'<span class="{status_cls}">{status}</span>'
    if bill_type and bill_type != "—":
        badges_html += f' <span class="signal signal-neutral">{bill_type}</span>'
    if current_house:
        badges_html += f' <span class="signal signal-dark">{current_house}</span>'

    bill_title = row.get("bill_title", "—") or "—"
    bill_no = row.get("bill_no", "")
    bill_year = row.get("bill_year", "")
    ref = f"Bill {bill_no} of {bill_year}" if bill_no and bill_year else ""

    st.markdown(
        f"""
        <div style="padding:0.75rem 0 0.5rem 0;">
            <div style="display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap;margin-bottom:0.5rem;">
                {badges_html}
            </div>
            <div class="leg-bill-title">{bill_title}</div>
            {f'<div class="leg-bill-ref">{ref}</div>' if ref else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Stat strip ────────────────────────────────────────────────────────────
    introduced = _fmt_date(row.get("introduced_date"))
    sponsor = row.get("sponsor", "—") or "—"
    current_stage = row.get("current_stage") or "—"
    method = row.get("method") or "—"

    render_stat_strip(
        stat_item(introduced, "Introduced"),
        stat_item(sponsor, "Sponsor"),
        stat_item(current_stage, "Current stage"),
        stat_item(method, "Method"),
    )

    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    # ── Two-column detail ─────────────────────────────────────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown('<p class="section-heading">Stage Timeline</p>', unsafe_allow_html=True)
        _render_stage_timeline(timeline_df)

    with col_right:
        st.markdown('<p class="section-heading">Official Sources</p>', unsafe_allow_html=True)
        _render_sources(row, sources_df)

        long_title = row.get("long_title") or ""
        if long_title.strip():
            st.markdown('<p class="section-heading">Full Title</p>', unsafe_allow_html=True)
            st.markdown(
                f'<p style="font-size:0.88rem;line-height:1.6;color:var(--text-secondary);">'
                f"{long_title}</p>",
                unsafe_allow_html=True,
            )

    # ── CSV export of this bill's timeline ────────────────────────────────────
    if not timeline_df.empty:
        st.download_button(
            label="Export stage timeline as CSV",
            data=_to_csv_bytes(timeline_df),
            file_name=f"bill_{bill_no}_{bill_year}_timeline.csv",
            mime="text/csv",
            key="leg_timeline_csv",
        )

    # ── Provenance ────────────────────────────────────────────────────────────
    with st.expander("About & data provenance", expanded=False):
        last_updated = row.get("last_updated") or "—"
        source = row.get("source") or "—"
        st.markdown(
            f"""
            **Bill ID:** {bill_id}

            **Source:** {source}  |  **Last updated (API):** {last_updated}

            **Data origin:** Houses of the Oireachtas Open Data API →
            pipeline → data/silver/parquet/sponsors.parquet, stages.parquet

            **Stage timeline** sourced from data/silver/parquet/stages.parquet.

            TODO_PIPELINE_VIEW_REQUIRED: Government Bills absent — pipeline scoped to Private Members only.
            """
        )


# ── Entry point ────────────────────────────────────────────────────────────────

def legislation_page() -> None:
    inject_css()

    selected_bill_id: str | None = st.session_state.get("leg_selected_bill_id")

    with st.sidebar:
        st.markdown('<div class="page-kicker">Dáil Tracker</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-title">Legislation</div>', unsafe_allow_html=True)
        if selected_bill_id:
            st.markdown(
                '<div class="page-subtitle">Bill detail</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="page-subtitle">Source: Oireachtas Open Data API</div>',
                unsafe_allow_html=True,
            )

    if selected_bill_id:
        _render_bill_detail(selected_bill_id)
    else:
        _render_legislation_index()
