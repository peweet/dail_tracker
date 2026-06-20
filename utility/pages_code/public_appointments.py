"""
Public Appointments — standalone browser page.

Sources from the registered DuckDB view v_public_appointments
(sql_views/appointments/appointments_public_appointments.sql), which reads
data/gold/parquet/public_appointments.parquet — produced by
public_appointments_enrichment.py (repo root).

Civic angle (and editorial spine): who the State puts into public office.
The Irish/English split in the data tracks the appointing authority
(Article 8: formal acts of the President and Government are executed in
Irish, the first official language; ministerial appointments in English)
— that becomes the page's primary facet, not a footnote.

Sections (top → bottom):
  1. Editorial hero + one-line constitutional caption
  2. Featured: special advisers, who advises which minister + year sparkline
  3. Search the full record (any year) + year pills + tabbed facets
  4. Active-filter chips (clearable)
  5. Recent feed — month-grouped appointment cards with pagination
  6. Detail view (with original Irish text + Iris source) on ?ref=
"""

from __future__ import annotations

import datetime
import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.appointments_data import (
    fetch_public_appointments,
    fetch_stateboards_boards,
    fetch_stateboards_roster,
)
from data_access.freshness_data import freshness_line
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    fmt_civic_date as _fmt_date,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
    text_search_mask,
)
from ui.export_controls import export_button
from ui.source_pdfs import iris_archive_url, provenance_expander

PAGE_SIZE = 12
FEATURED_TOP_N = 8  # ministers shown in the SpAd panel
_PA_TYPES = ["state_board", "special_adviser", "judicial"]
_TYPE_LABEL = {
    "state_board": "Board / agency",
    "special_adviser": "Special adviser",
    "judicial": "Judicial",
}
_AUTH_ORDER = ["President", "Government", "Minister", "Unknown"]


# ──────────────────────────────────────────────────────────────────────────────
# Page-local CSS. Reuses dt-* tokens but lives here rather than shared_css.py
# so the canonical class set is not polluted by this page's pa-* classes.
# Matches the precedent set by statutory_instruments.py.
# ──────────────────────────────────────────────────────────────────────────────
def _inject_pa_css() -> None:
    # st.markdown injects into the document head; st.html would scope <style>
    # to its iframe (see feedback_streamlit_css_and_state). Same pattern as
    # the SI page.
    st.markdown(
        """
        <style>
        /* ── constitutional caption under the hero ─────────────────────── */
        .pa-context {
            font-size: 0.82rem; color: #5b6b73; line-height: 1.5;
            margin: 0.35rem 0 1.2rem; max-width: 62rem;
        }
        .pa-context strong { color: #14232b; font-weight: 600; }

        /* ── FEATURED special-adviser panel ────────────────────────────── */
        .pa-featured {
            background: #fbf8f1;
            border: 1px solid #e5dfd0;
            border-radius: 10px;
            padding: 1.1rem 1.3rem 1.25rem;
            margin: 0 0 1.6rem;
            display: grid;
            grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr);
            gap: 1.4rem;
        }
        @media (max-width: 760px) {
            .pa-featured { grid-template-columns: 1fr; gap: 1.1rem; padding: 0.95rem 1rem 1.1rem; }
        }
        .pa-featured-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; font-weight: 600;
        }
        .pa-featured-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.18rem;
            line-height: 1.35; margin: 0.25rem 0 0.55rem; color: #14232b;
        }
        .pa-featured-sub {
            font-size: 0.82rem; color: #5b6b73; margin-bottom: 0.85rem;
        }
        .pa-rank-row {
            display: grid; grid-template-columns: 13rem 1fr 2.5rem;
            align-items: center; gap: 0.65rem;
            padding: 0.2rem 0; font-size: 0.86rem;
        }
        .pa-rank-name {
            color: #14232b; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap;
        }
        .pa-rank-bar {
            background: #efe6cf; border-radius: 2px; height: 0.55rem;
            overflow: hidden; position: relative;
        }
        .pa-rank-bar > span {
            display: block; height: 100%; background: #6b3f00; border-radius: 2px;
        }
        .pa-rank-count {
            text-align: right; color: #5b6b73; font-variant-numeric: tabular-nums;
            font-size: 0.85rem;
        }

        /* sparkline (clickable bars; no JS — anchors set URL params) */
        .pa-spark-wrap { display: flex; flex-direction: column; gap: 0.35rem; }
        .pa-spark-label {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #7a5a00; font-weight: 600;
        }
        .pa-spark-row {
            display: flex; align-items: flex-end; gap: 4px;
            height: 78px; padding: 0.25rem 0.1rem 0;
            border-bottom: 1px solid #d8cda9;
        }
        .pa-spark-bar {
            flex: 1 1 0; min-width: 10px; max-width: 26px;
            background: #c8b787; border-radius: 2px 2px 0 0;
            text-decoration: none;
            transition: background 100ms ease-out;
            position: relative;
        }
        .pa-spark-bar:hover { background: #6b3f00; }
        .pa-spark-bar.is-spike { background: #6b3f00; }
        .pa-spark-bar.is-spike:hover { background: #14232b; }
        .pa-spark-bar.is-current { outline: 2px solid #14232b; outline-offset: 2px; }
        .pa-spark-years {
            display: flex; justify-content: space-between;
            font-size: 0.65rem; color: #7a5a00; letter-spacing: 0.04em;
            font-variant-numeric: tabular-nums;
        }
        .pa-spark-note {
            font-size: 0.75rem; color: #5b6b73; margin-top: 0.35rem;
        }

        /* ── active filter chip bar (clearable) ────────────────────────── */
        .pa-active-bar {
            display: flex; flex-wrap: wrap; gap: 0.4rem; align-items: center;
            padding: 0.5rem 0.75rem; background: #f5f1ea;
            border: 1px solid #e5e2db; border-radius: 6px;
            margin: 0.4rem 0 0.85rem;
        }
        .pa-active-label {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #5b6b73; margin-right: 0.3rem;
        }
        .pa-active-chip {
            display: inline-flex; align-items: center; gap: 0.3rem;
            background: #ffffff; border: 1px solid #cfdde6; border-radius: 999px;
            padding: 0.18rem 0.45rem 0.18rem 0.7rem; font-size: 0.78rem;
            color: #14232b; line-height: 1.4; white-space: nowrap;
            text-decoration: none;
            transition: background 120ms ease-out, border-color 120ms ease-out;
        }
        .pa-active-chip:hover { background: #fef2f2; border-color: #fca5a5; color: #7f1d1d; }
        .pa-active-chip:focus-visible { outline: 2px solid #14232b; outline-offset: 1px; }
        .pa-active-chip-x { font-size: 0.95rem; line-height: 1; color: #5b6b73; font-weight: 400; }
        .pa-active-chip:hover .pa-active-chip-x { color: #7f1d1d; }
        .pa-active-chip-all {
            background: transparent; border-color: #5b6b73; color: #5b6b73; padding-right: 0.7rem;
        }
        .pa-active-chip-all:hover { background: #14232b; border-color: #14232b; color: #ffffff; }

        /* ── recent feed ───────────────────────────────────────────────── */
        .pa-month-h {
            font-family: ui-serif, Georgia, serif;
            font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.12em;
            color: #5b6b73; font-weight: 700;
            margin: 1.2rem 0 0.45rem;
            padding-bottom: 0.3rem; border-bottom: 1px solid #e5e2db;
        }
        .pa-month-h:first-of-type { margin-top: 0.4rem; }

        .pa-card-link {
            display: block; text-decoration: none; color: inherit;
            margin-bottom: 0.5rem;
        }
        .pa-card-link:focus-visible {
            outline: 2px solid #14232b; outline-offset: 2px; border-radius: 8px;
        }
        .pa-card-link:hover .pa-card { border-color: #c9cfd3;
            box-shadow: 0 1px 3px rgba(20,35,43,0.08); }
        .pa-card-link:hover .pa-card-who { color: #0a1418; }

        .pa-card {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 8px;
            padding: 0.85rem 1.05rem;
            display: grid; grid-template-columns: 5.5rem 1fr auto;
            gap: 1rem; align-items: baseline;
            transition: border-color 120ms ease-out, box-shadow 120ms ease-out;
        }
        @media (max-width: 640px) {
            .pa-card { grid-template-columns: 1fr; gap: 0.35rem; padding: 0.75rem 0.9rem; }
            .pa-card-meta { order: -1; }
        }
        .pa-card-date {
            font-variant-numeric: tabular-nums; font-size: 0.82rem; color: #5b6b73;
        }
        .pa-card-who {
            font-family: ui-serif, Georgia, serif; font-size: 1.0rem; line-height: 1.45;
            color: #14232b; margin: 0;
        }
        .pa-card-who .name { font-weight: 600; }
        .pa-card-who .role { color: #5b6b73; font-style: italic; font-size: 0.94rem; }
        .pa-card-who .body { color: #14232b; }
        .pa-card-who .body-prefix { color: #5b6b73; font-weight: 400; }
        .pa-card-meta {
            display: inline-flex; gap: 0.35rem; flex-wrap: wrap;
            justify-content: flex-end; align-items: center;
        }

        /* pills */
        .pa-pill {
            display: inline-block; padding: 0.16rem 0.55rem; border-radius: 999px;
            font-size: 0.7rem; line-height: 1.45; white-space: nowrap;
        }
        .pa-pill-auth { border: 1px solid; }
        .pa-pill-auth-president  { background: #f0eaf6; border-color: #cdbfe2; color: #41306a; }
        .pa-pill-auth-government { background: #ecf2f6; border-color: #cfdde6; color: #1f3a4a; }
        .pa-pill-auth-minister   { background: #eef3ec; border-color: #cfe0c8; color: #2c4a23; }
        .pa-pill-auth-unknown    { background: #f3f0ea; border-color: #d6cdb9; color: #5b4a1c; }
        .pa-pill-type {
            background: #f5f1ea; border: 1px solid #e5e2db; color: #14232b;
        }
        .pa-pill-ga {
            background: #fff7e6; border: 1px solid #f0d99b; color: #7a5a00;
            font-variant-caps: small-caps; letter-spacing: 0.04em;
        }
        .pa-pill-count {
            background: #ffffff; border: 1px solid #e5e2db; color: #5b6b73;
        }

        .pa-missing { color: #8a7a4a; font-style: italic; }

        /* ── detail view ───────────────────────────────────────────────── */
        .pa-detail {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 10px;
            padding: 1.4rem 1.55rem; margin-top: 0.6rem;
        }
        .pa-detail-ref {
            font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.78rem;
            color: #5b6b73;
        }
        .pa-detail-title {
            font-family: ui-serif, Georgia, serif; font-size: 1.45rem; line-height: 1.3;
            color: #14232b; margin: 0.35rem 0 1rem;
        }
        .pa-detail-row {
            display: flex; gap: 0.85rem; padding: 0.55rem 0;
            border-top: 1px solid #f0ece5; align-items: flex-start;
        }
        .pa-detail-row:first-of-type { border-top: none; padding-top: 0; }
        .pa-detail-label {
            width: 170px; flex-shrink: 0; font-size: 0.72rem; text-transform: uppercase;
            letter-spacing: 0.06em; color: #5b6b73; padding-top: 0.18rem;
        }
        .pa-detail-val { flex: 1; font-size: 0.95rem; color: #14232b; line-height: 1.55; }
        .pa-detail-irish {
            background: #f9f5ec; border: 1px solid #ead9b3; border-radius: 6px;
            padding: 0.85rem 1rem; margin-top: 0.8rem;
            font-size: 0.9rem; line-height: 1.6; color: #14232b;
            white-space: pre-wrap;
        }
        .pa-detail-irish-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; margin-bottom: 0.4rem;
        }

        /* ── STATE BOARDS register tab ─────────────────────────────────── */
        .sb-intro {
            font-size: 0.9rem; color: #5b6b73; line-height: 1.6;
            max-width: 62rem; margin: 0.3rem 0 1.2rem;
        }
        .sb-intro strong { color: #14232b; font-weight: 600; }

        .sb-summary {
            display: flex; flex-wrap: wrap; gap: 0.6rem; margin: 0 0 1.3rem;
        }
        .sb-stat {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 8px;
            padding: 0.65rem 1rem; flex: 1 1 0; min-width: 8.5rem;
        }
        .sb-stat-n {
            font-family: ui-serif, Georgia, serif; font-size: 1.5rem;
            color: #14232b; font-variant-numeric: tabular-nums; line-height: 1.1;
        }
        .sb-stat-l {
            font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em;
            color: #5b6b73; margin-top: 0.2rem;
        }
        /* shared gender split bar */
        .sb-gbar {
            display: flex; height: 0.55rem; border-radius: 3px; overflow: hidden;
            margin-top: 0.45rem; background: #efe6cf;
        }
        .sb-gbar-f { background: #6b3f00; }
        .sb-gbar-m { background: #c8b787; }
        .sb-glegend {
            font-size: 0.7rem; color: #5b6b73; margin-top: 0.25rem;
            display: flex; gap: 0.65rem; font-variant-numeric: tabular-nums;
        }
        .sb-glegend .f::before { content: ""; display: inline-block; width: 0.6rem; height: 0.6rem;
            background: #6b3f00; border-radius: 2px; margin-right: 0.25rem; vertical-align: middle; }
        .sb-glegend .m::before { content: ""; display: inline-block; width: 0.6rem; height: 0.6rem;
            background: #c8b787; border-radius: 2px; margin-right: 0.25rem; vertical-align: middle; }

        .sb-count-line { margin: 0.4rem 0 0.6rem; font-size: 0.85rem; color: #5b6b73; }

        .sb-board {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 10px;
            padding: 1.05rem 1.25rem 0.6rem; margin-bottom: 0.9rem;
        }
        .sb-board-head {
            display: grid; grid-template-columns: minmax(0,1fr) 13rem;
            gap: 1rem; align-items: start;
            padding-bottom: 0.7rem; border-bottom: 1px solid #f0ece5;
        }
        @media (max-width: 700px) {
            .sb-board-head { grid-template-columns: 1fr; gap: 0.55rem; }
        }
        .sb-board-dept {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #7a5a00; font-weight: 600;
        }
        .sb-board-name {
            font-family: ui-serif, Georgia, serif; font-size: 1.12rem; line-height: 1.3;
            color: #14232b; margin: 0.2rem 0 0.3rem;
        }
        .sb-board-basis { font-size: 0.78rem; color: #5b6b73; line-height: 1.5; }
        .sb-board-basis a { color: #1f4757; text-decoration: none; border-bottom: 1px solid #b9d0dc; }
        .sb-board-stats { font-size: 0.78rem; color: #5b6b73; }
        .sb-board-seats { font-variant-numeric: tabular-nums; }
        .sb-board-seats strong { color: #14232b; font-size: 1rem; font-family: ui-serif, Georgia, serif; }

        .sb-members { padding-top: 0.5rem; }
        .sb-member {
            display: grid; grid-template-columns: minmax(0,1fr) auto;
            gap: 0.5rem 1rem; align-items: baseline;
            padding: 0.5rem 0; border-top: 1px solid #f6f3ec;
        }
        .sb-member:first-child { border-top: none; }
        .sb-member-name { font-weight: 600; color: #14232b; font-size: 0.92rem; }
        .sb-member-pos {
            display: inline-block; margin-left: 0.5rem; padding: 0.08rem 0.5rem;
            border-radius: 999px; font-size: 0.68rem; background: #f5f1ea;
            border: 1px solid #e5e2db; color: #5b6b73; white-space: nowrap;
            vertical-align: middle;
        }
        .sb-member-pos.is-chair { background: #fff7e6; border-color: #f0d99b; color: #7a5a00; }
        .sb-member-meta {
            font-size: 0.78rem; color: #5b6b73; text-align: right;
            font-variant-numeric: tabular-nums; white-space: nowrap;
        }
        .sb-member-roles {
            grid-column: 1 / -1; font-size: 0.8rem; color: #41306a;
            background: #f6f2fb; border: 1px solid #e3d9f2; border-radius: 6px;
            padding: 0.4rem 0.6rem; margin-top: 0.1rem; line-height: 1.5;
        }
        .sb-member-roles a { color: #41306a; font-weight: 600; text-decoration: none;
            border-bottom: 1px solid #c9b8e6; }
        .sb-roles-tag {
            font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.06em;
            color: #6b4fa0; font-weight: 700; margin-right: 0.35rem;
        }

        /* mobile hero tightening (same pattern as SI page audit P1-2) */
        @media (max-width: 640px) {
            .dt-hero .dt-kicker { display: none; }
            .dt-hero .dt-dek {
                display: -webkit-box; -webkit-line-clamp: 2;
                -webkit-box-orient: vertical; overflow: hidden;
                font-size: 0.82rem; margin-top: 0.25rem;
            }
            .dt-hero { padding: 0.7rem 0.95rem 0.65rem !important; }
            .dt-hero h1 { font-size: 1.2rem !important; }
            /* The fixed 170px detail label eats ~44% of a phone row; stack
               label over value so the value gets the full width. */
            .pa-detail-row { flex-direction: column; gap: 0.15rem; }
            .pa-detail-label { width: auto; padding-top: 0; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading Public Appointments…")
def load_appointments() -> pd.DataFrame:
    df = fetch_public_appointments()
    if df.empty:
        return df
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["year"] = df["issue_date"].dt.year
    df = df.reset_index(drop=True)
    # Stable display-time identifier when notice_ref is null (record-split
    # fragments). Lets the detail-view URL stay stable for those rows.
    fallback_ref = pd.Series([f"row-{i}" for i in range(len(df))], index=df.index)
    has_ref = df["notice_ref"].notna() & (df["notice_ref"].astype(str).str.strip() != "")
    df["display_ref"] = df["notice_ref"].where(has_ref, fallback_ref)
    return df


@st.cache_data(show_spinner="Loading State Boards register…")
def load_stateboards() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Live DPER membership roster (seat grain) + the board universe (board
    grain). Two registered views; the tab facets/groups in pandas off them."""
    return fetch_stateboards_roster(), fetch_stateboards_boards()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _safe(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v)


def _auth_pill_class(authority: str) -> str:
    return {
        "President": "pa-pill-auth-president",
        "Government": "pa-pill-auth-government",
        "Minister": "pa-pill-auth-minister",
    }.get(authority, "pa-pill-auth-unknown")


def _pretty_authority(a: str) -> str:
    return a if a in _AUTH_ORDER else "Authority not detected"


def _pretty_type(t: str) -> str:
    return _TYPE_LABEL.get(t, t.replace("_", " ").capitalize() if t else "—")


# ──────────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────────
def _apply_filters(
    df: pd.DataFrame,
    years: list[int],
    authority: str,
    atype: str,
    body: str,
    minister: str,
    lang_filter: str,
    search: str,
) -> pd.DataFrame:
    out = df
    if years:
        out = out[out["year"].isin(years)]
    if authority and authority != "All":
        out = out[out["appointing_authority"] == authority]
    if atype and atype != "All":
        out = out[out["appointment_type"] == atype]
    if body and body != "All":
        out = out[out["body"] == body]
    if minister and minister != "All":
        out = out[out["portfolio"] == minister]
    if lang_filter and lang_filter != "All":
        out = out[out["lang"] == lang_filter]
    if search and search.strip():
        out = out[text_search_mask(out, search, ["appointee", "body", "portfolio", "english_summary"])]
    return out


def _clear_all_filters() -> None:
    st.session_state.pa_year_filter = []
    st.session_state.pa_authority_filter = "All"
    st.session_state.pa_type_filter = "All"
    st.session_state.pa_body_filter = "All"
    st.session_state.pa_minister_filter = "All"
    st.session_state.pa_lang_filter = "All"
    st.session_state.pa_search = ""


def _clear_facet(key: str) -> None:
    if key == "year":
        st.session_state.pa_year_filter = []
    elif key == "auth":
        st.session_state.pa_authority_filter = "All"
    elif key == "type":
        st.session_state.pa_type_filter = "All"
    elif key == "body":
        st.session_state.pa_body_filter = "All"
    elif key == "min":
        st.session_state.pa_minister_filter = "All"
    elif key == "lang":
        st.session_state.pa_lang_filter = "All"
    elif key == "search":
        st.session_state.pa_search = ""
    elif key == "all":
        _clear_all_filters()


# ──────────────────────────────────────────────────────────────────────────────
# Featured: special advisers — who advises which minister + year sparkline
# ──────────────────────────────────────────────────────────────────────────────
def _render_featured_spads(df: pd.DataFrame) -> None:
    """logic_firewall: display_only — ranks ministers by SpAd count + a small
    yearly trend. All math here is presentation aggregation on the loaded frame."""
    sa = df[df["appointment_type"] == "special_adviser"]
    if sa.empty:
        return

    # Top ministers by SpAd count (portfolio carries the minister/dept).
    pf = sa["portfolio"].dropna()
    top = pf.value_counts().head(FEATURED_TOP_N)  # logic_firewall: display_only
    if top.empty:
        return
    max_n = int(top.iloc[0])

    rows_html: list[str] = []
    for name, n in top.items():
        width = max(8, int(round(100 * (int(n) / max_n))))
        rows_html.append(
            f'<div class="pa-rank-row">'
            f'<div class="pa-rank-name" title="{html.escape(str(name))}">{html.escape(str(name))}</div>'
            f'<div class="pa-rank-bar"><span style="width:{width}%"></span></div>'
            f'<div class="pa-rank-count">{int(n)}</div>'
            f"</div>"
        )

    # Year sparkline. Mark the busiest years (top-3 by appointment count).
    yc = sa["year"].dropna().astype(int).value_counts().sort_index()  # logic_firewall: display_only
    if yc.empty:
        spark_html = ""
    else:
        ymin, ymax = int(yc.index.min()), int(yc.index.max())
        years_full = list(range(ymin, ymax + 1))
        counts = [int(yc.get(y, 0)) for y in years_full]
        spike_threshold = sorted(counts, reverse=True)[2] if len(counts) >= 3 else (max(counts) if counts else 0)
        peak = max(counts) if counts else 1
        current_year = st.session_state.get("pa_year_filter") or []
        current_year_set = set(int(y) for y in current_year)

        bars: list[str] = []
        for y, c in zip(years_full, counts, strict=True):
            h_pct = max(4, int(round(100 * (c / peak)))) if peak else 4
            klass = ["pa-spark-bar"]
            if spike_threshold > 0 and c >= spike_threshold:
                klass.append("is-spike")
            if y in current_year_set:
                klass.append("is-current")
            tip = f"{y}: {c} appointment{'s' if c != 1 else ''}"
            bars.append(
                f'<a class="{" ".join(klass)}" '
                f'href="?spark={y}" target="_self" '
                f'style="height:{h_pct}%" '
                f'aria-label="{tip}" title="{tip}"></a>'
            )
        spark_html = (
            '<div class="pa-spark-wrap">'
            '<div class="pa-spark-label">Hiring by year</div>'
            f'<div class="pa-spark-row">{"".join(bars)}</div>'
            f'<div class="pa-spark-years"><span>{ymin}</span><span>{ymax}</span></div>'
            '<div class="pa-spark-note">Click a year to filter. Taller bars mark the busiest hiring years.</div>'
            "</div>"
        )

    st.markdown(
        '<section class="pa-featured" aria-label="Special advisers featured panel">'
        "<div>"
        '<div class="pa-featured-kicker">Special advisers</div>'
        '<h2 class="pa-featured-h">Who advises which minister</h2>'
        f'<div class="pa-featured-sub">Top portfolios by special-adviser appointments, since 2016. '
        f"Total notices: {sa.shape[0]:,}.</div>" + "".join(rows_html) + "</div>"
        f"<div>{spark_html}</div>"
        "</section>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Search + facet controls
# ──────────────────────────────────────────────────────────────────────────────
def _active_filter_chips(full_df: pd.DataFrame) -> list[tuple[str, str]]:
    chips: list[tuple[str, str]] = []
    all_yrs = set(int(y) for y in full_df["year"].dropna().unique())
    yrs = st.session_state.get("pa_year_filter") or []
    if yrs and set(int(y) for y in yrs) != all_yrs:
        if len(yrs) <= 2:
            for y in sorted(yrs, reverse=True):
                chips.append((str(int(y)), "year"))
        else:
            chips.append((f"Years ({len(yrs)})", "year"))
    if (v := st.session_state.get("pa_authority_filter")) and v != "All":
        chips.append((v, "auth"))
    if (v := st.session_state.get("pa_type_filter")) and v != "All":
        chips.append((_pretty_type(v), "type"))
    if (v := st.session_state.get("pa_body_filter")) and v != "All":
        chips.append((v, "body"))
    if (v := st.session_state.get("pa_minister_filter")) and v != "All":
        chips.append((v, "min"))
    if (v := st.session_state.get("pa_lang_filter")) and v != "All":
        chips.append((f"Language: {v}", "lang"))
    s = (st.session_state.get("pa_search") or "").strip()
    if s:
        chips.append((f'"{s}"', "search"))
    return chips


def _render_facets(full_df: pd.DataFrame) -> None:
    if full_df.empty:
        return

    # Row 1 — full-record search.
    st.text_input(
        "Search the full record",
        placeholder="Search appointee, body, or minister, across every year.",
        key="pa_search",
        label_visibility="collapsed",
    )

    # Active-filter chip bar (clickable to clear individual facets).
    active = _active_filter_chips(full_df)
    if active:
        chip_html = "".join(
            f'<a class="pa-active-chip" href="?clear={k}" target="_self" '
            f'aria-label="Remove filter: {html.escape(label, quote=True)}">'
            f'{html.escape(label)}<span class="pa-active-chip-x" aria-hidden="true">×</span>'
            "</a>"
            for label, k in active
        )
        chip_html += (
            '<a class="pa-active-chip pa-active-chip-all" href="?clear=all" '
            'target="_self" aria-label="Clear all filters">Clear all</a>'
        )
        st.html(f'<div class="pa-active-bar"><span class="pa-active-label">Filtered by</span>{chip_html}</div>')

    # Row 2 — year pills, always visible.
    yrs = sorted((int(y) for y in full_df["year"].dropna().unique()), reverse=True)
    yc = full_df["year"].astype("Int64").value_counts().to_dict()  # logic_firewall: display_only
    current_year = datetime.date.today().year
    st.pills(
        "Year",
        yrs,
        default=[],
        selection_mode="multi",
        key="pa_year_filter",
        format_func=lambda y: f"{y} · {yc.get(y, 0):,} YTD" if y == current_year else f"{y} · {yc.get(y, 0):,}",
    )

    # Row 3 — tabbed primary facets (one set of controls visible at a time).
    auth_sel = st.session_state.get("pa_authority_filter")
    type_sel = st.session_state.get("pa_type_filter")
    body_sel = st.session_state.get("pa_body_filter")
    min_sel = st.session_state.get("pa_minister_filter")
    lang_sel = st.session_state.get("pa_lang_filter")

    def _tab_label(base: str, val: str | None) -> str:
        if not val or val == "All":
            return base
        v = str(val)
        return f"{base}: {v[:22]}…" if len(v) > 22 else f"{base}: {v}"

    tabs = st.tabs(
        [
            _tab_label("Authority", auth_sel),
            _tab_label("Type", _pretty_type(type_sel) if type_sel and type_sel != "All" else None),
            _tab_label("Body", body_sel),
            _tab_label("Minister (advisers)", min_sel),
            _tab_label("Language", lang_sel),
        ]
    )

    with tabs[0]:
        ac = full_df["appointing_authority"].dropna().value_counts().to_dict()  # logic_firewall: display_only
        # Frequency-descending, like every other facet pill row (SI page,
        # language tab below) — not the curated _AUTH_ORDER reading order.
        opts = ["All"] + sorted(ac, key=ac.get, reverse=True)
        st.pills(
            "Appointing authority",
            opts,
            default="All",
            key="pa_authority_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All authorities" if x == "All" else f"{x} · {ac.get(x, 0):,}",
        )

    with tabs[1]:
        tc = full_df["appointment_type"].dropna().value_counts().to_dict()  # logic_firewall: display_only
        opts = ["All"] + sorted(tc, key=tc.get, reverse=True)
        st.pills(
            "Type",
            opts,
            default="All",
            key="pa_type_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All types" if x == "All" else f"{_pretty_type(x)} · {tc.get(x, 0):,}",
        )

    with tabs[2]:
        bc = full_df["body"].dropna().value_counts()  # logic_firewall: display_only
        # Bodies are long-tail. Show the top 30 in a searchable selectbox; below
        # that there's a long tail of one-offs and a junk tail (FOGRA, etc.)
        # being cleaned upstream.
        body_opts = ["All"] + bc.head(30).index.tolist()
        st.selectbox(
            "Body",
            body_opts,
            index=0,
            key="pa_body_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All bodies (top 30 listed)" if x == "All" else f"{x} · {int(bc.get(x, 0)):,}",
        )

    with tabs[3]:
        pf = (  # logic_firewall: display_only
            full_df[full_df["appointment_type"] == "special_adviser"]["portfolio"].dropna().value_counts()
        )
        min_opts = ["All"] + pf.index.tolist()
        st.selectbox(
            "Minister or department (advisers only)",
            min_opts,
            index=0,
            key="pa_minister_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All ministers" if x == "All" else f"{x} · {int(pf.get(x, 0)):,}",
        )

    with tabs[4]:
        lc = full_df["lang"].dropna().value_counts().to_dict()  # logic_firewall: display_only
        opts = ["All"] + sorted(lc, key=lc.get, reverse=True)
        st.pills(
            "Language",
            opts,
            default="All",
            key="pa_lang_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All languages" if x == "All" else f"{x} · {lc.get(x, 0):,}",
        )


# ──────────────────────────────────────────────────────────────────────────────
# Recent feed
# ──────────────────────────────────────────────────────────────────────────────
def _render_card(row: pd.Series) -> str:
    ref = html.escape(_safe(row.get("display_ref")) or "", quote=True)
    auth = _safe(row.get("appointing_authority")) or "Unknown"
    auth_cls = _auth_pill_class(auth)
    auth_label = _pretty_authority(auth)
    atype = _safe(row.get("appointment_type"))
    type_label = _pretty_type(atype)
    body = _safe(row.get("body"))
    appointee = _safe(row.get("appointee"))
    count = int(row.get("appointee_count") or 0)
    role = _safe(row.get("role"))
    lang = _safe(row.get("lang"))

    date_str = _fmt_date(row.get("issue_date"))

    # Appointee display: "Name + N others" already in the appointee column when
    # the enrichment captured a list. Show "—" gracefully when missing.
    if appointee:
        first = appointee.split(";")[0].strip()
        rest = max(0, count - 1)
        who_html = f'<span class="name">{html.escape(first)}</span>'
        if rest > 0:
            who_html += f' <span class="role">and {rest} other{"s" if rest != 1 else ""}</span>'
    else:
        who_html = '<span class="pa-missing">Appointee not recorded in this notice</span>'

    role_html = f' · <span class="role">{html.escape(role)}</span>' if role else ""

    body_html = ""
    if body:
        body_html = f'<span class="body-prefix">to</span> <span class="body">{html.escape(body)}</span>'

    pills = [f'<span class="pa-pill pa-pill-auth {auth_cls}">{html.escape(auth_label)}</span>']
    if type_label:
        pills.append(f'<span class="pa-pill pa-pill-type">{html.escape(type_label)}</span>')
    if lang == "Irish":
        pills.append('<span class="pa-pill pa-pill-ga" title="Original notice in Irish">Gaeilge</span>')
    if count > 1:
        pills.append(f'<span class="pa-pill pa-pill-count">{count} appointed</span>')

    return (
        f'<a class="pa-card-link" href="?ref={ref}" target="_self">'
        '<div class="pa-card">'
        f'<div class="pa-card-date">{html.escape(date_str)}</div>'
        f'<div class="pa-card-who">{who_html}{role_html}{(" " + body_html) if body_html else ""}</div>'
        f'<div class="pa-card-meta">{"".join(pills)}</div>'
        "</div></a>"
    )


def _render_feed(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state(
            "No appointments match these filters",
            "Try widening the year, dropping the language filter, or clearing the search.",
        )
        return

    total = len(df)
    st.html(
        f'<div style="margin:0.4rem 0 0.2rem;font-size:0.85rem;color:#5b6b73;">'
        f"<strong>{total:,}</strong> appointment{'s' if total != 1 else ''} "
        f"match the current filters, sorted newest first."
        f"</div>"
    )

    page_idx = paginate(total, key_prefix="pa_feed", page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    last_month: str | None = None
    cards_html_parts: list[str] = []
    for _, r in visible.iterrows():
        d = r.get("issue_date")
        try:
            month_key = pd.Timestamp(d).strftime("%B %Y").upper() if pd.notna(d) else "UNDATED"
        except Exception:
            month_key = "UNDATED"
        if month_key != last_month:
            cards_html_parts.append(f'<div class="pa-month-h">{html.escape(month_key)}</div>')
            last_month = month_key
        cards_html_parts.append(_render_card(r))

    st.html("".join(cards_html_parts))

    pagination_controls(
        total,
        key_prefix="pa_feed",
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="appointments",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Detail view
# ──────────────────────────────────────────────────────────────────────────────
def _render_detail(row: pd.Series) -> None:
    if back_button("← Back to appointments", key="pa_detail"):
        st.session_state.pop("pa_selected_ref", None)
        st.query_params.clear()
        st.rerun()

    auth = _safe(row.get("appointing_authority")) or "Unknown"
    auth_cls = _auth_pill_class(auth)
    auth_label = _pretty_authority(auth)
    atype = _safe(row.get("appointment_type"))
    body = _safe(row.get("body"))
    appointee = _safe(row.get("appointee"))
    role = _safe(row.get("role"))
    portfolio = _safe(row.get("portfolio"))
    summary = _safe(row.get("english_summary"))
    title_raw = _safe(row.get("title"))
    lang = _safe(row.get("lang"))
    src_pdf = _safe(row.get("iris_source_pdf"))
    date_str = _fmt_date(row.get("issue_date"))
    ref = _safe(row.get("notice_ref")) or _safe(row.get("display_ref"))

    # The english_summary IS the headline of a detail page — it's the cleanest
    # readable distillation. Fall back to title only if summary is empty.
    headline = summary or title_raw or "—"

    ga_pill = ' <span class="pa-pill pa-pill-ga">Gaeilge</span>' if lang == "Irish" else ""

    st.html(
        '<div class="pa-detail">'
        f'<div class="pa-detail-ref">{html.escape(ref)}</div>'
        f'<div class="pa-detail-title">{html.escape(headline)}</div>'
        f'<span class="pa-pill pa-pill-auth {auth_cls}">{html.escape(auth_label)}</span> '
        f'<span class="pa-pill pa-pill-type">{html.escape(_pretty_type(atype))}</span>'
        f"{ga_pill}"
        "</div>"
    )

    rows_html: list[str] = []

    def _row(label: str, value_html: str) -> None:
        rows_html.append(
            f'<div class="pa-detail-row">'
            f'<div class="pa-detail-label">{html.escape(label)}</div>'
            f'<div class="pa-detail-val">{value_html}</div>'
            f"</div>"
        )

    _row("Date", html.escape(date_str))
    if appointee:
        _row("Appointee", html.escape(appointee))
    else:
        _row("Appointee", '<span class="pa-missing">Not recorded in this notice</span>')
    if role:
        _row("Role", html.escape(role))
    if body:
        _row("Body", html.escape(body))
    if portfolio:
        _row("Minister / portfolio", html.escape(portfolio))

    iris_url = iris_archive_url(row.get("issue_date"))
    if src_pdf or iris_url:
        if iris_url:
            val_html = (
                f'<a href="{html.escape(iris_url, quote=True)}" target="_blank" rel="noopener" '
                f'style="color:#1f4757;text-decoration:none;border-bottom:1px solid #b9d0dc;">'
                f"Iris Oifigiúil — {html.escape(date_str)} ↗</a>"
                f'<div style="font-family:ui-monospace,Menlo,monospace;font-size:0.78rem;'
                f'color:#8a8a8a;margin-top:0.2rem;">{html.escape(src_pdf)} '
                f"· opens the issue PDF</div>"
            )
        else:
            val_html = (
                f'<span style="font-family:ui-monospace,Menlo,monospace;'
                f'font-size:0.85rem;color:#5b6b73;">{html.escape(src_pdf)}</span>'
            )
        rows_html.append(
            '<div class="pa-detail-row">'
            '<div class="pa-detail-label">Iris Oifigiúil source</div>'
            f'<div class="pa-detail-val">{val_html}</div>'
            "</div>"
        )

    st.html('<div class="pa-detail">' + "".join(rows_html) + "</div>")

    # Original notice text — primary provenance, especially for Irish notices.
    if title_raw and title_raw.strip():
        irish_pretty = title_raw.replace(" // ", "\n").replace(" | ", "\n")
        st.html(
            '<div class="pa-detail-irish">'
            '<div class="pa-detail-irish-kicker">Original notice text</div>'
            f"{html.escape(irish_pretty)}"
            "</div>"
        )


# ──────────────────────────────────────────────────────────────────────────────
# State Boards register tab
# ──────────────────────────────────────────────────────────────────────────────
SB_BOARDS_PER_PAGE = 8


def _role_group(pos: str) -> str:
    p = (pos or "").lower()
    if "chair" in p:
        return "Chairs"
    if "member" in p:
        return "Members"
    return "Other"


def _term(first, expiry) -> str:
    def _yr(v) -> str:
        s = _safe(v).strip()
        return s[:4] if len(s) >= 4 and s[:4].isdigit() else ""

    a, b = _yr(first), _yr(expiry)
    if a and b:
        return f"{a}–{b}"
    if a:
        return f"from {a}"
    if b:
        return f"until {b}"
    return ""


def _gender_bar(fpct, mpct) -> str:
    """logic_firewall: display_only — renders the published per-board gender
    split (already computed in v_stateboards_boards) as a bar. No math."""
    try:
        f = float(fpct)
        m = float(mpct)
    except (TypeError, ValueError):
        return ""
    if f <= 0 and m <= 0:
        return ""
    return (
        f'<div class="sb-gbar"><span class="sb-gbar-f" style="width:{f:.0f}%"></span>'
        f'<span class="sb-gbar-m" style="width:{m:.0f}%"></span></div>'
        f'<div class="sb-glegend"><span class="f">{f:.0f}% women</span>'
        f'<span class="m">{m:.0f}% men</span></div>'
    )


def _filter_roster(roster: pd.DataFrame, dept: str, role_grp: str, outside_only: bool, search: str) -> pd.DataFrame:
    out = roster
    if dept and dept != "All":
        out = out[out["department"] == dept]
    if role_grp and role_grp != "All":
        out = out[out["position_type"].fillna("").map(_role_group) == role_grp]
    if outside_only:
        out = out[out["wikidata_qid"].notna()]
    if search and search.strip():
        out = out[text_search_mask(out, search, ["member_name", "body", "body_full", "department"])]
    return out


def _render_member(row: pd.Series) -> str:
    name = html.escape(_safe(row.get("member_name")) or "—")
    pos = _safe(row.get("position_type"))
    pos_cls = " is-chair" if "chair" in pos.lower() else ""
    pos_html = f'<span class="sb-member-pos{pos_cls}">{html.escape(pos)}</span>' if pos else ""
    basis = _safe(row.get("basis_of_appointment"))
    term = _term(row.get("first_appointed"), row.get("expiry_date"))
    meta_bits = [b for b in (basis, term) if b]
    meta_html = html.escape(" · ".join(meta_bits)) if meta_bits else ""

    roles_html = ""
    if _safe(row.get("wikidata_qid")):
        desc = _safe(row.get("wikidata_description"))
        occ = _safe(row.get("wikidata_occupations"))
        url = _safe(row.get("wikidata_url"))
        label = _safe(row.get("wikidata_label")) or _safe(row.get("member_name"))
        detail = desc or occ
        if len(detail) > 140:
            detail = detail[:139].rstrip() + "…"
        link = (
            f'<a href="{html.escape(url, quote=True)}" target="_blank" rel="noopener">{html.escape(label)} ↗</a>'
            if url
            else html.escape(label)
        )
        roles_html = (
            f'<div class="sb-member-roles"><span class="sb-roles-tag">Public record</span>'
            f"{link}{(' — ' + html.escape(detail)) if detail else ''}</div>"
        )

    return (
        '<div class="sb-member">'
        f'<div class="sb-member-main"><span class="sb-member-name">{name}</span>{pos_html}</div>'
        f'<div class="sb-member-meta">{meta_html}</div>'
        f"{roles_html}"
        "</div>"
    )


def _render_board_card(body: str, members: pd.DataFrame, meta: dict | None) -> str:
    meta = meta or {}
    dept = html.escape(_safe(members.iloc[0].get("department")))
    body_full = _safe(members.iloc[0].get("body_full")) or body
    name = html.escape(body_full)

    basis_html = ""
    legal = _safe(meta.get("legal_basis"))
    legal_url = _safe(meta.get("legal_basis_url"))
    if legal:
        if legal_url:
            basis_html = (
                f'<div class="sb-board-basis">Statutory basis: '
                f'<a href="{html.escape(legal_url, quote=True)}" target="_blank" rel="noopener">'
                f"{html.escape(legal)} ↗</a></div>"
            )
        else:
            basis_html = f'<div class="sb-board-basis">Statutory basis: {html.escape(legal)}</div>'

    listed = len(members)
    max_pos = meta.get("max_positions")
    if isinstance(max_pos, (int, float)) and not pd.isna(max_pos) and int(max_pos) > 0:
        seats_html = f'<div class="sb-board-seats"><strong>{listed}</strong> of {int(max_pos)} seats shown</div>'
    else:
        seats_html = f'<div class="sb-board-seats"><strong>{listed}</strong> member{"s" if listed != 1 else ""}</div>'

    gender_html = _gender_bar(meta.get("gender_female_pct"), meta.get("gender_male_pct"))

    members_html = "".join(_render_member(r) for _, r in members.iterrows())

    return (
        '<section class="sb-board">'
        '<div class="sb-board-head">'
        f'<div><div class="sb-board-dept">{dept}</div>'
        f'<h3 class="sb-board-name">{name}</h3>{basis_html}</div>'
        f'<div class="sb-board-stats">{seats_html}{gender_html}</div>'
        "</div>"
        f'<div class="sb-members">{members_html}</div>'
        "</section>"
    )


def _render_stateboards_tab(roster: pd.DataFrame, boards: pd.DataFrame) -> None:
    if roster.empty:
        empty_state(
            "State Boards register unavailable",
            "The view returned no rows. Check that data/gold/parquet/stateboards_roster.parquet "
            "is present and that sql_views/appointments/appointments_stateboards_roster.sql "
            "registered cleanly.",
        )
        return

    st.html(
        '<p class="sb-intro">'
        "A live snapshot of <strong>who currently sits on Ireland's State boards</strong> — the "
        "membership register maintained by the Department of Public Expenditure. Unlike the "
        "appointment notices, which record individual gazette <em>events</em>, this is the "
        "standing roster: every named seat, its basis of appointment, and term. Where a member's "
        "identity has been hand-verified, their public-record roles are linked."
        "</p>"
    )

    # ── Summary strip (full register; the editorial frame, not a filtered slice).
    n_boards = roster["body"].nunique()
    n_seats = len(roster)
    n_depts = roster["department"].nunique()
    # Overall gender balance from the board universe (bare Series sums, not a
    # groupby rollup). logic_firewall: display_only
    f_n = int(boards["gender_female_n"].sum()) if not boards.empty else 0
    m_n = int(boards["gender_male_n"].sum()) if not boards.empty else 0
    tot = f_n + m_n
    f_pct = round(100 * f_n / tot) if tot else 0
    m_pct = 100 - f_pct if tot else 0
    gender_block = (
        f'<div class="sb-stat"><div class="sb-stat-n">{f_pct}% / {m_pct}%</div>'
        f'<div class="sb-stat-l">Women / men</div>'
        f"{_gender_bar(f_pct, m_pct)}</div>"
        if tot
        else ""
    )
    st.html(
        '<div class="sb-summary">'
        f'<div class="sb-stat"><div class="sb-stat-n">{n_boards:,}</div><div class="sb-stat-l">State boards</div></div>'
        f'<div class="sb-stat"><div class="sb-stat-n">{n_seats:,}</div><div class="sb-stat-l">Board seats</div></div>'
        f'<div class="sb-stat"><div class="sb-stat-n">{n_depts:,}</div><div class="sb-stat-l">Departments</div></div>'
        f"{gender_block}"
        "</div>"
    )

    # ── Facets.
    st.text_input(
        "Search the register",
        placeholder="Search by member name, board, or department.",
        key="sb_search",
        label_visibility="collapsed",
    )

    c1, c2, c3 = st.columns([2, 1.4, 1.6])
    with c1:
        dc = roster["department"].dropna().value_counts()  # logic_firewall: display_only
        dept_opts = ["All"] + dc.index.tolist()
        st.selectbox(
            "Department",
            dept_opts,
            index=0,
            key="sb_dept",
            format_func=lambda x: "All departments" if x == "All" else f"{x} · {int(dc.get(x, 0)):,}",
        )
    with c2:
        st.selectbox(
            "Role",
            ["All", "Chairs", "Members", "Other"],
            index=0,
            key="sb_role",
            format_func=lambda x: "All roles" if x == "All" else x,
        )
    with c3:
        st.checkbox(
            "Members with a public record only",
            key="sb_outside_only",
            help="Show only seats held by people whose identity has been hand-verified against the public record.",
        )

    filtered = _filter_roster(
        roster,
        dept=st.session_state.get("sb_dept", "All"),
        role_grp=st.session_state.get("sb_role", "All"),
        outside_only=st.session_state.get("sb_outside_only", False),
        search=st.session_state.get("sb_search", ""),
    )

    if filtered.empty:
        empty_state(
            "No board members match these filters",
            "Try clearing the search, widening the role, or turning off the public-record filter.",
        )
        return

    # Board universe metadata, keyed by board name for display-time lookup
    # (a dict lookup, not a pandas merge — keeps the join in the pipeline view).
    board_meta = {str(r["body"]): r for r in boards.to_dict("records")}

    # Boards in published order (the view is ORDER BY department, body), paginated.
    ordered_bodies = filtered["body"].dropna().drop_duplicates().tolist()
    total_boards = len(ordered_bodies)

    st.html(
        f'<div class="sb-count-line"><strong>{len(filtered):,}</strong> '
        f"board seat{'s' if len(filtered) != 1 else ''} across "
        f"<strong>{total_boards:,}</strong> board{'s' if total_boards != 1 else ''} "
        f"match the current filters.</div>"
    )

    page_idx = paginate(total_boards, key_prefix="sb_feed", page_size=SB_BOARDS_PER_PAGE)
    page_bodies = ordered_bodies[page_idx * SB_BOARDS_PER_PAGE : (page_idx + 1) * SB_BOARDS_PER_PAGE]

    cards = [_render_board_card(b, filtered[filtered["body"] == b], board_meta.get(str(b))) for b in page_bodies]
    st.html("".join(cards))

    pagination_controls(
        total_boards,
        key_prefix="sb_feed",
        page_sizes=(SB_BOARDS_PER_PAGE,),
        default_page_size=SB_BOARDS_PER_PAGE,
        label="boards",
    )

    # CSV export of the filtered roster.
    csv_cols = [
        "department",
        "body",
        "member_name",
        "position_type",
        "basis_of_appointment",
        "first_appointed",
        "expiry_date",
        "wikidata_url",
        "source_url",
    ]
    export_button(
        filtered[csv_cols],
        label=f"Download {len(filtered):,} board seats (CSV)",
        filename="dail_tracker_state_boards_register.csv",
        key="sb_csv_download",
    )

    provenance_expander(
        sections=[
            "**Data source:** the State Boards membership register published at "
            "membership.stateboards.ie by the Department of Public Expenditure — the standing "
            "roster of board members across ~250 State bodies and 19 departments. This is a "
            "current snapshot, not a history of appointment events.",
            "Where a member's identity has been **hand-verified**, their public-record roles "
            "(from Wikidata) are shown. Automated name-matching was removed because roughly one "
            "in four auto-matches was the wrong same-named person — so a member with no public-"
            "record line simply has not been curated, which does not imply they hold no outside "
            "roles.",
        ],
        source_caption="Source: membership.stateboards.ie (DPER) · gender balance and legal basis as published",
        freshness=freshness_line("stateboards"),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
def public_appointments_page() -> None:
    inject_css()
    hide_sidebar()
    _inject_pa_css()

    df = load_appointments()

    # URL handlers (run before widgets render so session_state is correct).
    # ?ref=… selects a notice for the detail view.
    url_ref = st.query_params.get("ref")
    if url_ref:
        st.session_state["pa_selected_ref"] = url_ref

    # ?clear=<key> removes a single facet (or all). Mutates state, then reruns.
    url_clear = st.query_params.get("clear")
    if url_clear:
        _clear_facet(url_clear)
        del st.query_params["clear"]
        st.rerun()

    # ?spark=YYYY — clicking a sparkline bar in the featured panel filters the
    # whole page to that year. Implemented as URL params (no JS).
    url_spark = st.query_params.get("spark")
    if url_spark:
        try:
            yr = int(url_spark)
            st.session_state.pa_year_filter = [yr]
        except ValueError:
            pass
        del st.query_params["spark"]
        st.rerun()

    selected = st.session_state.get("pa_selected_ref")

    # ── Detail view ───────────────────────────────────────────────────────
    if selected:
        if df.empty:
            empty_state(
                "Appointments data unavailable",
                "The underlying gold parquet did not load. If this persists, the data file "
                "may be stale or the view registration may have failed.",
            )
            return
        match = df[(df["notice_ref"] == selected) | (df["display_ref"] == selected)]
        if match.empty:
            empty_state(
                f"Appointment {selected!r} isn't in the index",
                "The Dáil Tracker appointments corpus covers 2016 onwards. Old bookmark or "
                "typed URL? Try browsing the index, or search by name.",
            )
            if back_button("← Back to appointments", key="pa_detail_nf"):
                st.session_state.pop("pa_selected_ref", None)
                st.query_params.clear()
                st.rerun()
            return
        _render_detail(match.iloc[0])
        return

    # ── Index view ────────────────────────────────────────────────────────
    hero_banner(
        kicker="Iris Oifigiúil · Public office",
        title="Public Appointments",
        dek=(
            "Who the State puts into public office: state-board and agency members, "
            "judicial appointments, and the special advisers who serve in ministers' "
            "offices. Browse the gazette record since 2016, or the live register of "
            "who sits on the State's boards today."
        ),
    )

    tab_notices, tab_boards = st.tabs(["Appointment notices", "State Boards register"])
    with tab_notices:
        _render_notices_tab(df)
    with tab_boards:
        roster, boards = load_stateboards()
        _render_stateboards_tab(roster, boards)


def _render_notices_tab(df: pd.DataFrame) -> None:
    # Quiet civic-context line about the constitutional Irish/English split.
    st.html(
        '<p class="pa-context">'
        "These appointments are published in <strong>Iris Oifigiúil</strong>, the "
        "official State gazette. "
        "Formal acts of the <strong>President</strong> and <strong>Government</strong> "
        "(judicial appointments, senior board appointments) are recorded in Irish, the "
        "first official language under Article 8. Ministerial appointments are issued "
        "in English. The pattern is preserved on every card, with structured English "
        "summaries built from curated translations of the constitutional templates."
        "</p>"
    )

    if df.empty:
        empty_state(
            "Appointments data unavailable",
            "The view returned no rows. If this persists, check that "
            "data/gold/parquet/public_appointments.parquet is present and that "
            "sql_views/appointments/appointments_public_appointments.sql registered cleanly.",
        )
        return

    # Featured: special advisers (independent of filters — it's the editorial
    # frame, not a slice of the active query). Computed off the full corpus.
    _render_featured_spads(df)

    # Search + facet pills.
    _render_facets(df)

    # Apply filters then render feed.
    filtered = _apply_filters(
        df,
        years=st.session_state.get("pa_year_filter") or [],
        authority=st.session_state.get("pa_authority_filter"),
        atype=st.session_state.get("pa_type_filter"),
        body=st.session_state.get("pa_body_filter"),
        minister=st.session_state.get("pa_minister_filter"),
        lang_filter=st.session_state.get("pa_lang_filter"),
        search=st.session_state.get("pa_search"),
    )

    # CSV export for journalists, secondary placement (above the feed
    # rather than buried at the bottom).
    if not filtered.empty:
        csv_cols = [
            "issue_date",
            "appointing_authority",
            "appointment_type",
            "body",
            "appointee",
            "appointee_count",
            "role",
            "portfolio",
            "english_summary",
            "lang",
            "iris_source_pdf",
        ]
        export_button(
            filtered[csv_cols],
            label=f"Download {len(filtered):,} appointments (CSV)",
            filename="dail_tracker_public_appointments.csv",
            key="pa_csv_download",
        )

    _render_feed(filtered)

    # ── Provenance ────────────────────────────────────────────────────────
    provenance_expander(
        sections=[
            "**Data source:** Iris Oifigiúil, the official State gazette, where "
            "appointments to public office are formally notified — state-board and "
            "agency members, judicial appointments, and ministers' special advisers. "
            "The corpus covers 2016 onwards.",
            "Acts of the President and Government are gazetted in Irish (Article 8); "
            "ministerial appointments in English. Structured English summaries are "
            "built from curated translations of the constitutional templates, and "
            "every card links to the original gazette notice.",
        ],
        source_caption="Source: Iris Oifigiúil (irisoifigiuil.ie)",
        freshness=freshness_line("iris"),
    )


if __name__ == "__main__":
    public_appointments_page()
