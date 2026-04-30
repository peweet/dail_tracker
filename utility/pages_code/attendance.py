"""
Dáil Attendance Tracker — attendance.py

Retrieval-only Streamlit page. All aggregation, metric definitions, and
enrichment live in sql_views/attendance_*.sql (pipeline layer).
This file: SELECT / WHERE / ORDER BY / LIMIT against registered views only.

TODO_PIPELINE_VIEW_REQUIRED: per-year source PDF URL on v_attendance_summary
    (source will eventually point to the official Oireachtas attendance PDF for each
    calendar year — 2025.pdf, 2024.pdf etc — once the pipeline exposes per-year source URLs)
TODO_PIPELINE_VIEW_REQUIRED: session_type column on v_attendance_timeline
    (attendance_status is hardcoded 'Present'; source CSV has sitting-day and other-day rows
    for the same date producing duplicates — pipeline must expose original session-type label)
"""
from __future__ import annotations

import datetime
from html import escape as _h

import altair as alt
import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.components import (
    clean_meta,
    empty_state,
    evidence_heading,
    hero_banner,
    member_profile_header,
    render_notable_chips,
    sidebar_member_filter,
    sidebar_page_header,
    todo_callout,
    year_selector,
)
from ui.export_controls import export_button
from ui.source_pdfs import ATTENDANCE, provenance_expander

from config import NOTABLE_TDS, SITTING_DAYS_BY_YEAR
from data_access.attendance_data import get_attendance_conn

_CAVEAT = (
    "Attendance figures reflect days a member was recorded present in the Dáil chamber "
    "on scheduled sitting days. The record does not capture committee hearings, ministerial "
    "duties, illness, bereavement, parental leave, or constituency work. "
    "Low attendance figures have many legitimate explanations that are not visible in this data. "
    "This page presents the official record — it does not make a judgement about the reasons behind it."
)

_MINISTER_NOTE = (
    "**Why are cabinet ministers shown separately?** "
    "Members holding ministerial office — including the Taoiseach, cabinet ministers, and Ministers of State — "
    "are constitutionally required to attend cabinet meetings, conduct bilateral engagements, "
    "represent the State at EU Council, and discharge executive duties that frequently "
    "conflict with scheduled plenary sitting days. Their plenary attendance figures are not "
    "a reliable indicator of their parliamentary or public service engagement and are excluded "
    "from the lowest-attendance ranking to avoid a misleading comparison."
)




# ── Bootstrap ──────────────────────────────────────────────────────────────────

# ── Retrieval (SELECT / WHERE / ORDER BY / LIMIT only) ────────────────────────

@st.cache_data(ttl=300)
def _views_ready() -> bool:
    return not get_attendance_conn().execute("SELECT 1 FROM v_attendance_summary LIMIT 1").df().empty


@st.cache_data(ttl=300)
def _fetch_filter_options() -> dict[str, list]:
    conn = get_attendance_conn()
    members = conn.execute(
        "SELECT DISTINCT member_name FROM v_attendance_member_summary"
        " ORDER BY member_name LIMIT 2000"
    ).fetchall()
    years = conn.execute(
        "SELECT DISTINCT year FROM v_attendance_member_year_summary"
        " ORDER BY year DESC LIMIT 100"
    ).fetchall()
    return {
        "members": [r[0] for r in members],
        "years":   [r[0] for r in years],
    }



@st.cache_data(ttl=300)
def _fetch_td_profile(td_name: str) -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT member_name, party_name, constituency"
        " FROM v_attendance_member_summary WHERE member_name = ? LIMIT 1",
        [td_name],
    ).df()


@st.cache_data(ttl=300)
def _fetch_member_years(td_name: str) -> pd.DataFrame:
    """Returns years DESC — used to populate the profile year pills."""
    return get_attendance_conn().execute(
        "SELECT year, attended_count FROM v_attendance_member_year_summary"
        " WHERE member_name = ? ORDER BY year DESC LIMIT 100",
        [td_name],
    ).df()


@st.cache_data(ttl=300)
def _fetch_year_ranking(year: int) -> pd.DataFrame:
    """Top and bottom attenders for a given year from v_attendance_year_rank."""
    return get_attendance_conn().execute(
        "SELECT member_name, party_name, constituency,"
        " attended_count, is_minister, rank_high, rank_low"
        " FROM v_attendance_year_rank WHERE year = ?"
        " ORDER BY rank_high ASC LIMIT 500",
        [year],
    ).df()


@st.cache_data(ttl=300)
def _fetch_timeline_for_year(td_name: str, year: int) -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT sitting_date, attendance_status"
        " FROM v_attendance_timeline"
        " WHERE member_name = ? AND year(sitting_date) = ?"
        " ORDER BY sitting_date ASC LIMIT 400",
        [td_name, year],
    ).df()


@st.cache_data(ttl=300)
def _fetch_timeline_stats(td_name: str, year: int) -> pd.DataFrame:
    """MIN/MAX sitting dates for a member-year — avoids pandas aggregation in the profile."""
    return get_attendance_conn().execute(
        "SELECT MIN(sitting_date) AS first_date, MAX(sitting_date) AS last_date"
        " FROM v_attendance_timeline"
        " WHERE member_name = ? AND year(sitting_date) = ?"
        " LIMIT 1",
        [td_name, year],
    ).df()


# ── Good cop / bad cop browse view ────────────────────────────────────────────

_GOOD_MEDALS = ["🥇", "🥈", "🥉"]
_HALL_SIZE   = 15


def _hall_card(row: pd.Series, medal: str, side: str, rank: int = 1) -> str:
    name  = _h(str(row["member_name"]))
    party = str(row.get("party_name", "") or "")
    const = str(row.get("constituency", "") or "")
    meta  = _h(clean_meta(party, const))
    days  = int(row["attended_count"])
    return (
        f'<div class="att-hall-card-{side}">'
        f'<span class="att-hall-rank">#{rank}</span>'
        f'<span class="att-hall-medal">{medal}</span>'
        f'<div class="att-hall-body">'
        f'<p class="att-hall-name">{name}</p>'
        f'<p class="att-hall-meta">{meta}</p>'
        f'</div>'
        f'<div class="att-hall-badge-{side}">'
        f'<span class="att-hall-badge-num">{days}</span>'
        f'<span class="att-hall-badge-label">days</span>'
        f'</div>'
        f'</div>'
    )



def _render_good_bad(ranking_df: pd.DataFrame, year: int) -> str | None:
    """
    Full-year: top/bottom _HALL_SIZE attenders side by side with per-card nav.
    Partial/current year: flat ranked list with an in-progress notice.
    Returns member_name on navigation, otherwise None.
    """
    today = datetime.date.today()

    # ── Partial / current year ─────────────────────────────────────────────────
    if year >= today.year:
        st.info(
            f"**{year} is in progress** — the Dáil year is not yet complete so a "
            f"full attendance ranking would be misleading. Showing all members by "
            f"days attended so far."
        )
        partial = (
            ranking_df.copy()
            .sort_values("attended_count", ascending=False)
            .reset_index(drop=True)
        )
        clicked: str | None = None
        for i, (_, row) in enumerate(partial.iterrows()):
            name = str(row["member_name"])
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, "", "good", rank=i + 1))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"att_partial_{i}", help=f"View {name}"):
                clicked = name
        return clicked

    # ── Full year ──────────────────────────────────────────────────────────────
    top = (
        ranking_df.sort_values(["rank_high", "attended_count"], ascending=[True, False])
        .head(_HALL_SIZE)
        .reset_index(drop=True)
    )
    # Ministers excluded from lowest-attendance list (data quality: source PDFs
    # do not record ministerial attendance — contract: known_data_quality_issues).
    non_ministers = ranking_df[ranking_df["is_minister"].astype(str).str.lower() != "true"]
    bottom = (
        non_ministers.sort_values(["rank_low", "attended_count"], ascending=[True, True])
        .head(_HALL_SIZE)
        .reset_index(drop=True)
    )

    col_good, col_bad = st.columns(2)
    clicked: str | None = None

    with col_good:
        st.html('<p class="att-hall-heading-good">Highest recorded attendance</p>')
        for i, (_, row) in enumerate(top.iterrows()):
            name  = str(row["member_name"])
            medal = _GOOD_MEDALS[i] if i < len(_GOOD_MEDALS) else ""
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, medal, "good", rank=i + 1))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"att_good_{i}", help=name):
                clicked = name

    with col_bad:
        st.html('<p class="att-hall-heading-bad">Lowest recorded attendance</p>')
        for i, (_, row) in enumerate(bottom.iterrows()):
            name = str(row["member_name"])
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, "", "bad", rank=i + 1))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"att_bad_{i}", help=name):
                clicked = name

    return clicked


# ── Attendance timeline strip (profile — one year at a time) ──────────────────

def _render_attendance_strip(timeline: pd.DataFrame, year: int) -> None:
    """Timeline strip: one tick per sitting day attended, month labels on x-axis.

    Month grid lines span the full year so recess gaps are naturally visible.
    Tooltip shows the exact date for each mark.
    """
    df = timeline.copy()
    df["sitting_date"] = pd.to_datetime(df["sitting_date"], errors="coerce")
    df = df.dropna(subset=["sitting_date"])
    if df.empty:
        return

    df["date_str"] = df["sitting_date"].dt.strftime("%d %b %Y")

    today = datetime.date.today()
    domain_end   = today.isoformat() if year >= today.year else f"{year}-12-31"
    domain_start = f"{year}-01-01"

    chart = (
        alt.Chart(df)
        .mark_tick(size=120, thickness=7, opacity=0.9)
        .encode(
            x=alt.X(
                "sitting_date:T",
                title=None,
                axis=alt.Axis(
                    format="%b",
                    tickCount="month",
                    labelAngle=0,
                    labelFontSize=14,
                    labelFontWeight="bold",
                    labelColor="#374151",
                    grid=True,
                    gridColor="#e5e7eb",
                    gridDash=[3, 3],
                    domain=True,
                    domainColor="#d1d5db",
                    tickSize=6,
                    tickColor="#d1d5db",
                    labelPadding=10,
                ),
                scale=alt.Scale(domain=[domain_start, domain_end], padding=12),
            ),
            color=alt.value("#16a34a"),
            tooltip=[alt.Tooltip("date_str:N", title="Date attended")],
        )
        .properties(height=170)
        .configure_view(strokeWidth=1, stroke="#d1d5db", fill="#ffffff")
        .configure_axis(labelFont="sans-serif")
    )
    st.altair_chart(chart, use_container_width=True)


# ── Profile view ───────────────────────────────────────────────────────────────

def _render_profile(td_name: str) -> None:
    profile = _fetch_td_profile(td_name)
    if profile.empty:
        empty_state(
            "No attendance data found for this member",
            "v_attendance_member_summary returned no rows for this name.",
        )
        return

    row   = profile.iloc[0]
    party = str(row.get("party_name")   or "—")
    const = str(row.get("constituency") or "—")

    member_profile_header(td_name, f"{party} · {const}")

    # ── Year pills (profile-level, newest first) ───────────────────────────────
    member_years_df = _fetch_member_years(td_name)
    if member_years_df.empty:
        empty_state("No year data available", "v_attendance_member_year_summary returned no rows.")
        return

    year_options = [str(int(y)) for y in member_years_df["year"].tolist()]

    selected_year = year_selector(year_options, key="att_profile_year", skip_current=False)

    # ── Stats for selected year (from pipeline — no pandas aggregation) ──────────
    yr_row     = member_years_df[member_years_df["year"] == selected_year]
    n_attended = int(yr_row["attended_count"].iloc[0]) if not yr_row.empty else 0

    ts_df  = _fetch_timeline_stats(td_name, selected_year)
    ts_row = ts_df.iloc[0] if not ts_df.empty else None
    if ts_row is not None and pd.notna(ts_row.get("first_date")):
        first_d = pd.Timestamp(ts_row["first_date"]).strftime("%d %b %Y")
        last_d  = pd.Timestamp(ts_row["last_date"]).strftime("%d %b %Y")
    else:
        first_d = "—"
        last_d  = "—"

    c1, c2, c3 = st.columns(3)
    c1.metric(
        f"Days attended · {selected_year}",
        n_attended,
        delta="plenary + committee days",
        delta_color="off",
    )
    c2.metric("First sitting",  first_d)
    c3.metric("Most recent",    last_d)

    st.divider()

    # ── Sitting calendar ───────────────────────────────────────────────────────
    timeline       = _fetch_timeline_for_year(td_name, selected_year)
    _required_cols = {"sitting_date", "attendance_status"}
    missing_cols   = _required_cols - set(timeline.columns)

    evidence_heading(f"Sitting calendar · {selected_year}")

    if missing_cols:
        todo_callout(f"v_attendance_timeline — missing columns: {', '.join(sorted(missing_cols))}")
    elif timeline.empty:
        empty_state(
            f"No sitting records for {selected_year}",
            "v_attendance_timeline returned no rows for this member and year.",
        )
    else:
        st.caption(
            f"{n_attended} days attended in {selected_year} (plenary + committee). "
            "Each mark below is a day the member was recorded present. "
            "Gaps between marks are Dáil recess periods."
        )
        todo_callout(
            "TODO_PIPELINE_VIEW_REQUIRED: session_type column on v_attendance_timeline. "
            "The source CSV (aggregated_td_tables.csv) contains both plenary sitting-day rows "
            "and committee/other-day rows for the same date, producing duplicates. "
            "attendance_status is hardcoded 'Present' — the pipeline must expose the original "
            "session-type label (e.g. 'Sitting day' / 'Other day') so the UI can deduplicate "
            "and correctly classify each record."
        )

        tl_all = timeline.copy()
        tl_all["sitting_date"] = pd.to_datetime(tl_all["sitting_date"], errors="coerce")
        tl_all = tl_all.dropna(subset=["sitting_date"]).sort_values("sitting_date")

        _render_attendance_strip(timeline, selected_year)

        with st.expander(f"Sitting dates · {n_attended} records", expanded=False):
            tl_table = tl_all.copy()
            tl_table["#"]       = range(1, len(tl_table) + 1)
            tl_table["Date"]    = tl_table["sitting_date"].dt.strftime("%d %b %Y")
            tl_table["Weekday"] = tl_table["sitting_date"].dt.strftime("%A")
            st.dataframe(
                tl_table[["#", "Date", "Weekday"]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "#":       st.column_config.NumberColumn("#",    width="small"),
                    "Date":    st.column_config.TextColumn("Date",   width="medium"),
                    "Weekday": st.column_config.TextColumn("Day",    width="medium"),
                },
            )

        export_button(
            timeline,
            label=f"Export {td_name} · {selected_year} · {n_attended} rows",
            filename=f"dail_tracker_attendance_{td_name.replace(' ', '_')}_{selected_year}.csv",
            key="att_td_export",
        )

    st.divider()
    _render_year_breakdown(td_name, member_years_df)


# ── Year breakdown table (profile secondary view) ─────────────────────────────

def _render_year_breakdown(td_name: str, years_df: pd.DataFrame) -> None:
    evidence_heading("Attendance by year")
    if years_df.empty:
        empty_state("No year data available", "v_attendance_member_year_summary returned no rows.")
        return

    rows = []
    for _, r in years_df.iterrows():
        y     = int(r["year"])
        days  = int(r["attended_count"])
        total = SITTING_DAYS_BY_YEAR.get(y)
        # Rate computation uses the contract-permitted hardcoded sitting-day totals
        # (permitted_hardcoded_values.year_sitting_days in attendance.yaml).
        pct   = days / total if total else None
        rows.append({"Year": y, "Days": days, "Total": str(total) if total else "—", "Rate": pct})

    table_df = pd.DataFrame(rows)
    st.dataframe(
        table_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Year":  st.column_config.NumberColumn("Year",          format="%d",  width="small"),
            "Days":  st.column_config.NumberColumn("Days attended",               width="small"),
            "Total": st.column_config.TextColumn("Sitting days",                  width="small"),
            "Rate":  st.column_config.ProgressColumn(
                         "Attendance",
                         min_value=0.0, max_value=1.0, format="%.0%",
                     ),
        },
    )
    safe = td_name.replace(" ", "_")
    export_button(
        table_df[["Year", "Days", "Total"]].rename(columns={"Total": "Sitting days"}),
        label=f"Export year breakdown · {td_name}",
        filename=f"dail_tracker_attendance_years_{safe}.csv",
        key="att_years_export",
    )


# ── Provenance footer ──────────────────────────────────────────────────────────

def _render_provenance(year: int | None = None) -> None:
    provenance_expander(
        sections=[_CAVEAT, _MINISTER_NOTE],
        source_caption=(
            "Data: Oireachtas TAA verification records (data.oireachtas.ie)"
            + (f" · {year}" if year else "")
        ),
        pdf_links=list(ATTENDANCE),
    )


# ── Page entry point ───────────────────────────────────────────────────────────

def attendance_page() -> None:
    inject_css()

    try:
        ready = _views_ready()
        opts  = _fetch_filter_options()
    except Exception as exc:
        empty_state(
            "Attendance views not available",
            "Run the pipeline to register v_attendance_member_summary and v_attendance_summary.",
        )
        st.caption(str(exc))
        return

    if not ready:
        empty_state("No attendance data found", "v_attendance_summary returned no rows.")
        return

    if not opts["years"]:
        empty_state("No year data found", "v_attendance_member_year_summary returned no rows.")
        return

    selected_td = st.session_state.get("selected_td_att")

    # ── Sidebar ────────────────────────────────────────────────────────────────
    with st.sidebar:
        sidebar_page_header("Plenary<br>Attendance")

        chosen = sidebar_member_filter(
            "Browse all members",
            opts["members"],
            key_search="att_sidebar_search",
            key_select="att_member_sel",
        )
        if chosen and st.session_state.get("selected_td_att") != chosen:
            st.session_state["selected_td_att"] = chosen
            st.rerun()

        st.divider()

        if render_notable_chips(NOTABLE_TDS, opts["members"], "chip_att", "selected_td_att"):
            st.rerun()

    # ── Page header ────────────────────────────────────────────────────────────
    if not selected_td:
        hero_banner(
            kicker="DÁIL PLENARY ATTENDANCE",
            title="The attendance record",
        )

    # ── Profile view ───────────────────────────────────────────────────────────
    if selected_td:
        if st.button("← Back to all members", key="att_back"):
            st.session_state["selected_td_att"] = None
            st.session_state.pop("att_member_sel", None)
            st.rerun()

        st.divider()
        _render_profile(selected_td)
        st.divider()
        _render_provenance()
        return

    # ── Primary view: year selector ────────────────────────────────────────────
    year_options = [str(y) for y in opts["years"]]  # DESC from query
    selected_year = year_selector(year_options, key="att_year")

    # ── Good cop / bad cop ────────────────────────────────────────────────────
    ranking_df = _fetch_year_ranking(selected_year)

    if ranking_df.empty:
        empty_state(
            "No records found",
            "Try a different year — v_attendance_year_rank returned no rows.",
        )
        _render_provenance(selected_year)
        return

    total_days = SITTING_DAYS_BY_YEAR.get(selected_year)
    n_members  = len(ranking_df)
    rate_note  = f" · {total_days} scheduled sitting days" if total_days else ""

    st.caption(f"{n_members} members on record{rate_note}")

    st.info(
        "Plenary attendance only — does not include committee hearings, ministerial duties, "
        "illness, or other absences with legitimate reasons. "
        "Low figures are not evidence of poor engagement.",
        icon=":material/info:",
    )

    clicked = _render_good_bad(ranking_df, selected_year)
    if clicked:
        st.session_state["selected_td_att"] = clicked
        st.rerun()

    # Export full list for the year
    today_str = datetime.date.today().isoformat()
    export_df = ranking_df[["member_name", "party_name", "constituency", "attended_count", "rank_high"]].copy()
    export_df = export_df.rename(columns={"rank_high": "rank_by_attendance"})
    export_button(
        export_df,
        label=f"Export full ranking · {selected_year} · {n_members} members",
        filename=f"dail_tracker_attendance_{selected_year}_{today_str}.csv",
        key="att_export",
    )

    _render_provenance(selected_year)
