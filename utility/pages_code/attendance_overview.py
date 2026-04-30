"""
Dáil Attendance — Total Overview since records began.

Year pills run ASC (oldest on the left → newest on the right).
All-time view uses v_attendance_member_summary (total days across all years).
Year-specific view uses v_attendance_year_rank (same as the per-year page).
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
    render_stat_strip,
    sidebar_member_filter,
    sidebar_page_header,
    stat_item,
    render_notable_chips,
)
from ui.export_controls import export_button
from ui.source_pdfs import ATTENDANCE, provenance_expander

from config import NOTABLE_TDS, SITTING_DAYS_BY_YEAR
from data_access.attendance_data import get_attendance_conn

_CAVEAT = (
    "Attendance figures reflect days a member was recorded present in the Dáil chamber "
    "on scheduled sitting days. The record does not capture committee hearings, ministerial "
    "duties, illness, bereavement, parental leave, or constituency work. "
    "Low attendance figures have many legitimate explanations that are not visible in this data."
)

_MINISTER_NOTE = (
    "**Why are cabinet ministers shown separately in year rankings?** "
    "Members holding ministerial office are constitutionally required to attend cabinet "
    "meetings and discharge executive duties that frequently conflict with plenary sittings. "
    "Their plenary figures are excluded from the lowest-attendance ranking to avoid a "
    "misleading comparison."
)

_GOOD_MEDALS = ["🥇", "🥈", "🥉"]
_HALL_SIZE   = 15


# ── Data retrieval ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _fetch_global_stats() -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT members_count, sitting_count, first_sitting_date, last_sitting_date"
        " FROM v_attendance_summary LIMIT 1"
    ).df()


@st.cache_data(ttl=300)
def _fetch_available_years() -> list[int]:
    """Returns years ASC — oldest on the left, newest on the right."""
    rows = get_attendance_conn().execute(
        "SELECT DISTINCT year FROM v_attendance_member_year_summary"
        " WHERE year IS NOT NULL ORDER BY year ASC"
    ).fetchall()
    return [int(r[0]) for r in rows]


@st.cache_data(ttl=300)
def _fetch_alltime_ranking() -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT member_name, party_name, constituency,"
        " attended_count, sitting_count"
        " FROM v_attendance_member_summary"
        " ORDER BY attended_count DESC LIMIT 500"
    ).df()


@st.cache_data(ttl=300)
def _fetch_year_ranking(year: int) -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT member_name, party_name, constituency,"
        " attended_count, is_minister, rank_high, rank_low"
        " FROM v_attendance_year_rank WHERE year = ?"
        " ORDER BY rank_high ASC LIMIT 500",
        [year],
    ).df()


@st.cache_data(ttl=300)
def _fetch_year_member_counts() -> pd.DataFrame:
    """Per-year member count — used for the year summary strip."""
    return get_attendance_conn().execute(
        "SELECT CAST(year AS INTEGER) AS year,"
        " COUNT(DISTINCT member_name) AS members_count"
        " FROM v_attendance_member_year_summary"
        " WHERE year IS NOT NULL GROUP BY year ORDER BY year ASC"
    ).df()


@st.cache_data(ttl=300)
def _fetch_ov_member_years(td_name: str) -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT CAST(year AS INTEGER) AS year, attended_count"
        " FROM v_attendance_member_year_summary"
        " WHERE member_name = ? ORDER BY year DESC LIMIT 100",
        [td_name],
    ).df()


@st.cache_data(ttl=300)
def _fetch_ov_profile(td_name: str) -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT member_name, party_name, constituency"
        " FROM v_attendance_member_summary WHERE member_name = ? LIMIT 1",
        [td_name],
    ).df()


@st.cache_data(ttl=300)
def _fetch_ov_timeline(td_name: str, year: int) -> pd.DataFrame:
    return get_attendance_conn().execute(
        "SELECT sitting_date, attendance_status"
        " FROM v_attendance_timeline"
        " WHERE member_name = ? AND year(sitting_date) = ?"
        " ORDER BY sitting_date ASC LIMIT 400",
        [td_name, year],
    ).df()


# ── Card / HTML helpers ────────────────────────────────────────────────────────

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


def _year_strip_html(year_counts_df: pd.DataFrame, selected: str) -> str:
    today = datetime.date.today().year
    cards = ""
    for _, row in year_counts_df.iterrows():
        y = int(row["year"])
        n = int(row["members_count"])
        sched = SITTING_DAYS_BY_YEAR.get(y)
        sub = f"{sched} days" if sched else ("in progress" if y >= today else "—")
        active = "att-ov-year-card-active" if str(y) == selected else ""
        cards += (
            f'<div class="att-ov-year-card {active}">'
            f'<span class="att-ov-year-num">{y}</span>'
            f'<span class="att-ov-year-members">{n} members</span>'
            f'<span class="att-ov-year-days">{sub}</span>'
            f'</div>'
        )
    return f'<div class="att-ov-year-strip">{cards}</div>'


# ── Render: attendance timeline chart (per-year calendar strip) ────────────────

def _render_timeline_chart(timeline: pd.DataFrame, year: int) -> None:
    df = timeline.copy()
    df["sitting_date"] = pd.to_datetime(df["sitting_date"], errors="coerce")
    df = df.dropna(subset=["sitting_date"])
    if df.empty:
        return
    df["date_str"] = df["sitting_date"].dt.strftime("%d %b %Y")
    today     = datetime.date.today()
    end_bound = today.isoformat() if year >= today.year else f"{year}-12-31"
    chart = (
        alt.Chart(df)
        .mark_tick(size=120, thickness=7, opacity=0.9)
        .encode(
            x=alt.X(
                "sitting_date:T",
                title=None,
                axis=alt.Axis(
                    format="%b", tickCount="month", labelAngle=0,
                    labelFontSize=14, labelFontWeight="bold", labelColor="#374151",
                    grid=True, gridColor="#e5e7eb", gridDash=[3, 3],
                    domain=True, domainColor="#d1d5db", tickSize=6, tickColor="#d1d5db",
                    labelPadding=10,
                ),
                scale=alt.Scale(domain=[f"{year}-01-01", end_bound], padding=12),
            ),
            color=alt.value("#16a34a"),
            tooltip=[alt.Tooltip("date_str:N", title="Date attended")],
        )
        .properties(height=170)
        .configure_view(strokeWidth=1, stroke="#d1d5db", fill="#ffffff")
        .configure_axis(labelFont="sans-serif")
    )
    st.altair_chart(chart, use_container_width=True)


# ── Render: good / bad two-column cards ───────────────────────────────────────

def _render_good_bad(ranking_df: pd.DataFrame, year: int, key_pfx: str) -> str | None:
    today = datetime.date.today()
    clicked: str | None = None

    if year >= today.year:
        st.info(
            f"**{year} is in progress** — the Dáil year is not yet complete so a full "
            f"ranking would be misleading. Showing all members by days attended so far.",
        )
        partial = ranking_df.sort_values("attended_count", ascending=False).reset_index(drop=True)
        for i, (_, row) in enumerate(partial.iterrows()):
            name  = str(row["member_name"])
            medal = _GOOD_MEDALS[i] if i < len(_GOOD_MEDALS) else ""
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, medal, "good", rank=i + 1))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"{key_pfx}_partial_{i}", help=name):
                clicked = name
        return clicked

    top = (
        ranking_df
        .sort_values(["rank_high", "attended_count"], ascending=[True, False])
        .head(_HALL_SIZE)
        .reset_index(drop=True)
    )
    non_min = ranking_df[ranking_df["is_minister"].astype(str).str.lower() != "true"]
    bottom = (
        non_min
        .sort_values(["rank_low", "attended_count"], ascending=[True, True])
        .head(_HALL_SIZE)
        .reset_index(drop=True)
    )

    col_good, col_bad = st.columns(2)
    with col_good:
        st.html('<p class="att-hall-heading-good">Highest recorded attendance</p>')
        for i, (_, row) in enumerate(top.iterrows()):
            name  = str(row["member_name"])
            medal = _GOOD_MEDALS[i] if i < len(_GOOD_MEDALS) else ""
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, medal, "good", rank=i + 1))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"{key_pfx}_good_{i}", help=name):
                clicked = name
    with col_bad:
        st.html('<p class="att-hall-heading-bad">Lowest recorded attendance</p>')
        for i, (_, row) in enumerate(bottom.iterrows()):
            name = str(row["member_name"])
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, "", "bad", rank=i + 1))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"{key_pfx}_bad_{i}", help=name):
                clicked = name
    return clicked


# ── Render: all-time view ──────────────────────────────────────────────────────

def _render_alltime(df: pd.DataFrame, year_counts_df: pd.DataFrame) -> str | None:
    top    = df.head(_HALL_SIZE).reset_index(drop=True)
    bottom = df.tail(_HALL_SIZE).reset_index(drop=True)
    total  = len(df)

    col_good, col_right = st.columns([3, 2])
    clicked: str | None = None

    with col_good:
        st.html('<p class="att-hall-heading-good">Highest total attendance — all years</p>')
        st.html(
            '<p class="att-hall-subheading">Total days attended across entire record period</p>'
        )
        for i, (_, row) in enumerate(top.iterrows()):
            name  = str(row["member_name"])
            medal = _GOOD_MEDALS[i] if i < len(_GOOD_MEDALS) else ""
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, medal, "good", rank=i + 1))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"ov_at_good_{i}", help=name):
                clicked = name

    with col_right:
        st.html('<p class="att-hall-heading-bad">Lowest total attendance — all years</p>')
        st.html(
            '<p class="att-hall-subheading">Includes ministers — select a year for a filtered ranking</p>'
        )
        for i, (_, row) in enumerate(bottom.iterrows()):
            name = str(row["member_name"])
            rank = total - _HALL_SIZE + 1 + i
            cc, bc = st.columns([14, 1])
            cc.html(_hall_card(row, "", "bad", rank=rank))
            bc.html('<div class="dt-nav-anchor"></div>')
            if bc.button("→", key=f"ov_at_bad_{i}", help=name):
                clicked = name

    return clicked


# ── Render: compact inline profile ────────────────────────────────────────────

def _render_profile(td_name: str) -> None:
    if st.button("← Back to overview", key="ov_back"):
        st.session_state.pop("_ov_td", None)
        st.rerun()

    profile = _fetch_ov_profile(td_name)
    if profile.empty:
        empty_state("Member not found", f"No record found for {_h(td_name)}.")
        return

    row    = profile.iloc[0]
    party  = str(row.get("party_name",   "") or "")
    const  = str(row.get("constituency", "") or "")
    member_profile_header(td_name, clean_meta(party, const))

    years_df = _fetch_ov_member_years(td_name)
    if years_df.empty:
        empty_state("No year data", "v_attendance_member_year_summary returned no rows.")
        return

    year_opts = [str(int(y)) for y in years_df["year"].tolist()]

    selected_str = st.pills(
        "Year",
        options=year_opts,
        default=year_opts[0],
        key="ov_profile_year",
        label_visibility="collapsed",
    )
    selected_year = int(selected_str) if selected_str else int(year_opts[0])

    yr_row    = years_df[years_df["year"] == selected_year]
    n_attended = int(yr_row["attended_count"].iloc[0]) if not yr_row.empty else 0
    sched      = SITTING_DAYS_BY_YEAR.get(selected_year)
    rate_str   = f"{n_attended / sched:.0%}" if sched else "—"

    c1, c2, c3 = st.columns(3)
    c1.metric(f"Days attended · {selected_year}", n_attended)
    c2.metric("Scheduled sitting days", str(sched) if sched else "—")
    c3.metric("Attendance rate", rate_str)

    st.divider()
    evidence_heading(f"Sitting calendar · {selected_year}")

    timeline = _fetch_ov_timeline(td_name, selected_year)
    if timeline.empty:
        empty_state(
            f"No sitting records for {selected_year}",
            "v_attendance_timeline returned no rows.",
        )
    else:
        st.caption(
            f"{n_attended} days attended in {selected_year}. "
            "Each mark is a day the member was recorded present. Gaps are Dáil recess."
        )
        _render_timeline_chart(timeline, selected_year)

    st.divider()
    evidence_heading("Attendance by year")
    rows = []
    for _, r in years_df.iterrows():
        y     = int(r["year"])
        days  = int(r["attended_count"])
        total = SITTING_DAYS_BY_YEAR.get(y)
        pct   = days / total if total else None
        rows.append({"Year": y, "Days": days, "Sitting days": str(total) if total else "—", "Rate": pct})
    st.dataframe(
        pd.DataFrame(rows),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Year":         st.column_config.NumberColumn("Year", format="%d", width="small"),
            "Days":         st.column_config.NumberColumn("Days attended",     width="small"),
            "Sitting days": st.column_config.TextColumn("Sitting days",        width="small"),
            "Rate":         st.column_config.ProgressColumn(
                                "Attendance", min_value=0.0, max_value=1.0, format="%.0%",
                            ),
        },
    )
    export_button(
        pd.DataFrame(rows)[["Year", "Days", "Sitting days"]],
        label=f"Export {td_name} year breakdown",
        filename=f"dail_tracker_att_overview_{td_name.replace(' ', '_')}.csv",
        key="ov_profile_export",
    )


# ── Page entry point ───────────────────────────────────────────────────────────

def attendance_overview_page() -> None:
    inject_css()

    try:
        stats_df    = _fetch_global_stats()
        years_asc   = _fetch_available_years()
        year_counts = _fetch_year_member_counts()
    except Exception as exc:
        empty_state(
            "Attendance views not available",
            "Run the pipeline to populate v_attendance_summary.",
        )
        st.caption(str(exc))
        return

    if stats_df.empty or not years_asc:
        empty_state("No data", "v_attendance_summary returned no rows.")
        return

    stats_row  = stats_df.iloc[0]
    n_members  = int(stats_row.get("members_count", 0))
    n_days     = int(stats_row.get("sitting_count", 0))
    first_date = stats_row.get("first_sitting_date")
    last_date  = stats_row.get("last_sitting_date")
    first_year = pd.Timestamp(first_date).year if pd.notna(first_date) else years_asc[0]
    last_year  = pd.Timestamp(last_date).year  if pd.notna(last_date)  else years_asc[-1]

    selected_td: str | None = st.session_state.get("_ov_td")

    # ── Sidebar ────────────────────────────────────────────────────────────────
    with st.sidebar:
        sidebar_page_header("Attendance<br>Overview")

        all_members_df = _fetch_alltime_ranking()
        all_names = all_members_df["member_name"].tolist() if not all_members_df.empty else []
        chosen = sidebar_member_filter(
            "Browse all members",
            all_names,
            key_search="ov_sidebar_search",
            key_select="ov_member_sel",
        )
        if chosen and st.session_state.get("_ov_td") != chosen:
            st.session_state["_ov_td"] = chosen
            st.rerun()

        st.divider()
        if render_notable_chips(NOTABLE_TDS, all_names, "chip_ov", "_ov_td"):
            st.rerun()

    # ── Profile view ───────────────────────────────────────────────────────────
    if selected_td:
        _render_profile(selected_td)
        st.divider()
        provenance_expander(
            sections=[_CAVEAT],
            source_caption="Data: Oireachtas TAA verification records (data.oireachtas.ie)",
            pdf_links=list(ATTENDANCE),
        )
        return

    # ── Hero ───────────────────────────────────────────────────────────────────
    hero_banner(
        kicker="DÁIL PLENARY ATTENDANCE · TOTAL RECORD",
        title="Total attendance since records began",
        dek=(
            f"All plenary sitting days recorded from {first_year} to {last_year}. "
            "Select a year below for that year's ranking, or view the all-time totals."
        ),
    )
    render_stat_strip(
        stat_item(f"{n_members:,}", "members on record"),
        stat_item(f"{n_days:,}",    "total sitting days"),
        stat_item(f"{first_year}–{last_year}", "record period"),
    )

    # ── Year pills — ASC (oldest left → newest right) ─────────────────────────
    pill_opts  = ["All years"] + [str(y) for y in years_asc]
    year_sel   = st.pills(
        "Year",
        options=pill_opts,
        default="All years",
        key="ov_year_pill",
        label_visibility="collapsed",
    ) or "All years"

    # ── Year summary strip ────────────────────────────────────────────────────
    if not year_counts.empty:
        st.html(_year_strip_html(year_counts, year_sel))

    st.info(
        "Plenary attendance only — does not include committee hearings, ministerial duties, "
        "illness, or other absences with legitimate reasons. "
        "Low figures are not evidence of poor engagement.",
        icon=":material/info:",
    )

    # ── All-years view ─────────────────────────────────────────────────────────
    if year_sel == "All years":
        alltime_df = _fetch_alltime_ranking()
        if alltime_df.empty:
            empty_state("No data", "v_attendance_member_summary returned no rows.")
        else:
            clicked = _render_alltime(alltime_df, year_counts)
            if clicked:
                st.session_state["_ov_td"] = clicked
                st.rerun()

            today_str = datetime.date.today().isoformat()
            export_button(
                alltime_df[["member_name", "party_name", "constituency", "attended_count"]],
                label=f"Export all-time ranking · {n_members} members",
                filename=f"dail_tracker_attendance_alltime_{today_str}.csv",
                key="ov_alltime_export",
            )

    # ── Year-specific view ─────────────────────────────────────────────────────
    else:
        sel_year  = int(year_sel)
        total_days = SITTING_DAYS_BY_YEAR.get(sel_year)
        ranking_df = _fetch_year_ranking(sel_year)

        if ranking_df.empty:
            empty_state(
                "No records found",
                f"v_attendance_year_rank returned no rows for {sel_year}.",
            )
        else:
            n_yr      = len(ranking_df)
            rate_note = f" · {total_days} scheduled sitting days" if total_days else ""
            st.caption(f"{n_yr} members on record{rate_note}")

            clicked = _render_good_bad(ranking_df, sel_year, "ov_yr")
            if clicked:
                st.session_state["_ov_td"] = clicked
                st.rerun()

            today_str = datetime.date.today().isoformat()
            export_df = ranking_df[
                ["member_name", "party_name", "constituency", "attended_count", "rank_high"]
            ].rename(columns={"rank_high": "rank"})
            export_button(
                export_df,
                label=f"Export {sel_year} ranking · {n_yr} members",
                filename=f"dail_tracker_attendance_{sel_year}_{today_str}.csv",
                key="ov_yr_export",
            )

    # ── Provenance ─────────────────────────────────────────────────────────────
    provenance_expander(
        sections=[_CAVEAT, _MINISTER_NOTE],
        source_caption="Data: Oireachtas TAA verification records (data.oireachtas.ie)",
        pdf_links=list(ATTENDANCE),
    )
