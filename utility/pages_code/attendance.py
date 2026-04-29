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
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.components import empty_state, evidence_heading, member_profile_header, todo_callout
from ui.export_controls import export_button

_SQL_VIEWS = Path(__file__).resolve().parents[2] / "sql_views"

# Official plenary sitting-day counts from Houses of the Oireachtas Commission
# annual reports — hardcoded because they are fixed historical facts and routing
# the denominator through the full pipeline is disproportionate to the gain.
# Sources: oireachtas.ie/en/press-centre/press-releases/
_YEAR_SITTING_DAYS: dict[int, int] = {
    2020: 82,
    2021: 94,
    2022: 106,
    2023: 100,
    2024: 83,
    2025: 82,   # partial period per Houses of the Oireachtas 2025 report
}

# Dáil Standing Orders require a minimum of 120 plenary sitting days per year.
# Used as context for in-progress years where the final count is not yet known.
_DAIL_MIN_SITTING_DAYS = 120

_NOTABLE_TDS: list[str] = [
    "Michael Healy-Rae",
    "Michael Lowry",
    "Mary Lou McDonald",
    "Micheál Martin",
    "Simon Harris",
    "Pauline Tully",
]

_CAVEAT = (
    "Plenary attendance records days a member was present in the full chamber on "
    "scheduled sitting days. It does not capture committee hearings, ministerial duties, "
    "or constituency casework. Lower figures should not be read as a complete measure of "
    "a member's parliamentary engagement."
)

_YEAR_SOURCE_NOTE = (
    "Each year's data will link to the official Oireachtas attendance PDF for that year "
    "(e.g. 2025, 2024, 2023) once the pipeline exposes per-year source URLs."
)

_MINISTER_NOTE = (
    "**Why are cabinet ministers excluded from the Hall of Shame?** "
    "Members holding ministerial office — including the Taoiseach, cabinet ministers, and Ministers of State — "
    "are constitutionally and operationally required to attend cabinet meetings, conduct bilateral engagements, "
    "represent the State at EU Council and international forums, and discharge executive duties that frequently "
    "conflict with scheduled plenary sitting days. As a result, their plenary attendance figures, as recorded in "
    "the official Oireachtas PDFs, are not a reliable indicator of parliamentary or public service engagement "
    "and would mislead any comparative ranking of members' attendance."
)




# ── Bootstrap ──────────────────────────────────────────────────────────────────

@st.cache_resource
def _get_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("attendance_*.sql")):
        conn.execute(sql_file.read_text(encoding="utf-8"))
    return conn


# ── Retrieval (SELECT / WHERE / ORDER BY / LIMIT only) ────────────────────────

@st.cache_data(ttl=300)
def _fetch_summary() -> pd.DataFrame:
    return _get_conn().execute(
        "SELECT members_count, sitting_count, first_sitting_date, last_sitting_date,"
        " latest_fetch_timestamp_utc, source_summary, mart_version, code_version"
        " FROM v_attendance_summary LIMIT 1"
    ).df()


@st.cache_data(ttl=300)
def _fetch_filter_options() -> dict[str, list]:
    conn = _get_conn()
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
    return _get_conn().execute(
        "SELECT member_name, party_name, constituency"
        " FROM v_attendance_member_summary WHERE member_name = ? LIMIT 1",
        [td_name],
    ).df()


@st.cache_data(ttl=300)
def _fetch_member_years(td_name: str) -> pd.DataFrame:
    """Returns years DESC — used to populate the profile year pills."""
    return _get_conn().execute(
        "SELECT year, attended_count FROM v_attendance_member_year_summary"
        " WHERE member_name = ? ORDER BY year DESC LIMIT 100",
        [td_name],
    ).df()


@st.cache_data(ttl=300)
def _fetch_year_ranking(year: int) -> pd.DataFrame:
    """Top and bottom attenders for a given year from v_attendance_year_rank."""
    return _get_conn().execute(
        "SELECT member_name, party_name, constituency, attended_count, is_minister,"
        " rank_high, rank_low"
        " FROM v_attendance_year_rank WHERE year = ?"
        " ORDER BY rank_high ASC LIMIT 500",
        [year],
    ).df()


@st.cache_data(ttl=300)
def _fetch_timeline_for_year(td_name: str, year: int) -> pd.DataFrame:
    return _get_conn().execute(
        "SELECT sitting_date, attendance_status"
        " FROM v_attendance_timeline"
        " WHERE member_name = ? AND year(sitting_date) = ?"
        " ORDER BY sitting_date ASC LIMIT 400",
        [td_name, year],
    ).df()


# ── Good cop / bad cop browse view ────────────────────────────────────────────

_GOOD_MEDALS = ["🥇", "🥈", "🥉"]
_BAD_MEDALS  = ["💀", "👻", "😴"]


def _hall_card(row: pd.Series, medal: str, side: str, year: int | None = None, rank: int = 1) -> str:
    name  = str(row["member_name"])
    party = str(row.get("party_name", "") or "")
    const = str(row.get("constituency", "") or "")
    meta  = " · ".join(p for p in [party, const] if p and p.lower() not in ("nan", ""))
    days  = int(row["attended_count"])
    total = _YEAR_SITTING_DAYS.get(year) if year else None
    of_label = f"of {total}" if total else "days"
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
        f'<span class="att-hall-badge-label">{of_label}</span>'
        f'</div>'
        f'</div>'
    )


def _list_row(row: pd.Series, rank: int) -> str:
    """Rank number + name/meta pill. Days badge is rendered separately in its own column."""
    name  = str(row["member_name"])
    party = str(row.get("party_name", "") or "")
    const = str(row.get("constituency", "") or "")
    meta  = " · ".join(p for p in [party, const] if p and p.lower() not in ("nan", ""))
    return (
        f'<div class="att-list-row">'
        f'<span class="att-list-rank">{rank}</span>'
        f'<div class="att-list-pill">'
        f'<div class="att-list-pill-name">{name}</div>'
        f'<div class="att-list-pill-meta">{meta}</div>'
        f'</div>'
        f'</div>'
    )


def _days_badge(row: pd.Series, year: int | None = None) -> str:
    """Days-attended badge using the dt-success-* calm-blue theme."""
    days     = int(row["attended_count"])
    total    = _YEAR_SITTING_DAYS.get(year) if year else None
    of_label = f"of {total}" if total else "days"
    return (
        f'<div class="dt-success-badge">'
        f'<span class="dt-success-num">{days}</span>'
        f'<span class="dt-success-lbl">{of_label}</span>'
        f'</div>'
    )


def _render_good_bad(ranking_df: pd.DataFrame, year: int, n: int = 3) -> str | None:
    """
    Full-year: Hall of Fame (top-N) and Hall of Shame (bottom-N) side by side.
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
            c1, c2, c3 = st.columns([7, 1, 2])
            c1.markdown(_list_row(row, rank=i + 1), unsafe_allow_html=True)
            if c2.button("→", key=f"att_partial_{i}", use_container_width=True):
                clicked = str(row["member_name"])
            c3.markdown(_days_badge(row, year), unsafe_allow_html=True)
        return clicked

    # ── Full year ──────────────────────────────────────────────────────────────
    # Sort by attended_count as secondary key so ties are broken deterministically
    # and top/bottom lists don't overlap when many members share the same count.
    top = (
        ranking_df.sort_values(["rank_high", "attended_count"], ascending=[True, False])
        .head(n)
        .reset_index(drop=True)
    )
    # Ministers are excluded from the Hall of Shame — lower plenary attendance
    # is expected for cabinet members with mandatory ministerial duties.
    non_ministers = ranking_df[ranking_df["is_minister"].astype(str).str.lower() != "true"]
    bottom = (
        non_ministers.sort_values(["rank_low", "attended_count"], ascending=[True, True])
        .head(n)
        .reset_index(drop=True)
    )

    col_good, col_bad = st.columns(2)
    clicked: str | None = None

    with col_good:
        st.markdown(
            '<p class="att-hall-heading-good">🏆 Hall of Fame</p>'
            + "".join(_hall_card(row, _GOOD_MEDALS[i], "good", year, rank=i + 1)
                      for i, (_, row) in enumerate(top.iterrows())),
            unsafe_allow_html=True,
        )

    with col_bad:
        st.markdown(
            '<p class="att-hall-heading-bad">🚨 Hall of Shame</p>'
            + "".join(_hall_card(row, _BAD_MEDALS[i], "bad", year, rank=i + 1)
                      for i, (_, row) in enumerate(bottom.iterrows())),
            unsafe_allow_html=True,
        )

    # Navigation buttons in a single aligned row below both columns
    all_rows = list(top.iterrows()) + list(bottom.iterrows())
    keys     = [f"att_good_{i}" for i in range(n)] + [f"att_bad_{i}" for i in range(n)]
    btn_cols = st.columns(n * 2)
    for col, (_, row), key in zip(btn_cols, all_rows, keys, strict=False):
        label = str(row["member_name"]).split()[-1]
        if col.button(label, key=key, use_container_width=True):
            clicked = str(row["member_name"])

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

    selected_year_str: str | None = st.pills(
        "Year",
        options=year_options,
        default=year_options[0],
        key="att_profile_year",
        label_visibility="collapsed",
    )
    selected_year = int(selected_year_str) if selected_year_str else int(year_options[0])

    # ── Stats for selected year ────────────────────────────────────────────────
    timeline = _fetch_timeline_for_year(td_name, selected_year)
    _required_cols = {"sitting_date", "attendance_status"}
    missing_cols   = _required_cols - set(timeline.columns)

    if not timeline.empty and not missing_cols:
        tl = timeline.copy()
        tl["sitting_date"] = pd.to_datetime(tl["sitting_date"], errors="coerce")
        tl = tl.dropna(subset=["sitting_date"])
        n_attended = len(tl)
        first_d    = tl["sitting_date"].min().strftime("%d %b %Y")
        last_d     = tl["sitting_date"].max().strftime("%d %b %Y")
    else:
        yr_row     = member_years_df[member_years_df["year"] == selected_year]
        n_attended = int(yr_row["attended_count"].iloc[0]) if not yr_row.empty else 0
        first_d    = "—"
        last_d     = "—"

    total_days = _YEAR_SITTING_DAYS.get(selected_year)
    c1, c2, c3 = st.columns(3)
    if total_days:
        c1.metric(
            f"Days attended · {selected_year}",
            n_attended,
            delta=f"of {total_days} official sitting days",
            delta_color="off",
        )
    else:
        c1.metric(
            f"Days attended · {selected_year}",
            n_attended,
            delta=f"min. {_DAIL_MIN_SITTING_DAYS} days required per year",
            delta_color="off",
        )
    c2.metric("First sitting",  first_d)
    c3.metric("Most recent",    last_d)

    st.divider()

    # ── Sitting calendar ───────────────────────────────────────────────────────
    evidence_heading(f"Sitting calendar · {selected_year}")

    if missing_cols:
        todo_callout(f"v_attendance_timeline — missing columns: {', '.join(sorted(missing_cols))}")
    elif timeline.empty:
        empty_state(
            f"No sitting records for {selected_year}",
            "v_attendance_timeline returned no rows for this member and year.",
        )
    else:
        if total_days:
            st.caption(
                f"{n_attended} of {total_days} official sitting days attended in {selected_year}. "
                "Each mark below is a day the member was recorded present in the Dáil chamber. "
                "Gaps between marks are Dáil recess periods."
            )
        else:
            st.caption(
                f"{n_attended} sitting days attended so far in {selected_year} "
                f"(year in progress — Dáil required to sit a minimum of {_DAIL_MIN_SITTING_DAYS} days per year). "
                "Each mark below is a day the member was recorded present in the Dáil chamber. "
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


# ── Provenance footer ──────────────────────────────────────────────────────────

def _render_provenance(summary: pd.Series, year: int | None = None) -> None:
    source   = str(summary.get("source_summary") or "—")
    fetch_ts = str(summary.get("latest_fetch_timestamp_utc") or "—")[:19]
    mart_v   = str(summary.get("mart_version") or "—")
    code_v   = str(summary.get("code_version") or "—")
    with st.expander("About & data provenance", expanded=False):
        st.markdown(_CAVEAT)
        st.markdown(_MINISTER_NOTE)
        if year:
            st.caption(f"Showing data for: {year}. {_YEAR_SOURCE_NOTE}")
        else:
            st.caption(_YEAR_SOURCE_NOTE)
        st.caption(f"Source: {source}  ·  Fetched: {fetch_ts}  ·  Mart: {mart_v}  ·  Code: {code_v}")


# ── Page entry point ───────────────────────────────────────────────────────────

def attendance_page() -> None:
    if "selected_td_att" not in st.session_state:
        st.session_state["selected_td_att"] = None

    inject_css()

    try:
        summary_df = _fetch_summary()
        opts       = _fetch_filter_options()
    except Exception as exc:
        empty_state(
            "Attendance views not available",
            "Run the pipeline to register v_attendance_member_summary and v_attendance_summary.",
        )
        st.caption(str(exc))
        return

    if summary_df.empty:
        empty_state("No attendance data found", "v_attendance_summary returned no rows.")
        return

    if not opts["years"]:
        empty_state("No year data found", "v_attendance_member_year_summary returned no rows.")
        return

    summary     = summary_df.iloc[0]
    selected_td = st.session_state.get("selected_td_att")

    # ── Sidebar ────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<p class="page-kicker">Dáil Tracker</p>', unsafe_allow_html=True)
        st.markdown('<p class="page-title">Plenary<br>Attendance</p>', unsafe_allow_html=True)

        att_search: str = st.text_input(
            "",
            placeholder="Search a member…",
            key="att_sidebar_search",
            label_visibility="collapsed",
        )
        sq = att_search.strip().lower()
        att_filtered = [n for n in opts["members"] if sq in n.lower()] if sq else opts["members"]
        chosen = st.selectbox(
            "Browse all members",
            ["— select a member —"] + att_filtered,
            key="att_member_sel",
            label_visibility="collapsed",
        )
        if chosen and chosen != "— select a member —" and st.session_state.get("selected_td_att") != chosen:
            st.session_state["selected_td_att"] = chosen
            st.rerun()

        st.divider()

        st.markdown('<p class="sidebar-label">Notable members</p>', unsafe_allow_html=True)
        available_notable = [n for n in _NOTABLE_TDS if n in opts["members"]]
        chip_cols = st.columns(2)
        for i, name in enumerate(available_notable):
            if chip_cols[i % 2].button(name, key=f"chip_att_{name}", use_container_width=True):
                st.session_state["selected_td_att"] = name
                st.rerun()

    # ── Page header ────────────────────────────────────────────────────────────
    st.markdown(
        '<p class="dt-kicker">Dáil Plenary Attendance</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<h1 style="margin:0.1rem 0 0.6rem;font-size:1.65rem;font-weight:700">'
        "Who shows up to the chamber?</h1>",
        unsafe_allow_html=True,
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
        _render_provenance(summary)
        return

    # ── Primary view: year selector ────────────────────────────────────────────
    year_options = [str(y) for y in opts["years"]]  # DESC from query
    selected_year_str: str | None = st.pills(
        "Year",
        options=year_options,
        default=year_options[0],
        key="att_year",
        label_visibility="collapsed",
    )
    selected_year = int(selected_year_str) if selected_year_str else int(year_options[0])

    # ── Good cop / bad cop ────────────────────────────────────────────────────
    ranking_df = _fetch_year_ranking(selected_year)

    if ranking_df.empty:
        empty_state(
            "No records found",
            "Try a different year — v_attendance_year_rank returned no rows.",
        )
        _render_provenance(summary, selected_year)
        return

    total_days = _YEAR_SITTING_DAYS.get(selected_year)
    n_members  = len(ranking_df)
    rate_note  = f" · {total_days} official sitting days" if total_days else ""

    st.caption(f"{n_members} members on record{rate_note}")

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

    _render_provenance(summary, selected_year)
