"""
Dáil Attendance Tracker — Dáil Tracker.

Temporary data source: reads directly from silver CSV files.
Once the pipeline delivers data/gold/dail.duckdb with the views below, replace all
_simulate_* functions with direct SELECT queries against those pipeline-owned views.

TODO_PIPELINE_VIEW_REQUIRED: CREATE OR REPLACE VIEW v_attendance_member_summary AS
    SELECT
        CONCAT(a.first_name, ' ', a.last_name)               AS member_name,
        a.identifier                                          AS member_id,
        COALESCE(m.party, '')                                 AS party_name,
        COALESCE(m.constituency_name, '')                     AS constituency,
        a.sitting_days_count                                  AS attended_count,
        <total_sitting_count> - a.sitting_days_count          AS absent_count,
        <total_sitting_count>                                 AS sitting_count,
        a.sitting_days_count::DOUBLE / <total_sitting_count>  AS attendance_rate,
        MIN(a.iso_sitting_days_attendance)                    AS first_sitting_date,
        MAX(a.iso_sitting_days_attendance)                    AS last_sitting_date,
        'pipeline'                                            AS latest_run_id,
        current_timestamp                                     AS latest_fetch_timestamp_utc,
        'data/silver/aggregated_td_tables.csv'                AS source_summary,
        NULL::VARCHAR                                         AS mart_version,
        NULL::VARCHAR                                         AS code_version
    FROM (SELECT DISTINCT identifier, first_name, last_name, sitting_days_count
          FROM read_csv_auto('data/silver/aggregated_td_tables.csv')) a
    LEFT JOIN (SELECT first_name, last_name, constituency_name, party
               FROM read_csv_auto('data/silver/flattened_members.csv')) m
           ON a.first_name = m.first_name AND a.last_name = m.last_name;

TODO_PIPELINE_VIEW_REQUIRED: CREATE OR REPLACE VIEW v_attendance_member_year_summary AS
    SELECT
        CONCAT(a.first_name, ' ', a.last_name)               AS member_name,
        a.identifier                                          AS member_id,
        a.year,
        MAX(a.sitting_days_count)                             AS attended_count,
        COALESCE(m.party, '')                                 AS party_name,
        COALESCE(m.constituency_name, '')                     AS constituency
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv') a
    LEFT JOIN (SELECT first_name, last_name, constituency_name, party
               FROM read_csv_auto('data/silver/flattened_members.csv')) m
           ON a.first_name = m.first_name AND a.last_name = m.last_name
    GROUP BY member_name, member_id, a.year, party_name, constituency;

TODO_PIPELINE_VIEW_REQUIRED: CREATE OR REPLACE VIEW v_attendance_timeline AS
    SELECT
        row_number() OVER ()                                  AS attendance_timeline_id,
        iso_sitting_days_attendance                           AS sitting_date,
        identifier                                            AS member_id,
        CONCAT(first_name, ' ', last_name)                    AS member_name,
        TRUE                                                  AS present_flag,
        'Present'                                             AS attendance_status,
        COALESCE(m.party, '')                                 AS party_name,
        COALESCE(m.constituency_name, '')                     AS constituency,
        'pipeline'                                            AS latest_run_id,
        current_timestamp                                     AS latest_fetch_timestamp_utc,
        'data/silver/aggregated_td_tables.csv'                AS source_summary,
        NULL::VARCHAR                                         AS mart_version,
        NULL::VARCHAR                                         AS code_version
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv') a
    LEFT JOIN (SELECT first_name, last_name, constituency_name, party
               FROM read_csv_auto('data/silver/flattened_members.csv')) m
           ON a.first_name = m.first_name AND a.last_name = m.last_name;

TODO_PIPELINE_VIEW_REQUIRED: CREATE OR REPLACE VIEW v_attendance_summary AS
    SELECT
        'pipeline'                                            AS latest_run_id,
        COUNT(DISTINCT CONCAT(first_name, ' ', last_name))   AS members_count,
        COUNT(DISTINCT iso_sitting_days_attendance)           AS sitting_count,
        NULL::DOUBLE                                          AS avg_attendance_rate,
        MIN(iso_sitting_days_attendance)                      AS first_sitting_date,
        MAX(iso_sitting_days_attendance)                      AS last_sitting_date,
        current_timestamp                                     AS latest_fetch_timestamp_utc,
        'data/silver/aggregated_td_tables.csv'                AS source_summary,
        NULL::VARCHAR                                         AS mart_version,
        NULL::VARCHAR                                         AS code_version
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv');

TODO_PIPELINE_VIEW_REQUIRED: absent_count — requires total scheduled sitting count per period
TODO_PIPELINE_VIEW_REQUIRED: sitting_count — requires total scheduled sitting count per period
TODO_PIPELINE_VIEW_REQUIRED: attendance_rate — requires total scheduled sitting count per period
TODO_PIPELINE_VIEW_REQUIRED: absent rows in v_attendance_timeline — requires full sitting schedule
TODO_PIPELINE_VIEW_REQUIRED: party_name — requires pipeline enrichment join (enrich.py)
TODO_PIPELINE_VIEW_REQUIRED: constituency — requires pipeline enrichment join (enrich.py)
"""

from __future__ import annotations

import datetime
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT     = Path(__file__).resolve().parents[2]
_CSS      = _ROOT / "utility" / "styles" / "base.css"
_CSV_ATT  = _ROOT / "data" / "silver" / "aggregated_td_tables.csv"
_CSV_MEM  = _ROOT / "data" / "silver" / "flattened_members.csv"
_CSV_SOURCE = "data/silver/aggregated_td_tables.csv"

_REQUIRED_COLS: set[str] = {
    "member_name", "member_id", "attended_count",
    "first_sitting_date", "last_sitting_date",
}

_NOTABLE_TDS: list[str] = [
    "Michael Healy-Rae",
    "Michael Lowry",
    "Mary Lou McDonald",
    "Micheál Martin",
    "Simon Harris",
    "Pauline Tully",
]

_CAVEAT = (
    "Plenary attendance records the days a member was present in the full chamber on "
    "scheduled sitting days. It does not capture committee hearings, ministerial duties, "
    "or constituency casework. Members with ministerial responsibilities or committee "
    "leadership frequently conduct substantial parliamentary work outside plenary hours. "
    "Attendance rates and denominators are owned by the pipeline and are not recalculated "
    "here. Lower figures should not be read as a complete measure of a member's "
    "parliamentary engagement."
)


# ── Pipeline-substitute layer (_simulate_* functions) ─────────────────────────
# These functions use GROUP BY and dict-based joins — both forbidden in production
# Streamlit retrieval SQL. Isolated here until the pipeline delivers the views.

@st.cache_data(ttl=3600)
def _load_attendance() -> pd.DataFrame:
    """Load raw attendance CSV (one row per member per attended sitting date)."""
    df = pd.read_csv(_CSV_ATT, low_memory=False)
    df["member_name"] = df["first_name"].str.strip() + " " + df["last_name"].str.strip()
    df["iso_sitting_days_attendance"] = pd.to_datetime(
        df["iso_sitting_days_attendance"], errors="coerce"
    )
    return df


@st.cache_data(ttl=3600)
def _simulate_member_summary() -> pd.DataFrame:
    """
    TODO_PIPELINE_VIEW_REQUIRED: Replace with SELECT from v_attendance_member_summary.
    Uses GROUP BY (forbidden in production retrieval) and a dict lookup for party/
    constituency (forbidden JOIN in production). Isolated as pipeline substitute.
    """
    att = _load_attendance()

    party_lookup: dict[str, str] = {}
    const_lookup: dict[str, str] = {}
    try:
        mem = pd.read_csv(
            _CSV_MEM,
            usecols=["first_name", "last_name", "party", "constituency_name"],
            low_memory=False,
        )
        mem["member_name"] = mem["first_name"].str.strip() + " " + mem["last_name"].str.strip()
        mem = mem.drop_duplicates("member_name").set_index("member_name")
        party_lookup = mem["party"].fillna("").to_dict()
        const_lookup = mem["constituency_name"].fillna("").to_dict()
    except Exception:
        pass

    con = duckdb.connect()
    con.register("att", att)
    summary = con.execute("""
        SELECT
            member_name,
            identifier                                             AS member_id,
            COUNT(DISTINCT iso_sitting_days_attendance)           AS attended_count,
            NULL::INTEGER                                         AS absent_count,
            NULL::INTEGER                                         AS sitting_count,
            NULL::DOUBLE                                         AS attendance_rate,
            CAST(MIN(iso_sitting_days_attendance) AS VARCHAR)     AS first_sitting_date,
            CAST(MAX(iso_sitting_days_attendance) AS VARCHAR)     AS last_sitting_date,
            current_timestamp                                     AS latest_fetch_timestamp_utc
        FROM att
        WHERE iso_sitting_days_attendance IS NOT NULL
        GROUP BY member_name, identifier
        ORDER BY member_name
    """).df()
    con.close()

    summary["party_name"]   = summary["member_name"].map(party_lookup).fillna("")
    summary["constituency"] = summary["member_name"].map(const_lookup).fillna("")
    return summary


@st.cache_data(ttl=3600)
def _simulate_member_year_summary() -> pd.DataFrame:
    """
    TODO_PIPELINE_VIEW_REQUIRED: Replace with SELECT from v_attendance_member_year_summary.
    sitting_days_count is the yearly total repeated on every row — MAX extracts it per year.
    Uses GROUP BY — forbidden in production retrieval. Isolated as pipeline substitute.
    """
    att = _load_attendance()
    mem_summary = _simulate_member_summary()
    party_map = mem_summary.set_index("member_name")["party_name"].to_dict()
    const_map = mem_summary.set_index("member_name")["constituency"].to_dict()

    con = duckdb.connect()
    con.register("att", att)
    year_df = con.execute("""
        SELECT
            member_name,
            identifier      AS member_id,
            year,
            MAX(sitting_days_count) AS attended_count
        FROM att
        WHERE iso_sitting_days_attendance IS NOT NULL
          AND year IS NOT NULL
        GROUP BY member_name, identifier, year
        ORDER BY year DESC, attended_count DESC
    """).df()
    con.close()

    year_df["party_name"]   = year_df["member_name"].map(party_map).fillna("")
    year_df["constituency"] = year_df["member_name"].map(const_map).fillna("")
    year_df["year"]         = year_df["year"].astype(int)
    return year_df


@st.cache_data(ttl=3600)
def _simulate_summary() -> pd.Series:
    """
    TODO_PIPELINE_VIEW_REQUIRED: Replace with SELECT from v_attendance_summary.
    Uses GROUP BY — forbidden in production Streamlit retrieval.
    """
    att = _load_attendance()
    con = duckdb.connect()
    con.register("att", att)
    row = con.execute("""
        SELECT
            'pipeline'                                            AS latest_run_id,
            COUNT(DISTINCT member_name)                           AS members_count,
            COUNT(DISTINCT iso_sitting_days_attendance)           AS sitting_count,
            NULL::DOUBLE                                         AS avg_attendance_rate,
            CAST(MIN(iso_sitting_days_attendance) AS VARCHAR)     AS first_sitting_date,
            CAST(MAX(iso_sitting_days_attendance) AS VARCHAR)     AS last_sitting_date,
            current_timestamp                                     AS latest_fetch_timestamp_utc,
            ? AS source_summary,
            NULL::VARCHAR                                         AS mart_version,
            NULL::VARCHAR                                         AS code_version
        FROM att
        WHERE iso_sitting_days_attendance IS NOT NULL
    """, [_CSV_SOURCE]).df()
    con.close()
    return row.iloc[0]


# ── Retrieval layer (SELECT / WHERE / ORDER BY / LIMIT only) ──────────────────

@st.cache_data(ttl=300)
def _fetch_filter_options() -> dict[str, list]:
    """Distinct values for sidebar widgets — SELECT only."""
    summary = _simulate_member_summary()
    year_df = _simulate_member_year_summary()
    con = duckdb.connect()
    con.register("s", summary)
    con.register("y", year_df)
    parties = con.execute(
        "SELECT DISTINCT party_name FROM s WHERE party_name IS NOT NULL AND party_name <> '' ORDER BY party_name"
    ).fetchall()
    consts = con.execute(
        "SELECT DISTINCT constituency FROM s WHERE constituency IS NOT NULL AND constituency <> '' ORDER BY constituency"
    ).fetchall()
    members = con.execute(
        "SELECT DISTINCT member_name FROM s ORDER BY member_name"
    ).fetchall()
    years = con.execute(
        "SELECT DISTINCT year FROM y ORDER BY year DESC"
    ).fetchall()
    con.close()
    return {
        "parties":        [r[0] for r in parties],
        "constituencies": [r[0] for r in consts],
        "members":        [r[0] for r in members],
        "years":          [r[0] for r in years],
    }


@st.cache_data(ttl=300)
def _fetch_members(
    name_q: str,
    parties: list[str],
    constituencies: list[str],
) -> pd.DataFrame:
    """All-time member totals — SELECT/WHERE/ORDER BY/LIMIT only."""
    summary = _simulate_member_summary()
    con = duckdb.connect()
    con.register("v_attendance_member_summary", summary)

    where_parts: list[str] = []
    params: list = []
    if name_q.strip():
        where_parts.append("member_name ILIKE ?")
        params.append(f"%{name_q.strip()}%")
    if parties:
        phs = ", ".join(["?" for _ in parties])
        where_parts.append(f"party_name IN ({phs})")
        params.extend(parties)
    if constituencies:
        phs = ", ".join(["?" for _ in constituencies])
        where_parts.append(f"constituency IN ({phs})")
        params.extend(constituencies)

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    result = con.execute(
        f"""
        SELECT member_name, party_name, constituency,
               attended_count, first_sitting_date, last_sitting_date
        FROM v_attendance_member_summary
        {where_sql}
        ORDER BY attended_count DESC NULLS LAST, member_name ASC
        LIMIT 1000
        """,
        params,
    ).df()
    con.close()
    return result


@st.cache_data(ttl=300)
def _fetch_members_for_year(
    year: int,
    name_q: str,
    parties: list[str],
    constituencies: list[str],
) -> pd.DataFrame:
    """Year-scoped member totals — SELECT/WHERE/ORDER BY/LIMIT only."""
    year_df = _simulate_member_year_summary()
    con = duckdb.connect()
    con.register("v_attendance_member_year_summary", year_df)

    where_parts: list[str] = ["year = ?"]
    params: list = [year]
    if name_q.strip():
        where_parts.append("member_name ILIKE ?")
        params.append(f"%{name_q.strip()}%")
    if parties:
        phs = ", ".join(["?" for _ in parties])
        where_parts.append(f"party_name IN ({phs})")
        params.extend(parties)
    if constituencies:
        phs = ", ".join(["?" for _ in constituencies])
        where_parts.append(f"constituency IN ({phs})")
        params.extend(constituencies)

    result = con.execute(
        f"""
        SELECT member_name, party_name, constituency, year, attended_count
        FROM v_attendance_member_year_summary
        WHERE {" AND ".join(where_parts)}
        ORDER BY attended_count DESC NULLS LAST, member_name ASC
        LIMIT 1000
        """,
        params,
    ).df()
    con.close()
    return result


@st.cache_data(ttl=300)
def _fetch_td_profile(td_name: str) -> pd.DataFrame:
    """Single member summary row — SELECT/WHERE/LIMIT only."""
    summary = _simulate_member_summary()
    con = duckdb.connect()
    con.register("v_attendance_member_summary", summary)
    result = con.execute(
        """
        SELECT member_name, party_name, constituency,
               attended_count, first_sitting_date, last_sitting_date,
               latest_fetch_timestamp_utc
        FROM v_attendance_member_summary
        WHERE member_name = ?
        LIMIT 1
        """,
        [td_name],
    ).df()
    con.close()
    return result


@st.cache_data(ttl=300)
def _fetch_member_years(td_name: str) -> pd.DataFrame:
    """Year-by-year breakdown for one member — SELECT/WHERE/ORDER BY/LIMIT only."""
    year_df = _simulate_member_year_summary()
    con = duckdb.connect()
    con.register("v_attendance_member_year_summary", year_df)
    result = con.execute(
        """
        SELECT year, attended_count
        FROM v_attendance_member_year_summary
        WHERE member_name = ?
        ORDER BY year ASC
        LIMIT 1000
        """,
        [td_name],
    ).df()
    con.close()
    return result


@st.cache_data(ttl=300)
def _fetch_timeline(
    td_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """
    Retrieval SQL — SELECT/WHERE/ORDER BY/LIMIT only.
    TODO_PIPELINE_VIEW_REQUIRED: absent rows need the full sitting schedule.
    Currently returns attended days only.
    """
    att = _load_attendance()
    con = duckdb.connect()
    con.register("v_attendance_timeline", att)

    where_parts = ["member_name = ?"]
    params: list = [td_name]
    if date_from:
        where_parts.append("iso_sitting_days_attendance >= ?")
        params.append(date_from)
    if date_to:
        where_parts.append("iso_sitting_days_attendance <= ?")
        params.append(date_to)

    result = con.execute(
        f"""
        SELECT
            iso_sitting_days_attendance AS sitting_date,
            member_name,
            TRUE                        AS present_flag,
            'Present'                   AS attendance_status
        FROM v_attendance_timeline
        WHERE {" AND ".join(where_parts)}
        ORDER BY sitting_date ASC
        LIMIT 1000
        """,
        params,
    ).df()
    con.close()
    return result


# ── Render helpers ─────────────────────────────────────────────────────────────

def _render_hero(summary: pd.Series) -> None:
    members_count = int(summary["members_count"]) if pd.notna(summary.get("members_count")) else 0
    sitting_count = int(summary["sitting_count"]) if pd.notna(summary.get("sitting_count")) else 0
    first_d = str(summary.get("first_sitting_date") or "")[:10] or "—"
    last_d  = str(summary.get("last_sitting_date")  or "")[:10] or "—"
    date_range = f"{first_d} – {last_d}" if first_d != "—" else "—"

    st.markdown("""
        <div class="dt-hero">
            <div class="dt-kicker">Dáil Tracker · Plenary Sittings</div>
            <h2 style="margin:0.2rem 0 0.35rem 0;font-size:1.55rem;line-height:1.2;">
                Dáil Attendance Tracker
            </h2>
            <p class="dt-dek">
                Inspect how often each member shows up for plenary sittings.
                Absence from the chamber does not mean absence from parliamentary work —
                committee hearings and ministerial duties run concurrently.
            </p>
        </div>
    """, unsafe_allow_html=True)

    with st.expander("About attendance figures", expanded=False):
        st.markdown(_CAVEAT)

    c1, c2, c3 = st.columns(3)
    c1.metric("Members tracked",        f"{members_count:,}")
    c2.metric("Sitting dates recorded", f"{sitting_count:,}")
    c3.metric("Date coverage",          date_range)


def _render_cohort_heatmap(member_summary: pd.DataFrame, year_df: pd.DataFrame) -> None:
    """
    Member × year attendance heatmap (top 50 members by all-time attended count).
    Shows attendance density across the full parliamentary record at a glance.
    """
    top_members = (
        member_summary.nlargest(50, "attended_count")["member_name"].tolist()
    )
    df = year_df[year_df["member_name"].isin(top_members)].copy()
    if df.empty:
        return

    n = len(top_members)
    base = (
        alt.Chart(df)
        .mark_rect(stroke="white", strokeWidth=0.5)
        .encode(
            x=alt.X(
                "year:O",
                title="Year",
                axis=alt.Axis(labelAngle=-45, labelFontSize=10),
            ),
            y=alt.Y(
                "member_name:N",
                title=None,
                sort=alt.EncodingSortField(
                    field="attended_count", op="sum", order="descending"
                ),
                axis=alt.Axis(labelLimit=200, labelFontSize=10),
            ),
            color=alt.Color(
                "attended_count:Q",
                scale=alt.Scale(scheme="blues"),
                title="Days attended",
                legend=alt.Legend(orient="bottom", direction="horizontal", gradientLength=200),
            ),
            tooltip=[
                alt.Tooltip("member_name:N", title="Member"),
                alt.Tooltip("year:O",        title="Year"),
                alt.Tooltip("attended_count:Q", title="Days attended"),
                alt.Tooltip("party_name:N",  title="Party"),
            ],
        )
        .properties(height=max(280, n * 13))
    )
    st.altair_chart(base, use_container_width=True)


def _render_year_bar_chart(rows: pd.DataFrame, year: int) -> None:
    """Horizontal ranked bar chart of members by attended_count for a single year."""
    top = rows.head(40)
    chart = (
        alt.Chart(top)
        .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
        .encode(
            y=alt.Y(
                "member_name:N",
                sort="-x",
                title=None,
                axis=alt.Axis(labelLimit=200, labelFontSize=11),
            ),
            x=alt.X("attended_count:Q", title="Days attended"),
            color=alt.Color(
                "party_name:N",
                scale=alt.Scale(scheme="tableau10"),
                legend=alt.Legend(title="Party", orient="bottom"),
            ),
            tooltip=[
                alt.Tooltip("member_name:N",    title="Member"),
                alt.Tooltip("attended_count:Q", title="Days attended"),
                alt.Tooltip("party_name:N",     title="Party"),
                alt.Tooltip("constituency:N",   title="Constituency"),
            ],
        )
        .properties(
            height=max(220, min(len(top) * 22, 700)),
            title=f"Sittings attended — {year} (top {len(top)})",
        )
    )
    st.altair_chart(chart, use_container_width=True)


def _render_calendar_heatmap(timeline: pd.DataFrame) -> None:
    """GitHub-style sitting calendar — one cell per (week, weekday), faceted by year."""
    df = timeline.copy()
    df["sitting_date"] = pd.to_datetime(df["sitting_date"], errors="coerce")
    df = df.dropna(subset=["sitting_date"])
    if df.empty:
        return

    df["year"]     = df["sitting_date"].dt.year
    df["week"]     = df["sitting_date"].dt.isocalendar().week.astype(int)
    df["weekday"]  = df["sitting_date"].dt.weekday
    df["attended"] = 1
    df["date_str"] = df["sitting_date"].dt.strftime("%Y-%m-%d")

    base = (
        alt.Chart(df)
        .mark_rect(stroke="white", strokeWidth=1.5, cornerRadius=2)
        .encode(
            x=alt.X(
                "week:O",
                title="Week of year",
                axis=alt.Axis(labelAngle=0, labelFontSize=9),
            ),
            y=alt.Y(
                "weekday:O",
                title=None,
                scale=alt.Scale(domain=[0, 1, 2, 3, 4]),
                axis=alt.Axis(
                    labelExpr="['Mon','Tue','Wed','Thu','Fri'][datum.value]",
                    labelFontSize=9,
                ),
            ),
            color=alt.Color(
                "attended:Q",
                scale=alt.Scale(range=["#bbf7d0", "#15803d"]),
                legend=None,
            ),
            tooltip=[alt.Tooltip("date_str:N", title="Date")],
        )
        .properties(width=180, height=60)
    )
    chart = base.facet(
        facet=alt.Facet("year:O", title=None),
        columns=4,
    ).resolve_scale(x="independent")
    st.altair_chart(chart, use_container_width=True)


def _render_provenance(summary: pd.Series) -> None:
    source   = summary.get("source_summary") or "—"
    fetch_ts = str(summary.get("latest_fetch_timestamp_utc") or "—")[:19]
    mart_v   = summary.get("mart_version") or "—"
    code_v   = summary.get("code_version") or "—"
    st.markdown(
        f"""<div class="dt-provenance-box" style="margin-top:1.5rem;font-size:0.82rem;color:var(--dt-text-muted);">
            <span class="dt-kicker">Source</span><br>
            {source}<br>
            Fetched: {fetch_ts}&nbsp;&nbsp;Mart: {mart_v}&nbsp;&nbsp;Code: {code_v}
        </div>""",
        unsafe_allow_html=True,
    )


def _render_profile(td_name: str, date_from: str | None, date_to: str | None) -> None:
    profile = _fetch_td_profile(td_name)
    if profile.empty:
        st.markdown("""
            <div class="dt-callout"><strong>No attendance data found for this member.</strong></div>
        """, unsafe_allow_html=True)
        return

    row      = profile.iloc[0]
    party    = row.get("party_name")   or "—"
    const    = row.get("constituency") or "—"
    attended = int(row["attended_count"]) if pd.notna(row.get("attended_count")) else "—"
    first_d  = str(row.get("first_sitting_date") or "—")[:10]
    last_d   = str(row.get("last_sitting_date")  or "—")[:10]

    st.markdown(f"""
        <div class="dt-hero" style="margin-bottom:0.75rem;">
            <div class="dt-kicker">{party} · {const}</div>
            <h2 style="margin:0.15rem 0 0.5rem 0;font-size:1.4rem;">{td_name}</h2>
            <div style="display:flex;gap:1rem;flex-wrap:wrap;align-items:center;">
                <span class="dt-badge">{attended} sittings attended (all years)</span>
                <span class="dt-badge">{first_d} → {last_d}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

    # Calendar heatmap
    timeline = _fetch_timeline(td_name, date_from, date_to)
    if timeline.empty:
        st.markdown("""
            <div class="dt-callout">
                No sitting records found for this date range. Try clearing the date filter.
            </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(
            '<p class="dt-kicker" style="margin:1.25rem 0 0.35rem 0;">Sitting calendar</p>',
            unsafe_allow_html=True,
        )
        st.caption(
            f"{len(timeline)} sitting days recorded. "
            "Green cells = days attended. "
            "Absent days will appear once the pipeline includes the full sitting schedule."
        )
        _render_calendar_heatmap(timeline)

        today_str = datetime.date.today().isoformat()
        st.download_button(
            label=f"Export {td_name} sitting dates ({len(timeline)} rows)",
            data=timeline.to_csv(index=False),
            file_name=f"dail_tracker_attendance_{td_name.replace(' ', '_')}_{today_str}.csv",
            mime="text/csv",
            key="att_td_export",
        )

    # Year-by-year breakdown table
    member_years = _fetch_member_years(td_name)
    if not member_years.empty:
        st.markdown(
            '<p class="dt-kicker" style="margin:1.5rem 0 0.35rem 0;">Year by year</p>',
            unsafe_allow_html=True,
        )
        max_att = int(member_years["attended_count"].max())
        st.dataframe(
            member_years,
            hide_index=True,
            use_container_width=True,
            column_config={
                "year": st.column_config.NumberColumn("Year", format="%d", width="small"),
                "attended_count": st.column_config.ProgressColumn(
                    "Days attended",
                    min_value=0,
                    max_value=max_att,
                    format="%d",
                    width="medium",
                ),
            },
        )


# ── Page entry point ───────────────────────────────────────────────────────────

def attendance_page() -> None:
    if "selected_td_att" not in st.session_state:
        st.session_state["selected_td_att"] = None

    # CSS — wrapped in <style> to prevent raw text from rendering
    if _CSS.exists():
        st.markdown(
            f"<style>{_CSS.read_text(encoding='utf-8')}</style>",
            unsafe_allow_html=True,
        )

    # Relation / column guard
    try:
        summary   = _simulate_summary()
        member_df = _simulate_member_summary()
    except Exception as exc:
        st.markdown("""
            <div class="dt-callout">
                <strong>Dáil Attendance Tracker view is missing.</strong><br>
                The approved relation <code>v_attendance_member_summary</code> is not available.
                Run the pipeline or implement the required view.
            </div>
        """, unsafe_allow_html=True)
        st.caption(str(exc))
        return

    missing = _REQUIRED_COLS - set(member_df.columns)
    if missing:
        st.markdown(f"""
            <div class="dt-callout">
                <strong>View shape changed.</strong><br>
                Required columns missing from <code>v_attendance_member_summary</code>:
                {", ".join(sorted(missing))}. Align the pipeline view and YAML contract.
            </div>
        """, unsafe_allow_html=True)
        return

    opts = _fetch_filter_options()

    # ── Sidebar ────────────────────────────────────────────────────────────────
    st.sidebar.markdown(
        '<p class="dt-kicker" style="margin:0.5rem 0 0.3rem 0;">Notable members</p>',
        unsafe_allow_html=True,
    )
    available_notable = [n for n in _NOTABLE_TDS if n in opts["members"]]
    chip_cols = st.sidebar.columns(2)
    for i, name in enumerate(available_notable):
        if chip_cols[i % 2].button(name, key=f"chip_att_{name}", use_container_width=True):
            st.session_state["selected_td_att"] = name
            st.session_state.pop("att_member_sel", None)
            st.rerun()

    st.sidebar.divider()

    member_q = st.sidebar.text_input(
        "Search member", key="att_search_q", placeholder="Type a name…"
    )
    filtered_members = [
        m for m in opts["members"]
        if not member_q or member_q.lower() in m.lower()
    ]
    selectbox_opts = ["— browse all —"] + filtered_members
    sel_idx = 0
    current_td = st.session_state.get("selected_td_att")
    if current_td and current_td in filtered_members:
        sel_idx = filtered_members.index(current_td) + 1

    chosen = st.sidebar.selectbox(
        "Select member", selectbox_opts, index=sel_idx, key="att_member_sel"
    )
    if chosen and chosen != "— browse all —" and st.session_state.get("selected_td_att") != chosen:
        st.session_state["selected_td_att"] = chosen
        st.rerun()

    st.sidebar.divider()

    selected_td = st.session_state.get("selected_td_att")
    date_from = date_to = None

    if selected_td:
        # Profile mode sidebar
        if st.sidebar.button("← Back to all members", key="att_back"):
            st.session_state["selected_td_att"] = None
            st.session_state.pop("att_member_sel", None)
            st.rerun()
        st.sidebar.markdown(
            f'<p style="color:var(--dt-text-muted);font-size:0.82rem;">'
            f'Viewing: <strong>{selected_td}</strong></p>',
            unsafe_allow_html=True,
        )
        st.sidebar.divider()
        st.sidebar.markdown(
            '<p class="dt-kicker">Date range (calendar)</p>',
            unsafe_allow_html=True,
        )
        date_range_val = st.sidebar.date_input(
            "Sitting date range",
            value=[],
            key="att_date_range",
            label_visibility="collapsed",
        )
        date_from = str(date_range_val[0]) if len(date_range_val) > 0 else None
        date_to   = str(date_range_val[1]) if len(date_range_val) > 1 else None

    else:
        # Browse mode sidebar
        year_options = ["All years"] + [str(y) for y in opts["years"]]
        year_label   = st.sidebar.selectbox("Year", year_options, key="att_year")
        selected_year = None if year_label == "All years" else int(year_label)

        st.sidebar.divider()
        # Constituency first — more vertical room below the dropdown
        sel_consts  = st.sidebar.multiselect("Constituency", opts["constituencies"], key="att_consts")
        sel_parties = st.sidebar.multiselect("Party",        opts["parties"],        key="att_parties")
        name_q      = st.sidebar.text_input(
            "Filter by name", key="att_name_q", placeholder="Type name…"
        )

    # ── Main area ──────────────────────────────────────────────────────────────
    _render_hero(summary)

    if selected_td:
        _render_profile(selected_td, date_from, date_to)
        _render_provenance(summary)
        return

    # Browse mode — fetch and visualise
    if selected_year:
        rows = _fetch_members_for_year(selected_year, name_q, sel_parties, sel_consts)
    else:
        rows = _fetch_members(name_q, sel_parties, sel_consts)

    if rows.empty:
        st.markdown("""
            <div class="dt-callout">
                <strong>No attendance records match these filters.</strong><br>
                Try clearing one or more filters.
            </div>
        """, unsafe_allow_html=True)
        _render_provenance(summary)
        return

    # Chart
    if selected_year:
        _render_year_bar_chart(rows, selected_year)
    else:
        year_df_all = _simulate_member_year_summary()
        st.markdown(
            '<p class="dt-kicker" style="margin:1.25rem 0 0.15rem 0;">Attendance by member and year (top 50)</p>',
            unsafe_allow_html=True,
        )
        st.caption("Darker = more days attended. Select a year in the sidebar to rank all members.")
        _render_cohort_heatmap(member_df, year_df_all)

    # Export + table
    today_str = datetime.date.today().isoformat()
    export_col, label_col = st.columns([1, 4])
    with export_col:
        st.download_button(
            label=f"Export {len(rows):,} rows",
            data=rows.to_csv(index=False),
            file_name=f"dail_tracker_attendance_{today_str}.csv",
            mime="text/csv",
            key="att_export",
        )
    with label_col:
        label = str(selected_year) if selected_year else "all years"
        st.caption(f"{len(rows):,} members · {label}")

    if selected_year:
        max_att = int(rows["attended_count"].max()) if not rows["attended_count"].isna().all() else 1
        st.dataframe(
            rows,
            hide_index=True,
            use_container_width=True,
            column_config={
                "member_name":    st.column_config.TextColumn("Member",       width="medium"),
                "party_name":     st.column_config.TextColumn("Party",        width="small"),
                "constituency":   st.column_config.TextColumn("Constituency", width="medium"),
                "year":           st.column_config.NumberColumn("Year",       format="%d", width="small"),
                "attended_count": st.column_config.ProgressColumn(
                    "Days attended", min_value=0, max_value=max_att, format="%d", width="small"
                ),
            },
        )
    else:
        max_att = int(rows["attended_count"].max()) if not rows["attended_count"].isna().all() else 1
        st.dataframe(
            rows,
            hide_index=True,
            use_container_width=True,
            column_config={
                "member_name":        st.column_config.TextColumn("Member",          width="medium"),
                "party_name":         st.column_config.TextColumn("Party",           width="small"),
                "constituency":       st.column_config.TextColumn("Constituency",    width="medium"),
                "attended_count":     st.column_config.ProgressColumn(
                    "Attended (all years)", min_value=0, max_value=max_att, format="%d", width="small"
                ),
                "first_sitting_date": st.column_config.TextColumn("First sitting",   width="small"),
                "last_sitting_date":  st.column_config.TextColumn("Last sitting",    width="small"),
            },
        )

    _render_provenance(summary)
