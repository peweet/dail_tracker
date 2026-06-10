"""Per-TD attendance panel — embedded in the /member-overview Attendance section.

Extracted from ``pages_code/attendance.py`` (2026-06-01) so member-overview no
longer imports a render body out of another page. Pure rendering + data-access
retrieval, no business logic — mirrors ``ui/vote_explorer.py``.
"""

from __future__ import annotations

import datetime
from html import escape as _h

import altair as alt
import pandas as pd
import streamlit as st
from data_access.attendance_data import fetch_chamber_sitting_days, get_attendance_conn
from ui.components import empty_state, evidence_heading, stat_strip, todo_callout, year_selector
from ui.export_controls import export_button

from config import SITTING_DAYS_BY_YEAR


@st.cache_data(ttl=300)
def _fetch_td_profile(td_name: str) -> pd.DataFrame:
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency"
            " FROM v_attendance_member_summary WHERE member_name = ? LIMIT 1",
            [td_name],
        )
        .df()
    )


@st.cache_data(ttl=300)
def _fetch_member_years(td_name: str) -> pd.DataFrame:
    """Returns years DESC — used to populate the profile year pills."""
    return (
        get_attendance_conn()
        .execute(
            "SELECT year, attended_count, sitting_days, other_days"
            " FROM v_attendance_member_year_summary"
            " WHERE member_name = ? ORDER BY year DESC LIMIT 100",
            [td_name],
        )
        .df()
    )


@st.cache_data(ttl=300)
def _fetch_timeline_for_year(td_name: str, year: int) -> pd.DataFrame:
    return (
        get_attendance_conn()
        .execute(
            "SELECT sitting_date, attendance_status"
            " FROM v_attendance_timeline"
            " WHERE member_name = ? AND year(sitting_date) = ?"
            " ORDER BY sitting_date ASC LIMIT 400",
            [td_name, year],
        )
        .df()
    )


@st.cache_data(ttl=300)
def _fetch_timeline_stats(td_name: str, year: int) -> pd.DataFrame:
    """MIN/MAX sitting dates for a member-year — avoids pandas aggregation in the profile."""
    return (
        get_attendance_conn()
        .execute(
            "SELECT MIN(sitting_date) AS first_date, MAX(sitting_date) AS last_date"
            " FROM v_attendance_timeline"
            " WHERE member_name = ? AND year(sitting_date) = ?"
            " LIMIT 1",
            [td_name, year],
        )
        .df()
    )


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
    domain_end = today.isoformat() if year >= today.year else f"{year}-12-31"
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
    st.altair_chart(chart, width="stretch")


# ── Member profile body (lifted into member-overview Attendance expander) ─────


def render_member_attendance(
    td_name: str,
    *,
    house: str = "Dáil",
    show_member_header: bool = False,
    year_pill_key: str = "att_profile_year",
    export_key_suffix: str = "",
) -> None:
    """Render the per-TD attendance body embedded inside /member-overview.

    The ``show_member_header`` kwarg is retained for API compatibility but
    is no longer load-bearing: every reachable caller (member_overview)
    passes False, and the legacy True paths — `member_profile_header`,
    inner Sitting-dates `st.expander`, year-breakdown `st.dataframe` —
    were dead code that also nested expanders (forbidden) and rendered
    `st.dataframe` on a primary view (banned). Removed 2026-05-27.

    ``export_key_suffix`` namespaces export-button widget keys so the
    embedded copy doesn't collide with the stand-alone page state.
    """
    profile = _fetch_td_profile(td_name)
    if profile.empty:
        empty_state(
            "No attendance data found for this member",
            "v_attendance_member_summary returned no rows for this name.",
        )
        return

    # ── Year pills (profile-level, newest first) ───────────────────────────────
    member_years_df = _fetch_member_years(td_name)
    if member_years_df.empty:
        empty_state("No year data available", "v_attendance_member_year_summary returned no rows.")
        return

    year_options = [str(int(y)) for y in member_years_df["year"].tolist()]

    # Default to the newest year that actually has plenary sitting records —
    # opening on a year whose calendar is empty front-loaded the section with
    # a "No sitting records" notice and two em-dash stats.
    default_year = next(
        (str(int(r["year"])) for _, r in member_years_df.iterrows() if int(r.get("sitting_days", 0) or 0) > 0),
        year_options[0],
    )
    selected_year = year_selector(year_options, key=year_pill_key, skip_current=False, default=default_year)

    # ── Stats for selected year (from pipeline — no pandas aggregation) ──────────
    yr_row = member_years_df[member_years_df["year"] == selected_year]
    n_attended = int(yr_row["attended_count"].iloc[0]) if not yr_row.empty else 0
    n_sitting = int(yr_row["sitting_days"].iloc[0]) if not yr_row.empty else 0
    n_other = int(yr_row["other_days"].iloc[0]) if not yr_row.empty else 0

    ts_df = _fetch_timeline_stats(td_name, selected_year)
    ts_row = ts_df.iloc[0] if not ts_df.empty else None

    # First/Most-recent cells render only when there are dated sitting rows —
    # a pair of bare em-dashes read as broken data rather than "no record".
    stats: list[tuple[str, str, str] | tuple[str, str, str, str]] = [
        (
            str(n_attended),
            f"Days recorded · {selected_year}",
            "var(--text-primary)",
            f"{n_sitting} plenary + {n_other} other",
        ),
    ]
    if ts_row is not None and pd.notna(ts_row.get("first_date")):
        first_d = pd.Timestamp(ts_row["first_date"]).strftime("%d %b %Y")
        last_d = pd.Timestamp(ts_row["last_date"]).strftime("%d %b %Y")
        stats.append((first_d, "First sitting", "var(--text-secondary)"))
        stats.append((last_d, "Most recent", "var(--text-secondary)"))
    stat_strip(stats)

    # ── Sitting calendar ───────────────────────────────────────────────────────
    timeline = _fetch_timeline_for_year(td_name, selected_year)
    _required_cols = {"sitting_date", "attendance_status"}
    missing_cols = _required_cols - set(timeline.columns)

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
            f"{n_attended} days recorded in {selected_year} "
            f"({n_sitting} plenary + {n_other} other). "
            "Each mark below is a day the member was recorded present. "
            f"Gaps between marks are {house} recess periods."
        )

        tl_all = timeline.copy()
        tl_all["sitting_date"] = pd.to_datetime(tl_all["sitting_date"], errors="coerce")
        tl_all = tl_all.dropna(subset=["sitting_date"]).sort_values("sitting_date")

        _render_attendance_strip(timeline, selected_year)

        export_button(
            timeline,
            label=f"Export {td_name} · {selected_year} · {n_attended} rows",
            filename=f"dail_tracker_attendance_{td_name.replace(' ', '_')}_{selected_year}.csv",
            key=f"att_td_export{export_key_suffix}",
        )

    _render_year_breakdown(td_name, member_years_df, house=house, export_key_suffix=export_key_suffix)


# ── Year breakdown table (profile secondary view) ─────────────────────────────


def _render_year_breakdown(
    td_name: str,
    years_df: pd.DataFrame,
    *,
    house: str = "Dáil",
    export_key_suffix: str = "",
) -> None:
    """Per-year attendance summary as a card list with a CSS-width bar
    replacing ProgressColumn. ``st.dataframe`` is banned on member-overview
    embedded sections per feedback_member_overview_no_dataframes.
    """
    evidence_heading("Attendance by year")
    if years_df.empty:
        empty_state("No year data available", "v_attendance_member_year_summary returned no rows.")
        return

    # Bar denominator = the chamber's official sitting days that year. Dáil uses
    # the curated config figures; Seanad uses the data-derived per-year count
    # (the two chambers sit on different days).
    denom_map = SITTING_DAYS_BY_YEAR if house == "Dáil" else fetch_chamber_sitting_days("Seanad")

    rows = []
    for _, r in years_df.iterrows():
        y = int(r["year"])
        days = int(r["attended_count"])
        sitting = int(r["sitting_days"]) if "sitting_days" in years_df.columns else days
        total = denom_map.get(y)
        # Rate uses sitting_days (chamber only) over the official chamber
        # sitting-day count — both sides of the ratio are plenary, so the
        # bar stays in [0,1] even though the headline `days` figure
        # includes committee/other days.
        pct = sitting / total if total else None
        rows.append(
            {
                "Year": y,
                "Days": days,
                "Sitting": sitting,
                "Total": str(total) if total else "—",
                "Rate": pct,
            }
        )

    cards_html: list[str] = []
    for row_d in rows:
        y = row_d["Year"]
        days = row_d["Days"]
        total = row_d["Total"]
        pct = row_d["Rate"]
        pct_pad = max(0.0, min(1.0, float(pct))) * 100 if pct is not None else 0.0
        pct_label = f"{pct * 100:.0f}%" if pct is not None else "—"
        # Numerator must match the bar/percentage grain (plenary only) —
        # "107 / 83 · 88%" mixed all recorded days with plenary sitting days
        # and read as broken arithmetic.
        total_label = f"{row_d['Sitting']} / {total}" if total != "—" else f"{days}"
        cards_html.append(
            f'<div class="att-year-row">'
            f'<span class="att-year-yr">{y}</span>'
            f'<div class="att-year-bar-track">'
            f'<div class="att-year-bar-fill" style="width:{pct_pad:.1f}%"></div>'
            f"</div>"
            f'<span class="att-year-days">{_h(total_label)}</span>'
            f'<span class="att-year-pct">{_h(pct_label)}</span>'
            f"</div>"
        )
    st.html(f'<div class="att-year-list">{"".join(cards_html)}</div>')

    safe = td_name.replace(" ", "_")
    table_df = pd.DataFrame(rows)
    export_button(
        table_df[["Year", "Days", "Sitting", "Total"]].rename(
            columns={"Sitting": "Plenary days attended", "Total": "Sitting days"}
        ),
        label=f"Export year breakdown · {td_name}",
        filename=f"dail_tracker_attendance_years_{safe}.csv",
        key=f"att_years_export{export_key_suffix}",
    )
