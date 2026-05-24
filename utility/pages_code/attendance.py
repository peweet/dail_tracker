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
TODO_PIPELINE_VIEW_REQUIRED: unique_member_code on v_attendance_member_summary,
    v_attendance_member_year_summary, v_attendance_year_rank — required for cross-page
    member-name links (utility/ui/entity_links.member_link_html). Until then this page
    cannot link member names out to /member-overview without an in-Streamlit name lookup,
    which is forbidden by the data-access rule.
"""

from __future__ import annotations

import datetime
from html import escape as _h

import altair as alt
import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.avatars import avatar_credit_html, avatar_data_url, initials as _initials
from ui.components import (
    clean_meta,
    clickable_card_link,
    empty_state,
    evidence_heading,
    glossary_strip,
    hero_banner,
    member_profile_header,
    page_error_boundary,
    render_notable_chips,
    sidebar_member_filter,
    sidebar_page_header,
    stat_strip,
    todo_callout,
    year_selector,
)
from ui.entity_links import member_profile_url, name_join_key
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
        "SELECT DISTINCT member_name FROM v_attendance_member_summary ORDER BY member_name LIMIT 2000"
    ).fetchall()
    years = conn.execute(
        "SELECT DISTINCT year FROM v_attendance_member_year_summary ORDER BY year DESC LIMIT 100"
    ).fetchall()
    return {
        "members": [r[0] for r in members],
        "years": [r[0] for r in years],
    }


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
            "SELECT year, attended_count FROM v_attendance_member_year_summary"
            " WHERE member_name = ? ORDER BY year DESC LIMIT 100",
            [td_name],
        )
        .df()
    )


@st.cache_data(ttl=300)
def _fetch_missing_members() -> pd.DataFrame:
    """Roster TDs with no row in the attendance parquet.

    Two groups via the `missing_reason` column:
      • office_holder      — ministers/ministers-of-state; documented TAA gap
      • no_record_on_file  — everyone else (Taoiseach + genuine roster gaps)
    """
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency,"
            " ministerial_office, departments_held, missing_reason"
            " FROM v_attendance_missing_members"
            " ORDER BY missing_reason, member_name LIMIT 500"
        )
        .df()
    )


@st.cache_data(ttl=300)
def _fetch_year_ranking(year: int) -> pd.DataFrame:
    """Top and bottom attenders for a given year from v_attendance_year_rank."""
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency,"
            " attended_count, is_minister, rank_high, rank_low"
            " FROM v_attendance_year_rank WHERE year = ?"
            " ORDER BY rank_high ASC LIMIT 500",
            [year],
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


# ── Good cop / bad cop browse view ────────────────────────────────────────────

_GOOD_MEDALS = ["🥇", "🥈", "🥉"]
_HALL_SIZE = 15


def _hall_card(row: pd.Series, medal: str, side: str, rank: int = 1) -> str:
    name = _h(str(row["member_name"]))
    party = str(row.get("party_name", "") or "")
    const = str(row.get("constituency", "") or "")
    meta = _h(clean_meta(party, const))
    days = int(row["attended_count"])
    return (
        f'<div class="att-hall-card-{side}">'
        f'<span class="att-hall-rank">#{rank}</span>'
        f'<span class="att-hall-medal">{medal}</span>'
        f'<div class="att-hall-body">'
        f'<p class="att-hall-name">{name}</p>'
        f'<p class="att-hall-meta">{meta}</p>'
        f"</div>"
        f'<div class="att-hall-badge-{side}">'
        f'<span class="att-hall-badge-num">{days}</span>'
        f'<span class="att-hall-badge-label">days</span>'
        f"</div>"
        f"</div>"
    )


def _att_card_link(row: pd.Series, *, side: str, rank: int, medal: str = "") -> str:
    """Full-card-clickable hall card linking to the canonical profile.

    Cross-page jump: every card on /rankings-attendance now lands on
    /member-overview?member=<code>#attendance. The in-page ?att_td= contract
    is gone (Phase 6); legacy URLs are caught by attendance_page().
    """
    name = str(row["member_name"])
    return clickable_card_link(
        href=member_profile_url(name_join_key(name), section="attendance"),
        inner_html=_hall_card(row, medal, side, rank=rank),
        aria_label=f"View {name}'s profile",
        show_arrow=False,
    )


def _render_good_bad(ranking_df: pd.DataFrame, year: int) -> None:
    """
    Full-year: top/bottom _HALL_SIZE attenders side by side, full-card click.
    Partial/current year: flat ranked list with an in-progress notice.

    Each card is wrapped in clickable_card_link — the entire card is the click
    target (no separate arrow button). Navigation is by ?att_td=<name> query
    param, which attendance_page() copies into selected_td_att on load.
    """
    today = datetime.date.today()

    # ── Partial / current year ─────────────────────────────────────────────────
    if year >= today.year:
        st.info(
            f"**{year} is in progress** — the Dáil year is not yet complete so a "
            f"full attendance ranking would be misleading. Showing all members by "
            f"days attended so far."
        )
        partial = ranking_df.copy().sort_values("attended_count", ascending=False).reset_index(drop=True)
        cards = [_att_card_link(row, side="good", rank=i + 1) for i, (_, row) in enumerate(partial.iterrows())]
        st.html("\n".join(cards))
        return

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

    col_good, col_bad = st.columns(2, gap="medium")
    with col_good:
        st.html('<p class="att-hall-heading-good">Highest recorded attendance</p>')
        good_cards = [
            _att_card_link(
                row,
                side="good",
                rank=i + 1,
                medal=_GOOD_MEDALS[i] if i < len(_GOOD_MEDALS) else "",
            )
            for i, (_, row) in enumerate(top.iterrows())
        ]
        st.html("\n".join(good_cards))

    with col_bad:
        st.html('<p class="att-hall-heading-bad">Lowest recorded attendance</p>')
        bad_cards = [_att_card_link(row, side="bad", rank=i + 1) for i, (_, row) in enumerate(bottom.iterrows())]
        st.html("\n".join(bad_cards))


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
    st.altair_chart(chart, use_container_width=True)


# ── Member profile body (lifted into member-overview Attendance expander) ─────


def render_member_attendance(
    td_name: str,
    *,
    show_member_header: bool = True,
    year_pill_key: str = "att_profile_year",
    export_key_suffix: str = "",
) -> None:
    """Render the per-TD attendance body.

    Public so :mod:`pages_code.member_overview` can embed it inside the
    Attendance expander. When ``show_member_header=False``: skip the
    avatar/name/meta header (the embedding page provides it), skip the inner
    "Sitting dates · N records" `st.expander` (Streamlit forbids nested
    expanders), and render the year breakdown as a card list instead of a
    `st.dataframe` (member_overview is dataframe-free).

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

    row = profile.iloc[0]
    party = str(row.get("party_name") or "—")
    const = str(row.get("constituency") or "—")

    if show_member_header:
        member_profile_header(
            td_name,
            f"{party} · {const}",
            avatar_url=avatar_data_url(td_name),
            avatar_initials=_initials(td_name),
            avatar_credit_html=avatar_credit_html(td_name),
        )

    # ── Year pills (profile-level, newest first) ───────────────────────────────
    member_years_df = _fetch_member_years(td_name)
    if member_years_df.empty:
        empty_state("No year data available", "v_attendance_member_year_summary returned no rows.")
        return

    year_options = [str(int(y)) for y in member_years_df["year"].tolist()]

    selected_year = year_selector(year_options, key=year_pill_key, skip_current=False)

    # ── Stats for selected year (from pipeline — no pandas aggregation) ──────────
    yr_row = member_years_df[member_years_df["year"] == selected_year]
    n_attended = int(yr_row["attended_count"].iloc[0]) if not yr_row.empty else 0

    ts_df = _fetch_timeline_stats(td_name, selected_year)
    ts_row = ts_df.iloc[0] if not ts_df.empty else None
    if ts_row is not None and pd.notna(ts_row.get("first_date")):
        first_d = pd.Timestamp(ts_row["first_date"]).strftime("%d %b %Y")
        last_d = pd.Timestamp(ts_row["last_date"]).strftime("%d %b %Y")
    else:
        first_d = "—"
        last_d = "—"

    stat_strip(
        [
            (str(n_attended), f"Days attended · {selected_year}", "var(--text-primary)", "plenary + committee days"),
            (first_d, "First sitting", "var(--text-secondary)"),
            (last_d, "Most recent", "var(--text-secondary)"),
        ]
    )

    st.divider()

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

        # Inner expander is forbidden inside the member-overview Attendance
        # expander (Streamlit nests fail). Stand-alone page keeps the
        # collapsed sitting-dates table; embedded mode drops it (the
        # calendar above already plots every date — tooltips show specifics).
        if show_member_header:
            with st.expander(f"Sitting dates · {n_attended} records", expanded=False):
                tl_table = tl_all.copy()
                tl_table["#"] = range(1, len(tl_table) + 1)
                tl_table["Date"] = tl_table["sitting_date"].dt.strftime("%d %b %Y")
                tl_table["Weekday"] = tl_table["sitting_date"].dt.strftime("%A")
                st.dataframe(
                    tl_table[["#", "Date", "Weekday"]],
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "#": st.column_config.NumberColumn("#", width="small"),
                        "Date": st.column_config.TextColumn("Date", width="medium"),
                        "Weekday": st.column_config.TextColumn("Day", width="medium"),
                    },
                )

        export_button(
            timeline,
            label=f"Export {td_name} · {selected_year} · {n_attended} rows",
            filename=f"dail_tracker_attendance_{td_name.replace(' ', '_')}_{selected_year}.csv",
            key=f"att_td_export{export_key_suffix}",
        )

    st.divider()
    _render_year_breakdown(
        td_name,
        member_years_df,
        as_dataframe=show_member_header,
        export_key_suffix=export_key_suffix,
    )


# ── Year breakdown table (profile secondary view) ─────────────────────────────


def _render_year_breakdown(
    td_name: str,
    years_df: pd.DataFrame,
    *,
    as_dataframe: bool = True,
    export_key_suffix: str = "",
) -> None:
    """Per-year attendance summary.

    Stand-alone page (``as_dataframe=True``): sortable `st.dataframe` with a
    ProgressColumn — drill-down + export-adjacent so allowed per
    feedback_dataframes_secondary_only. Embedded in member-overview
    (``as_dataframe=False``): card list of `.att-year-row`s with a CSS-width
    bar in lieu of ProgressColumn — required by
    feedback_member_overview_no_dataframes.
    """
    evidence_heading("Attendance by year")
    if years_df.empty:
        empty_state("No year data available", "v_attendance_member_year_summary returned no rows.")
        return

    rows = []
    for _, r in years_df.iterrows():
        y = int(r["year"])
        days = int(r["attended_count"])
        total = SITTING_DAYS_BY_YEAR.get(y)
        # Rate computation uses the contract-permitted hardcoded sitting-day totals
        # (permitted_hardcoded_values.year_sitting_days in attendance.yaml).
        pct = days / total if total else None
        rows.append({"Year": y, "Days": days, "Total": str(total) if total else "—", "Rate": pct})

    if as_dataframe:
        table_df = pd.DataFrame(rows)
        st.dataframe(
            table_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Year": st.column_config.NumberColumn("Year", format="%d", width="small"),
                "Days": st.column_config.NumberColumn("Days attended", width="small"),
                "Total": st.column_config.TextColumn("Sitting days", width="small"),
                "Rate": st.column_config.ProgressColumn(
                    "Attendance",
                    min_value=0.0,
                    max_value=1.0,
                    format="%.0%",
                ),
            },
        )
    else:
        # Card list — one row per year with a CSS-width bar replacing the
        # ProgressColumn. Keeps the same information density without st.dataframe.
        cards_html: list[str] = []
        for row_d in rows:
            y = row_d["Year"]
            days = row_d["Days"]
            total = row_d["Total"]
            pct = row_d["Rate"]
            pct_pad = max(0.0, min(1.0, float(pct))) * 100 if pct is not None else 0.0
            pct_label = f"{pct * 100:.0f}%" if pct is not None else "—"
            total_label = f"{days} / {total}" if total != "—" else f"{days}"
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
        table_df[["Year", "Days", "Total"]].rename(columns={"Total": "Sitting days"}),
        label=f"Export year breakdown · {td_name}",
        filename=f"dail_tracker_attendance_years_{safe}.csv",
        key=f"att_years_export{export_key_suffix}",
    )


# ── Missing-members section (TDs not in the attendance parquet) ───────────────


def _name_pill(row: pd.Series, *, with_office: bool) -> str:
    name = _h(str(row["member_name"]))
    party = str(row.get("party_name", "") or "")
    const = str(row.get("constituency", "") or "")
    meta = _h(clean_meta(party, const))
    office = str(row.get("departments_held", "") or "") if with_office else ""
    office_html = (
        f'<span class="att-miss-office">{_h(office)}</span>' if office else ""
    )
    return (
        '<div class="att-miss-row">'
        f'<span class="att-miss-name">{name}</span>'
        f'<span class="att-miss-meta">{meta}</span>'
        f"{office_html}"
        "</div>"
    )


def _render_missing_members() -> None:
    df = _fetch_missing_members()
    if df.empty:
        return

    office = df[df["missing_reason"] == "office_holder"]
    no_record = df[df["missing_reason"] == "no_record_on_file"]
    total = len(df)

    with st.expander(
        f"⚠ {total} TDs do not appear in the attendance record · why?",
        expanded=False,
    ):
        st.markdown(
            "The official Oireachtas Travel & Accommodation Allowance (TAA) PDFs — "
            "the source for this page — do not capture attendance for members holding "
            "ministerial office. A small number of other TDs are also absent from the "
            "source data. **Their non-appearance here is not evidence of non-attendance.**"
        )

        if not office.empty:
            evidence_heading(f"Ministers and ministers of state · {len(office)}")
            st.caption(
                "TAA records exclude office-holders by design — they are not absent, "
                "they are not recorded."
            )
            st.html("\n".join(_name_pill(r, with_office=True) for _, r in office.iterrows()))

        if not no_record.empty:
            evidence_heading(f"Other TDs with no record on file · {len(no_record)}")
            st.caption(
                "Includes the Taoiseach (whose office is not classified as a department "
                "in the source data) and any roster / name-match gaps in the upstream ETL."
            )
            st.html("\n".join(_name_pill(r, with_office=False) for _, r in no_record.iterrows()))


# ── Provenance footer ──────────────────────────────────────────────────────────


def _render_provenance(year: int | None = None) -> None:
    provenance_expander(
        sections=[_CAVEAT, _MINISTER_NOTE],
        source_caption=(
            "Data: Oireachtas TAA verification records (data.oireachtas.ie)" + (f" · {year}" if year else "")
        ),
        pdf_links=list(ATTENDANCE),
    )


# ── Page entry point ───────────────────────────────────────────────────────────


@page_error_boundary
def attendance_page() -> None:
    inject_css()

    try:
        ready = _views_ready()
        opts = _fetch_filter_options()
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

    # Legacy ?att_td=<name> URLs (from before Phase 6) and the legacy in-page
    # ?member=<name> bookmark both now redirect to /member-overview. Card
    # hrefs already route cross-page; this catches bookmarks / external links.
    qp_legacy = st.query_params.get("att_td") or st.query_params.get("member")
    if qp_legacy:
        target = member_profile_url(name_join_key(qp_legacy), section="attendance")
        st.html(
            f'<div class="dt-callout" style="margin:0.5rem 0 1rem;">'
            f"<strong>Member profiles have moved.</strong><br>"
            f'<span style="color:var(--text-meta)">Per-TD attendance now lives on the '
            f'canonical member-overview page. Bookmarks to <code>?att_td={_h(qp_legacy)}</code> '
            f"redirect here.</span><br>"
            f'<a class="dt-member-link" href="{_h(target)}" target="_self" '
            f'style="margin-top:0.6rem;display:inline-block;">'
            f"Open {_h(qp_legacy)}'s profile →</a>"
            f"</div>"
        )
        for k in ("att_td", "member"):
            st.query_params.pop(k, None)
        st.session_state.pop("selected_td_att", None)

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
    hero_banner(
        kicker="DÁIL PLENARY ATTENDANCE",
        title="The attendance record",
    )
    glossary_strip(
        [
            ("TD", "Teachta Dála, a member of the Dáil"),
            ("Plenary", "the full chamber sitting (does not include committees or ministerial duties)"),
            ("TAA", "Travel & Accommodation Allowance, the official attendance record"),
        ]
    )

    # Sidebar-driven member selection also routes cross-page now — the
    # in-page profile branch is gone (lifted into /member-overview).
    selected_td = st.session_state.get("selected_td_att")
    if selected_td:
        target = member_profile_url(name_join_key(selected_td), section="attendance")
        st.html(
            f'<div class="dt-callout" style="margin:0.5rem 0 1rem;">'
            f"<strong>{_h(selected_td)}</strong> &nbsp;·&nbsp; "
            f'<a class="dt-member-link" href="{_h(target)}" target="_self">'
            f"Open this member's attendance profile →</a>"
            f"</div>"
        )
        st.session_state.pop("selected_td_att", None)

    # ── Primary view: year selector ────────────────────────────────────────────
    year_options = [str(y) for y in opts["years"]]  # DESC from query
    selected_year = year_selector(year_options, key="att_year", skip_current=False)

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
    n_members = len(ranking_df)
    rate_note = f" · {total_days} scheduled sitting days" if total_days else ""

    st.caption(f"{n_members} members on record{rate_note}")

    st.info(
        "Plenary attendance only — does not include committee hearings, ministerial duties, "
        "illness, or other absences with legitimate reasons. "
        "Low figures are not evidence of poor engagement.",
        icon=":material/info:",
    )

    _render_good_bad(ranking_df, selected_year)

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

    _render_missing_members()

    _render_provenance(selected_year)
