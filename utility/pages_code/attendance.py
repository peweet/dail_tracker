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
from ui.components import (
    clean_meta,
    clickable_card_link,
    empty_state,
    evidence_heading,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    member_jump_panel,
    page_error_boundary,
    stat_strip,
    todo_callout,
    year_selector,
)
from data_access.identity_resolver import resolve_member_code
from ui.components import member_moved_callout
from ui.entity_links import member_profile_url
from ui.export_controls import export_button
from ui.source_pdfs import ATTENDANCE, provenance_expander

from config import NOTABLE_TDS, SITTING_DAYS_BY_YEAR
from data_access.attendance_data import get_attendance_conn

_CAVEAT = (
    "Attendance figures combine days a member was recorded present in the Dáil chamber "
    "on scheduled sitting days with other recorded business (committee days etc.) "
    "exactly as published in the official Oireachtas member-attendance PDFs. "
    "The record does not capture ministerial duties, illness, bereavement, parental leave, "
    "or constituency work. Low figures have many legitimate explanations that are not visible "
    "in this data. This page presents the official record — it does not make a judgement "
    "about the reasons behind it."
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

    Cross-page jump: every card on /rankings-attendance lands on
    /member-overview?member=<code>#attendance. Resolves the actual
    unique_member_code via the registry (round-3 audit fix). Members not
    in the registry render unwrapped (non-clickable).
    """
    name = str(row["member_name"])
    code = resolve_member_code(name)
    inner = _hall_card(row, medal, side, rank=rank)
    if not code:
        return inner
    return clickable_card_link(
        href=member_profile_url(code, section="attendance"),
        inner_html=inner,
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
    is_partial = year >= today.year

    # P1-2 audit fix: previously the in-progress year branch dumped every
    # member as one long ranked column, losing the editorial good/bad
    # split that's the page's whole point. Keep the split year-round; add
    # a one-sentence YTD caveat above so readers know the "lowest" column
    # is provisional.
    if is_partial:
        st.caption(
            f"**{year} is in progress** — the Dáil year is not yet complete, "
            "so the lowest column is provisional and may change as more "
            "sitting days are added."
        )

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
        st.html('<h2 class="att-hall-heading-good">Highest recorded attendance</h2>')
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
        bad_label = (
            "Lowest recorded attendance (so far)" if is_partial else "Lowest recorded attendance"
        )
        st.html(f'<h2 class="att-hall-heading-bad">{bad_label}</h2>')
        bad_cards = [_att_card_link(row, side="bad", rank=i + 1) for i, (_, row) in enumerate(bottom.iterrows())]
        st.html("\n".join(bad_cards))


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
    # ?member=<name> bookmark both redirect to /member-overview. The shared
    # helper resolves the real unique_member_code, scrubs `att_td`, and
    # calls st.stop() so the rankings page doesn't render under the callout.
    # Scrub `member` first since the helper only handles one legacy_param.
    qp_legacy = st.query_params.get("att_td") or st.query_params.get("member")
    if qp_legacy:
        st.query_params.pop("member", None)
        member_moved_callout(
            qp_legacy,
            section="attendance",
            section_label="Per-TD attendance",
            legacy_param="att_td",
            state_keys=("selected_td_att",),
        )

    # ── Page header ────────────────────────────────────────────────────────────
    # Sidebar→filter-bar migration: identity via top-nav tab + hero; the member
    # picker + notable chips move into a main-panel jump under the hero.
    hide_sidebar()
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

    # ── Member jump (was the sidebar) ───────────────────────────────────────────
    picked = member_jump_panel(
        opts["members"],
        search_key_prefix="att",
        session_key="selected_td_att",
        label="Browse all members",
        notable=NOTABLE_TDS,
        chip_key_prefix="chip_att",
    )
    if picked and st.session_state.get("selected_td_att") != picked:
        st.session_state["selected_td_att"] = picked
        st.rerun()

    # Member selection routes cross-page — the in-page profile branch is gone
    # (lifted into /member-overview Phase 6).
    selected_td = st.session_state.get("selected_td_att")
    if selected_td:
        member_moved_callout(
            selected_td,
            section="attendance",
            section_label="Per-TD attendance",
            state_keys=("selected_td_att",),
        )

    # ── Primary view: year selector ────────────────────────────────────────────
    # skip_current=True defaults to the most-recent COMPLETED year so the
    # hall-of-fame/shame split renders on first load (the in-progress year
    # falls through to the partial-year flat list, which is editorially
    # weaker — the audit doc P1-1 flagged this).
    year_options = [str(y) for y in opts["years"]]  # DESC from query
    selected_year = year_selector(year_options, key="att_year", skip_current=True)

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

    st.caption(
        f"{n_members} members on record{rate_note}. "
        "Days recorded include both plenary chamber sittings and other "
        "recorded business (committee days etc.) as published in the "
        "official member-attendance PDFs. Ministerial duties, illness, "
        "and constituency work are still outside the record, so low "
        "figures are not evidence of poor engagement (full caveat in "
        "About & data provenance below)."
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
