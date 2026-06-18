"""Courts & Judiciary — the bench, appointments, and the daily Legal Diary.

Four tabs on one page (design brief confirmed 2026-06-06, impeccable `shape`):

  ① The Bench               current roster, one card per judge, click -> ?judge= profile
  ② Appointments & Govt     who appointed whom, the elevation ladder, vacancy lifecycle
  ③ The Courts              system health: case clearance, waiting times, courthouse map
  ④ Legal Diary             the existing daily-sittings feature, unchanged

Civic frame (no inference): who sits on Ireland's courts, who appointed them, and how
they got there — source-linked public records. The page does NOT rate judges, infer
bias, or imply misconduct. Coverage gaps are shown, not hidden: judges appointed before
the 2016 Iris spine carry an honest "record begins 2016" note; low-confidence joins
carry a "needs review" chip.

DATA BOUNDARY (logic firewall): every join / classification (current court, rank,
is_elevation, salary band, match confidence) lives in the SQL views
(sql_views/judiciary_*.sql); this page reads them via data_access.judiciary_data and does
presentation faceting only. Bench/appointments/profile read v_judiciary_roster /
v_judiciary_appointments / v_judiciary_profile / v_judiciary_nominations; The Courts
reads v_courts_clearance / v_courts_waiting_times / v_courthouses (clearance_pct,
week-parsing and the curated jurisdiction/list_context computed/joined in those views)
— all from extractors/judiciary_bench_extract.py. The Legal Diary tab reads the
v_judiciary_legal_diary_* views; the profile's "Before the court" section and the
cross-day "Who is suing" league read v_judiciary_judge_sittings / v_judiciary_judge_diary
/ v_judiciary_plaintiff_league (the diary→roster name join and the applicant ranking are
pipeline-owned — extractors/judiciary_diary_link.py and the league view's GROUPING SETS).
"""

from __future__ import annotations

import datetime
import html
import sys
import urllib.parse
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.judiciary_data import (
    fetch_appointments,
    fetch_authority_summary,
    fetch_courthouses,
    fetch_courts_clearance,
    fetch_courts_clearance_by_area,
    fetch_courts_waiting_times,
    fetch_elevation_ladder,
    fetch_judge_diary,
    fetch_judge_sittings,
    fetch_legal_diary_cases,
    fetch_legal_diary_counts,
    fetch_legal_diary_schedule,
    fetch_nominations,
    fetch_plaintiff_league,
    fetch_profile,
    fetch_roster,
)
from shared_css import inject_css  # noqa: F401  (kept parallel to other pages)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    text_search_mask,
    year_selector,
)

# Court display order — constitutional seniority, the natural reading order.
_COURT_ORDER = [
    "Supreme Court",
    "Court of Appeal",
    "Court of Appeal (Criminal)",
    "Central Criminal Court",
    "High Court",
    "Circuit Court",
    "District Court",
]
# The five courts the bench/roster spans (a subset of the legal-diary courts above).
_BENCH_COURTS = [
    "Supreme Court",
    "Court of Appeal",
    "High Court",
    "Circuit Court",
    "District Court",
]
_CATEGORIES = [
    ("public-law", "Public law", "v the State / a Minister / public body"),
    ("commercial", "Commercial", "a company or financial party named"),
    ("criminal", "Criminal", "prosecution; defendant shown by initials"),
    ("civil", "Civil", "private litigation; parties shown by initials"),
]
# Plaintiff-kind display: pipeline value -> (css-class, short row-chip label, plural label).
# Colour by CSS class (blue=State, amber=company, grey=individual) — deuteranopia-safe
# AND text-labelled. State prosecutor + State body share the State-blue hue.
_PKIND = {
    "state-prosecutor": ("prosecutor", "DPP", "DPP prosecutions"),
    "state-body": ("statebody", "State", "State bodies"),
    "organisation": ("organisation", "Company", "Companies"),
    "individual": ("individual", "Individual", "Individuals"),
}
# breakdown-strip order: institutions first (the accountability signal), individuals last
_PKIND_ORDER = ["state-prosecutor", "organisation", "state-body", "individual"]
_NAMED_PKINDS = ["organisation", "state-body"]  # kept in clear -> can be ranked by name
_MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
TIER_C_PAGE = 60  # max matters rendered inside a single sitting expander
TIER_C_GROUPS = 12  # max sittings (expanders) shown per court before the rest are summarised

# Appointing-authority display + chip class (blue=Government, amber=President).
_AUTHORITY = {
    "Government": ("the Government", "gov"),
    "President": ("the President", "pres"),
    "Minister": ("the Minister for Justice", "other"),
    "Unknown": ("authority not recorded", "other"),
}


def _esc(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return html.escape(str(val))


def _fmt_day(s: str) -> str:
    """'2026-06-05' -> 'Thu 5 Jun 2026' (platform-independent — no %-d)."""
    try:
        y, m, d = (int(p) for p in str(s)[:10].split("-"))
        wd = datetime.date(y, m, d).strftime("%a")
        return f"{wd} {d} {_MONTHS[m]} {y}"
    except Exception:  # noqa: BLE001
        return str(s)


def _fmt_day_short(s: str) -> str:
    """'2026-06-08' -> 'Mon 8 Jun' — a compact pill label (year carried by the month group)."""
    try:
        y, m, d = (int(p) for p in str(s)[:10].split("-"))
        wd = datetime.date(y, m, d).strftime("%a")
        return f"{wd} {d} {_MONTHS[m]}"
    except Exception:  # noqa: BLE001
        return str(s)


def _fmt_month(ym: str) -> str:
    """'2026-06' -> 'Jun 2026'."""
    try:
        y, m = (int(p) for p in str(ym).split("-")[:2])
        return f"{_MONTHS[m]} {y}"
    except Exception:  # noqa: BLE001
        return str(ym)


def _year(s) -> str:
    return str(s)[:4] if s is not None and not (isinstance(s, float) and pd.isna(s)) else ""


# ──────────────────────────────────────────────────────────────────────────────
# Legal-diary-local CSS (jd-* family). Bench/timeline CSS (jud-*) lives in
# shared_css.py; only the daily-diary classes stay page-local. Injected via
# st.markdown so it reaches <head> (st.html would iframe the <style>).
# ──────────────────────────────────────────────────────────────────────────────
def _inject_jd_css() -> None:
    st.markdown(
        """
        <style>
        .jd-context {
            font-size: 0.82rem; color: #5b6b73; line-height: 1.55;
            margin: 0.35rem 0 1.1rem; max-width: 64rem;
        }
        .jd-context strong { color: #14232b; font-weight: 600; }
        .jd-section-head {
            font-size: 1.15rem; font-weight: 700; color: #14232b;
            margin: 0.2rem 0 0.1rem; padding: 0;
        }
        .jd-court-head {
            font-size: 0.95rem; font-weight: 700; color: #14232b;
            margin: 1.4rem 0 0.6rem; padding-bottom: 0.3rem;
            border-bottom: 2px solid #e4e9ec;
        }
        .jd-court-head span { font-weight: 500; color: #6b7b83; font-size: 0.82rem; }
        .jd-grid {
            display: grid; grid-template-columns: repeat(auto-fill, minmax(17rem, 1fr));
            gap: 0.7rem;
        }
        .jd-card {
            background: #ffffff; border: 1px solid #e4e9ec; border-radius: 10px;
            padding: 0.75rem 0.85rem; display: flex; flex-direction: column; gap: 0.2rem;
        }
        .jd-judge { font-weight: 650; color: #14232b; font-size: 0.92rem; }
        .jd-meta { font-size: 0.76rem; color: #6b7b83; }
        .jd-list { font-size: 0.82rem; color: #2c3e46; margin-top: 0.15rem; }
        .jd-items {
            align-self: flex-start; margin-top: 0.4rem; font-size: 0.72rem;
            font-weight: 600; color: #2c5f6b; background: #eaf3f5;
            border-radius: 999px; padding: 0.1rem 0.55rem;
        }
        .jd-items.zero { color: #8a9aa1; background: #f1f4f5; font-weight: 500; }
        .jd-rank {
            display: flex; align-items: center; gap: 0.7rem; background: #ffffff;
            border: 1px solid #e4e9ec; border-radius: 10px; padding: 0.6rem 0.85rem;
            margin-bottom: 0.45rem;
        }
        .jd-rank-body { flex: 1; min-width: 0; }
        .jd-rank-title { font-weight: 600; color: #14232b; font-size: 0.88rem; }
        .jd-rank-sub { font-size: 0.76rem; color: #6b7b83; }
        .jd-bar-track { background: #eef2f3; border-radius: 999px; height: 7px; margin-top: 0.35rem; }
        .jd-bar-fill { background: #3d7c8a; border-radius: 999px; height: 7px; }
        .jd-rank-n { font-weight: 700; color: #2c5f6b; font-size: 1.05rem; min-width: 2.2rem; text-align: right; }
        .jd-catwrap { display: grid; grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr)); gap: 0.6rem; margin: 0.4rem 0 0.6rem; }
        .jd-cat-card { background: #ffffff; border: 1px solid #e4e9ec; border-radius: 10px; padding: 0.7rem 0.85rem; }
        .jd-cat-n { font-size: 1.5rem; font-weight: 700; color: #14232b; line-height: 1; }
        .jd-cat-label { font-weight: 600; color: #2c3e46; font-size: 0.85rem; margin-top: 0.2rem; }
        .jd-cat-desc { font-size: 0.72rem; color: #7b8b92; margin-top: 0.1rem; }
        .jd-case-row {
            display: flex; align-items: baseline; gap: 0.5rem; padding: 0.3rem 0;
            border-bottom: 1px solid #f0f3f4; font-size: 0.86rem; color: #1f2d33;
        }
        .jd-case-row:last-child { border-bottom: none; }
        .jd-case-link { font-size: 0.72rem; color: #6b7b83; text-decoration: none; white-space: nowrap; margin-left: auto; }
        .jd-case-link:hover { color: #2c5f6b; text-decoration: underline; }
        .jd-foot { font-size: 0.76rem; color: #7b8b92; line-height: 1.5; margin-top: 1.6rem;
            border-top: 1px solid #e4e9ec; padding-top: 0.8rem; max-width: 64rem; }
        .jd-foot a { color: #2c5f6b; }
        /* The Courts — clearance bars carry a backlog hue: under 100% = clearing slower
           than cases arrive (amber), at/over 100% = keeping pace or cutting backlog (teal). */
        .jd-bar-fill.under { background: #c98a3a; }
        .jd-bar-fill.over { background: #3d7c8a; }
        .jd-rank-n.under { color: #b3781f; }
        .jd-rank-n.over { color: #2c5f6b; }
        .jd-outlier {
            background: #fbf3e8; border: 1px solid #ecd6b3; border-left: 4px solid #c98a3a;
            border-radius: 8px; padding: 0.7rem 0.9rem; margin: 0.5rem 0 0.9rem;
            font-size: 0.86rem; color: #5a4a2c; line-height: 1.5; max-width: 64rem;
        }
        .jd-outlier strong { color: #14232b; }
        .jd-wait-n { display: flex; align-items: baseline; gap: 0.4rem; margin-top: 0.15rem; }
        .jd-wait-weeks { font-size: 1.4rem; font-weight: 700; color: #14232b; line-height: 1; }
        .jd-wait-unit { font-size: 0.72rem; color: #7b8b92; }
        .jd-delta { font-size: 0.72rem; font-weight: 600; margin-top: 0.25rem; }
        .jd-delta.down { color: #3f7a4b; }   /* shorter wait than last year */
        .jd-delta.up   { color: #b3563f; }   /* longer wait than last year */
        .jd-delta.flat { color: #8a9aa1; }
        /* Legal Diary — who's bringing the cases (plaintiff side). State=blue,
           company=amber, individual=grey: deuteranopia-safe AND always text-labelled. */
        .jd-subhead { font-size: 1.0rem; font-weight: 700; color: #14232b; margin: 1.4rem 0 0.1rem; }
        .jd-pstrip { display: flex; flex-wrap: wrap; gap: 0.45rem; margin: 0.5rem 0 0.3rem; }
        .jd-pstat { display: flex; align-items: center; gap: 0.45rem; background: #ffffff;
            border: 1px solid #e4e9ec; border-radius: 999px; padding: 0.25rem 0.75rem; font-size: 0.8rem; }
        .jd-pstat b { font-weight: 700; color: #14232b; font-size: 0.95rem; font-variant-numeric: tabular-nums; }
        .jd-pstat span { color: #5b6b73; }
        .jd-pdot { width: 0.6rem; height: 0.6rem; border-radius: 999px; flex: none; }
        .jd-pdot.prosecutor, .jd-pdot.statebody { background: #3a6ea5; }
        .jd-pdot.organisation { background: #c98a3a; }
        .jd-pdot.individual { background: #9aa7ad; }
        .jd-pchip { font-size: 0.62rem; font-weight: 700; letter-spacing: 0.02em; text-transform: uppercase;
            border-radius: 4px; padding: 0.05rem 0.4rem; white-space: nowrap; flex: none; }
        .jd-pchip.prosecutor, .jd-pchip.statebody { background: #e8eef9; color: #2f4b86; }
        .jd-pchip.organisation { background: #fbf3e8; color: #8a5a2a; }
        .jd-pchip.individual { background: #f1f0f4; color: #5a5470; }
        .jd-party-p { font-weight: 600; color: #1f2d33; }
        .jd-vs { color: #9aa7ad; font-style: italic; padding: 0 0.15rem; }
        .jd-party-d { color: #3a4a51; }
        /* listing status ("For Mention" / "For Hearing" / "In Custody") — the
           practitioner signal a bare party line was missing */
        .jd-status { font-size: 0.64rem; font-weight: 600; color: #4a5a61; background: #eef2f3;
            border-radius: 4px; padding: 0.05rem 0.4rem; white-space: nowrap; flex: none; }
        /* judge-profile "Before the court" — one block per captured court day */
        .jd-day-head { display: flex; align-items: baseline; gap: 0.6rem; margin: 1.1rem 0 0.4rem; }
        .jd-day-date { font-size: 0.95rem; font-weight: 700; color: #14232b; }
        .jd-day-meta { font-size: 0.76rem; color: #6b7b83; }
        .jd-sitting-line { font-size: 0.8rem; color: #2c3e46; margin: 0.15rem 0 0.35rem; }
        .jd-sitting-line b { color: #14232b; font-weight: 650; }
        /* waiting-times court/list grouping */
        .jd-wait-court { font-size: 1.0rem; font-weight: 700; color: #14232b; margin: 1.3rem 0 0.2rem; }
        .jd-wait-ctx { font-size: 0.8rem; font-weight: 650; color: #4a5a61; margin: 0.7rem 0 0.3rem;
            text-transform: none; letter-spacing: 0; }
        .jd-court-link { color: #2c5f6b; text-decoration: none; }
        .jd-court-link:hover { text-decoration: underline; }
        /* Legal Diary — sitting cards and case rows expand client-side (native
           <details>, no Streamlit rerun — same pattern as the questions cards).
           Everything revealed is the already-anonymised Tier C layer: people are
           initials and in-camera matters were dropped upstream, so the expanded
           view is safe by construction. */
        .jd-sit { background: #ffffff; border: 1px solid #e4e9ec; border-radius: 10px; overflow: hidden; }
        .jd-sit > summary { list-style: none; cursor: pointer; padding: 0.75rem 0.85rem;
            display: flex; flex-direction: column; gap: 0.2rem; }
        .jd-sit > summary::-webkit-details-marker { display: none; }
        .jd-sit > summary:hover { background: #f7fafb; }
        .jd-sit[open] > summary { border-bottom: 1px solid #eef2f3; }
        .jd-sit-matters { padding: 0.35rem 0.85rem 0.55rem; }
        .jd-expand-hint { align-self: flex-start; font-size: 0.7rem; font-weight: 600;
            color: #2c5f6b; margin-top: 0.15rem; }
        .jd-sit[open] .jd-expand-hint { display: none; }
        .jd-case { border-bottom: 1px solid #f0f3f4; }
        .jd-case:last-child { border-bottom: none; }
        .jd-case > summary { list-style: none; cursor: pointer; display: flex;
            align-items: baseline; gap: 0.5rem; padding: 0.32rem 0; font-size: 0.86rem; color: #1f2d33; }
        .jd-case > summary::-webkit-details-marker { display: none; }
        .jd-case > summary:hover { color: #14232b; }
        .jd-case-detail { padding: 0.1rem 0 0.55rem 0.9rem; display: flex; flex-wrap: wrap;
            gap: 0.25rem 1.1rem; font-size: 0.78rem; color: #5b6b73; line-height: 1.5; }
        .jd-case-detail b { color: #14232b; font-weight: 600; }
        .jd-case-detail .jd-case-link { margin-left: 0; }
        /* Courthouses — expandable register cards (address / eircode / circuit). */
        .jd-ch-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(15rem, 1fr)); gap: 0.6rem; }
        .jd-ch { background: #ffffff; border: 1px solid #e4e9ec; border-radius: 10px; overflow: hidden; }
        .jd-ch > summary { list-style: none; cursor: pointer; padding: 0.6rem 0.8rem; font-weight: 650;
            color: #14232b; font-size: 0.86rem; display: flex; justify-content: space-between; gap: 0.5rem; }
        .jd-ch > summary::-webkit-details-marker { display: none; }
        .jd-ch > summary:hover { background: #f7fafb; }
        .jd-ch-county { font-weight: 500; color: #8a9aa1; font-size: 0.76rem; white-space: nowrap; }
        .jd-ch-detail { padding: 0.2rem 0.8rem 0.7rem; font-size: 0.78rem; color: #5b6b73; line-height: 1.55; }
        .jd-ch-detail b { color: #14232b; font-weight: 600; }
        .jd-ch-detail a { color: #2c5f6b; }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════ ① THE BENCH
def _bench_card_html(row) -> str:
    # The cards are already filtered by the court pill, so the court sits in the
    # section caption — repeating it on every card is noise. Salary band is uniform
    # within a court (the ordinary-judge figure), so it lives on each judge's
    # profile rather than 11× down the roster; only the senior-office exception
    # (Chief Justice / President / ex-officio premium) is chipped here.
    chips = []
    if row.get("has_spine"):
        auth = row.get("first_appointing_authority")
        label, _cls = _AUTHORITY.get(auth, (auth or "", "other"))
        appt = f'<div class="jud-appt">Appointed {_year(row.get("first_appointed_date"))} by {_esc(label)}</div>'
        if row.get("is_elevation") and not row.get("requires_manual_review"):
            chips.append('<span class="jud-chip elev">Elevated</span>')
    else:
        appt = ""
        chips.append('<span class="jud-chip gap">Record begins 2016</span>')
    if row.get("is_ex_officio_or_multi"):
        chips.append('<span class="jud-chip office">Senior office</span>')
    if isinstance(row.get("assignment"), str) and row["assignment"]:
        chips.append('<span class="jud-chip assign">Specialist list</span>')
    if row.get("requires_manual_review"):
        chips.append('<span class="jud-chip review">Needs review</span>')
    chiprow = f'<div class="jud-chiprow">{"".join(chips)}</div>' if chips else ""
    return f'<div class="jud-card"><div class="jud-jn">{_esc(row.get("judge_name"))}</div>{appt}{chiprow}</div>'


def _render_the_bench(roster: pd.DataFrame) -> None:
    if roster is None or roster.empty:
        empty_state(
            "The bench isn't loaded yet",
            "Run extractors/judiciary_bench_extract.py to populate the judiciary "
            "roster, appointments and profile views.",
        )
        return
    st.caption(
        "Every judge currently sitting, grouped by court. Click a judge for their "
        "full appointment history. Coverage of appointments before 2016 is partial — "
        "those judges are shown with an honest note, not hidden."
    )
    present = [c for c in _BENCH_COURTS if c in set(roster["court"].dropna())]
    if not present:
        empty_state("No courts found", "The roster loaded but no recognised courts were present.")
        return
    chosen = st.pills("Court", present, default=present[0], key="jud_court", label_visibility="collapsed") or present[0]
    sub = roster[roster["court"] == chosen].sort_values("judge_name")
    n_spine = int(sub["has_spine"].sum())
    st.caption(f"{len(sub)} judges on the {chosen} · {n_spine} with an appointment record since 2016.")
    cards = [
        clickable_card_link(
            href=f"?judge={urllib.parse.quote(str(r.judge_key))}",
            inner_html=_bench_card_html(r._asdict()),
            aria_label=f"View the appointment history of {r.judge_name}",
        )
        for r in sub.itertuples()
    ]
    st.html(f'<div class="jud-grid">{"".join(cards)}</div>')


# ══════════════════════════════════════════════════════ JUDGE PROFILE (?judge=)
def _render_profile(judge_key: str) -> None:
    profile = fetch_profile()
    appts = fetch_appointments()
    if back_button("← Back to the bench", key="judprof"):
        st.query_params.clear()
        st.rerun()

    row = None
    if profile is not None and not profile.empty:
        match = profile[profile["judge_key"] == judge_key]
        if not match.empty:
            row = match.iloc[0]
    if row is None:
        empty_state(
            "Judge not found", "That profile link didn't match a sitting judge. Use Back to return to the bench."
        )
        return

    rank_note = " · ex-officio member of more senior courts" if row.get("is_ex_officio_or_multi") else ""
    st.html(
        f'<div class="jud-prof-head"><h1 class="jud-prof-name">{_esc(row["judge_name"])}</h1>'
        f'<div class="jud-prof-sub">{_esc(row.get("current_court"))}{_esc(rank_note)}</div></div>'
    )

    # career arc — one step per court, oldest → current. An elevation files each notice
    # under the court appointed TO, so a judge who rose through the courts has one step
    # per court, not one per notice.
    ev = (
        appts[appts["judge_key"] == judge_key].sort_values("issue_date")
        if appts is not None and not appts.empty
        else pd.DataFrame()
    )
    if not ev.empty:
        st.html('<h2 class="jd-section-head">Career arc</h2>')
        # Collapse consecutive same-court notices (re-notices / corrections) into one
        # step, keeping the earliest notice + its Iris source link. Then append the
        # roster's current court as the final "now" step whenever the last captured
        # step is for a different court — the elevation notice to the current court can
        # predate the 2016 Iris spine (so it is not captured as an event), and the arc
        # must still reach the court the judge sits on today (e.g. Court of Appeal
        # judges elevated from the High Court).
        steps: list[dict] = []
        for e in ev.itertuples():
            if steps and steps[-1]["court"] == e.appointed_court:
                continue
            steps.append(
                {
                    "court": e.appointed_court,
                    "year": _year(e.issue_date),
                    "auth": _AUTHORITY.get(e.appointing_authority, (e.appointing_authority or "", ""))[0],
                    "url": e.source_url if isinstance(e.source_url, str) and e.source_url else "",
                }
            )
        current = row.get("current_court")
        if isinstance(current, str) and current and (not steps or steps[-1]["court"] != current):
            steps.append({"court": current, "year": "", "auth": "", "url": ""})

        nodes = []
        last = len(steps) - 1
        for i, s in enumerate(steps):
            date = s["year"] or ("current" if i == last else "")
            auth = f'<div class="jud-node-auth">by {_esc(s["auth"])}</div>' if s["auth"] else ""
            link = (
                f'<a class="jud-node-link" href="{_esc(s["url"])}" target="_blank" rel="noopener">Iris notice ↗</a>'
                if s["url"]
                else ""
            )
            nodes.append(
                f'<div class="jud-node{" now" if i == last else ""}">'
                f'<div class="jud-node-dot"></div>'
                f'<div class="jud-node-court">{_esc(s["court"])}</div>'
                f'<div class="jud-node-date">{_esc(date)}</div>'
                f"{auth}{link}</div>"
            )
        st.html(f'<div class="jud-arc">{"".join(nodes)}</div>')
    else:
        st.html(
            '<div class="jud-context"><strong>Appointment record begins 2016.</strong> '
            f"{_esc(row['judge_name'])} sits on the {_esc(row.get('current_court'))}, but their "
            "appointment predates the public Iris Oifigiúil spine, so the earlier career arc is "
            "not yet recorded here.</div>"
        )

    # facts
    facts = []
    band = row.get("salary_band_eur")
    if band is not None and not pd.isna(band):
        facts.append(
            f"<strong>Salary band</strong> €{int(band):,} ({_esc(row.get('salary_office'))}, "
            f"{_esc(row.get('salary_source'))})"
        )
    elif row.get("is_ex_officio_or_multi"):
        facts.append("<strong>Salary band</strong> President / ex-officio premium (not attributed here)")
    if isinstance(row.get("assignment"), str) and row["assignment"]:
        facts.append(f"<strong>Assignment</strong> {_esc(row['assignment'])} ({_esc(row.get('assignment_term'))})")
    if facts:
        st.html('<div class="jud-context">' + "<br>".join(facts) + "</div>")

    # vacancy context (gov.ie)
    if isinstance(row.get("govie_vacancy_cause"), str) and row["govie_vacancy_cause"]:
        pred = row.get("govie_predecessor")
        prior = row.get("govie_prior_career")
        bits = [
            f'<div class="jud-vac-cause">Filled a vacancy created by {_esc(row["govie_vacancy_cause"])}'
            + (f": {_esc(pred)}" if isinstance(pred, str) and pred else "")
            + "</div>"
        ]
        if isinstance(prior, str) and prior:
            bits.append(f'<div class="jud-vac-nom">Prior career: {_esc(prior)}</div>')
        st.html(f'<div class="jud-vac">{"".join(bits)}</div>')

    if row.get("requires_manual_review"):
        st.caption(
            "Note: one appointment record for this judge is a low-confidence match and is flagged for manual review."
        )

    _render_profile_diary(judge_key, row.get("current_court"))

    # provenance
    links = []
    if isinstance(row.get("appt_source_url"), str) and row["appt_source_url"]:
        links.append(
            f'<a href="{_esc(row["appt_source_url"])}" target="_blank" rel="noopener">Iris Oifigiúil notice ↗</a>'
        )
    if isinstance(row.get("source_url"), str) and row["source_url"]:
        links.append(f'<a href="{_esc(row["source_url"])}" target="_blank" rel="noopener">Courts Service roster ↗</a>')
    if isinstance(row.get("govie_source_url"), str) and row["govie_source_url"]:
        links.append(
            f'<a href="{_esc(row["govie_source_url"])}" target="_blank" rel="noopener">gov.ie nomination (search) ↗</a>'
        )
    st.html(
        '<div class="jud-foot"><strong>Sources:</strong> '
        + " · ".join(links)
        + f". Roster snapshot {_esc(row.get('source_published_at'))}. This profile records "
        "appointment, office and assignment only — not performance, conduct or any ranking.</div>"
    )


# ── judge profile: Before the court (Legal Diary bridge) ─────────────────────
_PROFILE_DIARY_DAYS = 5  # most recent captured court days shown
_PROFILE_DIARY_ROWS = 20  # matters rendered per day before an honest remainder count


def _render_profile_diary(judge_key: str, current_court: str | None = None) -> None:
    """What the Legal Diary lists before this judge — sittings (courtroom / time /
    list) plus the anonymised matters, newest day first. The diary→roster name join
    is PIPELINE-owned (v_judiciary_judge_sittings / v_judiciary_judge_diary via
    judiciary_diary_judge_map); this section filters to one judge and renders.
    A schedule, never a workload or performance measure."""
    sittings = fetch_judge_sittings()
    diary = fetch_judge_diary()
    sittings = sittings[sittings["judge_key"] == judge_key] if sittings is not None and not sittings.empty else None
    diary = diary[diary["judge_key"] == judge_key] if diary is not None and not diary.empty else None

    st.html('<h2 class="jd-section-head">Before the court</h2>')
    if (sittings is None or sittings.empty) and (diary is None or diary.empty):
        # District Court is a SOURCE gap, not a match failure: the Courts Service Legal
        # Diary publishes a District Court sittings schedule only (no party-level lists),
        # so there is nothing to list here — say so honestly rather than implying the
        # judge simply wasn't matched. (Circuit + the higher courts ARE covered.)
        if isinstance(current_court, str) and current_court == "District Court":
            st.html(
                '<div class="jud-context">The Courts Service Legal Diary does not publish '
                "party-level lists for the District Court — only its sittings schedule — so "
                "there are no listed matters to show for District Court judges here.</div>"
            )
        else:
            st.html(
                '<div class="jud-context">No sittings for this judge in our Legal Diary capture '
                "(diary entries that can't be matched to the roster with certainty are left out "
                "rather than guessed).</div>"
            )
        return
    st.caption(
        "From the daily Legal Diary: where and when this judge sat, and the anonymised "
        "matters listed before them. People are shown by initials; organisations and the "
        "State are named. This is a schedule of listings — not a workload or performance measure."
    )

    days: list[str] = sorted(
        set([] if sittings is None or sittings.empty else sittings["diary_date"].dropna().tolist())
        | set([] if diary is None or diary.empty else diary["diary_date"].dropna().tolist()),
        reverse=True,
    )
    hidden_days = max(0, len(days) - _PROFILE_DIARY_DAYS)
    for day in days[:_PROFILE_DIARY_DAYS]:
        day_sit = sittings[sittings["diary_date"] == day] if sittings is not None and not sittings.empty else None
        day_cases = diary[diary["diary_date"] == day] if diary is not None and not diary.empty else None
        n_matters = 0 if day_cases is None else len(day_cases)
        head_meta = f"{n_matters} listed matter{'s' if n_matters != 1 else ''}" if n_matters else "schedule only"
        st.html(
            f'<div class="jd-day-head"><span class="jd-day-date">{_esc(_fmt_day(day))}</span>'
            f'<span class="jd-day-meta">{head_meta}</span></div>'
        )
        if day_sit is not None and not day_sit.empty:
            lines = []
            for s in day_sit.sort_values(["time", "list_type"], na_position="last").itertuples():
                # Circuit Court sittings carry a venue (Galway, Ennis …); the Dublin courts
                # don't. A panel sitting (Supreme / Court of Appeal, 3–5 judges) is chipped so
                # it's clear this judge heard the matter on a bench, not alone.
                venue = _esc(getattr(s, "venue", "") or "")
                bits = " · ".join(
                    p for p in (_esc(s.court), venue, _esc(s.courtroom), _esc(s.time), _esc(s.list_type)) if p
                )
                n = int(getattr(s, "n_items", 0) or 0)
                tail = f" — {n} listed" if n else ""
                psize = int(getattr(s, "panel_size", 1) or 1)
                panel = f' <span class="jd-status">panel of {psize}</span>' if psize > 1 else ""
                lines.append(f'<div class="jd-sitting-line"><b>Sitting</b> {bits}{tail}{panel}</div>')
            st.html("".join(lines))
        if day_cases is not None and not day_cases.empty:
            rows = []
            for r in day_cases.head(_PROFILE_DIARY_ROWS).itertuples():
                link = (
                    f'<a class="jd-case-link" href="{_esc(r.source_url)}" target="_blank" '
                    f'rel="noopener">official diary ↗</a>'
                    if isinstance(r.source_url, str) and r.source_url
                    else ""
                )
                rows.append(f'<div class="jd-case-row">{_case_party_html(r)}{link}</div>')
            st.html("".join(rows))
            if n_matters > _PROFILE_DIARY_ROWS:
                st.caption(f"… and {n_matters - _PROFILE_DIARY_ROWS} more matters this day — see the Legal Diary tab.")
    if hidden_days:
        st.caption(
            f"… and {hidden_days} earlier captured day{'s' if hidden_days != 1 else ''} — "
            "the Legal Diary tab holds the full archive."
        )
    # transparency: say so when the diary names this judge by surname only
    methods = set()
    for frame in (sittings, diary):
        if frame is not None and not frame.empty and "match_method" in frame.columns:
            methods |= set(frame["match_method"].dropna())
    if methods - {"exact"}:
        st.caption(
            "The diary names judges by surname; these listings are matched to this judge by "
            "surname within their court. Ambiguous names are excluded rather than guessed."
        )


# ══════════════════════════════════════════════ ② APPOINTMENTS & GOVERNMENT
def _render_appointments(appts: pd.DataFrame, noms: pd.DataFrame) -> None:
    if appts is None or appts.empty:
        empty_state(
            "Appointments aren't loaded yet",
            "Run extractors/judiciary_bench_extract.py to populate the appointments view.",
        )
        return
    st.caption(
        "Every judicial appointment recorded since 2016, who made it, and how judges "
        "have moved up through the courts. The Government appoints the unelected bench; "
        "this is the record of that power."
    )

    # Authority split + elevation ladder are PIPELINE-OWNED rollups
    # (v_judiciary_authority_summary / v_judiciary_elevation_ladder); the page only
    # renders the pre-aggregated frames — no counting in-app.
    authority = fetch_authority_summary()
    ladder = fetch_elevation_ladder()

    if authority is not None and not authority.empty:
        st.html('<h2 class="jd-section-head">How each appointment was recorded</h2>')
        st.caption(
            "Under the Constitution the Government decides judicial appointments and the "
            "President formally makes them. These are counts of how each notice recorded the "
            "authority, not a measure of who exercised the choice."
        )
        cells = []
        for r in authority.itertuples():
            label, cls = _AUTHORITY.get(r.appointing_authority, (r.appointing_authority or "", "other"))
            cells.append(
                f'<div class="jud-stat"><span class="jud-stat-n">{int(r.n)}</span>'
                f'<span class="jud-auth {cls}">{_esc(label)}</span></div>'
            )
        st.html(f'<div class="jud-statwrap">{"".join(cells)}</div>')

    if ladder is not None and not ladder.empty:
        st.html('<h2 class="jd-section-head">The elevation ladder</h2>')
        st.caption("Judges promoted from one court to a more senior one (a fresh appointment notice each time).")
        rungs = [
            f'<div class="jud-rung"><span class="jud-rung-n">{int(r.n)}</span>'
            f'<span class="jud-rung-path">{_esc(r.appointed_court)} → {_esc(r.elevated_to)}</span></div>'
            for r in ladder.itertuples()
        ]
        st.html(f'<div class="jud-ladder">{"".join(rungs)}</div>')

    # vacancy lifecycle from gov.ie nominations
    if noms is not None and not noms.empty:
        st.html('<h2 class="jd-section-head">Vacancy lifecycle</h2>')
        st.caption(
            "From gov.ie nomination announcements: what created each seat, who filled it, "
            "and the career they came from. Text is preserved from the release, not inferred."
        )
        cards = []
        for r in noms.itertuples():
            pred = f" — {_esc(r.predecessor)}" if isinstance(r.predecessor, str) and r.predecessor else ""
            prior = (
                f'<div class="jud-vac-nom">{_esc(r.nominee)} · {_esc(r.prior_career)}</div>'
                if isinstance(r.prior_career, str) and r.prior_career
                else f'<div class="jud-vac-nom">{_esc(r.nominee)}</div>'
            )
            link = (
                f' <a href="{_esc(r.source_url)}" target="_blank" rel="noopener" '
                f'style="font-size:0.72rem">find announcement on gov.ie ↗</a>'
                if isinstance(r.source_url, str) and r.source_url
                else ""
            )
            cards.append(
                f'<div class="jud-vac"><div class="jud-vac-cause">{_esc(r.target_court)} · '
                f"{_esc(r.vacancy_cause)}</div>"
                f'<div class="jud-vac-pred">Seat created by: {_esc(r.vacancy_cause)}{pred}</div>'
                f"{prior}{link}</div>"
            )
        st.html("".join(cards))


# ══════════════════════════════════════════════════════════ ③ THE COURTS
# System-health only: case throughput, waiting times, and where the courts sit.
# Aggregate by court — NEVER attributed to a named judge (privacy rule). Every metric
# (clearance_pct, parsed weeks) is computed in the SQL views; this renders the frames.
def _clearance_bar(row, label_key: str = "jurisdiction") -> str:
    """One ranked clearance bar. label_key picks the row field shown as the title, so
    the same primitive renders both the court ranking and the per-court area breakdown."""
    pct = row.get("clearance_pct")
    if pct is None or pd.isna(pct):
        return ""
    pct = float(pct)
    state = "under" if pct < 100 else "over"
    fill = min(pct, 100.0)
    inc, res = row.get("incoming"), row.get("resolved")
    sub = f"{int(inc):,} received · {int(res):,} resolved" if pd.notna(inc) and pd.notna(res) else ""
    return (
        '<div class="jd-rank">'
        f'<div class="jd-rank-body"><div class="jd-rank-title">{_esc(row.get(label_key))}</div>'
        f'<div class="jd-rank-sub">{sub}</div>'
        f'<div class="jd-bar-track"><div class="jd-bar-fill {state}" style="width:{fill:.0f}%"></div></div></div>'
        f'<div class="jd-rank-n {state}">{pct:.0f}%</div></div>'
    )


def _clearance_trend_chart(clr: pd.DataFrame) -> alt.LayerChart:
    """Multi-year clearance lines, one per court, with a 100% break-even reference rule.
    Lines below the rule = backlog growing that year; above = backlog shrinking."""
    df = clr[["jurisdiction", "year", "clearance_pct", "incoming", "resolved"]].dropna(subset=["clearance_pct"])
    courts = [c for c in _COURT_ORDER if c in set(df["jurisdiction"])]
    lines = (
        alt.Chart(df)
        .mark_line(point=alt.OverlayMarkDef(size=42, filled=True), strokeWidth=2.2)
        .encode(
            x=alt.X("year:O", title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("clearance_pct:Q", title="Clearance %", scale=alt.Scale(zero=False)),
            color=alt.Color(
                "jurisdiction:N",
                title="Court",
                scale=alt.Scale(domain=courts, scheme="tableau10"),
                legend=alt.Legend(orient="bottom", columns=3, labelLimit=200),
            ),
            tooltip=[
                alt.Tooltip("jurisdiction:N", title="Court"),
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("clearance_pct:Q", title="Clearance %", format=".0f"),
                alt.Tooltip("incoming:Q", title="Received", format=","),
                alt.Tooltip("resolved:Q", title="Resolved", format=","),
            ],
        )
    )
    rule = alt.Chart(pd.DataFrame({"y": [100]})).mark_rule(color="#9aa8ad", strokeDash=[4, 4]).encode(y="y:Q")
    return (rule + lines).properties(height=320)


def _render_clearance(clr: pd.DataFrame) -> None:
    st.html('<h2 class="jd-section-head">Case clearance by court</h2>')
    st.caption(
        "Clearance is cases resolved as a share of cases received that year. Below 100% means a "
        "court resolved fewer than it took in (its backlog grew); at or above 100% it kept pace or "
        "cut into the backlog. These are throughput counts only — not a measure of any judge."
    )
    years = sorted(clr["year"].dropna().astype(int).unique(), reverse=True)
    if not years:
        return
    # Shared year_selector — defaults to the most recent COMPLETED year, the
    # app-wide convention (a partial YTD year would misread as a collapse).
    sel = year_selector([str(y) for y in years], key="courts_clear_year")
    # Lowest clearance first — the courts under most pressure surface at the top.
    year_df = clr[clr["year"] == sel].sort_values("clearance_pct", na_position="last")
    st.caption(
        f"{int(sel)} · {len(year_df)} courts, ordered by clearance rate (lowest first). "
        "Select a court for its full breakdown — area of law, trend and waiting times."
    )
    # Each bar is a clickable tile → ?court= per-court detail page (soft-nav).
    st.html(
        "".join(
            clickable_card_link(
                href=f"?court={urllib.parse.quote(str(r.jurisdiction))}",
                inner_html=_clearance_bar(r._asdict()),
                aria_label=f"See full court statistics for the {r.jurisdiction}",
            )
            for r in year_df.itertuples()
        )
    )

    # The time dimension — every court's trajectory across the whole 2017–2024 window.
    st.html('<h2 class="jd-section-head" style="margin-top:1.4rem">Clearance over time</h2>')
    st.caption("Each line is a court; the dashed rule marks 100% (cases resolved equalling cases received).")
    st.altair_chart(_clearance_trend_chart(clr), width="stretch")


def _court_clearance_chart(df: pd.DataFrame) -> alt.LayerChart:
    """One court's clearance trajectory across the window, with the 100% break-even
    rule. A single teal line (no per-court colour legend — there is only one court)."""
    d = df[["year", "clearance_pct", "incoming", "resolved"]].dropna(subset=["clearance_pct"])
    line = (
        alt.Chart(d)
        .mark_line(point=alt.OverlayMarkDef(size=46, filled=True), strokeWidth=2.4, color="#3d7c8a")
        .encode(
            x=alt.X("year:O", title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("clearance_pct:Q", title="Clearance %", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("clearance_pct:Q", title="Clearance %", format=".0f"),
                alt.Tooltip("incoming:Q", title="Received", format=","),
                alt.Tooltip("resolved:Q", title="Resolved", format=","),
            ],
        )
    )
    rule = alt.Chart(pd.DataFrame({"y": [100]})).mark_rule(color="#9aa8ad", strokeDash=[4, 4]).encode(y="y:Q")
    return (rule + line).properties(height=300)


def _stat_card(value: str, label: str) -> str:
    return f'<div class="jd-cat-card"><div class="jd-cat-n">{_esc(value)}</div><div class="jd-cat-label">{_esc(label)}</div></div>'


def _render_court_detail(court: str) -> None:
    """Full-page drill-down for one court (?court=…): its clearance headline + trend,
    the latest-year area-of-law breakdown, and the waiting lists published under it.
    All system-throughput aggregates — never attributed to a named judge. Every frame
    is read straight from the views and filtered to this court (display_only)."""
    clr = fetch_courts_clearance()
    area = fetch_courts_clearance_by_area()
    wt = fetch_courts_waiting_times()
    if back_button("← Back to The Courts", key="court_detail"):
        st.query_params.clear()
        st.rerun()

    court_clr = clr[clr["jurisdiction"] == court].copy() if clr is not None and not clr.empty else pd.DataFrame()
    has_wait_ctx = wt is not None and not wt.empty and "jurisdiction" in wt.columns
    court_wt = wt[wt["jurisdiction"] == court].copy() if has_wait_ctx else pd.DataFrame()

    if court_clr.empty and court_wt.empty:
        empty_state(
            "Court not found",
            "That court link didn't match a court in the statistics. Use Back to return to The Courts.",
        )
        return

    st.html(
        f'<div class="jud-prof-head"><h1 class="jud-prof-name">{_esc(court)}</h1>'
        '<div class="jud-prof-sub">System throughput — case clearance and waiting times. '
        "Never attributed to a named judge.</div></div>"
    )

    if not court_clr.empty:
        latest = court_clr.sort_values("year").iloc[-1]
        st.html('<h2 class="jd-section-head">Case clearance over time</h2>')
        st.caption(
            "Cases resolved as a share of cases received each year. Below 100% means the backlog "
            f"grew that year; at or above 100% the {court} kept pace or cut into it."
        )
        cells = []
        if pd.notna(latest.get("clearance_pct")):
            cells.append(_stat_card(f"{float(latest['clearance_pct']):.0f}%", f"clearance {int(latest['year'])}"))
        if pd.notna(latest.get("incoming")):
            cells.append(_stat_card(f"{int(latest['incoming']):,}", f"cases received {int(latest['year'])}"))
        if pd.notna(latest.get("resolved")):
            cells.append(_stat_card(f"{int(latest['resolved']):,}", f"cases resolved {int(latest['year'])}"))
        if cells:
            st.html(f'<div class="jd-catwrap">{"".join(cells)}</div>')
        st.altair_chart(_court_clearance_chart(court_clr), width="stretch")

        # Area-of-law breakdown for a chosen year (the finer-grain view, filtered here).
        court_area = area[area["jurisdiction"] == court] if area is not None and not area.empty else pd.DataFrame()
        if not court_area.empty:
            ayears = sorted(court_area["year"].dropna().astype(int).unique(), reverse=True)
            st.html('<h2 class="jd-section-head" style="margin-top:1.4rem">By area of law</h2>')
            sel = year_selector([str(y) for y in ayears], key="court_detail_area_year")
            sub = court_area[court_area["year"] == sel].sort_values("clearance_pct", na_position="last")
            if sub.empty:
                st.caption(f"No area breakdown recorded for the {court} in {int(sel)}.")
            else:
                st.caption(f"{court} · {int(sel)} · clearance by area of law (lowest first).")
                st.html("".join(_clearance_bar(r._asdict(), label_key="area_of_law") for r in sub.itertuples()))

    if not court_wt.empty:
        st.html('<h2 class="jd-section-head" style="margin-top:1.4rem">Waiting times</h2>')
        st.caption(
            "Published waiting times from the Courts Service Annual Report 2024, with the change on "
            "2023 — the time to a hearing or first return date for each list."
        )
        _render_waiting_groups(court_wt)

    st.html(
        '<div class="jd-foot"><strong>Sources:</strong> '
        '<a href="https://data.courts.ie" target="_blank" rel="noopener">Courts Service annual statistics ↗</a> '
        "(clearance, CC-BY 4.0) · "
        '<a href="https://www.courts.ie/annual-report" target="_blank" rel="noopener">'
        "Courts Service Annual Report 2024 ↗</a> (waiting times). System-level throughput only — "
        "no judge is named, ranked, or assessed. Clearance above 100% reflects backlog reduction, not error.</div>"
    )


def _wait_card(row, label: str | None = None) -> str:
    w24, w23 = row.get("weeks_2024"), row.get("weeks_2023")
    # The big value comes from the PARSED weeks, not the raw wait_2024 text — that text can
    # be a phrase ("Date immediately available") that doesn't belong in a number slot. Zero
    # weeks = no wait; render that as "Immediate" with correct singular/plural otherwise.
    if pd.isna(w24):
        big = f'<span class="jd-wait-weeks">{_esc(row.get("wait_2024")) or "—"}</span>'
    elif float(w24) == 0:
        big = '<span class="jd-wait-weeks">Immediate</span>'
    else:
        w = float(w24)
        unit = "week" if w == 1 else "weeks"
        big = f'<span class="jd-wait-weeks">{w:g}</span><span class="jd-wait-unit">{unit}</span>'
    delta = ""
    if pd.notna(w24) and pd.notna(w23):
        d = float(w24) - float(w23)
        if abs(d) < 0.01:
            delta = '<div class="jd-delta flat">no change on 2023</div>'
        else:
            cls = "up" if d > 0 else "down"
            arrow = "▲" if d > 0 else "▼"
            delta = f'<div class="jd-delta {cls}">{arrow} {abs(d):g} wk vs 2023</div>'
    return (
        '<div class="jd-cat-card"><div class="jd-cat-label">'
        f"{_esc(label if label is not None else row.get('matter_or_venue'))}</div>"
        f'<div class="jd-wait-n">{big}</div>'
        f"{delta}</div>"
    )


# Court reading order for the waiting-times section (publication covers these three).
_WAIT_COURT_ORDER = ["High Court", "Central Criminal Court", "Court of Appeal"]


def _render_waiting(wt: pd.DataFrame) -> None:
    st.html('<h2 class="jd-section-head">Waiting times — what to expect, by court and list</h2>')
    st.caption(
        "Published waiting times from the Courts Service Annual Report 2024, grouped by the "
        "court and list they were published under, with the change on 2023. A wait is the "
        "time to a hearing date or first return date for that list — context for anyone "
        "deciding where and how to issue proceedings."
    )

    has_ctx = "jurisdiction" in wt.columns and wt["jurisdiction"].notna().any()
    if not has_ctx:
        # context columns not built yet — fall back to the flat longest-first grid
        clean = wt[wt["is_clean_label"]].copy() if "is_clean_label" in wt.columns else wt.copy()
        clean = clean.sort_values("weeks_2024", ascending=False, na_position="last")
        st.html(f'<div class="jd-catwrap">{"".join(_wait_card(r._asdict()) for r in clean.itertuples())}</div>')
        return

    # The standout outlier — surfaced as a callout, not buried in the grid.
    lim = wt[wt["matter_or_venue"].astype(str).str.fullmatch("Limerick")]
    if not lim.empty:
        r = lim.iloc[0]
        st.html(
            '<div class="jd-outlier"><strong>Limerick</strong> stands out among the High Court\'s '
            f"personal-injury trial venues at <strong>{_esc(r['wait_2024'])}</strong>, against about "
            f"four weeks elsewhere — though down from {_esc(r['wait_2023'])} the year before.</div>"
        )

    courts = [c for c in _WAIT_COURT_ORDER if c in set(wt["jurisdiction"].dropna())]
    courts += sorted(set(wt["jurisdiction"].dropna()) - set(_WAIT_COURT_ORDER))
    for court in courts:
        sub = wt[wt["jurisdiction"] == court]
        n_lists = sub["list_context"].nunique()
        # Court name links to the full per-court detail page (clearance + waiting).
        st.html(
            f'<div class="jd-wait-court">'
            f'<a class="jd-court-link" href="?court={urllib.parse.quote(str(court))}">{_esc(court)}</a> '
            f'<span class="jd-day-meta">· {n_lists} list{"s" if n_lists != 1 else ""}</span></div>'
        )
        _render_waiting_groups(sub)


def _render_waiting_groups(sub: pd.DataFrame) -> None:
    """Render one court's waiting lists — a card grid per list_context, in publication
    order (the view orders by report page + row; groupby(sort=False) preserves it).
    Shared by the overview waiting section and the per-court detail page.
    logic_firewall: display_only."""
    for ctx, g in sub.groupby("list_context", sort=False):
        st.html(f'<div class="jd-wait-ctx">{_esc(ctx)}</div>')
        cards = []
        for r in g.itertuples():
            row = r._asdict()
            # mojibake source labels (flagged upstream) read better as the curated
            # list name; clean labels keep the published wording.
            label = None if bool(getattr(r, "is_clean_label", True)) else str(ctx)
            cards.append(_wait_card(row, label=label))
        st.html(f'<div class="jd-catwrap">{"".join(cards)}</div>')


def _render_courthouses(ch: pd.DataFrame) -> None:
    st.html('<h2 class="jd-section-head">Where the courts sit</h2>')
    st.caption(
        f"{len(ch)} active courthouses across {ch['county'].nunique()} counties (Courts Service "
        "register). Filter by county and open a courthouse for its address and circuit."
    )
    st.map(ch[["latitude", "longitude"]], color="#2c5f6b")

    counties = sorted(ch["county"].dropna().unique())
    pick = (
        st.pills(
            "County",
            ["All counties", *counties],
            default="All counties",
            key="courts_ch_county",
            label_visibility="collapsed",
        )
        or "All counties"
    )
    sub = ch if pick == "All counties" else ch[ch["county"] == pick]
    cards = []
    for r in sub.sort_values("court_house").itertuples():
        bits = []
        for field, lbl in (
            ("address", "Address"),
            ("eircode", "Eircode"),
            ("circuit", "Circuit"),
            ("region", "Region"),
        ):
            val = getattr(r, field, None)
            if isinstance(val, str) and val:
                bits.append(f"<b>{_esc(lbl)}</b> {_esc(val)}")
        src = (
            f'<a href="{_esc(r.source_url)}" target="_blank" rel="noopener">Courts Service register ↗</a>'
            if isinstance(getattr(r, "source_url", None), str) and r.source_url
            else ""
        )
        detail = "<br>".join(bits) + (f"<br>{src}" if src else "")
        cards.append(
            f'<details class="jd-ch"><summary>{_esc(r.court_house)}'
            f'<span class="jd-ch-county">{_esc(r.county)}</span></summary>'
            f'<div class="jd-ch-detail">{detail}</div></details>'
        )
    st.html(f'<div class="jd-ch-grid">{"".join(cards)}</div>')


def _render_courts() -> None:
    clr = fetch_courts_clearance()
    wt = fetch_courts_waiting_times()
    ch = fetch_courthouses()

    if (clr is None or clr.empty) and (wt is None or wt.empty) and (ch is None or ch.empty):
        empty_state(
            "Court statistics aren't loaded yet",
            "Run extractors/judiciary_bench_extract.py to populate the clearance, waiting-time and courthouse views.",
        )
        return

    st.caption("How the courts are performing as a system — never attributed to a named judge.")
    if clr is not None and not clr.empty:
        _render_clearance(clr)
    if wt is not None and not wt.empty:
        _render_waiting(wt)
    if ch is not None and not ch.empty:
        _render_courthouses(ch)

    st.html(
        '<div class="jd-foot"><strong>Sources:</strong> '
        '<a href="https://data.courts.ie" target="_blank" rel="noopener">Courts Service annual statistics ↗</a> '
        "(clearance, CC-BY 4.0) · "
        '<a href="https://www.courts.ie/annual-report" target="_blank" rel="noopener">Courts Service Annual Report 2024 ↗</a> '
        "(waiting times) · "
        '<a href="https://data.courts.ie/files/court-offices/court-offices.csv" target="_blank" rel="noopener">'
        "court-office register ↗</a> (courthouses, CC-BY). System-level throughput only — no judge is named, "
        "ranked, or assessed. Clearance above 100% reflects backlog reduction, not error.</div>"
    )


# ══════════════════════════════════════════════════════════ ④ LEGAL DIARY
def _session_matters(court_cases: pd.DataFrame | None, sitting) -> list[str]:
    """The anonymised Tier C matters listed before one sitting — a DISPLAY-ONLY
    filter of the already-published cases set by (court, judge, list_type). The
    judge↔diary attribution is the same surname-within-court parse that produced
    both frames; an unmatched sitting (no certain attribution, or a private list)
    simply returns nothing and the card stays a plain schedule tile.
    logic_firewall: display_only."""
    if court_cases is None or court_cases.empty:
        return []
    judge = getattr(sitting, "judge", None)
    if not (isinstance(judge, str) and judge):
        return []
    by_judge = court_cases[court_cases["judge"] == judge]
    if by_judge.empty:
        return []
    list_type = getattr(sitting, "list_type", None)
    sub = by_judge[by_judge["list_type"] == list_type] if isinstance(list_type, str) and list_type else by_judge
    if sub.empty:  # list label drifted between schedule + cases — keep the judge match
        sub = by_judge
    return [_case_row_html(r) for r in sub.head(TIER_C_PAGE).itertuples()]


def _render_ld_schedule(day_sched: pd.DataFrame, day_cases: pd.DataFrame | None) -> None:
    st.html('<h2 class="jd-section-head">Today on the bench</h2>')
    st.caption(
        "Each card is a judge's sitting session — court, list and start time. Open a card "
        "with listed matters to see them, shown anonymised: people by initials, organisations "
        "and the State named. Private hearings are excluded; this is a schedule, not a workload."
    )
    has_cases = day_cases is not None and not day_cases.empty
    present = [c for c in _COURT_ORDER if c in set(day_sched["court"].dropna())]
    extra = sorted(set(day_sched["court"].dropna()) - set(_COURT_ORDER))
    for court in present + extra:
        rows = day_sched[day_sched["court"] == court]
        court_cases = day_cases[day_cases["court"] == court] if has_cases else None
        cards = []
        for r in rows.sort_values(["courtroom", "judge"], na_position="last").itertuples():
            n = int(getattr(r, "n_items", 0) or 0)
            items = (
                f'<span class="jd-items">{n} listed</span>' if n else '<span class="jd-items zero">schedule only</span>'
            )
            meta = " · ".join(p for p in (_esc(r.courtroom), _esc(r.time)) if p)
            face = (
                f'<div class="jd-judge">{_esc(r.judge)}</div>'
                f'<div class="jd-meta">{meta}</div>'
                f'<div class="jd-list">{_esc(r.list_type) or "—"}</div>{items}'
            )
            matters = _session_matters(court_cases, r)
            if matters:
                plural = "s" if len(matters) != 1 else ""
                cards.append(
                    f'<details class="jd-sit"><summary>{face}'
                    f'<span class="jd-expand-hint">Show {len(matters)} matter{plural} ↓</span>'
                    f'</summary><div class="jd-sit-matters">{"".join(matters)}</div></details>'
                )
            else:
                cards.append(f'<div class="jd-card">{face}</div>')
        st.html(
            f'<div class="jd-court-head">{_esc(court)} '
            f"<span>· {len(rows)} sitting{'s' if len(rows) != 1 else ''}</span></div>"
            f'<div class="jd-grid">{"".join(cards)}</div>'
        )


def _render_ld_busiest(day_counts: pd.DataFrame) -> None:
    st.html('<h2 class="jd-section-head">Most active lists today</h2>')
    st.caption(
        "Lists with the most scheduled items on this day — a count of listed "
        "matters, not a measure of judicial workload or performance."
    )
    top = day_counts.sort_values("n_items", ascending=False).head(8)
    if top.empty or int(top["n_items"].max() or 0) == 0:
        empty_state("No scheduled items", "No lists had listed matters on this day.")
        return
    mx = int(top["n_items"].max())
    for r in top.itertuples():
        n = int(r.n_items)
        pct = round(100 * n / mx) if mx else 0
        st.html(
            f'<div class="jd-rank"><div class="jd-rank-body">'
            f'<div class="jd-rank-title">{_esc(r.list_type) or "—"}</div>'
            f'<div class="jd-rank-sub">{_esc(r.court)}</div>'
            f'<div class="jd-bar-track"><div class="jd-bar-fill" style="width:{pct}%"></div></div>'
            f'</div><div class="jd-rank-n">{n}</div></div>'
        )


def _case_party_html(row) -> str:
    """A case row as plaintiff -> defendant with a plaintiff-kind chip and the listing
    status ("For Mention" / "For Hearing" …) when the diary recorded one. Falls back to
    the joined title for single-party matters (e.g. 'In the matter of … a bankrupt')."""
    cls, chip_label, _ = _PKIND.get(getattr(row, "plaintiff_kind", "") or "", ("individual", "—", ""))
    chip = f'<span class="jd-pchip {cls}">{_esc(chip_label)}</span>'
    plaintiff = getattr(row, "plaintiff", "") or ""
    defendant = getattr(row, "defendant", "") or ""
    if plaintiff and defendant:
        party = (
            f'<span class="jd-party-p">{_esc(plaintiff)}</span>'
            f'<span class="jd-vs">v</span>'
            f'<span class="jd-party-d">{_esc(defendant)}</span>'
        )
    else:
        party = f'<span class="jd-party-p">{_esc(row.case_anonymised)}</span>'
    status = getattr(row, "status", None)
    status_chip = f'<span class="jd-status">{_esc(status)}</span>' if isinstance(status, str) and status else ""
    return chip + party + status_chip


_CAT_LABEL = {key: label for key, label, _desc in _CATEGORIES}


def _case_row_html(r) -> str:
    """One listed matter as a client-side expander (<details>): the anonymised
    party line is the summary; opening it reveals the full anonymised detail
    (court / judge / list / status / type) and the official-diary link. There is
    no un-anonymised layer to reveal — people are reduced to initials and the
    statutory in-camera categories (minors, family, wards, childcare, asylum)
    were dropped at the extractor — so the expanded view is safe by construction.
    """
    link = (
        f'<a class="jd-case-link" href="{_esc(r.source_url)}" target="_blank" rel="noopener">official diary ↗</a>'
        if isinstance(getattr(r, "source_url", None), str) and r.source_url
        else ""
    )
    bits = []
    for field, lbl, mapper in (
        ("court", "Court", None),
        ("venue", "Venue", None),
        ("judge", "Judge", None),
        ("list_type", "List", None),
        ("status", "Status", None),
        ("category", "Type", _CAT_LABEL.get),
    ):
        val = getattr(r, field, None)
        if mapper is not None:
            val = mapper(val or "", "")
        if isinstance(val, str) and val:
            bits.append(f"<span><b>{lbl}</b> {_esc(val)}</span>")
    detail = "".join(bits) + link
    return (
        f'<details class="jd-case"><summary>{_case_party_html(r)}</summary>'
        f'<div class="jd-case-detail">{detail}</div></details>'
    )


def _render_ld_plaintiffs(day_cases: pd.DataFrame) -> None:
    """'Who's bringing these cases' — the applicant (plaintiff/prosecutor) side. The
    split + classification are PIPELINE-owned columns (plaintiff / plaintiff_kind);
    this only counts and ranks the already-classified rows for display."""
    if "plaintiff_kind" not in day_cases.columns:
        return
    st.html('<h3 class="jd-subhead">Who\'s bringing these cases</h3>')
    st.caption(
        "The applicant side of each listing. Individuals stay as initials; named "
        "companies and State bodies — the accountability signal — are shown in clear."
    )
    # logic_firewall: display_only (counts over the pipeline-classified plaintiff_kind)
    kind_counts = day_cases["plaintiff_kind"].value_counts().to_dict()
    chips = []
    for key in _PKIND_ORDER:
        n = int(kind_counts.get(key, 0))
        if not n:
            continue
        cls, _row_label, plural = _PKIND[key]
        chips.append(
            f'<div class="jd-pstat"><span class="jd-pdot {cls}"></span><b>{n}</b><span>{_esc(plural)}</span></div>'
        )
    if chips:
        st.html(f'<div class="jd-pstrip">{"".join(chips)}</div>')

    # named institutional plaintiffs (orgs + State bodies, kept in clear) — repeat
    # applicants ranked by how often they appear. logic_firewall: display_only
    named = day_cases[day_cases["plaintiff_kind"].isin(_NAMED_PKINDS)]
    top = named["plaintiff"].value_counts().head(8)
    if top.empty:
        return
    kind_by_name = named.drop_duplicates("plaintiff").set_index("plaintiff")["plaintiff_kind"].to_dict()
    mx = int(top.iloc[0]) or 1
    st.caption("Named institutions appearing most often as the applicant:")
    rows = []
    for name, n in top.items():
        n = int(n)
        pct = round(100 * n / mx)
        label = _PKIND.get(kind_by_name.get(name, ""), ("", "", "Company"))[2]
        rows.append(
            f'<div class="jd-rank"><div class="jd-rank-body">'
            f'<div class="jd-rank-title">{_esc(name)}</div>'
            f'<div class="jd-rank-sub">{_esc(label)}</div>'
            f'<div class="jd-bar-track"><div class="jd-bar-fill" style="width:{pct}%"></div></div>'
            f'</div><div class="jd-rank-n">{n}</div></div>'
        )
    st.html("".join(rows))


def _render_ld_cases(day_cases: pd.DataFrame, day_label: str) -> None:
    st.html('<h2 class="jd-section-head">What\'s before the courts</h2>')
    st.caption(
        "Anonymised list entries. People are shown by initials; organisations and "
        "the State are named. Private hearings (family, childcare, wards, minors) are "
        "not published. Each entry links to the official diary."
    )

    # category summary cards — render-time count over the already day-filtered set.
    # logic_firewall: display_only
    counts = day_cases["category"].value_counts().to_dict()
    cat_cards = []
    for key, label, desc in _CATEGORIES:
        cat_cards.append(
            f'<div class="jd-cat-card"><div class="jd-cat-n">{int(counts.get(key, 0))}</div>'
            f'<div class="jd-cat-label">{label}</div><div class="jd-cat-desc">{_esc(desc)}</div></div>'
        )
    st.html(f'<div class="jd-catwrap">{"".join(cat_cards)}</div>')

    if day_cases.empty:
        empty_state(
            "Every listed matter this day was private",
            "All matters listed for this day were private hearings and are not published. "
            "The sitting schedule above still shows which judges sat.",
        )
        return

    _render_ld_plaintiffs(day_cases)

    st.html('<h3 class="jd-subhead">Every listed matter</h3>')
    search = st.text_input(
        "Search by party or judge",
        key="jd_search",
        placeholder="A company, a State body, the DPP, or a judge's name…",
    ).strip()
    present_cats = [(k, lbl) for k, lbl, _ in _CATEGORIES if k in counts]
    options = ["All types"] + [lbl for _, lbl in present_cats]
    pick = st.segmented_control("Filter by type", options, default="All types", key="jd_cat_filter") or "All types"
    if pick != "All types":
        chosen = next(k for k, lbl in present_cats if lbl == pick)
        view = day_cases[day_cases["category"] == chosen]
    else:
        view = day_cases
    if search:
        # display-only text filter over the anonymised title (which carries plaintiff +
        # defendant) and the judge. logic_firewall: display_only
        view = view[text_search_mask(view, search, ["case_anonymised", "judge"])]

    if view.empty:
        st.caption(f'No listed matter matches "{search}" on {day_label}.' if search else f"No matters · {day_label}")
        return
    matched = f' matching "{search}"' if search else ""
    st.caption(f"{len(view)} listed matter{'s' if len(view) != 1 else ''}{matched} · {day_label}")
    # Render court-by-court in seniority order (mirrors the schedule section above), each
    # court's sittings shown busiest-first as collapsed expanders. The cap is on the NUMBER
    # of sittings per court (TIER_C_GROUPS) plus the rows WITHIN each (TIER_C_PAGE) — not a
    # flat per-court row budget. The old row budget, walked in alphabetical judge order, let
    # one large early sitting (e.g. a 147-matter Court of Appeal list) exhaust it and SILENTLY
    # drop every later sitting — including all unattributed matters — so whole courts looked
    # near-empty. Busiest-first + a per-court-and-per-sitting cap guarantees each court's
    # substantive lists surface and any remainder is summarised with an honest count. A search
    # auto-expands the matching sittings so hits are visible without clicking.
    present = [c for c in _COURT_ORDER if c in set(view["court"].dropna())]
    extra = sorted({str(c) for c in view["court"].dropna()} - set(_COURT_ORDER))
    for court in present + extra:
        cv = view[view["court"] == court]
        st.html(
            f'<div class="jd-court-head">{_esc(court)} '
            f"<span>· {len(cv)} listed matter{'s' if len(cv) != 1 else ''}</span></div>"
        )
        # Sittings, biggest first (NaN-safe groupby keeps unattributed matters as their own group).
        groups = sorted(
            cv.groupby(["judge", "list_type"], dropna=False),
            key=lambda kv: len(kv[1]),
            reverse=True,
        )
        hidden_groups = hidden_rows = 0
        for i, ((judge, list_type), g) in enumerate(groups):
            if i >= TIER_C_GROUPS:
                hidden_groups += 1
                hidden_rows += len(g)
                continue
            # NaN-safe labels (groupby keeps NaN keys, which are truthy floats — `or` won't catch them)
            jlabel = judge if isinstance(judge, str) and judge else "Court"
            llabel = list_type if isinstance(list_type, str) and list_type else "List"
            with st.expander(f"{jlabel} — {llabel}  ({len(g)})", expanded=bool(search)):
                st.caption("Tap any matter to see its court, list, status and a link to the official diary.")
                st.html("".join(_case_row_html(r) for r in g.head(TIER_C_PAGE).itertuples()))
                if len(g) > TIER_C_PAGE:
                    st.caption(f"… and {len(g) - TIER_C_PAGE} more in this list — open the official diary.")
        if hidden_groups:
            st.caption(
                f"… and {hidden_groups} more sitting{'s' if hidden_groups != 1 else ''} "
                f"({hidden_rows} matter{'s' if hidden_rows != 1 else ''}) in the {court}. "
                "Narrow with the search box or type filter, or open the official diary."
            )


def _render_plaintiff_league() -> None:
    """Repeat institutional applicants across EVERY captured diary day — the
    cross-day accountability signal the per-day breakdown can't show. The ranking
    (canonicalised name, appearance + day counts, per-court split) is PIPELINE-owned
    (v_judiciary_plaintiff_league, GROUPING SETS); this filters to one scope and
    renders. Orgs and State bodies only — individuals are never ranked."""
    league = fetch_plaintiff_league()
    if league is None or league.empty:
        return
    st.html('<h2 class="jd-section-head">Who is suing — across all captured days</h2>')
    st.caption(
        "Named companies and State bodies appearing most often as the applicant across the "
        "whole diary archive, not just the day above. Counts are list appearances (a matter "
        "listed on three days counts three times) — they describe the court lists, not the "
        "merits of any case."
    )
    courts_present = [c for c in _COURT_ORDER if c in set(league["court"].dropna())]
    pick = (
        st.pills(
            "Court",
            ["All courts", *courts_present],
            default="All courts",
            key="jd_league_court",
            label_visibility="collapsed",
        )
        or "All courts"
    )
    # scope filter only — the view owns both grains; no re-summing here.
    # logic_firewall: display_only
    scoped = league[league["is_overall"]] if pick == "All courts" else league[league["court"] == pick]
    top = scoped.sort_values("n_appearances", ascending=False).head(12)
    if top.empty:
        st.caption(f"No named institutional applicants captured for the {pick}.")
        return
    mx = int(top["n_appearances"].max()) or 1
    rows = []
    for r in top.itertuples():
        n = int(r.n_appearances)
        n_days = int(r.n_days)
        pct = round(100 * n / mx)
        kind_label = _PKIND.get(r.plaintiff_kind, ("", "", "Company"))[2]
        sub = f"{kind_label} · listed on {n_days} day{'s' if n_days != 1 else ''}"
        rows.append(
            f'<div class="jd-rank"><div class="jd-rank-body">'
            f'<div class="jd-rank-title">{_esc(r.display_name)}</div>'
            f'<div class="jd-rank-sub">{_esc(sub)}</div>'
            f'<div class="jd-bar-track"><div class="jd-bar-fill" style="width:{pct}%"></div></div>'
            f'</div><div class="jd-rank-n">{n}</div></div>'
        )
    st.html("".join(rows))


def _render_legal_diary() -> None:
    schedule = fetch_legal_diary_schedule()
    counts = fetch_legal_diary_counts()
    cases = fetch_legal_diary_cases()

    if schedule is None or schedule.empty:
        empty_state(
            "The Legal Diary isn't loaded yet",
            "Run the daily capture (pdf_infra/legal_diary_poller.py) and "
            "extractors/legal_diary_extract.py to populate the diary views.",
        )
        return

    st.html(
        '<div class="jd-context">The daily court list from the Courts Service. We publish the '
        "<strong>sitting schedule</strong> and <strong>anonymised</strong> list entries. Matters "
        "heard in private — family law, childcare, wards of court and cases involving minors — are "
        "<strong>excluded</strong>. People are shown by <strong>initials only</strong>; organisations "
        "and the State are named. Every entry links to the official diary.</div>"
    )
    glossary_strip(
        [
            ("DPP", "Director of Public Prosecutions — the State's prosecutor in criminal cases"),
            ("For mention", "a short listing to manage a case, not a full hearing"),
            ("Ex parte", "an application made by one side only"),
            ("Judicial review", "a challenge to a decision of the State or a public body"),
        ]
    )

    # Diary-day selector. The diary is a forward-accumulating daily source, so a flat strip
    # of full "Thu 4 Jun 2026" labels grows without bound. Instead: recent court days are
    # quick pills with compact labels; once history builds past a fortnight we group by month
    # (month pills → day pills) so the control stays compact and scannable. Newest day first,
    # defaulting to the latest court day.
    days = [str(d) for d in sorted(schedule["diary_date"].dropna().unique(), reverse=True)]
    latest = days[0]
    if len(days) <= 14:
        short = {d: _fmt_day_short(d) for d in days}
        picked = (
            st.pills(
                "Diary day",
                [short[d] for d in days],
                default=short[latest],
                key="jd_day",
                label_visibility="collapsed",
            )
            or short[latest]
        )
        chosen = next((d for d in days if short[d] == picked), latest)
    else:
        months = sorted({d[:7] for d in days}, reverse=True)
        mlabels = {_fmt_month(m): m for m in months}
        mpick = st.pills(
            "Month",
            list(mlabels),
            default=_fmt_month(months[0]),
            key="jd_month",
            label_visibility="collapsed",
        ) or _fmt_month(months[0])
        mon = mlabels.get(mpick, months[0])
        mdays = [d for d in days if d.startswith(mon)]
        short = {d: _fmt_day_short(d) for d in mdays}
        # Key is scoped to the month so a day picked in one month isn't carried as a stale
        # selection into another month's (different) option set.
        picked = (
            st.pills(
                "Diary day",
                [short[d] for d in mdays],
                default=short[mdays[0]],
                key=f"jd_day_{mon}",
                label_visibility="collapsed",
            )
            or short[mdays[0]]
        )
        chosen = next((d for d in mdays if short[d] == picked), mdays[0])
    st.caption(f"Showing {_fmt_day(chosen)}{' · latest court day' if chosen == latest else ''}.")

    day_sched = schedule[schedule["diary_date"] == chosen]
    day_counts = counts[counts["diary_date"] == chosen] if not counts.empty else counts
    day_cases = (
        cases[cases["diary_date"] == chosen]
        if cases is not None and not cases.empty
        else pd.DataFrame(
            columns=[
                "court",
                "judge",
                "list_type",
                "status",
                "category",
                "case_anonymised",
                "plaintiff",
                "defendant",
                "plaintiff_kind",
                "source",
                "source_url",
                "source_sha256",
            ]
        )
    )

    if day_sched.empty:
        empty_state("No sittings listed for this day", "Courts may not have sat (vacation or a non-court day).")
        return

    _render_ld_schedule(day_sched, day_cases)
    st.divider()
    _render_ld_busiest(day_counts)
    st.divider()
    _render_ld_cases(day_cases, _fmt_day(chosen))
    st.divider()
    _render_plaintiff_league()

    sha = ""
    if day_cases is not None and not day_cases.empty and "source_sha256" in day_cases.columns:
        vals = [s for s in day_cases["source_sha256"].dropna().unique()]
        sha = vals[0] if vals else ""
    st.html(
        '<div class="jd-foot"><strong>Source:</strong> Courts Service Legal Diary '
        '(<a href="https://legaldiary.courts.ie/" target="_blank" rel="noopener">'
        "legaldiary.courts.ie ↗</a>). The official diary shows the current court day only; "
        "earlier days here come from our daily capture. Names are reduced to initials and "
        "private hearings are excluded."
        + (f" Captured file digest: <code>{_esc(sha)}</code>." if sha else "")
        + "</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
def judiciary_page() -> None:
    _inject_jd_css()
    hide_sidebar()

    # Profile drill-down — a single judge's career arc, full width, with back nav.
    judge_key = st.query_params.get("judge")
    if judge_key:
        _render_profile(judge_key)
        return

    # Court drill-down — one court's clearance, area breakdown and waiting times.
    court_key = st.query_params.get("court")
    if court_key:
        _render_court_detail(court_key)
        return

    roster = fetch_roster()
    appts = fetch_appointments()
    noms = fetch_nominations()
    ladder = fetch_elevation_ladder()

    badges = []
    if roster is not None and not roster.empty:
        badges.append(f"{len(roster)} sitting judges")
        badges.append(f"{roster['court'].nunique()} courts")
    if appts is not None and not appts.empty:
        badges.append(f"{len(appts)} appointments since 2016")
    if ladder is not None and not ladder.empty:
        badges.append(f"{int(ladder['n'].sum())} elevations")
    hero_banner(
        kicker="COURTS & JUDICIARY",
        title="The bench and the courts",
        dek="Who sits on Ireland's courts, who appointed them, and how they got there. "
        "Source-linked public records; this page does not rate judges or imply misconduct.",
        badges=badges,
    )
    glossary_strip(
        [
            ("Elevation", "promotion from one court to a more senior one"),
            ("Ex-officio", "a role held automatically by virtue of another office"),
            ("Iris Oifigiúil", "the State gazette where appointments are formally notified"),
        ]
    )

    tab_bench, tab_appt, tab_courts, tab_diary = st.tabs(
        ["The Bench", "Appointments & Government", "The Courts", "Legal Diary"]
    )
    with tab_bench:
        _render_the_bench(roster)
    with tab_appt:
        _render_appointments(appts, noms)
    with tab_courts:
        _render_courts()
    with tab_diary:
        _render_legal_diary()
