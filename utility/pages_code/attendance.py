"""
Dáil Tracker — Attendance & Participation ("Showing up")

Retrieval-only Streamlit page. The censored TAA "sitting days" Hall-of-Shame was
scrapped (it ranked data that stops recording at the 120-day allowance threshold,
falsely labelling compliant members as worst attenders). This page presents three
HONEST, verifiable signals from the registered participation views:

  1. Division turnout   — votes cast / missed (unfakeable; office-holders flagged)
  2. Notable absences   — longest stretch away from recorded votes (date-diff led)
  3. The 120-day allowance — who fell below + the TAA deduction (the money)

Plus the divergence headline ("present in the building, rarely voted") and a
sourced news chip that vindicates a publicly-explained absence (illness, leave).

Scope: CURRENT TERM ONLY. All aggregation lives in
sql_views/attendance/attendance_participation_*.sql. This file only
SELECT/WHERE/ORDER/LIMIT (via data_access) and renders. DISPLAYS verifiable data +
sourced links; it never infers a reason or a verdict.
"""

from __future__ import annotations

import datetime
from html import escape as _h

import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.components import (
    clickable_card_link,
    empty_state,
    evidence_heading,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    member_jump_panel,
    member_moved_callout,
    page_error_boundary,
    stat_strip,
    year_selector,
)
from data_access.identity_resolver import resolve_member_code
from ui.entity_links import member_profile_url
from ui.export_controls import export_button
from ui.source_pdfs import ATTENDANCE, provenance_expander

from config import NOTABLE_TDS
from data_access.attendance_data import (
    fetch_absences as _fetch_absences,
    fetch_divergence as _fetch_divergence,
    fetch_participation_years as _fetch_years,
    fetch_taa_compliance as _fetch_taa,
    fetch_taa_summary as _fetch_taa_summary,
    fetch_turnout as _fetch_turnout,
    views_ready as _views_ready,
)

_LIST_SIZE = 20

_CAVEAT = (
    "This page measures **recorded votes**, not the Travel & Accommodation "
    "Allowance (TAA) sign-in. The TAA attendance count stops recording at the "
    "120-day allowance threshold and can be reconciled afterwards, so it cannot "
    "show who takes part in the chamber. A division (vote) record cannot be "
    "reconciled — you either cast the vote or you did not — so it is the honest "
    "measure of participation. Absences have many legitimate reasons (illness, "
    "family leave, ministerial or constituency duty, pairing arrangements between "
    "the government and opposition that are invisible in this data). Where a public "
    "reason has been reported we link it. Where none is shown, that means no public "
    "explanation was found — not that one does not exist. This page reports the "
    "record; it does not judge the reasons."
)

_TURNOUT_NOTE = (
    "**What is turnout?** Each {chamber} division (vote) this term, and how many a "
    "member took part in. Office-holders are shown but flagged: the Ceann Comhairle "
    "/ Cathaoirleach votes only to break ties, and ministers and party leaders vote "
    "less because of executive duties and government–opposition pairing. A low "
    "figure for them is structural, not absence."
)

_TAA_NOTE = (
    "**The 120-day allowance.** The Travel & Accommodation Allowance is paid to "
    "members — other than the Taoiseach and ministers — on the basis that they "
    "personally attend Leinster House on at least **120 days** a year. It is "
    "calculated on a 150-day basis with a **1% deduction for each day below 120**. "
    "Office-holders are not paid TAA on this basis and are excluded here. "
    "Source: [Houses of the Oireachtas — Salaries & Allowances]"
    "(https://www.oireachtas.ie/en/members/salaries-and-allowances/)."
)


# ── shared bits ───────────────────────────────────────────────────────────────


def _role_chip(row: pd.Series) -> str:
    """Context chip for an office-holder / leader (kept in-list, not hidden)."""
    note = str(row.get("role_note", "") or "")
    if bool(row.get("is_chair")):
        label = note or "Chair — votes only to break ties"
    elif bool(row.get("is_minister")):
        label = f"{note or 'Minister'} — executive duty / pairing"
    elif bool(row.get("is_leader")):
        label = note or "Party leader — pairing"
    else:
        return ""
    return f'<span class="part-role-chip" title="{_h(label)}">{_h(label)}</span>'


def _news_chip(row: pd.Series) -> str:
    """Sourced explanation link (curated seed or live news). Display-only — the
    link is the evidence; we never assert a reason without one."""
    url = str(row.get("source_url", "") or "")
    if not url:
        return ""
    label = str(row.get("reason_label", "") or row.get("source_title", "") or "Reported reason")
    return (
        f'<a class="part-news-chip" href="{_h(url)}" target="_blank" rel="noopener" '
        f'title="{_h(str(row.get("source_title", "") or label))}">📰 {_h(label)} ↗</a>'
    )


def _profile_wrap(code: str, name: str, inner: str) -> str:
    if not code:
        return inner
    return clickable_card_link(
        href=member_profile_url(code, section="attendance"),
        inner_html=inner,
        aria_label=f"View {name}'s profile",
        show_arrow=False,
    )


def _meta(row: pd.Series) -> str:
    party = str(row.get("party_name", "") or "")
    const = str(row.get("constituency", "") or "")
    return _h(" · ".join(p for p in (party, const) if p))


# NOTE: the full-roster turnout LEADERBOARD was removed 2026-06-22 — attendance
# is not a competition, and ranking every member by turnout mixes incomparable
# roles (the chair never votes; ministers and leaders are paired). Per-member
# turnout now lives on the member profile; this page is member-lookup + the few
# notable patterns worth scrutiny. A full turnout CSV is still exportable below.


# ── Notable absences ──────────────────────────────────────────────────────────


def _absence_row(row: pd.Series) -> str:
    name = str(row["member_name"])
    code = resolve_member_code(name)
    run = int(row.get("longest_run_divisions") or 0)
    start = pd.to_datetime(row.get("run_start"), errors="coerce")
    end = pd.to_datetime(row.get("run_end"), errors="coerce")
    span = ""
    if pd.notna(start) and pd.notna(end):
        span = f"{start.strftime('%d %b')} → {end.strftime('%d %b %Y')}"
    cal = int(row.get("run_calendar_days") or 0)
    has_reason = bool(str(row.get("source_url", "") or ""))
    reason_html = (
        _news_chip(row)
        if has_reason
        else '<span class="part-noexpl">No public explanation found</span>'
    )
    inner = (
        f'<div class="part-absence-row">'
        f'<div class="part-absence-id">'
        f'<p class="part-name">{_h(name)}</p>'
        f'<p class="part-meta">{_meta(row)}{_role_chip(row)}</p>'
        f"</div>"
        f'<div class="part-absence-figure">'
        f'<span class="part-absence-run">{run} votes missed in a row</span>'
        f'<span class="part-absence-span">{_h(span)} · {cal} days</span>'
        f"{reason_html}"
        f"</div>"
        f"</div>"
    )
    return _profile_wrap(code, name, inner)


def _render_absences(df: pd.DataFrame, *, chamber: str) -> None:
    evidence_heading("Notable absences — longest stretch away from votes")
    if df.empty:
        empty_state(
            "No absence stretches to report",
            "Every member has voted at regular intervals this term.",
        )
        return
    st.caption(
        "The longest unbroken run of divisions a member missed — they voted on both "
        "sides of the gap, so it is a real absence, not the chamber being in recess. "
        "A reported reason is linked; otherwise it reads *no public explanation "
        "found*, which is a statement about the public record, not a judgement."
    )
    rows = [_absence_row(r) for _, r in df.head(_LIST_SIZE).iterrows()]
    st.html("\n".join(rows))
    export_button(
        df[["member_name", "party_name", "longest_run_divisions", "run_calendar_days", "run_start", "run_end"]],
        label=f"Export absences · {len(df)} members",
        filename="dail_tracker_absences.csv",
        key="part_absence_export",
    )


# ── Section 3: TAA allowance compliance ───────────────────────────────────────


def _taa_row(row: pd.Series) -> str:
    name = str(row["member_name"])
    code = resolve_member_code(name)
    days = int(row.get("total_days") or 0)
    ded = int(row.get("deduction_pct") or 0)
    inner = (
        f'<div class="part-taa-row">'
        f'<div class="part-absence-id"><p class="part-name">{_h(name)}</p>'
        f'<p class="part-meta">{_meta(row)}</p></div>'
        f'<span class="part-taa-days">{days} of 120 days</span>'
        f'<span class="part-taa-ded">−{ded}% allowance</span>'
        f"</div>"
    )
    return _profile_wrap(code, name, inner)


def _render_taa(df: pd.DataFrame, summary: dict[str, int], *, chamber: str) -> None:
    evidence_heading("The 120-day allowance")
    cleared = summary.get("n_cleared", 0)
    below = summary.get("n_below", 0)
    stat_strip(
        [
            (str(cleared), "cleared the 120-day threshold", "var(--text-primary)"),
            (str(below), "fell below — allowance deducted", "var(--text-secondary)"),
        ]
    )
    if df.empty:
        st.caption(
            "No members below the 120-day threshold are on record for this term "
            "(office-holders are not paid TAA on the attendance basis and are excluded)."
        )
        return
    st.caption(
        "Members paid the Travel & Accommodation Allowance who attended fewer than "
        "120 days, with the resulting deduction (1% per day below 120). Most-docked "
        "first. Office-holders are excluded. A low figure can reflect leave or "
        "illness — see Notable absences above."
    )
    st.html("\n".join(_taa_row(r) for _, r in df.head(_LIST_SIZE).iterrows()))
    export_button(
        df[["member_name", "party_name", "total_days", "days_below_minimum", "deduction_pct"]],
        label=f"Export below-threshold · {len(df)} members",
        filename="dail_tracker_taa_compliance.csv",
        key="part_taa_export",
    )


# ── divergence hero ───────────────────────────────────────────────────────────


def _divergence_card(row: pd.Series) -> str:
    name = str(row["member_name"])
    code = resolve_member_code(name)
    present = int(row.get("taa_days_present") or 0)
    votes = int(row.get("votes_cast") or 0)
    total = int(row.get("total_divisions") or 0)
    inner = (
        f'<div class="part-absence-row">'
        f'<div class="part-absence-id"><p class="part-name">{_h(name)}</p>'
        f'<p class="part-meta">{_meta(row)}</p></div>'
        f'<div class="part-absence-figure">'
        f'<span class="part-absence-run">{present} days present · {votes} of {total} votes</span>'
        f'<span class="part-absence-span">signed in near the full allowance, voted in few divisions</span>'
        f"</div></div>"
    )
    return _profile_wrap(code, name, inner)


def _render_divergence(df: pd.DataFrame) -> None:
    if df.empty:
        return
    evidence_heading("Present, but rarely voting")
    st.caption(
        "**Present ≠ participation.** These members signed in at Leinster House near "
        "the full 120-day allowance yet took part in few recorded votes — the gap the "
        "old attendance count hid. Both figures are the official record; no reason is inferred."
    )
    st.html("\n".join(_divergence_card(r) for _, r in df.head(_LIST_SIZE).iterrows()))


# ── page ──────────────────────────────────────────────────────────────────────


@page_error_boundary
def attendance_page() -> None:
    inject_css()

    try:
        ready = _views_ready()
    except Exception as exc:  # noqa: BLE001
        empty_state("Attendance views not available", "Run the pipeline to register the participation views.")
        st.caption(str(exc))
        return
    if not ready:
        empty_state("No attendance data found", "The participation views returned no rows.")
        return

    # Legacy ?member= / ?att_td= deep-links redirect to /member-overview.
    qp_legacy = st.query_params.get("att_td") or st.query_params.get("member")
    if qp_legacy:
        st.query_params.pop("member", None)
        member_moved_callout(
            qp_legacy, section="attendance", section_label="Per-member participation",
            legacy_param="att_td", state_keys=("selected_td_att",),
        )

    hide_sidebar()
    hero_banner(kicker="CHAMBER PARTICIPATION", title="Showing up")
    st.caption(
        "Being in the building isn't the same as taking part. This page measures "
        "**recorded votes**, not the allowance sign-in. Attendance isn't a "
        "league table — absences have many legitimate reasons — so look up a member "
        "below, or read the few patterns worth a closer look."
    )

    house = (
        st.segmented_control(
            "Chamber", options=["Dáil", "Seanad"], default="Dáil",
            key="part_house", label_visibility="collapsed",
        )
        or "Dáil"
    )
    is_seanad = house == "Seanad"
    term, terms = ("Senator", "Senators") if is_seanad else ("TD", "TDs")
    chamber_l = house

    glossary_strip(
        [
            (term, "Seanadóir, a member of the Seanad" if is_seanad else "Teachta Dála, a member of the Dáil"),
            ("Division", "a recorded vote in the chamber"),
            ("Pairing", "a government–opposition arrangement to offset an absence — invisible in this data"),
            ("TAA", "Travel & Accommodation Allowance, paid on attending ≥120 days"),
        ]
    )

    years = _fetch_years(house)
    if not years:
        empty_state("No participation data yet", f"No {house} division records are available for the current term.")
        _render_provenance(None, house)
        return

    # Default to the most recent COMPLETE year (the in-progress year is editorially
    # thinner — absence runs are still open); the current year stays available as a pill.
    year_options = [str(y) for y in years]  # DESC
    selected_year = int(year_selector(year_options, key="part_year", skip_current=True))

    today = datetime.date.today()
    if selected_year >= today.year:
        st.caption(f"**{selected_year} is in progress** — figures are to date and absence runs may still be open.")

    # ── Primary: look up a member ──────────────────────────────────────────────
    # theyworkforyou-style member-first flow — a participant record you look up,
    # NOT a chamber-wide ranking. Routes to /member-overview#attendance.
    turnout = _fetch_turnout(selected_year, house)  # used for the member list + CSV
    evidence_heading(f"Look up a {term.lower()}'s participation")
    st.caption(
        f"How often any {term.lower()} voted this term, and any stretches they were "
        "absent — on their profile."
    )
    if not turnout.empty:
        picked = member_jump_panel(
            sorted(turnout["member_name"].tolist()),
            search_key_prefix="part", session_key="selected_td_att",
            label=f"Find a {term.lower()}",
            notable=None if is_seanad else NOTABLE_TDS, chip_key_prefix="chip_part",
        )
        if picked:
            member_moved_callout(
                picked, section="attendance", section_label="Per-member participation",
                state_keys=("selected_td_att",),
            )

    # ── Notable patterns (editorial outliers, NOT a ranking of everyone) ────────
    st.divider()
    st.markdown("#### Patterns worth a closer look")
    st.caption(
        "Not a league table — just the handful of members whose record stands out, "
        "with a sourced explanation linked wherever one has been reported."
    )

    _render_absences(_fetch_absences(selected_year, house), chamber=chamber_l)
    _render_divergence(_fetch_divergence(selected_year, house))
    if not is_seanad:  # TAA money — Seanad basis differs and isn't curated
        _render_taa(
            _fetch_taa(selected_year, house),
            _fetch_taa_summary(selected_year, house),
            chamber=chamber_l,
        )

    # Full participation table stays exportable (data access without a leaderboard).
    if not turnout.empty:
        export_button(
            turnout[["member_name", "party_name", "constituency", "voted_in", "missed",
                     "total_divisions", "turnout_pct"]],
            label=f"Export full participation table · {len(turnout)} {terms.lower()}",
            filename=f"dail_tracker_participation_{house.lower()}_{selected_year}.csv",
            key="part_turnout_export",
        )

    _render_provenance(selected_year, house)


def _render_provenance(year: int | None, house: str) -> None:
    pdf_links = [] if house == "Seanad" else list(ATTENDANCE)
    provenance_expander(
        sections=[_CAVEAT, _TURNOUT_NOTE.format(chamber=house), _TAA_NOTE],
        source_caption=(
            "Data: Oireachtas division records + TAA verification (data.oireachtas.ie)"
            + (f" · {year}" if year else "")
        ),
        pdf_links=pdf_links,
    )
