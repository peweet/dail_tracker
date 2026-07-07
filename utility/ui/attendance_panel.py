"""Per-member participation panel — embedded in /member-overview's Attendance
section.

Rewritten 2026-06-22 for the participation & absence model (the censored TAA
"sitting days" breakdown was scrapped — see doc/ATTENDANCE_PARTICIPATION_REDESIGN.md).
Shows, per member, the same three honest signals as the standalone page:
division turnout, the longest absence run (with a sourced explanation if any),
and 120-day TAA compliance. Pure rendering + retrieval — no business logic.
"""

from __future__ import annotations

from html import escape as _h

import pandas as pd
import streamlit as st
from data_access.attendance_data import (
    fetch_member_absences,
    fetch_member_participation,
    fetch_member_taa,
)
from data_access.identity_resolver import resolve_member_code
from ui.components import empty_state, evidence_heading, stat_strip


def _role_label(row: pd.Series) -> str:
    note = str(row.get("role_note", "") or "")
    if bool(row.get("is_chair")):
        return note or "Chair — votes only to break ties"
    if bool(row.get("is_minister")):
        return f"{note or 'Minister'} — executive duty / pairing"
    if bool(row.get("is_leader")):
        return note or "Party leader — pairing"
    return ""


def render_member_attendance(
    td_name: str,
    *,
    house: str = "Dáil",
    show_member_header: bool = False,  # noqa: ARG001 — kept for API compatibility
    year_pill_key: str = "att_profile_year",  # noqa: ARG001
    export_key_suffix: str = "",  # noqa: ARG001
) -> None:
    """Render the per-member participation body inside /member-overview."""
    code = resolve_member_code(td_name)
    part = fetch_member_participation(code) if code else pd.DataFrame()
    if part.empty:
        empty_state(
            "No participation record",
            "This member has no recorded divisions in the current term "
            "(office-holders such as the Ceann Comhairle do not vote).",
        )
        return

    part = part.sort_values("year", ascending=False)
    latest = part.iloc[0]
    role = _role_label(latest)

    # Headline: latest-year turnout + role context.
    turnout = float(latest.get("turnout_pct") or 0.0)
    voted = int(latest.get("voted_in") or 0)
    total = int(latest.get("total_divisions") or 0)
    yr = int(latest["year"])
    stats: list = [
        (f"{turnout:.0f}%", f"votes cast · {yr}", "var(--text-primary)", f"{voted} of {total} divisions"),
    ]

    # Biggest absence run, with the sourced explanation if any.
    absences = fetch_member_absences(code)
    abs_row = None
    if not absences.empty:
        absences = absences.sort_values("longest_run_sitting_days", ascending=False)
        abs_row = absences.iloc[0]
        run = int(abs_row.get("longest_run_sitting_days") or 0)
        if run > 0:
            stats.append((f"{run}", "longest run of sitting days absent", "var(--text-secondary)"))
    stat_strip(stats)

    if role:
        st.caption(f"**{role}.** A lower turnout here is structural, not absence.")

    st.caption(
        "Share of the chamber's recorded votes this member took part in — the "
        "honest measure of participation (the TAA sign-in stops recording at 120 "
        "days and can't show this)."
    )

    # Per-year turnout bars.
    evidence_heading("Turnout by year")
    rows_html: list[str] = []
    for _, r in part.iterrows():
        pct = float(r.get("turnout_pct") or 0.0)
        rows_html.append(
            f'<div class="att-year-row">'
            f'<span class="att-year-yr">{int(r["year"])}</span>'
            f'<div class="att-year-bar-track"><div class="att-year-bar-fill" '
            f'style="width:{max(0.0, min(100.0, pct)):.0f}%"></div></div>'
            f'<span class="att-year-days">{int(r.get("voted_in") or 0)} / {int(r.get("total_divisions") or 0)}</span>'
            f'<span class="att-year-pct">{pct:.0f}%</span>'
            f"</div>"
        )
    st.html(f'<div class="att-year-list">{"".join(rows_html)}</div>')

    # Notable absence + sourced explanation.
    if abs_row is not None and int(abs_row.get("longest_run_sitting_days") or 0) > 0:
        start = pd.to_datetime(abs_row.get("run_start"), errors="coerce")
        end = pd.to_datetime(abs_row.get("run_end"), errors="coerce")
        span = f"{start.strftime('%d %b')} → {end.strftime('%d %b %Y')}" if pd.notna(start) and pd.notna(end) else ""
        url = str(abs_row.get("source_url", "") or "")
        reason = str(abs_row.get("reason_label", "") or abs_row.get("source_title", "") or "")
        evidence_heading("Longest absence")
        if url:
            chip = f'<a class="part-news-chip" href="{_h(url)}" target="_blank" rel="noopener">📰 {_h(reason)} ↗</a>'
        else:
            chip = '<span class="part-noexpl">No public explanation found</span>'
        st.html(
            f'<div class="part-absence-figure" style="padding:0.2rem 0">'
            f'<span class="part-absence-run">{int(abs_row["longest_run_sitting_days"])} sitting days absent in a row</span>'
            f'<span class="part-absence-span">{_h(span)}</span>{chip}</div>'
        )

    # TAA compliance line (Dáil only).
    if house != "Seanad":
        taa = fetch_member_taa(code)
        if not taa.empty:
            t = taa.sort_values("year", ascending=False).iloc[0]
            days = int(t.get("total_days") or 0)
            if bool(t.get("meets_120")):
                st.caption(
                    f"✓ Met the 120-day Travel & Accommodation Allowance threshold ({days} days) in {int(t['year'])}."
                )
            else:
                ded = int(t.get("deduction_pct") or 0)
                st.caption(
                    f"Attended {days} of 120 TAA days in {int(t['year'])} — a {ded}% allowance "
                    "deduction. A low figure can reflect leave or illness (see above)."
                )
