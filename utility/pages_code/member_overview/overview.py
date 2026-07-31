from __future__ import annotations

import datetime
from html import escape as _h

import pandas as pd
import streamlit as st

from data_access.committees_data import fetch_committee_assignments

from ._shared import (
    _att_all_years,
    _att_chamber_sitting_days,
    _legislation,
    _lobbying_rd,
    _pay_grand_total,
    _q_profile,
    _salary,
    _votes_summary,
)

# ── Overview summary cards (default landing) ───────────────────────────────────
# Each card is a one-domain summary that links into its section. All figures
# reuse the SAME cached retrieval helpers the full sections use (so the column
# keying — member_id for votes, unique_member_code elsewhere — is already
# correct). Every builder degrades to an honest "—" card; one empty domain never
# breaks the grid.


def _ov_card(sid: str, label: str, figure: str, signal_html: str, join_key: str, *, lead: bool = False) -> str:
    """One Overview card. ``signal_html`` is pre-escaped/safe HTML; ``figure``
    and ``label`` are escaped here."""
    lead_cls = " mo-ov-lead" if lead else ""
    return (
        f'<a class="mo-overview-card{lead_cls}" href="?member={_h(join_key)}&section={sid}">'
        f'<div class="mo-ov-label">{_h(label)}</div>'
        f'<div class="mo-ov-figure">{_h(figure)}</div>'
        f'<div class="mo-ov-signal">{signal_html}</div>'
        f'<div class="mo-ov-cta">View {_h(label.lower())} →</div>'
        "</a>"
    )


def _ov_votes(conn, join_key: str) -> str:
    df = _votes_summary(conn, join_key)
    if df.empty:
        return _ov_card("votes", "Voting record", "—", _h("No votes recorded for this member."), join_key, lead=True)
    r = df.iloc[0]
    div = int(r.get("division_count", 0) or 0)
    cast = int(r.get("yes_count", 0) or 0) + int(r.get("no_count", 0) or 0) + int(r.get("abstained_count", 0) or 0)
    rate = r.get("yes_rate_pct")
    parts = [f"{cast:,} votes cast"]
    if rate is not None and not pd.isna(rate):
        parts.append(f"voted with the question {float(rate):.0f}% of the time")
    return _ov_card("votes", "Voting record", f"{div:,} divisions", _h(" · ".join(parts)), join_key, lead=True)


def _ov_interests(house: str, member_name: str, join_key: str) -> str:
    try:
        from data_access.interests_data import fetch_td_interest_year_summary

        df = fetch_td_interest_year_summary(house, member_name)
    except Exception:  # noqa: BLE001 — degrade to empty card on any retrieval miss
        df = None
    if df is None or df.empty:
        return _ov_card("interests", "Interests", "—", _h("No declarations on file."), join_key)
    row = df.sort_values("declaration_year").iloc[-1]  # latest year
    total = int(row.get("total_declarations", 0) or 0)
    flags: list[str] = []
    if bool(row.get("is_landlord")):
        flags.append("Landlord")
    elif bool(row.get("is_property_owner")):
        flags.append("Property owner")
    prop = int(row.get("property_count", 0) or 0)
    if prop:
        flags.append(f"{prop} propert{'ies' if prop != 1 else 'y'}")
    share = int(row.get("share_count", 0) or 0)
    if share:
        flags.append(f"shareholder ×{share}")
    sig = "declarations" + (" · " + ", ".join(flags) if flags else "")
    return _ov_card("interests", "Interests", f"{total}", _h(sig), join_key)


def _ov_lobbying(conn, join_key: str) -> str:
    df = _lobbying_rd(conn, join_key)
    if df.empty:
        return _ov_card(
            "lobbying", "Lobbying", "—", _h("No revolving-door flag. Open for lobbying activity."), join_key
        )
    r = df.iloc[0]
    pos = str(r.get("former_position", "")).strip()
    rc = int(r.get("return_count", 0) or 0)
    firms = int(r.get("distinct_firms", 0) or 0)
    if pos and pos.upper() != "TD":
        sig = f"Former {pos} · {rc} return{'s' if rc != 1 else ''} across {firms} firm{'s' if firms != 1 else ''}"
        return _ov_card("lobbying", "Lobbying", "Revolving door", _h(sig), join_key)
    return _ov_card("lobbying", "Lobbying", "—", _h("No revolving-door flag. Open for lobbying activity."), join_key)


def _ov_payments(conn, join_key: str, house: str) -> str:
    total = _pay_grand_total(conn, join_key)
    sal = _salary(conn, join_key, house)
    basic = 0.0
    if not sal.empty:
        basic = float(sal.iloc[0].get("total_statutory_rate_eur") or sal.iloc[0].get("basic_rate") or 0)
    if not total and not basic:
        return _ov_card("payments", "Salary & expenses", "—", _h("No payment records on file."), join_key)
    figure = f"€{total:,.0f}" if total else "—"
    parts = ["expenses (PSA/TAA) — not salary"]
    if basic:
        parts.append(f"€{basic:,.0f} salary rate")
    return _ov_card("payments", "Salary & expenses", figure, _h(" · ".join(parts)), join_key)


def _ov_attendance(conn, join_key: str, house: str, is_minister: bool) -> str:
    df = _att_all_years(conn, join_key)
    if is_minister:
        # Members holding ministerial office aren't captured in the plenary-
        # attendance PDFs (documented source gap). They surface as a sparse row
        # (e.g. "1 of 94"), which on a standalone card reads as near-total
        # absence rather than a source limitation — so show the note instead of
        # the raw figure. (The hero stat strip keeps the number but qualifies it
        # with "· Minister"; the card has no room for that qualifier.)
        return _ov_card(
            "attendance",
            "Attendance",
            "Not recorded",
            _h("Members holding ministerial office aren't recorded in the attendance PDFs."),
            join_key,
        )
    if df.empty:
        return _ov_card("attendance", "Attendance", "—", _h("No attendance records on file."), join_key)
    this_year = datetime.date.today().year
    completed = df[df["year"] < this_year]
    row = completed.iloc[0] if not completed.empty else df.iloc[0]
    yr = int(row["year"])
    sitting = int(row["sitting_days"]) if pd.notna(row.get("sitting_days")) else 0
    denom = _att_chamber_sitting_days(conn, house).get(yr)
    figure = f"{sitting} of {denom}" if denom else str(sitting)
    return _ov_card("attendance", "Attendance", figure, _h(f"sitting days · {yr}"), join_key)


def _ov_questions(conn, join_key: str) -> str:
    p = _q_profile(conn, join_key)
    total = int(p.get("total_qs", 0) or 0)
    if total == 0:
        return _ov_card("questions", "Questions", "—", _h("No parliamentary questions on file."), join_key)
    top = str(p.get("top_ministry") or "").strip()
    sig = "questions asked" + (f" · most on {top}" if top else "")
    return _ov_card("questions", "Questions", f"{total:,}", _h(sig), join_key)


def _ov_legislation(conn, join_key: str) -> str:
    df = _legislation(conn, join_key)
    n = 0 if df is None or df.empty else len(df)
    if n == 0:
        return _ov_card("legislation", "Legislation", "—", _h("No bills sponsored."), join_key)
    return _ov_card("legislation", "Legislation", f"{n}", _h(f"bill{'s' if n != 1 else ''} sponsored"), join_key)


def _ov_committees(member_name: str, join_key: str) -> str:
    try:
        df = fetch_committee_assignments("Dáil")
        # logic_firewall: display_only — counting already-shaped assignment rows.
        n = int((df["member_name"] == member_name).sum()) if df is not None and "member_name" in df.columns else 0
    except Exception:  # noqa: BLE001 — degrade to empty card on any retrieval miss
        n = 0
    if n == 0:
        return _ov_card("committees", "Committees", "—", _h("No committee memberships on file."), join_key)
    return _ov_card("committees", "Committees", f"{n}", _h(f"committee assignment{'s' if n != 1 else ''}"), join_key)


def _render_overview(conn, join_key: str, house: str, member_name: str, is_minister: bool, is_seanad: bool) -> None:
    """Default profile landing: a one-screen summary grid. Votes leads (full
    width); every card links into its full section."""
    st.caption("A one-screen summary of the public record. Open any card for the full detail.")
    cards: list[str] = [
        _ov_votes(conn, join_key),  # lead, full width
        _ov_interests(house, member_name, join_key),
        _ov_lobbying(conn, join_key),
        _ov_payments(conn, join_key, house),
        _ov_attendance(conn, join_key, house, is_minister),
    ]
    if not is_seanad:
        cards.append(_ov_questions(conn, join_key))
    cards.append(_ov_legislation(conn, join_key))
    cards.append(_ov_committees(member_name, join_key))
    st.html('<div class="mo-overview-grid">' + "".join(c for c in cards if c) + "</div>")


def _render_pay_summary_tiles(conn, join_key: str, house: str) -> None:
    """Two compact tiles — statutory salary | reimbursed expenses — replacing the
    salary card + divider + lead paragraph. States the salary≠expenses point once.
    Display-only: every figure comes straight from v_member_salary / SUM(payments).
    """
    sal = _salary(conn, join_key, house)
    total = _pay_grand_total(conn, join_key)

    if not sal.empty and float(sal.iloc[0].get("basic_rate") or 0):
        r = sal.iloc[0]
        rate = float(r.get("total_statutory_rate_eur") or r.get("basic_rate") or 0)
        cur = str(r.get("current_office") or "").strip()
        office_note = f" incl. {cur} allowance" if bool(r.get("is_office_holder")) and cur else ""
        sal_tile = (
            '<div class="mo-pay-tile">'
            '<div class="mo-pay-tile-eyebrow">Salary · statutory rate</div>'
            f'<div class="mo-pay-tile-figure">€{rate:,.0f}<span class="mo-pay-tile-per"> / yr</span></div>'
            f'<div class="mo-pay-tile-note">Set published rate{_h(office_note)} — not earned or take-home pay.</div>'
            "</div>"
        )
    else:
        sal_tile = (
            '<div class="mo-pay-tile">'
            '<div class="mo-pay-tile-eyebrow">Salary · statutory rate</div>'
            '<div class="mo-pay-tile-figure">—</div>'
            '<div class="mo-pay-tile-note">No statutory salary rate on file for this member.</div>'
            "</div>"
        )

    exp_fig = f"€{total:,.0f}" if total else "—"
    exp_tile = (
        '<div class="mo-pay-tile mo-pay-tile-expenses">'
        '<div class="mo-pay-tile-eyebrow">Expenses &amp; allowances · all years</div>'
        f'<div class="mo-pay-tile-figure">{exp_fig}</div>'
        '<div class="mo-pay-tile-note">Parliamentary Standard Allowance (PSA/TAA): money to cover the cost of '
        "doing the job (travel, accommodation, office costs). Reimbursed expenses — <strong>not</strong> "
        "salary or income.</div>"
        "</div>"
    )
    st.html(f'<div class="mo-pay-tiles">{sal_tile}{exp_tile}</div>')
