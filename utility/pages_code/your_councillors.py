"""Your Councillors — who represents you on your local council (Your Area nav).

Editorial accountability register (PRODUCT.md): ink-on-paper, evidence cards, honest empty states.
Reads PROMOTED gold views via the data-access layer (display-only). Built on the shared UI components
(hero_banner / info_card / card_row / evidence_heading / empty_state / totals_strip / party_stripe_html).

IA — two levels:
  • COUNCIL view (after picking County→LEA): the council itself — its councillors, how it works
    (Mayor, the unelected Chief Executive's power, Standing Orders), and its agendas.
  • COUNCILLOR view (click one): that person's own voting record + the pay schedule.
Every section states its own data state; never implies a false zero (feedback_no_inference_in_app).
"""

from __future__ import annotations

import datetime
import re
import sys
from collections import Counter
from html import escape as _h
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from data_access import your_councillors_data as ycd  # noqa: E402
from ui.components import (  # noqa: E402
    back_button,
    card_row,
    empty_state,
    evidence_heading,
    hero_banner,
    hide_sidebar,
    info_card,
    page_error_boundary,
    party_stripe_html,
    subsection_heading,
    totals_strip,
)

_SEP = " | "
_ACCENT = "#b8430f"  # editorial accent (warm ink-red), used sparingly for the lead statement
_PARTY_COLOUR = {
    "fianna fáil": "#66bb6a",
    "fine gael": "#3f51b5",
    "sinn féin": "#1b8a5a",
    "labour": "#cc0000",
    "green": "#4caf50",
    "social democrat": "#752f8a",
    "independent ireland": "#5c6bc0",
    "independent": "#8a8a8a",
    "aontú": "#3d2b56",
}
_VOTE_COLOUR = {"for": "#1d4ed8", "against": "#c2410c", "abstain": "#b45309", "absent": "#6b7280"}
_TIER_LINE = {
    "proposer_seconder": "decides most matters by agreement (a proposer and seconder), so individual "
    "councillor votes are not recorded",
    "scanned_pending": "publishes its minutes as scanned images that have not been processed yet",
    "cmis_pending": "publishes its minutes through a meetings portal not processed yet",
    "unseeded": "has no processed minutes yet",
}
_TIER_BADGE = {
    "roll_call": "Named votes published",
    "proposer_seconder": "Decides by agreement",
    "scanned_pending": "Minutes scanned",
    "cmis_pending": "Portal pending",
    "unseeded": "Not yet processed",
}
_MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ],
        1,
    )
}


def _parse_date(s: str):
    s = str(s).strip()
    for rx, order in ((r"(20\d{2})-(\d{1,2})-(\d{1,2})", (1, 2, 3)), (r"(\d{1,2})/(\d{1,2})/(20\d{2})", (3, 2, 1))):
        m = re.search(rx, s)
        if m:
            try:
                return datetime.date(int(m[order[0]]), int(m[order[1]]), int(m[order[2]]))
            except ValueError:
                return None
    m = re.search(
        r"(\d{1,2})?\s*(january|february|march|april|may|june|july|august|september|"
        r"october|november|december)\s+(20\d{2})",
        s,
        re.I,
    )
    return datetime.date(int(m[3]), _MONTHS[m[2].lower()], int(m[1] or 28)) if m else None


def _council_name(la: str) -> str:
    """Proper council name from the local_authority key (which omits County/City inconsistently)."""
    if la in ("Limerick", "Waterford"):
        return f"{la} City and County Council"
    if la.endswith(("City", "County")):
        return f"{la} Council"
    return f"{la} County Council"


def _pcolour(p: str) -> str:
    pl = str(p).lower()
    return next((c for k, c in _PARTY_COLOUR.items() if k in pl), "#9e9e9e")


def _tier(la: str) -> str:
    r = ycd.fetch_coverage(la)
    return r.data.iloc[0]["tier"] if r.ok and not r.is_empty else "unseeded"


def _name_html(name: str, party: str) -> str:
    return (
        f'<span style="font-weight:600">{_h(name)}</span>'
        f'<span style="color:var(--text-meta);margin-left:.5rem">{_h(party)}</span>'
    )


# ── COUNCIL-VIEW tabs ────────────────────────────────────────────────────────--
def _tab_councillors(county: str, lea: str) -> None:
    rr = ycd.fetch_roster(county, lea)
    if not (rr.ok and not rr.is_empty):
        empty_state("No councillors found", f"We don't have a roster for {lea} yet.")
        return
    df = rr.data
    st.html(party_stripe_html(list(Counter(df["party"]).items()), show_legend=True))
    st.caption(f"{len(df)} councillors represent {lea}. Select one for their voting record and pay.")
    for i, r in df.reset_index(drop=True).iterrows():
        if card_row(
            _name_html(r["name"], r["party"]),
            btn_key=f"clr_{i}",
            btn_help=r["name"],
            border_left_color=_pcolour(r["party"]),
        ):
            st.query_params.update({"clr_county": county, "clr_lea": lea, "clr_name": r["name"]})
            st.rerun()


# The three blocks of "How it works" are split out so the consolidated Your Council page can compose
# the council-level ones (Cathaoirleach + Standing Orders) inline without repeating the Chief-Executive
# explainer it already carries in its "Who runs it" section.
def _render_cathaoirleach() -> None:
    subsection_heading("The Cathaoirleach / Mayor")
    info_card(
        "Each council elects a <b>Cathaoirleach</b> (the <b>Mayor</b> in the cities) from among its own "
        "councillors <b>each year</b>. The role chairs meetings and the agenda-setting Corporate Policy "
        "Group, but it rotates annually and carries no executive power of its own; it is largely civic "
        "and ceremonial. <span style='color:var(--text-meta)'>Since 2024, Limerick has a "
        "directly-elected Mayor with executive functions, the first in the State.</span>"
    )


def _render_who_holds_power(county: str) -> None:
    subsection_heading("Who really holds power")
    ce = ycd.fetch_chief_executive(county)
    nm = ce.data.iloc[0]["chief_executive"] if ce.ok and not ce.is_empty else ""
    title = ce.data.iloc[0]["head_title"] if ce.ok and not ce.is_empty else "Chief Executive"
    who = f"<b>{_h(nm)}</b>, {_h(title)}" if nm else "An appointed <b>Chief Executive</b>"
    info_card(
        f"{who}, is <b>not elected</b>, yet holds the council's real day-to-day power. By law the Chief "
        "Executive performs the <b>executive functions</b>: staff, contracts, planning permissions and "
        "spending. The councillors you elect hold only the short list of <b>reserved functions</b>: "
        "adopting the budget and development plan, setting rates and the Local Property Tax factor, "
        "borrowing, and appointing the Chief Executive. "
        '<a href="/local-government" target="_self">Who runs your county →</a>',
        border_left_color=_ACCENT,
    )


def _render_standing_orders(county: str) -> None:
    subsection_heading("How meetings and agendas are set")
    sr = ycd.fetch_standing_orders(county)
    if sr.ok and not sr.is_empty:
        so = sr.data.iloc[0]
        parts = []
        oob = [x for x in str(so["order_of_business"]).split(_SEP) if x.strip()]
        if oob:
            parts.append(
                "<b>Order of Business</b> (the fixed agenda template): " + "; ".join(_h(x) for x in oob[:8]) + "."
            )
        if str(so["notice_of_motion"]).strip():
            parts.append(
                f"<b>Notice of Motion</b> (how a councillor adds an item): "
                f'<span style="color:var(--text-meta)">"{_h(so["notice_of_motion"])}"</span>'
            )
        if str(so["voting"]).strip():
            vn = " These provide for a recorded roll-call vote." if bool(so["records_named_votes"]) else ""
            parts.append(f'<b>Voting</b>: <span style="color:var(--text-meta)">"{_h(so["voting"])}"</span>{vn}')
        src = so.get("source_url", "")
        foot = (
            (
                f'<div style="margin-top:.5rem"><a href="{_h(src)}" target="_blank">'
                f"{_h(county)}'s adopted Standing Orders →</a></div>"
            )
            if src
            else ""
        )
        info_card("<br><br>".join(parts) + foot)
    else:
        info_card(
            "The agenda is agreed by the Cathaoirleach/Mayor and the Corporate Policy Group (the "
            "committee chairs, Directors of Service and Meetings Administrator); the Chief Executive "
            "submits business; any councillor can add an item by tabling a Notice of Motion. "
            "(Local Government Act 2001, Schedule 10.)"
            "<div style='color:var(--text-meta);margin-top:.5rem'>General statutory rules: this "
            "council's own Standing Orders have not been parsed yet.</div>"
        )


def _tab_how_it_works(county: str) -> None:
    _render_cathaoirleach()
    _render_who_holds_power(county)
    _render_standing_orders(county)


def _tab_agendas(county: str) -> None:
    ar = ycd.fetch_agendas(county)
    if not (ar.ok and not ar.is_empty):
        empty_state("Agendas not available", f"{county}'s meeting agendas have not been processed yet.")
        return
    today = datetime.date.today()
    recs = sorted(
        ar.data.to_dict("records"),
        key=lambda m: _parse_date(m["meeting_date"]) or datetime.date(1900, 1, 1),
        reverse=True,
    )
    st.caption(
        "What the council tabled for discussion. The next meeting's agenda appears here once the council publishes it."
    )
    for m in recs[:8]:
        d = _parse_date(m["meeting_date"])
        tag = (
            '<span style="background:#1d4ed8;color:#fff;border-radius:3px;padding:0 .4rem;'
            'font-size:.7rem;font-weight:600;margin-left:.5rem">UPCOMING</span>'
            if d and d >= today
            else ""
        )
        items = "".join(f"<li>{_h(i)}</li>" for i in str(m["agenda"]).split(_SEP)[:12] if i.strip())
        src = f' · <a href="{_h(m["source_url"])}" target="_blank">source</a>' if m.get("source_url") else ""
        info_card(
            f'<div style="font-weight:600">{_h(str(m["meeting_date"]))}{tag}</div>'
            f'<div style="color:var(--text-meta);font-size:.8rem">Agenda{src}</div>'
            f'<ul style="margin:.4rem 0 0 1.1rem;padding:0">{items}</ul>'
        )


# ── PAGE ─────────────────────────────────────────────────────────────────────-
@page_error_boundary
def your_councillors_page() -> None:
    hide_sidebar()
    councils_r = ycd.fetch_councils()
    if not councils_r.ok or councils_r.is_empty:
        st.title("Your Councillors")
        st.info("Councillor data is not available in this build.")
        return
    councils = sorted(councils_r.data["local_authority"])
    qp = st.query_params
    county, lea, councillor = qp.get("clr_county"), qp.get("clr_lea"), qp.get("clr_name")

    # ── COUNCILLOR view (person) ──
    if councillor and county:
        cr = ycd.fetch_councillor(county, councillor)
        if not cr.ok or cr.is_empty:
            st.error("Councillor not found.")
            return
        r = cr.data.iloc[0]
        if back_button("← Back to your council", key="clr_back"):
            qp.update({"clr_county": county, "clr_lea": r["lea"]})
            qp.pop("clr_name", None)
            st.rerun()
        hero_banner("COUNCILLOR", r["name"], f"{r['party']} · {r['lea']} · {county}", badges=[str(r["status"]).title()])

        evidence_heading("Voting record")
        tier = _tier(county)
        vr = ycd.fetch_votes(county, councillor)
        if tier == "roll_call" and vr.ok and not vr.is_empty:
            for _, v in vr.data.head(25).iterrows():
                col = _VOTE_COLOUR.get(v["vote"], "#555")
                info_card(
                    f'<div style="color:var(--text-meta);font-size:.8rem">{_h(str(v["meeting_date"]))}</div>'
                    f'<div style="margin:.15rem 0">{_h((v["motion"] or "Motion").strip()[:170])}</div>'
                    f'<span style="background:{col};color:#fff;border-radius:3px;padding:0 .5rem;'
                    f'font-size:.72rem;font-weight:600;text-transform:uppercase">{_h(v["vote"])}</span>',
                    border_left_color=col,
                )
            st.caption(f"{len(vr.data)} recorded roll-call votes, from the council minutes.")
        else:
            line = _TIER_LINE.get(tier, _TIER_LINE["unseeded"])
            empty_state(
                "No individual voting record",
                f'{county} {line}. See the council\'s agendas and rules under "How it works" in the council view.',
            )

        evidence_heading("Pay & allowances")
        totals_strip(
            [("€32,059", "Salary / yr"), ("≈ €3,162", "Expenses allowance"), ("up to €5,160", "Local rep. allowance")]
        )
        st.caption(
            "The national entitlement schedule (DHLGH), not actual earnings. The salary "
            "(Representational Payment) is taxable; the expenses allowance needs 80% meeting "
            "attendance; the Local Representation Allowance is vouched. Officeholders "
            "(Cathaoirleach/Mayor, committee chairs) receive additional allowances."
        )
        return

    # ── COUNCIL view ──
    if county and lea:
        if back_button("← Change area", key="area_back"):
            for k in ("clr_county", "clr_lea", "clr_name"):
                qp.pop(k, None)
            st.rerun()
        tier = _tier(county)
        rc = ycd.fetch_roster(county, lea)
        n = len(rc.data) if rc.ok else 0
        hero_banner(
            "LOCAL COUNCIL",
            _council_name(county),
            f"Your area: {lea}",
            badges=[f"{n} councillors in this area", _TIER_BADGE.get(tier, "")],
        )

        # The civic lead — the power statement, stated once, up top.
        ce = ycd.fetch_chief_executive(county)
        ce_nm = ce.data.iloc[0]["chief_executive"] if ce.ok and not ce.is_empty else ""
        ce_clause = (
            f"The unelected Chief Executive, <b>{_h(ce_nm)}</b>, runs it day to day"
            if ce_nm
            else "An unelected Chief Executive runs it day to day"
        )
        info_card(
            "Your councillors set <b>policy</b>: the reserved functions, which are the development plan, "
            f"the budget and the rates. {ce_clause}, holding the executive functions: staff, contracts "
            "and planning permissions. The Cathaoirleach (Mayor in the cities) chairs meetings and is "
            "elected by the councillors each year.",
            border_left_color=_ACCENT,
        )

        t1, t2, t3 = st.tabs(["Your councillors", "How it works", "Agendas"])
        with t1:
            _tab_councillors(county, lea)
        with t2:
            _tab_how_it_works(county)
        with t3:
            _tab_agendas(county)
        return

    # ── PICKER ──
    hero_banner(
        "YOUR AREA",
        "Your Councillors",
        "Find who represents you on your local council, and how that council actually works.",
    )
    c = st.selectbox("County / city", councils, index=councils.index("Carlow") if "Carlow" in councils else 0)
    leas_r = ycd.fetch_leas(c)
    leas = sorted(leas_r.data["lea"]) if leas_r.ok and not leas_r.is_empty else []
    lsel = st.selectbox("Local Electoral Area", leas) if leas else None
    if lsel and st.button("Show my council →", type="primary"):
        qp.update({"clr_county": c, "clr_lea": lsel})
        st.rerun()
    with st.expander("About this data and its coverage"):
        st.markdown(
            "- **Roster** (~916 councillors across 31 councils) is sourced from Wikipedia and is about "
            "96% complete; a few councils are undercounted.\n"
            "- **Named votes** exist only where a council records roll-calls (currently Carlow). Most "
            "councils decide by agreement, so individual votes are not recorded.\n"
            "- **Standing Orders** are parsed for about 8 of 31 councils; the others show the general "
            "statutory rules.\n"
            "- Some councils' minutes are scanned images or behind portals not yet processed (Louth's "
            "are book-format scans). Each section states its own data state."
        )
