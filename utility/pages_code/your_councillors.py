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
# OPR/Ministerial-Direction outcomes → label + chip colour. Deuteranopia-safe (no red/green pair);
# the override is the warm accent, the council being UPHELD is the cool blue used for "for" votes.
_PLAN_OUTCOME = {
    "direction_issued": ("Plan overruled by the Minister", "#c2410c"),
    "minister_declined": ("Minister declined to overrule the council", "#1d4ed8"),
    "in_progress": ("Under review — not concluded", "#6b7280"),
    "suspension_notice": ("Part of the plan suspended (2024 Act)", "#6b7280"),
}


def _render_plan_overrides(county: str) -> None:
    """The reserved-function override: the members adopt the development plan (and the zonings in
    it), and the Planning Regulator can take that vote to the Minister, who may issue a Direction
    that is deemed written into the plan — with no appeal.

    HONESTY RAILS (see doc/LOCAL_DEMOCRACY_OVERRIDE_RESEARCH.md):
      * NOT an 'overrides' counter — the register also records the Minister DECLINING to override
        the members (Sligo, Kilkenny). Both outcomes are rendered, from the same view column.
      * The power is RESTRICTIVE only — a Direction can strike a zoning, never create one.
      * We do NOT characterise WHY any individual plan was overruled — the published Direction
        states the Minister's reasons, and we link to it. No inference in the copy.
      * This is NOT the appeals-board overturn rate (that overrules the Chief Executive's
        planners, not the councillors) — different actors, never combined.
    """
    res = ycd.fetch_plan_directions(county)
    if not res.ok:
        return
    evidence_heading("When the plan your councillors adopted was overruled")
    if res.is_empty:
        st.caption(
            "No Ministerial Directions on record for this council's plans. Since 2019 the Office "
            "of the Planning Regulator has reviewed every development plan — where it objects to "
            "what the elected members adopted, it can ask the Minister to overrule them. That has "
            "not happened here."
        )
        return

    st.html(
        '<p class="con-section-note">Adopting the development plan — and zoning land in it — is a '
        "<strong>reserved function</strong>: one of the few decisions your elected councillors "
        "actually take. But since 2019 the <strong>Office of the Planning Regulator</strong>, which "
        "is not elected, reviews what they adopt. Where the members go against its recommendations, "
        "it must refer the plan to the <strong>Minister</strong>, who can issue a <strong>Direction "
        "that is deemed written into the plan</strong> — the councillors' wording is deemed never to "
        "have been included. <strong>There is no appeal.</strong> The Minister can only <em>remove</em> "
        "what the members put in; a Direction can strike a zoning, never create one. The Minister "
        "does not always agree with the Regulator — where that happened, it is shown below.</p>"
    )
    for r in res.data.itertuples():
        label, colour = _PLAN_OUTCOME.get(str(r.plan_outcome), ("Referred to the Minister", "#6b7280"))
        span = (
            f"{_h(str(r.first_doc_date))} → {_h(str(r.last_doc_date))}"
            if str(r.first_doc_date) != str(r.last_doc_date)
            else _h(str(r.last_doc_date))
        )
        src = (
            f'<a href="{_h(str(r.outcome_doc_url))}" target="_blank" rel="noopener" '
            f'style="font-size:.78rem">Read the Minister\'s decision ↗</a>'
            if str(getattr(r, "outcome_doc_url", "") or "").startswith("http")
            else ""
        )
        info_card(
            f'<div style="font-weight:700;margin-bottom:.2rem">{_h(str(r.plan_name))}</div>'
            f'<span style="background:{colour};color:#fff;border-radius:3px;padding:.05rem .5rem;'
            f'font-size:.72rem;font-weight:600">{_h(label)}</span>'
            f'<div style="color:var(--text-meta);font-size:.78rem;margin-top:.35rem">{span} · '
            f"{int(r.n_documents)} published documents</div>"
            f'<div style="margin-top:.3rem">{src}</div>',
            border_left_color=colour,
        )
    st.caption(
        "Source: the Planning Regulator's own register of recommendations to the Minister — the "
        "de-facto national record, as these Directions are not published centrally. Each linked "
        "document sets out the Minister's reasons in full."
    )


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
        # ACTUAL s.142 register payments — only where the council publishes the register as
        # open data (South Dublin, Dublin City); the view pre-aggregates, this renders rows.
        pr = ycd.fetch_councillor_payments(county, councillor)
        if pr.ok and not pr.is_empty:
            latest_yr = int(pr.data["year"].max())
            latest = pr.data[pr.data["year"] == latest_yr]
            total_row = latest[latest["category"] == "total_payment"]
            parts = latest[latest["category"] != "total_payment"]
            lines = "".join(
                f'<div style="display:flex;justify-content:space-between;margin:.1rem 0">'
                f'<span>{_h(str(r["category"]).replace("_", " ").capitalize())}</span>'
                f'<span style="font-variant-numeric:tabular-nums">€{r["amount_eur"]:,.2f}</span></div>'
                for _, r in parts.iterrows()
            )
            head = (
                f'<div style="font-weight:700;margin-bottom:.25rem">What this councillor was actually '
                f"paid — {latest_yr}"
                + (f' · €{float(total_row.iloc[0]["amount_eur"]):,.2f} total' if not total_row.empty else "")
                + "</div>"
            )
            info_card(head + lines)
            st.caption(
                f"From the council's own statutory s.142 register of payments to members, "
                f"published as open data ({latest_yr}; categories as the council reports them). "
                "Only a handful of councils publish this register machine-readably — most "
                "publish PDFs or nothing, so this section appears only where the data exists."
            )
        return

    # ── COUNCIL view ──
    if county and lea:  # noqa: PLR1702
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

        _render_plan_overrides(county)

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
    rc = ycd.fetch_roll_call_councils()
    rc_names = ", ".join(rc.data["local_authority"]) if rc.ok and not rc.is_empty else "Carlow"
    with st.expander("About this data and its coverage"):
        st.markdown(
            "- **Roster** (~916 councillors across 31 councils) is sourced from Wikipedia and is about "
            "96% complete; a few councils are undercounted.\n"
            f"- **Named votes** exist only where a council records roll-calls (currently {rc_names}). Most "
            "councils decide by agreement, so individual votes are not recorded.\n"
            "- **Standing Orders** are parsed for about 8 of 31 councils; the others show the general "
            "statutory rules.\n"
            "- Some councils' minutes are scanned images or behind portals not yet processed (Louth's "
            "are book-format scans). Each section states its own data state."
        )
