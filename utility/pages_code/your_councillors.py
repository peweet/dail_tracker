"""Your Councillors — who represents you on your local council (Your Area nav).

Reads PROMOTED gold views via the data-access layer (v_la_councillors / _council_meeting_coverage /
_councillor_votes / _meeting_agendas / _standing_orders / _chief_executives) — display-only, no joins
or modelling in this layer. Flow: County→LEA picker → LEA roster → councillor dossier.

Honest degradation is deliberate: voting records exist only where a council holds named roll-call
votes; agendas/standing-orders are shown where parsed, with explicit caveats elsewhere.
"""
from __future__ import annotations

import datetime
import re
import sys
from collections import Counter
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from data_access import your_councillors_data as ycd  # noqa: E402
from ui.components import hide_sidebar, page_error_boundary  # noqa: E402

_SEP = " | "
_PARTY_COLOUR = {
    "fianna fáil": "#66bb6a", "fine gael": "#3f51b5", "sinn féin": "#1b8a5a", "labour": "#cc0000",
    "green": "#4caf50", "social democrat": "#752f8a", "independent ireland": "#5c6bc0",
    "independent": "#888888", "aontú": "#3d2b56",
}
_PAY = [
    ("Representational Payment (salary)", "€32,059 / yr", "taxable"),
    ("Annual Expenses Allowance", "≈ €3,162 / yr", "full amount needs 80% meeting attendance"),
    ("Local Representation Allowance", "up to €5,160 / yr", "vouched — receipts required"),
]
_TIER_MSG = {
    "roll_call": "",  # handled separately (real votes shown)
    "proposer_seconder": ("This council records most decisions <b>by agreement</b> rather than by named "
                          "vote, so individual councillor votes aren't published in its minutes. Motions "
                          "are recorded with a proposer and seconder instead."),
    "scanned_pending": ("This council's minutes are <b>scanned image PDFs</b> not yet OCR-processed — so "
                        "meeting activity isn't available for this councillor yet."),
    "cmis_pending": ("This council publishes minutes through a <b>ModernGov portal</b> not yet processed — "
                     "so meeting activity isn't available for this councillor yet."),
    "unseeded": ("This council's minutes haven't been harvested yet — meeting activity isn't available "
                 "for this councillor yet."),
}
_WHO_SETS_AGENDA = (
    "The agenda is agreed in advance by the <b>Cathaoirleach / Mayor</b> (who chairs the meeting) and the "
    "<b>Corporate Policy Group</b> — the chairs of the Strategic Policy Committees, the Directors of "
    "Service and the Meetings Administrator. The <b>Chief Executive</b> submits reports and business, and "
    "any councillor can add an item by tabling a <b>Notice of Motion</b>. (Local Government Act 2001, "
    "Schedule 10.)"
)
_MAYOR = (
    "Each council elects a <b>Cathaoirleach</b> (called the <b>Mayor</b> in the cities) from among its own "
    "councillors <b>each year</b>. The role chairs council meetings and the Corporate Policy Group that "
    "agrees the agenda — but it rotates annually and carries <b>no executive power</b> of its own; it is "
    "largely civic and ceremonial. <b>Exception:</b> since 2024 <b>Limerick</b> has a <b>directly-elected "
    "Mayor</b> with executive functions — the first in the State."
)


_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], 1)}


def _parse_date(s: str) -> datetime.date | None:
    """Parse the varied meeting_date strings (DD/MM/YYYY, '14 October 2024', 'Month YYYY')."""
    s = str(s).strip()
    m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})", s)  # ISO (fan-out dates)
    if m:
        try:
            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.search(r"(\d{1,2})/(\d{1,2})/(20\d{2})", s)
    if m:
        try:
            return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    m = re.search(r"(\d{1,2})?\s*(january|february|march|april|may|june|july|august|september|"
                  r"october|november|december)\s+(20\d{2})", s, re.I)
    if m:
        return datetime.date(int(m.group(3)), _MONTHS[m.group(2).lower()], int(m.group(1) or 28))
    return None


def _pcolour(p: str) -> str:
    pl = str(p).lower()
    return next((c for k, c in _PARTY_COLOUR.items() if k in pl), "#9e9e9e")


def _stripe(parties) -> str:
    c = Counter(parties)
    tot = sum(c.values()) or 1
    segs = "".join(f'<span style="display:inline-block;height:10px;width:{100*n/tot:.1f}%;'
                   f'background:{_pcolour(p)}"></span>' for p, n in c.most_common())
    return f'<div style="border-radius:5px;overflow:hidden;width:100%">{segs}</div>'


def _card(html: str) -> None:
    st.markdown(f'<div style="background:#ffffff;border:1px solid #e6e6e6;border-radius:10px;'
                f'padding:16px 18px;margin-bottom:12px">{html}</div>', unsafe_allow_html=True)


def _tier(la: str) -> str:
    r = ycd.fetch_coverage(la)
    return r.data.iloc[0]["tier"] if r.ok and not r.is_empty else "unseeded"


@page_error_boundary
def your_councillors_page() -> None:
    hide_sidebar()
    councils_r = ycd.fetch_councils()
    if not councils_r.ok or councils_r.is_empty:
        st.title("Your Councillors")
        st.info("Councillor data is not available in this build.")
        return
    councils = sorted(councils_r.data["local_authority"])

    st.markdown("## 🏛️ Your Councillors")
    st.caption("Who represents you on your local council — what they decide, what's on the agenda, and "
               "how their council works. Sources: Wikipedia roster + council minutes & Standing Orders + "
               "DHLGH pay schedule.")

    qp = st.query_params
    county, lea, councillor = qp.get("clr_county"), qp.get("clr_lea"), qp.get("clr_name")

    # ── Screen 3: councillor dossier ──
    if councillor and county:
        cr = ycd.fetch_councillor(county, councillor)
        if not cr.ok or cr.is_empty:
            st.error("Councillor not found.")
            return
        r = cr.data.iloc[0]
        if st.button("← Back to area"):
            qp.update({"clr_county": county, "clr_lea": r["lea"]}); qp.pop("clr_name", None); st.rerun()
        _card(f'<div style="border-left:6px solid {_pcolour(r["party"])};padding-left:12px">'
              f'<div style="font-size:1.5rem;font-weight:700">{r["name"]}</div>'
              f'<div style="color:#555">{r["party"]} · {r["lea"]} · {county} · {r["status"]}</div></div>')

        tier = _tier(county)

        # Voting record (named votes only where the council records them)
        st.markdown("#### Voting record")
        vr = ycd.fetch_votes(county, councillor)
        if tier == "roll_call" and vr.ok and not vr.is_empty:
            for _, v in vr.data.head(25).iterrows():
                col = {"for": "#2e7d32", "against": "#c62828", "abstain": "#ef6c00",
                       "absent": "#9e9e9e"}.get(v["vote"], "#555")
                _card(f'<div style="font-size:.8rem;color:#777">{v["meeting_date"]}</div>'
                      f'<div style="margin:2px 0">{(v["motion"] or "Motion").strip()[:170]}</div>'
                      f'<span style="background:{col};color:#fff;border-radius:4px;padding:1px 8px;'
                      f'font-size:.8rem;text-transform:uppercase">{v["vote"]}</span>')
            st.caption(f"{len(vr.data)} recorded roll-call votes · source: council minutes")
        elif tier == "roll_call":
            _card("No recorded roll-call votes for this councillor yet this term.")
        else:
            _card(f'<div style="color:#444">{_TIER_MSG.get(tier, _TIER_MSG["unseeded"])}</div>')

        # Meeting history — the agenda (incl. any upcoming)
        st.markdown("#### Meeting history — what's on the agenda")
        ar = ycd.fetch_agendas(county)
        if ar.ok and not ar.is_empty:
            today = datetime.date.today()
            recs = ar.data.to_dict("records")
            recs.sort(key=lambda m: _parse_date(m["meeting_date"]) or datetime.date(1900, 1, 1), reverse=True)
            st.caption(f"Your council's recent and upcoming meetings ({county}). The agenda is what the "
                       "council tables for discussion — agendas appear here as soon as the council "
                       "publishes them (you can see the next meeting once it's posted).")
            for m in recs[:6]:
                d = _parse_date(m["meeting_date"])
                badge = ('<span style="background:#1565c0;color:#fff;border-radius:4px;padding:1px 7px;'
                         'font-size:.72rem;margin-left:6px">📅 UPCOMING</span>' if d and d >= today else "")
                items = "".join(f'<li style="margin:1px 0">{i}</li>'
                                for i in str(m["agenda"]).split(_SEP)[:12] if i.strip())
                src = (f' · <a href="{m["source_url"]}" target="_blank">source</a>'
                       if m.get("source_url") else "")
                _card(f'<div style="font-size:.8rem;color:#777">{m["meeting_date"]}{src}{badge}</div>'
                      f'<ul style="margin:6px 0 0 18px;padding:0">{items}</ul>')
        else:
            _card("Meeting agendas for this council haven't been processed yet.")

        # How meetings & agendas work — the council's own Standing Orders (verbatim) or generic
        st.markdown("#### How meetings & agendas work")
        sr = ycd.fetch_standing_orders(county)
        if sr.ok and not sr.is_empty:
            so = sr.data.iloc[0]
            bits = []
            oob = [x for x in str(so["order_of_business"]).split(_SEP) if x.strip()]
            if oob:
                bits.append('<div style="margin-bottom:8px"><b>Order of Business</b> (the agenda template)'
                            f'<ul style="margin:4px 0 0 18px">{"".join(f"<li>{i}</li>" for i in oob[:8])}</ul></div>')
            if str(so["notice_of_motion"]).strip():
                bits.append(f'<div style="margin-bottom:8px"><b>Notice of Motion</b> (how councillors table '
                            f'an item): <span style="color:#444">"{so["notice_of_motion"]}"</span></div>')
            if str(so["voting"]).strip():
                vn = (" These standing orders provide for a <b>recorded roll-call vote</b>."
                      if bool(so["records_named_votes"]) else "")
                bits.append(f'<div><b>Voting</b>: <span style="color:#444">"{so["voting"]}"</span>{vn}</div>')
            src = so.get("source_url", "")
            foot = (f'<div style="color:#777;font-size:.8rem;margin-top:8px">From {county}\'s adopted '
                    f'Standing Orders · <a href="{src}" target="_blank">source</a></div>') if src else ""
            _card("".join(bits) + foot)
        else:
            _card(_WHO_SETS_AGENDA + '<div style="color:#777;font-size:.8rem;margin-top:8px">'
                  "(General rules — this council's own Standing Orders not yet parsed.)</div>")

        # The Mayor / Cathaoirleach
        st.markdown("#### The Mayor / Cathaoirleach")
        _card(_MAYOR)

        # Who really holds power — the unelected Chief Executive
        st.markdown("#### Who really holds power — the Chief Executive")
        ce = ycd.fetch_chief_executive(county)
        ce_name = (ce.data.iloc[0]["chief_executive"] if ce.ok and not ce.is_empty else "")
        ce_title = (ce.data.iloc[0]["head_title"] if ce.ok and not ce.is_empty else "Chief Executive")
        who = f"<b>{ce_name}</b> ({ce_title})" if ce_name else "An appointed <b>Chief Executive</b>"
        _card(f"{who} — <b>not elected</b> — holds the real day-to-day power. By law the Chief Executive "
              "performs <b>all executive functions</b>: staff, contracts, <b>planning permissions</b>, and "
              "day-to-day spending. Your elected councillors hold only the short list of <b>reserved "
              "functions</b> — adopt the budget & development plan, set the rates and Local Property Tax "
              "factor, borrow, and appoint the Chief Executive (Local Government Act 2001, Part 14). "
              '<a href="/local-government" target="_self">See who runs your county →</a>')

        # Pay & allowances
        st.markdown("#### Pay & allowances")
        rows = "".join(f'<tr><td style="padding:3px 10px 3px 0">{n}</td>'
                       f'<td style="padding:3px 10px;font-weight:600">{v}</td>'
                       f'<td style="padding:3px 0;color:#777;font-size:.85rem">{note}</td></tr>'
                       for n, v, note in _PAY)
        _card(f'<table style="width:100%">{rows}</table><div style="color:#777;font-size:.82rem;'
              'margin-top:8px">Entitlement schedule (DHLGH), not actual earnings. Officeholders '
              '(Cathaoirleach/Mayor, committee chairs) receive additional allowances.</div>')
        st.caption("Sources: Wikipedia (roster) · council minutes & Standing Orders · "
                   "DHLGH allowances directions · LA Chief Executive roster.")
        return

    # ── Screen 2: LEA roster ──
    if county and lea:
        if st.button("← Change area"):
            for k in ("clr_county", "clr_lea", "clr_name"):
                qp.pop(k, None)
            st.rerun()
        rr = ycd.fetch_roster(county, lea)
        tier = _tier(county)
        badge = {"roll_call": "🟢 named votes published", "proposer_seconder": "🟡 decisions by agreement",
                 "scanned_pending": "⚪ minutes scanned (OCR pending)",
                 "cmis_pending": "⚪ ModernGov portal (pending)",
                 "unseeded": "⚪ minutes not yet harvested"}.get(tier, "")
        st.markdown(f"#### {lea} — {county}")
        if rr.ok and not rr.is_empty:
            st.markdown(_stripe(list(rr.data["party"])), unsafe_allow_html=True)
            st.caption(f"{len(rr.data)} councillors · {badge}")
            for _, r in rr.data.iterrows():
                c1, c2 = st.columns([5, 1])
                with c1:
                    _card(f'<div style="border-left:6px solid {_pcolour(r["party"])};padding-left:10px">'
                          f'<b>{r["name"]}</b><br><span style="color:#666;font-size:.9rem">{r["party"]}</span></div>')
                with c2:
                    if st.button("View →", key=f"clr_{r['name']}"):
                        qp.update({"clr_county": county, "clr_lea": lea, "clr_name": r["name"]}); st.rerun()
        else:
            _card("No councillors found for this area.")
        return

    # ── Screen 1: picker ──
    st.markdown("#### Who represents you on your local council?")
    c = st.selectbox("County / city", councils,
                     index=councils.index("Carlow") if "Carlow" in councils else 0)
    leas_r = ycd.fetch_leas(c)
    leas = sorted(leas_r.data["lea"]) if leas_r.ok and not leas_r.is_empty else []
    lsel = st.selectbox("Local Electoral Area", leas) if leas else None
    if lsel and st.button("Show my councillors →", type="primary"):
        qp.update({"clr_county": c, "clr_lea": lsel}); st.rerun()
    st.caption("Councillors run your county's reserved decisions — the development plan, the budget, "
               "commercial rates. Try Carlow (named voting records available).")
    # Honest coverage caveats
    st.markdown("---")
    st.caption("⚠️ Coverage caveats: the roster (~916 councillors) is ~96% complete — a few councils are "
               "undercounted. **Named votes** exist only where a council records roll-calls (currently "
               "Carlow); most councils decide by agreement. **Standing Orders** are parsed for ~8 of 31 "
               "councils (others show the general statutory rules). Some councils' minutes are scanned or "
               "on portals not yet processed (Louth's are book-format scans). Each card states its own state.")
