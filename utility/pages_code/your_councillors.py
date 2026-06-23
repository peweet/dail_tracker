"""Your Councillors — PREVIEW page wired into the app nav (Your Area).

⚠️ PREVIEW / ASSESSMENT: reads SANDBOX data (pipeline_sandbox/council_minutes/), NOT gold/views.
Promotion to real pipeline views (v_la_councillors etc.) is pending — see
doc/YOUR_COUNCILLORS_UI_BRIEF.md §10. Display-only; honest per-council degradation of the voting card.

Flow (query-param routed): County→LEA picker → LEA roster → councillor dossier.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ui.components import hide_sidebar, page_error_boundary  # noqa: E402

_SBX = Path(__file__).resolve().parents[2] / "pipeline_sandbox" / "council_minutes"

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
    "proposer_seconder": ("This council records most decisions <b>by agreement</b> rather than by "
                          "named vote, so individual councillor votes aren't published in its minutes. "
                          "Motions are recorded with a proposer and seconder instead."),
    "scanned_pending": ("This council's minutes are <b>scanned image PDFs</b> not yet OCR-processed — "
                        "so meeting activity isn't available for this councillor yet."),
    "cmis_pending": ("This council publishes minutes through a <b>ModernGov meetings portal</b> not yet "
                     "processed — so meeting activity isn't available for this councillor yet."),
    "unseeded": ("This council's minutes haven't been harvested yet — meeting activity isn't available "
                 "for this councillor yet."),
}
_TIER_BADGE = {
    "roll_call": "🟢 named votes published", "proposer_seconder": "🟡 decisions by agreement",
    "scanned_pending": "⚪ minutes scanned (OCR pending)", "cmis_pending": "⚪ ModernGov portal (pending)",
    "unseeded": "⚪ minutes not yet harvested",
}


_WHO_SETS_AGENDA = (
    "The agenda is agreed in advance by the <b>Cathaoirleach / Mayor</b> (who chairs the meeting) and "
    "the <b>Corporate Policy Group</b> — the chairs of the Strategic Policy Committees, the Directors of "
    "Service and the Meetings Administrator. The <b>Chief Executive</b> submits reports and business, and "
    "any councillor can add an item by tabling a <b>Notice of Motion</b>. (Local Government Act 2001, "
    "Schedule 10.)"
)


@st.cache_data(show_spinner=False)
def _load():
    roster = pd.read_csv(_SBX / "councillors_roster.csv")
    cov = pd.read_csv(_SBX / "council_coverage.csv").set_index("local_authority")
    votes: dict[tuple, list] = defaultdict(list)
    vf = _SBX / "member_votes.jsonl"
    if vf.exists():
        for line in vf.read_text(encoding="utf-8").splitlines():
            if line.strip():
                v = json.loads(line)
                votes[(v["local_authority"], v["member"])].append(v)
    return roster, cov, dict(votes)


@st.cache_data(show_spinner=False)
def _load_history() -> dict:
    """council -> list of {date, agenda_items} (most recent first)."""
    hist: dict[str, list] = defaultdict(list)
    hf = _SBX / "meeting_history.jsonl"
    if hf.exists():
        for line in hf.read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                if r.get("agenda_items"):
                    hist[r["council"]].append(r)
    return dict(hist)


def _pcolour(p: str) -> str:
    pl = str(p).lower()
    for k, c in _PARTY_COLOUR.items():
        if k in pl:
            return c
    return "#9e9e9e"


def _stripe(parties) -> str:
    c = Counter(parties)
    tot = sum(c.values()) or 1
    segs = "".join(f'<span style="display:inline-block;height:10px;width:{100*n/tot:.1f}%;'
                   f'background:{_pcolour(p)}"></span>' for p, n in c.most_common())
    return f'<div style="border-radius:5px;overflow:hidden;width:100%">{segs}</div>'


def _card(html: str) -> None:
    st.markdown(f'<div style="background:#ffffff;border:1px solid #e6e6e6;border-radius:10px;'
                f'padding:16px 18px;margin-bottom:12px">{html}</div>', unsafe_allow_html=True)


def _mdate(fn: str) -> str:
    from urllib.parse import unquote
    fn = unquote(fn).rsplit("/", 1)[-1]
    m = re.search(r"(\d{1,2})[-\s](\d{1,2})[-\s](\d{2,4})", fn)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{4})", fn, re.I)
    return f"{m.group(1)} {m.group(2)}" if m else fn[:24]


@page_error_boundary
def your_councillors_page() -> None:
    hide_sidebar()
    if not (_SBX / "councillors_roster.csv").exists():
        st.title("Your Councillors")
        st.info("Preview data not available in this build (sandbox roster not present).")
        return
    roster, cov, votes = _load()
    history = _load_history()

    st.markdown("## 🏛️ Your Councillors")
    st.caption("Preview — sandbox data, not yet promoted to the live pipeline. "
               f"Roster: {len(roster)} councillors across {roster.local_authority.nunique()} councils "
               "(Wikipedia; ~80% complete, some councils undercounted).")

    qp = st.query_params
    county, lea, councillor = qp.get("clr_county"), qp.get("clr_lea"), qp.get("clr_name")

    # ── dossier ──
    if councillor and county:
        row = roster[(roster.local_authority == county) & (roster.name == councillor)]
        if row.empty:
            st.error("Councillor not found.")
            return
        r = row.iloc[0]
        if st.button("← Back to area"):
            qp.update({"clr_county": county, "clr_lea": r["lea"]})
            qp.pop("clr_name", None)
            st.rerun()
        _card(f'<div style="border-left:6px solid {_pcolour(r["party"])};padding-left:12px">'
              f'<div style="font-size:1.5rem;font-weight:700">{r["name"]}</div>'
              f'<div style="color:#555">{r["party"]} · {r["lea"]} · {county} · sitting</div></div>')

        tier = cov.loc[county, "tier"] if county in cov.index else "unseeded"
        st.markdown("#### Voting record")
        vlist = votes.get((county, councillor), [])
        if tier == "roll_call" and vlist:
            for v in vlist[:25]:
                col = {"for": "#2e7d32", "against": "#c62828", "abstain": "#ef6c00",
                       "absent": "#9e9e9e"}.get(v["vote"], "#555")
                _card(f'<div style="font-size:.8rem;color:#777">{_mdate(v["meeting"])}</div>'
                      f'<div style="margin:2px 0">{(v.get("motion") or "Motion").strip()[:170]}</div>'
                      f'<span style="background:{col};color:#fff;border-radius:4px;padding:1px 8px;'
                      f'font-size:.8rem;text-transform:uppercase">{v["vote"]}</span>')
            st.caption(f"{len(vlist)} recorded roll-call votes · source: council minutes")
        elif tier == "roll_call":
            _card("No recorded roll-call votes for this councillor yet this term.")
        else:
            _card(f'<div style="color:#444">{_TIER_MSG.get(tier, _TIER_MSG["unseeded"])}</div>')

        st.markdown("#### Meeting history — what's on the agenda")
        mtgs = sorted(history.get(county, []), key=lambda m: m.get("date", ""), reverse=True)[:6]
        if mtgs:
            st.caption(f"Your council's recent meetings ({county}). The agenda is what the council "
                       "tabled for discussion — councillor-level votes aren't published by this council.")
            for m in mtgs:
                items = "".join(f'<li style="margin:1px 0">{i}</li>' for i in m["agenda_items"][:12])
                _card(f'<div style="font-size:.8rem;color:#777">{m.get("date","")}</div>'
                      f'<ul style="margin:6px 0 0 18px;padding:0">{items}</ul>')
        else:
            _card("Meeting agendas for this council haven't been processed yet.")
        st.markdown("#### Who sets the agenda")
        _card(_WHO_SETS_AGENDA)

        st.markdown("#### Pay & allowances")
        rows = "".join(f'<tr><td style="padding:3px 10px 3px 0">{n}</td>'
                       f'<td style="padding:3px 10px;font-weight:600">{v}</td>'
                       f'<td style="padding:3px 0;color:#777;font-size:.85rem">{note}</td></tr>'
                       for n, v, note in _PAY)
        _card(f'<table style="width:100%">{rows}</table><div style="color:#777;font-size:.82rem;'
              f'margin-top:8px">Entitlement schedule (DHLGH), not actual earnings.</div>')

        st.markdown("#### What your councillors decide")
        _card('Councillors hold <b>reserved functions</b>: adopt the County/City Development Plan, set '
              'the budget, commercial rates and the Local Property Tax adjustment factor, and appoint '
              'the Chief Executive. Everything else is an <b>executive function</b> of the Chief '
              'Executive — see <a href="/local-government" target="_self">Who Runs Your County</a>.')
        st.caption("Sources: Wikipedia (roster) · council meeting minutes · DHLGH allowances directions.")
        return

    # ── LEA roster ──
    if county and lea:
        sub = roster[(roster.local_authority == county) & (roster.lea == lea)]
        if st.button("← Change area"):
            for k in ("clr_county", "clr_lea", "clr_name"):
                qp.pop(k, None)
            st.rerun()
        tier = cov.loc[county, "tier"] if county in cov.index else "unseeded"
        st.markdown(f"#### {lea} — {county}")
        st.markdown(_stripe(sub.party.tolist()), unsafe_allow_html=True)
        st.caption(f"{len(sub)} councillors · {_TIER_BADGE.get(tier, '')}")
        for _, r in sub.iterrows():
            c1, c2 = st.columns([5, 1])
            with c1:
                _card(f'<div style="border-left:6px solid {_pcolour(r["party"])};padding-left:10px">'
                      f'<b>{r["name"]}</b><br><span style="color:#666;font-size:.9rem">{r["party"]}</span></div>')
            with c2:
                if st.button("View →", key=f"clr_{r['name']}"):
                    qp.update({"clr_county": county, "clr_lea": lea, "clr_name": r["name"]})
                    st.rerun()
        return

    # ── picker ──
    st.markdown("#### Who represents you on your local council?")
    counties = sorted(roster.local_authority.unique())
    c = st.selectbox("County / city", counties,
                     index=counties.index("Carlow") if "Carlow" in counties else 0)
    leas = sorted(roster[roster.local_authority == c].lea.dropna().unique())
    lsel = st.selectbox("Local Electoral Area", leas)
    if st.button("Show my councillors →", type="primary"):
        qp.update({"clr_county": c, "clr_lea": lsel})
        st.rerun()
    st.caption("Councillors run your county's reserved decisions — the development plan, the budget, "
               "commercial rates. Tip: try Carlow (named voting records available).")
