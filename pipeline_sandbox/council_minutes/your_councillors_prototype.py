"""Your Councillors — runnable SANDBOX prototype (assessment only; NOT wired into the prod app/gold).

Implements the 3-screen flow from doc/YOUR_COUNCILLORS_UI_BRIEF.md over the sandbox data:
  councillors_roster.csv · council_coverage.csv · member_votes.jsonl
Run:  streamlit run pipeline_sandbox/council_minutes/your_councillors_prototype.py

Honest per-council degradation is the point: the Voting-record card renders real named votes only for
roll-call councils (Carlow), and a specific truthful empty state for every other tier.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
import streamlit as st

HERE = Path(__file__).resolve().parent
st.set_page_config(page_title="Your Councillors (prototype)", layout="centered")

# ── data (sandbox) ─────────────────────────────────────────────────────────────
@st.cache_data
def load():
    roster = pd.read_csv(HERE / "councillors_roster.csv")
    cov = pd.read_csv(HERE / "council_coverage.csv").set_index("local_authority")
    votes = defaultdict(list)
    vf = HERE / "member_votes.jsonl"
    if vf.exists():
        for line in vf.read_text(encoding="utf-8").splitlines():
            if line.strip():
                v = json.loads(line)
                votes[(v["local_authority"], v["member"])].append(v)
    return roster, cov, votes

ROSTER, COV, VOTES = load()

PARTY_COLOUR = {
    "Fianna Fáil": "#66bb6a", "Fine Gael": "#3f51b5", "Sinn Féin": "#1b8a5a",
    "Labour": "#cc0000", "Green Party": "#4caf50", "Social Democrats": "#752f8a",
    "Independent": "#888888", "Independent Ireland": "#5c6bc0", "Aontú": "#3d2b56",
}
PAY = [
    ("Representational Payment (salary)", "€32,059 / yr", "taxable"),
    ("Annual Expenses Allowance", "≈ €3,162 / yr", "full amount needs 80% meeting attendance"),
    ("Local Representation Allowance", "up to €5,160 / yr", "vouched — receipts required"),
]
TIER_VOTE_MSG = {
    "proposer_seconder": ("This council records most decisions **by agreement** rather than by named "
                          "vote, so individual councillor votes aren't published in its minutes. "
                          "Motions are recorded with a proposer and seconder instead."),
    "scanned_pending": ("This council's minutes are **scanned image PDFs** that haven't been OCR-processed "
                        "yet — so meeting activity isn't available for this councillor."),
    "cmis_pending": ("This council publishes minutes through a **ModernGov meetings portal** that hasn't "
                     "been processed yet — so meeting activity isn't available for this councillor."),
    "unseeded": ("This council's minutes haven't been harvested yet — meeting activity isn't available "
                 "for this councillor."),
}


def party_colour(p: str) -> str:
    for k, c in PARTY_COLOUR.items():
        if k.lower() in str(p).lower():
            return c
    return "#9e9e9e"


def stripe(parties) -> str:
    from collections import Counter
    c = Counter(parties)
    tot = sum(c.values()) or 1
    segs = "".join(f'<span style="display:inline-block;height:10px;width:{100*n/tot:.1f}%;'
                   f'background:{party_colour(p)}"></span>' for p, n in c.most_common())
    return f'<div style="border-radius:5px;overflow:hidden;width:100%">{segs}</div>'


def meeting_date(fn: str) -> str:
    m = re.search(r"(\d{1,2})[-\s]?(\d{1,2})[-\s]?(\d{2,4})", fn)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{4})", fn, re.I)
    return f"{m.group(1)} {m.group(2)}" if m else fn[:24]


def card(html: str):
    st.markdown(f'<div style="background:#ffffff;border:1px solid #e6e6e6;border-radius:10px;'
                f'padding:16px 18px;margin-bottom:12px">{html}</div>', unsafe_allow_html=True)


# ── routing via query params ───────────────────────────────────────────────────
qp = st.query_params
county = qp.get("county")
lea = qp.get("lea")
councillor = qp.get("councillor")

st.markdown("### 🏛️ Your Councillors  \n*sandbox prototype — not the live app*")
n_council = ROSTER.local_authority.nunique()
st.caption(f"Roster: {len(ROSTER)} councillors across {n_council} councils (Wikipedia; ~80% complete, "
           f"some councils undercounted — parser refinement pending).")

# ── Screen 3: councillor dossier ────────────────────────────────────────────────
if councillor and county:
    row = ROSTER[(ROSTER.local_authority == county) & (ROSTER.name == councillor)]
    if row.empty:
        st.error("Councillor not found.")
        st.stop()
    r = row.iloc[0]
    if st.button("← Back to area"):
        st.query_params.update({"county": county, "lea": r["lea"]}); st.query_params.pop("councillor", None); st.rerun()
    card(f'<div style="border-left:6px solid {party_colour(r["party"])};padding-left:12px">'
         f'<div style="font-size:1.5rem;font-weight:700">{r["name"]}</div>'
         f'<div style="color:#555">{r["party"]} · {r["lea"]} · {county} County/City Council · sitting</div></div>')

    tier = COV.loc[county, "tier"] if county in COV.index else "unseeded"
    st.markdown("#### Voting record")
    vlist = VOTES.get((county, councillor), [])
    if tier == "roll_call" and vlist:
        for v in vlist[:25]:
            col = {"for": "#2e7d32", "against": "#c62828", "abstain": "#ef6c00", "absent": "#9e9e9e"}.get(v["vote"], "#555")
            card(f'<div style="font-size:.8rem;color:#777">{meeting_date(v["meeting"])}</div>'
                 f'<div style="margin:2px 0">{(v.get("motion") or "Motion").strip()[:170]}</div>'
                 f'<span style="background:{col};color:#fff;border-radius:4px;padding:1px 8px;'
                 f'font-size:.8rem;text-transform:uppercase">{v["vote"]}</span>')
        st.caption(f"{len(vlist)} recorded roll-call votes · source: council minutes (Wikipedia-listed roster)")
    elif tier == "roll_call":
        card("No recorded roll-call votes for this councillor yet this term.")
    else:
        card(f'<div style="color:#444">{TIER_VOTE_MSG.get(tier, TIER_VOTE_MSG["unseeded"])}</div>')

    st.markdown("#### Pay & allowances")
    rows = "".join(f'<tr><td style="padding:3px 10px 3px 0">{n}</td>'
                   f'<td style="padding:3px 10px;font-weight:600">{v}</td>'
                   f'<td style="padding:3px 0;color:#777;font-size:.85rem">{note}</td></tr>'
                   for n, v, note in PAY)
    card(f'<table style="width:100%">{rows}</table>'
         f'<div style="color:#777;font-size:.82rem;margin-top:8px">Entitlement schedule (DHLGH), '
         f'not actual earnings. Actual expenses are published as open data by only ~5 councils.</div>')

    st.markdown("#### What your councillors decide")
    card('Councillors hold <b>reserved functions</b>: adopt the County/City Development Plan, set the '
         'annual budget, commercial rates and the Local Property Tax adjustment factor, and appoint the '
         'Chief Executive. Everything else is an <b>executive function</b> of the appointed Chief '
         'Executive. <span style="color:#777">(In the live app this links to "Who runs your county".)</span>')
    st.caption("Sources: Wikipedia (roster) · council meeting minutes · DHLGH allowances directions.")
    st.stop()

# ── Screen 2: LEA roster ────────────────────────────────────────────────────────
if county and lea:
    sub = ROSTER[(ROSTER.local_authority == county) & (ROSTER.lea == lea)]
    if st.button("← Change area"):
        st.query_params.clear(); st.rerun()
    tier = COV.loc[county, "tier"] if county in COV.index else "unseeded"
    badge = {"roll_call": "🟢 named votes published", "proposer_seconder": "🟡 votes by agreement",
             "scanned_pending": "⚪ minutes scanned (OCR pending)", "cmis_pending": "⚪ ModernGov portal (pending)",
             "unseeded": "⚪ minutes not yet harvested"}.get(tier, "")
    st.markdown(f"#### {lea} — {county}")
    st.markdown(stripe(sub.party.tolist()), unsafe_allow_html=True)
    st.caption(f"{len(sub)} councillors · {badge}")
    for _, r in sub.iterrows():
        c1, c2 = st.columns([5, 1])
        with c1:
            card(f'<div style="border-left:6px solid {party_colour(r["party"])};padding-left:10px">'
                 f'<b>{r["name"]}</b><br><span style="color:#666;font-size:.9rem">{r["party"]}</span></div>')
        with c2:
            if st.button("View →", key=r["name"]):
                st.query_params.update({"county": county, "lea": lea, "councillor": r["name"]}); st.rerun()
    st.stop()

# ── Screen 1: picker ────────────────────────────────────────────────────────────
st.markdown("#### Who represents you on your local council?")
counties = sorted(ROSTER.local_authority.unique())
c = st.selectbox("County / city", counties, index=counties.index("Carlow") if "Carlow" in counties else 0)
leas = sorted(ROSTER[ROSTER.local_authority == c].lea.dropna().unique())
l = st.selectbox("Local Electoral Area", leas)
if st.button("Show my councillors →", type="primary"):
    st.query_params.update({"county": c, "lea": l}); st.rerun()
st.caption("Councillors run your county's reserved decisions — the development plan, the budget, "
           "commercial rates. Tip: try Carlow (named voting records available).")
