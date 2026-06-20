"""Who Ministers Meet — ministerial diaries × the lobbying register.

Civic frame (NO inference — co-occurrence only, per the data boundary): ministers
publish their diaries; lobbyists separately file returns. Where BOTH name the same
organisation and minister, that is corroboration — two independent public records
agreeing. The page presents that, source-linked; it never claims influence.

DATA BOUNDARY (logic firewall): every join / flag / ranking (corroboration, state-body
split, sector, org overlap) lives in the SQL views (sql_views/diary/ministerial_diary_*.sql,
fed by the vetted sandbox->gold promotion extractors/diary_promote_gold.py). This page reads
them via data_access.ministerial_diary_data and does presentation faceting only (search,
sector filter, the outside-interest/state-body toggle, per-org drill-down).

HONEST COVERAGE (shown, not hidden): diaries are self-curated + non-exhaustive, published
QUARTERLY-IN-ARREARS (so this is "recent business", not "today"); meeting counts are
coverage-driven (more departments ingested over time) not a trend; the register cross-ref
only surfaces the POSITIVE `corroborated` flag — a missing match is "unknown", not "never
lobbied" (the org-name join is imperfect). Two departments (DPER, Taoiseach) are scan-only
and await OCR, so their meetings are absent.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.ministerial_diary_data import fetch_engagements, fetch_org_overlap
from shared_css import inject_css
from ui.components import empty_state, glossary_strip, hero_banner, hide_sidebar

_GLOSSARY = [
    ("Corroborated", "The org both MET the minister (diary) and filed a lobbying return naming them (register) — two independent records agree."),
    ("Co-occurrence", "A diary meeting is not a lobbying return. We show that both records exist, never that a meeting caused an outcome."),
    ("Outside interest", "A non-government organisation. State bodies (IDA, HSE…) meet ministers as government business and are shown separately."),
]


def _h(s: object) -> str:
    return html.escape(str(s if s is not None else ""))


def _org_card(row: pd.Series) -> str:
    corro = (
        '<span class="dt-pill dt-pill-accent">✓ corroborated</span>'
        if bool(row.get("corroborated"))
        else ""
    )
    sector = _h(str(row.get("sector", "")).replace("-", " "))
    org = _h(row["organisation"])
    return (
        f'<a class="dt-card dt-card-row" href="?org={_h(row["organisation"])}" target="_self" '
        f'aria-label="Meetings with {org}">'
        f'<div class="dt-card-main"><div class="dt-card-title">{org}</div>'
        f'<div class="dt-card-sub">{sector}</div></div>'
        f'<div class="dt-card-metrics">'
        f'<span class="dt-metric"><b>{int(row["meetings"])}</b> meetings</span>'
        f'<span class="dt-metric"><b>{int(row["ministers_met"])}</b> ministers</span>'
        f"{corro}</div><span class='dt-card-arrow' aria-hidden='true'>→</span></a>"
    )


def _drill_down(org: str, overlap: pd.DataFrame, eng: pd.DataFrame) -> None:
    st.html(f'<a class="dt-back" href="?" target="_self">← all organisations</a>')
    head = overlap[overlap["organisation"] == org]
    if head.empty:
        empty_state("Not found", f"No record for {org}.")
        return
    r = head.iloc[0]
    st.html(
        f'<div class="dt-hero-mini"><h2>{_h(org)}</h2>'
        f'<p>{int(r["meetings"])} meetings · {int(r["ministers_met"])} ministers · '
        f'{int(r["ministers_lobbied_and_met"])} also lobbied · '
        f'{_h(r["first_meeting"])} → {_h(r["last_meeting"])}</p></div>'
    )
    rows = eng[eng["organisation"] == org].sort_values("entry_date", ascending=False)
    if rows.empty:
        empty_state("No engagements", "No dated engagements held for this organisation.")
        return
    cards = []
    for _, e in rows.head(60).iterrows():
        src = e.get("source_pdf_url")
        link = f'<a class="dt-src" href="{_h(src)}" target="_blank">source ↗</a>' if pd.notna(src) and src else ""
        cards.append(
            f'<div class="dt-card dt-card-row"><div class="dt-card-main">'
            f'<div class="dt-card-title">{_h(e["subject"])}</div>'
            f'<div class="dt-card-sub">{_h(e["minister"])} · {_h(e["department"])} · {_h(e["entry_date"])}</div>'
            f'</div>{link}</div>'
        )
    st.html("\n".join(cards))


def ministerial_diaries_page() -> None:
    hide_sidebar()
    inject_css()
    hero_banner(
        kicker="Lobbying & access",
        title="Who Ministers Meet",
        dek="Ministers' published diaries, cross-referenced with the lobbying register — "
        "the organisations they meet, and which of those also lobbied them.",
        badges=["Quarterly-in-arrears", "Co-occurrence, not influence"],
    )
    glossary_strip(_GLOSSARY)

    overlap = fetch_org_overlap()
    eng = fetch_engagements()
    if overlap is None or overlap.empty:
        empty_state("Data unavailable", "The ministerial-diary views did not load.")
        return

    org = st.query_params.get("org")
    if org:
        _drill_down(org, overlap, eng)
        _provenance()
        return

    # primary view — ranked org cards, outside-interest by default
    view = st.segmented_control(
        "Show", ["Outside interests", "State bodies", "All"], default="Outside interests", key="diary_view"
    )
    q = st.text_input("Search organisation", "", placeholder="e.g. IBEC, Google, Wind Energy…")

    df = overlap.copy()
    if view == "Outside interests":
        df = df[~df["is_state_body"]]
    elif view == "State bodies":
        df = df[df["is_state_body"]]
    if q.strip():
        df = df[df["organisation"].str.contains(q.strip(), case=False, na=False)]

    st.caption(f"{len(df)} organisations · {int(df['meetings'].sum())} meetings logged")
    if df.empty:
        empty_state("No matches", "No organisations match that filter.")
    else:
        st.html("\n".join(_org_card(r) for _, r in df.head(120).iterrows()))
    _provenance()


def _provenance() -> None:
    st.html(
        '<div class="dt-provenance">'
        "<b>Source & limits.</b> Ministers' own diaries (gov.ie / enterprise.gov.ie) × the "
        "Register of Lobbying (lobbying.ie). Diaries are self-curated, non-exhaustive and "
        "published quarterly-in-arrears. Meeting counts reflect which departments are ingested, "
        "not lobbying intensity. A diary meeting is co-occurrence, not a lobbying return; we show "
        "the positive match (“also lobbied”) only — a missing match is unknown, not “never lobbied”. "
        "Two departments (Public Expenditure, Taoiseach) are scanned and await OCR.</div>"
    )
