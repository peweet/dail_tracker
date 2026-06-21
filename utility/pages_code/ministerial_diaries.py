"""Who Ministers Meet — the public record of ministerial access.

What this is: every external engagement a minister logged in their OWN published diary,
shown in full and made searchable. It is the access record — who ministers meet, when,
and about what — sourced to the original document per entry.

NO INFERENCE (logic firewall + feedback_no_inference_in_app): we present the meetings as
published. We do NOT rank influence, score importance, flag a meeting as "unofficial", or
cross-frame the diary as a lobbying tool — a diary meeting is not a lobbying return and we
never imply one caused an outcome. The value is transparency: the meeting someone might hope
goes unnoticed is here to be FOUND, not accused. (Whether an org also appears in other public
registers is a separate question for those pages — this page just shows the diary.)

DATA BOUNDARY: the views (sql_views/diary/ministerial_diary_*.sql, fed by the vetted
sandbox->gold promotion extractors/diary_promote_gold.py) own every join/flag; this page
reads them via data_access.ministerial_diary_data and does presentation faceting only
(period filter, search, by-minister / by-organisation browse, per-entity drill).

HONEST COVERAGE (shown, not hidden): diaries are self-curated + non-exhaustive, published
quarterly-in-arrears; counts are coverage-driven, not a trend; absence is not proof a meeting
didn't happen. Public Expenditure (DPER) is now included via OCR of its scanned diaries; the
Taoiseach's own diary is still scan-only and awaits OCR.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.identity_resolver import resolve_member_code
from data_access.ministerial_diary_data import fetch_engagements, fetch_meetings, fetch_org_overlap
from shared_css import inject_css
from ui.components import clickable_card_link, empty_state, glossary_strip, hero_banner, hide_sidebar
from ui.entity_links import member_profile_url

_GLOSSARY = [
    (
        "Meeting",
        "An external engagement a minister logged in their own published diary — every one is shown, sourced to the original document.",
    ),
    (
        "Coverage",
        "Diaries are self-curated, non-exhaustive and published quarterly-in-arrears. What's here is what departments published; an absence is not proof a meeting didn't happen.",
    ),
    (
        "No inference",
        "We show the access — who met whom, when, about what, as published. We don't rank, score, or imply influence.",
    ),
]


def _h(s: object) -> str:
    return html.escape(str(s if s is not None else ""))


# ── period filter (year / month) — display_only faceting on entry_date ──────────────────
_MONTHS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
_ALL_YEARS = "All years"
_ALL_MONTHS = "All months"


def _period_controls(meetings: pd.DataFrame) -> tuple[str, str]:
    d = pd.to_datetime(meetings["entry_date"], errors="coerce")
    years = sorted(d.dt.year.dropna().astype(int).unique().tolist(), reverse=True)
    c1, c2 = st.columns(2)
    year = c1.selectbox("Year", [_ALL_YEARS, *(str(y) for y in years)], key="diary_year")
    month = c2.selectbox("Month", [_ALL_MONTHS, *_MONTHS], key="diary_month", disabled=(year == _ALL_YEARS))
    return year, month


def _filter_period(frame: pd.DataFrame, year: str, month: str) -> pd.DataFrame:
    if frame is None or frame.empty or year == _ALL_YEARS:
        return frame
    d = pd.to_datetime(frame["entry_date"], errors="coerce")
    mask = d.dt.year == int(year)
    if month != _ALL_MONTHS:
        mask = mask & (d.dt.month == _MONTHS.index(month) + 1)
    return frame[mask]


# ── card / row builders (project clickable_card_link stretched-link pattern) ────────────
def _org_card(row: pd.Series) -> str:
    inner = (
        f'<div class="dt-diary-card">'
        f'<div class="dt-diary-main"><div class="dt-diary-title">{_h(row["organisation"])}</div>'
        f'<div class="dt-diary-sub">{_h(str(row.get("sector", "")).replace("-", " "))}</div></div>'
        f'<div class="dt-diary-metrics">'
        f'<span class="dt-diary-metric"><b>{int(row["meetings"])}</b> meetings</span>'
        f'<span class="dt-diary-metric"><b>{int(row["ministers_met"])}</b> ministers</span></div></div>'
    )
    return clickable_card_link(
        href=f"?org={quote(str(row['organisation']), safe='')}",
        inner_html=inner,
        aria_label=f"Meetings naming {row['organisation']}",
    )


def _minister_card(row: pd.Series) -> str:
    inner = (
        f'<div class="dt-diary-card">'
        f'<div class="dt-diary-main"><div class="dt-diary-title">{_h(row["minister"])}</div>'
        f'<div class="dt-diary-sub">{_h(row["first"])} → {_h(row["last"])}</div></div>'
        f'<div class="dt-diary-metrics">'
        f'<span class="dt-diary-metric"><b>{int(row["meetings"])}</b> meetings</span></div></div>'
    )
    return clickable_card_link(
        href=f"?minister={quote(str(row['minister']), safe='')}",
        inner_html=inner,
        aria_label=f"Meetings by Minister {row['minister']}",
    )


def _meeting_rows(rows: pd.DataFrame, *, show_minister: bool) -> str:
    cards = []
    for _, e in rows.head(120).iterrows():
        src = e.get("source_pdf_url")
        link = (
            f'<a class="dt-diary-src" href="{_h(src)}" target="_blank" rel="noopener">source ↗</a>'
            if pd.notna(src) and src
            else ""
        )
        who = f"{_h(e['minister'])} · " if show_minister and pd.notna(e.get("minister")) else ""
        cards.append(
            f'<div class="dt-diary-eng"><div class="dt-diary-eng-main">'
            f'<div class="dt-diary-eng-subj">{_h(e["subject"])}</div>'
            f'<div class="dt-diary-eng-meta">{who}{_h(e["department"])} · {_h(e["entry_date"])}</div>'
            f"</div>{link}</div>"
        )
    return "\n".join(cards)


def _strip_urls(df: pd.DataFrame) -> pd.DataFrame:
    """Drop pasted URLs (Google Meet / Maps / Webex links) from subjects so a search for
    'google' matches the COMPANY, not 'https://meet.google.com' venue text."""
    if df is None or df.empty or "subject" not in df.columns:
        return df
    return df.assign(
        subject=df["subject"].astype(str).str.replace(r"https?://\S+|www\.\S+", " ", regex=True).str.strip()
    )


# ── drill-downs ─────────────────────────────────────────────────────────────────────────
def _org_drill(org: str, engagements: pd.DataFrame) -> None:
    # Use the engagements view (the gazetteer already linked diary text → org name, so
    # "Google Ireland Limited" finds its "Meeting with Google" entries — a literal name
    # search would miss them).
    st.html('<a class="dt-diary-back" href="?" target="_self">← back</a>')
    rows = (
        engagements[engagements["organisation"] == org].sort_values("entry_date", ascending=False)
        if engagements is not None and not engagements.empty
        else engagements
    )
    n = 0 if rows is None else len(rows)
    st.html(f'<div class="dt-diary-hero"><h2>{_h(org)}</h2><p>{n} meetings naming this organisation</p></div>')
    if n == 0:
        empty_state("No meetings", f"No diary entries name {org} in this period.")
        return
    st.html(_meeting_rows(rows, show_minister=True))


def _minister_drill(minister: str, meetings: pd.DataFrame) -> None:
    st.html('<a class="dt-diary-back" href="?" target="_self">← back</a>')
    rows = meetings[meetings["minister"] == minister].sort_values("entry_date", ascending=False)
    if rows.empty:
        empty_state("Not found", f"No logged meetings for {minister}.")
        return
    # Forward edge: from a minister's diary, offer their full member profile.
    # Ministers are TDs, so the name resolves via the registry; guard on a hit
    # so an unresolved name (or a former office-holder) simply shows no link.
    code = resolve_member_code(minister)
    profile_link = (
        f' · <a class="dt-diary-src" href="{_h(member_profile_url(code))}" target="_self">'
        f"View {_h(minister)}'s profile →</a>"
        if code
        else ""
    )
    st.html(
        f'<div class="dt-diary-hero"><h2>Minister {_h(minister)}</h2>'
        f"<p>{len(rows)} external meetings logged · {_h(rows['entry_date'].min())} → "
        f"{_h(rows['entry_date'].max())}{profile_link}</p></div>"
    )
    st.html(_meeting_rows(rows, show_minister=False))


def _re_escape(s: str) -> str:
    import re

    return re.escape(s)


# ── page ────────────────────────────────────────────────────────────────────────────────
def ministerial_diaries_page() -> None:
    hide_sidebar()
    inject_css()
    hero_banner(
        kicker="Access & accountability",
        title="Who Ministers Meet",
        dek="The public record of ministerial access — every meeting in ministers' own "
        "published diaries, in full and searchable.",
        badges=["Self-curated · quarterly-in-arrears", "Shown as published — no inference"],
    )
    glossary_strip(_GLOSSARY)

    overlap = fetch_org_overlap()
    meetings_all = _strip_urls(fetch_meetings())
    eng_all = _strip_urls(fetch_engagements())
    if meetings_all is None or meetings_all.empty:
        empty_state("Data unavailable", "The ministerial-diary views did not load.")
        return

    year, month = _period_controls(meetings_all)
    meetings = _filter_period(meetings_all, year, month)
    eng = _filter_period(eng_all, year, month)
    period_active = year != _ALL_YEARS

    # drill-downs (URL-routed) — honour the same period filter
    if (org := st.query_params.get("org")) is not None:
        _org_drill(org, eng)
        _provenance()
        return
    if (minister := st.query_params.get("minister")) is not None:
        _minister_drill(minister, meetings)
        _provenance()
        return

    when = f" in {month + ' ' if month != _ALL_MONTHS else ''}{year}" if period_active else ""
    st.caption(
        f"{len(meetings):,} external meetings logged{when} across {meetings['minister'].nunique()} ministers — "
        "every one sourced to the minister's own published diary."
    )

    mode = st.segmented_control(
        "Browse", ["Search meetings", "By minister", "By organisation"], default="Search meetings", key="diary_mode"
    )
    if mode == "Search meetings":
        _render_search(meetings)
    elif mode == "By minister":
        _render_by_minister(meetings)
    else:
        _render_by_org(overlap, meetings, period_active)
    _provenance()


def _render_search(meetings: pd.DataFrame) -> None:
    q = st.text_input(
        "Search every meeting", "", placeholder="any name or topic — e.g. Apple, golf, data centre, Davos…"
    )
    if not q.strip():
        st.caption("Type a name or topic to search the subject of every logged meeting.")
        return
    rows = meetings[meetings["subject"].str.contains(_re_escape(q.strip()), case=False, na=False, regex=True)]
    rows = rows.sort_values("entry_date", ascending=False)
    st.caption(f"{len(rows):,} meetings mention “{q.strip()}”")
    if rows.empty:
        empty_state("No meetings", "No diary entry mentions that term in this period.")
    else:
        st.html(_meeting_rows(rows, show_minister=True))


def _render_by_minister(meetings: pd.DataFrame) -> None:
    m = meetings[meetings["minister"].notna() & (meetings["minister"] != "")]
    agg = (  # logic_firewall: display_only — counting the active (period-filtered) set, not a metric
        m.groupby("minister")
        .agg(meetings=("subject", "size"), first=("entry_date", "min"), last=("entry_date", "max"))
        .reset_index()
        .sort_values("meetings", ascending=False)
    )
    q = st.text_input("Search minister", "", placeholder="e.g. Burke, Martin, Ryan…")
    if q.strip():
        agg = agg[agg["minister"].str.contains(q.strip(), case=False, na=False)]
    if agg.empty:
        empty_state("No matches", "No ministers match that search.")
        return
    st.html("\n".join(_minister_card(r) for _, r in agg.iterrows()))


def _render_by_org(overlap: pd.DataFrame, meetings: pd.DataFrame, period_active: bool) -> None:
    if overlap is None or overlap.empty:
        empty_state("No data", "The organisation index did not load.")
        return
    st.caption(
        "Organisations we can identify by name in a meeting subject (a subset — most meetings name no org we can match)."
    )
    # dropdown to jump straight to an organisation's meetings. Loop-safe: only navigate on a
    # NEW pick (so the back link, which keeps the selectbox value, doesn't re-trigger the drill).
    org_names = sorted(overlap["organisation"].dropna().unique().tolist())
    pick = st.selectbox("Jump to an organisation", ["— select —", *org_names], key="diary_org_pick")
    if pick != "— select —" and pick != st.session_state.get("_diary_last_org_nav"):
        st.session_state["_diary_last_org_nav"] = pick
        st.query_params["org"] = pick
        st.rerun()
    view = st.segmented_control(
        "Show", ["Outside bodies", "State bodies", "All"], default="Outside bodies", key="diary_org_view"
    )
    q = st.text_input("Filter the list below", "", placeholder="e.g. IBEC, Google, Wind Energy…")
    df = _org_counts_for_period(meetings, overlap) if period_active else overlap.copy()
    if view == "Outside bodies":
        df = df[~df["is_state_body"]]
    elif view == "State bodies":
        df = df[df["is_state_body"]]
    if q.strip():
        df = df[df["organisation"].str.contains(q.strip(), case=False, na=False)]
    if df.empty:
        empty_state("No matches", "No organisations match that filter.")
    else:
        st.html("\n".join(_org_card(r) for _, r in df.head(120).iterrows()))


def _org_counts_for_period(meetings: pd.DataFrame, overlap: pd.DataFrame) -> pd.DataFrame:
    """Re-rank organisations for a filtered period by how many meetings name them (display_only).
    Counts a substring of the org name in the period's meeting subjects, keeping sector/state
    from the overlap index."""
    out = []
    subjects = meetings["subject"].fillna("")
    for r in overlap.itertuples():
        n = int(subjects.str.contains(_re_escape(r.organisation), case=False, regex=True).sum())
        if n:
            out.append(
                {
                    "organisation": r.organisation,
                    "sector": r.sector,
                    "is_state_body": r.is_state_body,
                    "meetings": n,
                    "ministers_met": r.ministers_met,
                }
            )
    return pd.DataFrame(out).sort_values("meetings", ascending=False) if out else overlap.iloc[0:0].copy()


def _provenance() -> None:
    st.html(
        '<div class="dt-diary-prov">'
        "<b>Source &amp; limits.</b> Ministers' own published diaries (gov.ie / enterprise.gov.ie), "
        "linked per entry. Self-curated, non-exhaustive and published quarterly-in-arrears — what's "
        "here is what departments published; an absence is not proof a meeting didn't happen. Public "
        "Expenditure (DPER) is now included via OCR of its scanned diaries; the Taoiseach's own diary "
        "is still scanned and awaits OCR. We present the record "
        "as published — no ranking, scoring, or inference of influence.</div>"
    )
