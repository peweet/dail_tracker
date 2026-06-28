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
didn't happen. Coverage spans 12 departments back to 2015; the scanned diaries (DPER, the
Taoiseach, Housing, DCCS and others) are now recovered via off-box GPU OCR.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.ministerial_diary_data import fetch_engagements, fetch_meetings, fetch_org_overlap
from shared_css import inject_css
from ui.components import clickable_card_link, empty_state, glossary_strip, hero_banner, hide_sidebar

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


# ── department friendly names (display formatting only) ─────────────────────────────────
# The gold view carries the terse source code per meeting (DETE / DPER / DCCS …). These map
# it to a human label — (badge_short, hero_full) — so a minister card can show "which
# ministry", and a department can be browsed as the recognisable entity it is. Full names
# track the current departmental titles on gov.ie.
_DEPT_LABELS: dict[str, tuple[str, str]] = {
    "DETE": ("Enterprise", "Enterprise, Trade & Employment"),
    "DPER": ("Public Expenditure", "Public Expenditure, NDP Delivery & Reform"),
    "DCCS": ("Culture & Sport", "Culture, Communications & Sport"),
    "DFHERIS": ("Higher Education", "Further & Higher Education, Research, Innovation & Science"),
    "DECC": ("Climate", "Environment, Climate & Communications"),
    "FINANCE": ("Finance", "Finance"),
    "HEALTH": ("Health", "Health"),
    "HOUSING": ("Housing", "Housing, Local Government & Heritage"),
    "JUSTICE": ("Justice", "Justice, Home Affairs & Migration"),
    "EDUCATION": ("Education", "Education"),
    "TRANSPORT": ("Transport", "Transport"),
    "TAOISEACH": ("Taoiseach", "The Taoiseach"),
    "RURAL": ("Rural & Community", "Rural & Community Development & the Gaeltacht"),
}


def _dept_short(code: object) -> str:
    c = str(code)
    return _DEPT_LABELS.get(c, (c.title(), c.title()))[0]


def _dept_full(code: object) -> str:
    c = str(code)
    return _DEPT_LABELS.get(c, (c.title(), c.title()))[1]


def _dept_badges(codes: list[str]) -> str:
    return "".join(f'<span class="dt-diary-badge">{_h(_dept_short(c))}</span>' for c in codes)


def _shorten(s: object, n: int = 30) -> str:
    t = str(s)
    return t if len(t) <= n else t[: n - 1].rstrip() + "…"


def _most_met_strip(orgs: list[str], *, tag: str = "div", style: str = "") -> str:
    """The subtle 'most-met · A · B · C' line shared by minister and department cards."""
    if not orgs:
        return ""
    sty = f' style="{style}"' if style else ""
    body = " · ".join(_h(_shorten(o)) for o in orgs)
    return f'<{tag} class="dt-diary-most"{sty}><b>Most-met</b> · {body}</{tag}>'


def _top_orgs(eng: pd.DataFrame, key_col: str, top: int = 3) -> dict[str, list[str]]:
    """Top organisations logged per minister / per department (display_only — counts org
    mentions in the active engagement set; NOT a ranked metric, just card context)."""
    if eng is None or eng.empty or key_col not in eng.columns or "organisation" not in eng.columns:
        return {}
    sub = eng[eng[key_col].notna() & eng["organisation"].notna()]
    if sub.empty:
        return {}
    counts = (  # logic_firewall: display_only
        sub.groupby([key_col, "organisation"]).size().reset_index(name="n").sort_values("n", ascending=False)
    )
    out: dict[str, list[str]] = {}
    for _, r in counts.iterrows():
        lst = out.setdefault(r[key_col], [])
        if len(lst) < top:
            lst.append(str(r["organisation"]))
    return out


def _minister_depts(meetings: pd.DataFrame) -> dict[str, list[str]]:
    """Each minister's portfolio(s) over the active set — a minister may hold several depts
    (e.g. Ryan = Transport + Climate). display_only context for the card badges."""
    m = meetings[meetings["minister"].notna() & (meetings["minister"] != "")]
    if m.empty:
        return {}
    g = (  # logic_firewall: display_only
        m.groupby("minister")["department"].agg(lambda s: sorted({x for x in s if pd.notna(x)}))
    )
    return g.to_dict()


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
# Render-completeness backstop: show the FULL trail (the biggest single minister drill is ~2.2k
# meetings, well within this), and NEVER truncate silently — if a list somehow exceeds the cap
# (a very broad search), say so and point to the year filter so the rest is still reachable.
_RENDER_CAP = 3000


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


def _minister_card(row: pd.Series, most_met: dict[str, list[str]]) -> str:
    badges = _dept_badges(list(row.get("depts") or []))
    most = _most_met_strip(most_met.get(row["minister"], []))
    inner = (
        f'<div class="dt-diary-card">'
        f'<div class="dt-diary-main">'
        f'<div class="dt-diary-title">{_h(row["minister"])}</div>'
        f'<div class="dt-diary-badges">{badges}</div>'
        f"{most}</div>"
        f'<div class="dt-diary-metrics">'
        f'<span class="dt-diary-metric"><b>{int(row["meetings"])}</b> meetings</span>'
        f'<span class="dt-diary-metric">{_h(row["first"])} → {_h(row["last"])}</span>'
        f"</div></div>"
    )
    return clickable_card_link(
        href=f"?minister={quote(str(row['minister']), safe='')}",
        inner_html=inner,
        aria_label=f"Meetings by Minister {row['minister']}",
    )


def _dept_card(row: pd.Series, most_met: dict[str, list[str]]) -> str:
    code = str(row["department"])
    most = _most_met_strip(most_met.get(code, []))
    inner = (
        f'<div class="dt-diary-card">'
        f'<div class="dt-diary-main">'
        f'<div class="dt-diary-title">{_h(_dept_full(code))}</div>'
        f"{most}</div>"
        f'<div class="dt-diary-metrics">'
        f'<span class="dt-diary-metric"><b>{int(row["meetings"])}</b> meetings</span>'
        f'<span class="dt-diary-metric"><b>{int(row["ministers"])}</b> ministers</span>'
        f"</div></div>"
    )
    return clickable_card_link(
        href=f"?dept={quote(code, safe='')}",
        inner_html=inner,
        aria_label=f"Ministers in {_dept_full(code)}",
    )


def _meeting_rows(rows: pd.DataFrame, *, show_minister: bool) -> str:
    cards = []
    for _, e in rows.head(_RENDER_CAP).iterrows():
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
    if len(rows) > _RENDER_CAP:  # never truncate silently — disclose + point to the year filter
        cards.append(
            f'<div class="dt-diary-eng-meta" style="padding:8px 0">Showing the most recent '
            f"{_RENDER_CAP:,} of {len(rows):,} — use the Year filter above to see the rest.</div>"
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


def _minister_drill(minister: str, meetings: pd.DataFrame, eng: pd.DataFrame) -> None:
    st.html('<a class="dt-diary-back" href="?" target="_self">← back</a>')
    rows = meetings[meetings["minister"] == minister].sort_values("entry_date", ascending=False)
    if rows.empty:
        empty_state("Not found", f"No logged meetings for {minister}.")
        return
    # NOTE: a forward edge minister → /member-overview is desirable here but the
    # diary keys ministers by SURNAME only ("Ryan", "Donohoe"), which the member
    # registry can't resolve unambiguously. Deferred until the diary view carries
    # a minister member_code (pipeline) — see project_ui_clutter_audit memory.
    badges = _dept_badges(sorted({x for x in rows["department"] if pd.notna(x)}))
    most = _most_met_strip(_top_orgs(eng, "minister", top=6).get(minister, []), tag="p", style="margin-top:0.4rem")
    st.html(
        f'<div class="dt-diary-hero"><h2>Minister {_h(minister)}</h2>'
        f'<div class="dt-diary-badges" style="margin:0.15rem 0 0.4rem">{badges}</div>'
        f"<p>{len(rows):,} external meetings logged · {_h(rows['entry_date'].min())} → "
        f"{_h(rows['entry_date'].max())}</p>{most}</div>"
    )
    st.html(_meeting_rows(rows, show_minister=False))


def _dept_drill(dept_code: str, meetings: pd.DataFrame, eng: pd.DataFrame) -> None:
    """A department as the entity: its ministers (current + former) and who they met — the
    reverse of 'minister → ministry' the flat list never showed."""
    st.html('<a class="dt-diary-back" href="?" target="_self">← back</a>')
    m = meetings[meetings["department"] == dept_code]
    if m.empty:
        empty_state("Not found", f"No logged meetings for {_dept_full(dept_code)}.")
        return
    agg = (  # logic_firewall: display_only
        m[m["minister"].notna() & (m["minister"] != "")]
        .groupby("minister")
        .agg(meetings=("subject", "size"), first=("entry_date", "min"), last=("entry_date", "max"))
        .reset_index()
        .sort_values("meetings", ascending=False)
    )
    agg["depts"] = agg["minister"].map(_minister_depts(meetings))  # full portfolio for badge context
    most = _most_met_strip(_top_orgs(eng, "department", top=6).get(dept_code, []), tag="p", style="margin-top:0.4rem")
    st.html(
        f'<div class="dt-diary-hero"><h2>{_h(_dept_full(dept_code))}</h2>'
        f"<p>{len(m):,} external meetings logged · {len(agg)} ministers · "
        f"{_h(m['entry_date'].min())} → {_h(m['entry_date'].max())}</p>{most}</div>"
    )
    st.caption("Ministers who held this department, by meetings logged — pick one for their full diary.")
    # most-met stays in the hero; the per-minister cards carry portfolio badges (no repeat strip)
    st.html("\n".join(_minister_card(r, {}) for _, r in agg.iterrows()))


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
    if (dept := st.query_params.get("dept")) is not None:
        _dept_drill(dept, meetings, eng)
        _provenance()
        return
    if (minister := st.query_params.get("minister")) is not None:
        _minister_drill(minister, meetings, eng)
        _provenance()
        return

    when = f" in {month + ' ' if month != _ALL_MONTHS else ''}{year}" if period_active else ""
    st.caption(
        f"{len(meetings):,} external meetings logged{when} across {meetings['minister'].nunique()} ministers — "
        "every one sourced to the minister's own published diary."
    )

    mode = st.segmented_control(
        "Browse",
        ["Search meetings", "By department", "By minister", "By organisation"],
        default="Search meetings",
        key="diary_mode",
    )
    if mode == "Search meetings":
        _render_search(meetings, _search_suggestions(overlap))
    elif mode == "By department":
        _render_by_dept(meetings, eng)
    elif mode == "By minister":
        _render_by_minister(meetings, eng)
    else:
        _render_by_org(overlap, meetings, period_active)
    _provenance()


def _search_suggestions(overlap: pd.DataFrame) -> list[str]:
    """Most-met organisations as click-to-search suggestions (real names that appear in subjects,
    ordered by how many meetings name them) — the dropdown proposes these but free text is still
    accepted via accept_new_options."""
    if overlap is None or overlap.empty or "organisation" not in overlap.columns:
        return []
    return overlap.sort_values("meetings", ascending=False)["organisation"].dropna().astype(str).head(40).tolist()


def _render_search(meetings: pd.DataFrame, suggestions: list[str]) -> None:
    pick = st.selectbox(
        "Search every meeting",
        suggestions,
        index=None,
        placeholder="any name or topic — e.g. Apple, golf, data centre, Davos…",
        accept_new_options=True,
        key="diary_search_q",
    )
    q = "" if pick is None else str(pick)
    if not q.strip():
        st.caption("Pick a suggested organisation or type any name or topic to search every logged meeting.")
        return
    rows = meetings[meetings["subject"].str.contains(_re_escape(q.strip()), case=False, na=False, regex=True)]
    rows = rows.sort_values("entry_date", ascending=False)
    st.caption(f"{len(rows):,} meetings mention “{q.strip()}”")
    if rows.empty:
        empty_state("No meetings", "No diary entry mentions that term in this period.")
    else:
        st.html(_meeting_rows(rows, show_minister=True))


def _render_by_minister(meetings: pd.DataFrame, eng: pd.DataFrame) -> None:
    m = meetings[meetings["minister"].notna() & (meetings["minister"] != "")]
    agg = (  # logic_firewall: display_only — counting the active (period-filtered) set, not a metric
        m.groupby("minister")
        .agg(meetings=("subject", "size"), first=("entry_date", "min"), last=("entry_date", "max"))
        .reset_index()
        .sort_values("meetings", ascending=False)
    )
    agg["depts"] = agg["minister"].map(_minister_depts(meetings))  # which ministry/ministries
    most_met = _top_orgs(eng, "minister")
    q = st.text_input("Search minister", "", placeholder="e.g. Burke, Martin, Ryan…")
    if q.strip():
        agg = agg[agg["minister"].str.contains(_re_escape(q.strip()), case=False, na=False, regex=True)]
    if agg.empty:
        empty_state("No matches", "No ministers match that search.")
        return
    st.html("\n".join(_minister_card(r, most_met) for _, r in agg.iterrows()))


def _render_by_dept(meetings: pd.DataFrame, eng: pd.DataFrame) -> None:
    m = meetings[meetings["department"].notna() & (meetings["department"] != "")]
    if m.empty:
        empty_state("No data", "No departmental diaries in this period.")
        return
    agg = (  # logic_firewall: display_only — counting the active (period-filtered) set, not a metric
        m.groupby("department")
        .agg(
            meetings=("subject", "size"),
            ministers=("minister", pd.Series.nunique),
            first=("entry_date", "min"),
            last=("entry_date", "max"),
        )
        .reset_index()
        .sort_values("meetings", ascending=False)
    )
    most_met = _top_orgs(eng, "department")
    st.caption(
        f"{len(agg)} departments publish a diary — pick one to see its ministers "
        "(current and former) and who they logged meeting."
    )
    st.html("\n".join(_dept_card(r, most_met) for _, r in agg.iterrows()))


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
        df = df[df["organisation"].str.contains(_re_escape(q.strip()), case=False, na=False, regex=True)]
    if df.empty:
        empty_state("No matches", "No organisations match that filter.")
    else:
        cards = [_org_card(r) for _, r in df.head(_RENDER_CAP).iterrows()]
        if len(df) > _RENDER_CAP:
            cards.append(
                f'<div class="dt-diary-eng-meta" style="padding:8px 0">Showing the top {_RENDER_CAP:,} '
                f"of {len(df):,} organisations — use the filter above to narrow.</div>"
            )
        st.html("\n".join(cards))


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
        "here is what departments published; an absence is not proof a meeting didn't happen. Coverage "
        "now spans 12 departments back to 2015, including the scanned diaries (DPER, the Taoiseach, "
        "Housing and others) recovered via OCR. We present the record "
        "as published — no ranking, scoring, or inference of influence.</div>"
    )
