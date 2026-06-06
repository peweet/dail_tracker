"""Courts & Judiciary — The Legal Diary (standalone browser page).

Sources from three registered DuckDB views (sql_views/judiciary_legal_diary_*.sql),
which read the gold parquets produced by extractors/legal_diary_extract.py from the
Courts Service daily Legal Diary (archived by pdf_infra/legal_diary_poller.py):

  v_judiciary_legal_diary_schedule  Tier A — judge sitting-sessions (officials only)
  v_judiciary_legal_diary_counts    Tier B — per-session case-item counts
  v_judiciary_legal_diary_cases     Tier C — ANONYMISED case listings + source link

Civic frame: the unelected bench doing public work in public — which judge sits on
which list, in which court, on which day, and how busy each list is. The judge and
the court are always the subject; parties are incidental, anonymised context.

PRIVACY (load-bearing — see memory project_judiciary_feature_validation):
statutory in-camera matters (minors / family / wards / childcare / asylum) are
DROPPED entirely upstream; every natural person is reduced to initials; orgs and
the State are named; each entry links to the official diary for verification. This
page presents Tier C through judge/list headings and aggregate category counts —
never as a flat, followable register of parties. NO inference in any copy.

Sections (top → bottom):
  Hero + method/privacy strip
  Day selector (segmented if few days, else selectbox; latest default)
  ① Today on the bench   (Tier A — grouped by court; the hero content)
  ② Most active lists today  (Tier B — ranked by listed-item volume, top 8, inline
     proportion bar; court only, NO judge name — a volume count, not performance)
  ③ What's before the courts (Tier C — category counts → anonymised entries
     nested under judge/list, each with a verification link)
  Provenance footer
"""

from __future__ import annotations

import datetime
import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.judiciary_data import (
    fetch_legal_diary_cases,
    fetch_legal_diary_counts,
    fetch_legal_diary_schedule,
)
from shared_css import inject_css  # noqa: F401  (kept parallel to other pages)
from ui.components import (
    empty_state,
    glossary_strip,
    hero_banner,
    hide_sidebar,
)

# Court display order — constitutional seniority, the natural reading order.
_COURT_ORDER = [
    "Supreme Court",
    "Court of Appeal",
    "Court of Appeal (Criminal)",
    "Central Criminal Court",
    "High Court",
    "Circuit Court",
    "District Court",
]
_CATEGORIES = [
    ("public-law", "Public law", "v the State / a Minister / public body"),
    ("commercial", "Commercial", "a company or financial party named"),
    ("criminal", "Criminal", "prosecution; defendant shown by initials"),
    ("civil", "Civil", "private litigation; parties shown by initials"),
]
_MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
TIER_C_PAGE = 40


def _esc(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return html.escape(str(val))


def _fmt_day(s: str) -> str:
    """'2026-06-05' -> 'Thu 5 Jun 2026' (platform-independent — no %-d)."""
    try:
        y, m, d = (int(p) for p in str(s)[:10].split("-"))
        wd = datetime.date(y, m, d).strftime("%a")
        return f"{wd} {d} {_MONTHS[m]} {y}"
    except Exception:  # noqa: BLE001
        return str(s)


# ──────────────────────────────────────────────────────────────────────────────
# Page-local CSS (jd-* family). Injected via st.markdown so it reaches <head>
# (st.html would iframe the <style>); same precedent as corp-* / si-*.
# ──────────────────────────────────────────────────────────────────────────────
def _inject_jd_css() -> None:
    st.markdown(
        """
        <style>
        .jd-context {
            font-size: 0.82rem; color: #5b6b73; line-height: 1.55;
            margin: 0.35rem 0 1.1rem; max-width: 64rem;
        }
        .jd-context strong { color: #14232b; font-weight: 600; }
        .jd-section-head {
            font-size: 1.15rem; font-weight: 700; color: #14232b;
            margin: 0.2rem 0 0.1rem; padding: 0;
        }
        .jd-court-head {
            font-size: 0.95rem; font-weight: 700; color: #14232b;
            margin: 1.4rem 0 0.6rem; padding-bottom: 0.3rem;
            border-bottom: 2px solid #e4e9ec;
        }
        .jd-court-head span { font-weight: 500; color: #6b7b83; font-size: 0.82rem; }
        .jd-grid {
            display: grid; grid-template-columns: repeat(auto-fill, minmax(17rem, 1fr));
            gap: 0.7rem;
        }
        .jd-card {
            background: #ffffff; border: 1px solid #e4e9ec; border-radius: 10px;
            padding: 0.75rem 0.85rem; display: flex; flex-direction: column; gap: 0.2rem;
        }
        .jd-judge { font-weight: 650; color: #14232b; font-size: 0.92rem; }
        .jd-meta { font-size: 0.76rem; color: #6b7b83; }
        .jd-list { font-size: 0.82rem; color: #2c3e46; margin-top: 0.15rem; }
        .jd-items {
            align-self: flex-start; margin-top: 0.4rem; font-size: 0.72rem;
            font-weight: 600; color: #2c5f6b; background: #eaf3f5;
            border-radius: 999px; padding: 0.1rem 0.55rem;
        }
        .jd-items.zero { color: #8a9aa1; background: #f1f4f5; font-weight: 500; }
        .jd-rank {
            display: flex; align-items: center; gap: 0.7rem; background: #ffffff;
            border: 1px solid #e4e9ec; border-radius: 10px; padding: 0.6rem 0.85rem;
            margin-bottom: 0.45rem;
        }
        .jd-rank-body { flex: 1; min-width: 0; }
        .jd-rank-title { font-weight: 600; color: #14232b; font-size: 0.88rem; }
        .jd-rank-sub { font-size: 0.76rem; color: #6b7b83; }
        .jd-bar-track { background: #eef2f3; border-radius: 999px; height: 7px; margin-top: 0.35rem; }
        .jd-bar-fill { background: #3d7c8a; border-radius: 999px; height: 7px; }
        .jd-rank-n { font-weight: 700; color: #2c5f6b; font-size: 1.05rem; min-width: 2.2rem; text-align: right; }
        .jd-catwrap { display: grid; grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr)); gap: 0.6rem; margin: 0.4rem 0 0.6rem; }
        .jd-cat-card { background: #ffffff; border: 1px solid #e4e9ec; border-radius: 10px; padding: 0.7rem 0.85rem; }
        .jd-cat-n { font-size: 1.5rem; font-weight: 700; color: #14232b; line-height: 1; }
        .jd-cat-label { font-weight: 600; color: #2c3e46; font-size: 0.85rem; margin-top: 0.2rem; }
        .jd-cat-desc { font-size: 0.72rem; color: #7b8b92; margin-top: 0.1rem; }
        .jd-case-row {
            display: flex; align-items: baseline; gap: 0.5rem; padding: 0.3rem 0;
            border-bottom: 1px solid #f0f3f4; font-size: 0.86rem; color: #1f2d33;
        }
        .jd-case-row:last-child { border-bottom: none; }
        .jd-chip { font-size: 0.64rem; font-weight: 700; letter-spacing: 0.02em; text-transform: uppercase;
            border-radius: 4px; padding: 0.05rem 0.4rem; white-space: nowrap; }
        .jd-chip.publiclaw { background: #e8eef9; color: #2f4b86; }
        .jd-chip.commercial { background: #eef5ea; color: #41663a; }
        .jd-chip.criminal { background: #f7ece9; color: #8a4a3a; }
        .jd-chip.civil { background: #f1f0f4; color: #5a5470; }
        .jd-case-link { font-size: 0.72rem; color: #6b7b83; text-decoration: none; white-space: nowrap; margin-left: auto; }
        .jd-case-link:hover { color: #2c5f6b; text-decoration: underline; }
        .jd-foot { font-size: 0.76rem; color: #7b8b92; line-height: 1.5; margin-top: 1.6rem;
            border-top: 1px solid #e4e9ec; padding-top: 0.8rem; max-width: 64rem; }
        .jd-foot a { color: #2c5f6b; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _chip_class(category: str) -> str:
    return {"public-law": "publiclaw", "commercial": "commercial",
            "criminal": "criminal", "civil": "civil"}.get(category, "civil")


# ──────────────────────────────────────────────────────────────────────────────
# Section renderers
# ──────────────────────────────────────────────────────────────────────────────
def _render_bench(day_sched: pd.DataFrame) -> None:
    st.html('<h2 class="jd-section-head">Today on the bench</h2>')
    st.caption("Each card is a judge's sitting session — court, list and start time. "
               "Item count is how many matters were listed for that session.")
    present = [c for c in _COURT_ORDER if c in set(day_sched["court"].dropna())]
    extra = sorted(set(day_sched["court"].dropna()) - set(_COURT_ORDER))
    for court in present + extra:
        rows = day_sched[day_sched["court"] == court]
        cards = []
        for r in rows.sort_values(["courtroom", "judge"], na_position="last").itertuples():
            n = int(getattr(r, "n_items", 0) or 0)
            items = (f'<span class="jd-items">{n} listed</span>' if n
                     else '<span class="jd-items zero">schedule only</span>')
            meta = " · ".join(p for p in (_esc(r.courtroom), _esc(r.time)) if p)
            cards.append(
                f'<div class="jd-card"><div class="jd-judge">{_esc(r.judge)}</div>'
                f'<div class="jd-meta">{meta}</div>'
                f'<div class="jd-list">{_esc(r.list_type) or "—"}</div>{items}</div>'
            )
        st.html(f'<div class="jd-court-head">{_esc(court)} '
                f'<span>· {len(rows)} sitting{"s" if len(rows) != 1 else ""}</span></div>'
                f'<div class="jd-grid">{"".join(cards)}</div>')


def _render_busiest(day_counts: pd.DataFrame) -> None:
    st.html('<h2 class="jd-section-head">Most active lists today</h2>')
    st.caption("Lists with the most scheduled items on this day — a count of listed "
               "matters, not a measure of judicial workload or performance.")
    top = day_counts.sort_values("n_items", ascending=False).head(8)
    if top.empty or int(top["n_items"].max() or 0) == 0:
        empty_state("No scheduled items", "No lists had listed matters on this day.")
        return
    mx = int(top["n_items"].max())
    for r in top.itertuples():
        n = int(r.n_items)
        pct = round(100 * n / mx) if mx else 0
        st.html(
            f'<div class="jd-rank"><div class="jd-rank-body">'
            f'<div class="jd-rank-title">{_esc(r.list_type) or "—"}</div>'
            f'<div class="jd-rank-sub">{_esc(r.court)}</div>'
            f'<div class="jd-bar-track"><div class="jd-bar-fill" style="width:{pct}%"></div></div>'
            f'</div><div class="jd-rank-n">{n}</div></div>'
        )


def _render_cases(day_cases: pd.DataFrame, day_label: str) -> None:
    st.html('<h2 class="jd-section-head">What\'s before the courts</h2>')
    st.caption("Anonymised list entries. People are shown by initials; organisations and "
               "the State are named. Private hearings (family, childcare, wards, minors) are "
               "not published. Each entry links to the official diary.")

    # category summary cards — render-time count over the already day-filtered set,
    # driving the category chip layout (not a pipeline rollup).
    # logic_firewall: display_only
    counts = day_cases["category"].value_counts().to_dict()
    cat_cards = []
    for key, label, desc in _CATEGORIES:
        cat_cards.append(
            f'<div class="jd-cat-card"><div class="jd-cat-n">{int(counts.get(key, 0))}</div>'
            f'<div class="jd-cat-label">{label}</div><div class="jd-cat-desc">{_esc(desc)}</div></div>'
        )
    st.html(f'<div class="jd-catwrap">{"".join(cat_cards)}</div>')

    if day_cases.empty:
        empty_state(
            "Every listed matter this day was private",
            "All matters listed for this day were private hearings and are not published. "
            "The sitting schedule above still shows which judges sat.",
        )
        return

    # category filter, then entries nested UNDER judge + list (judge stays the subject)
    present_cats = [(k, lbl) for k, lbl, _ in _CATEGORIES if k in counts]
    options = ["All"] + [lbl for _, lbl in present_cats]
    pick = st.segmented_control("Filter by type", options, default="All",
                                key="jd_cat_filter") or "All"
    if pick != "All":
        chosen = next(k for k, lbl in present_cats if lbl == pick)
        view = day_cases[day_cases["category"] == chosen]
    else:
        view = day_cases

    st.caption(f"{len(view)} listed matter{'s' if len(view) != 1 else ''} · {day_label}")
    grouped = view.groupby(["court", "judge", "list_type"], dropna=False)
    shown = 0
    for (court, judge, list_type), g in grouped:
        if shown >= TIER_C_PAGE:
            st.caption(f"… and {len(view) - shown} more. Use the type filter to narrow, "
                       "or open the official diary for the full day.")
            break
        head = f"{judge or 'Court'} — {list_type or 'List'}  ({len(g)})"
        with st.expander(f"{head}   ·   {court}"):
            rows = []
            for r in g.itertuples():
                link = (f'<a class="jd-case-link" href="{_esc(r.source_url)}" target="_blank" '
                        f'rel="noopener">official diary ↗</a>' if r.source_url else "")
                rows.append(
                    f'<div class="jd-case-row"><span class="jd-chip {_chip_class(r.category)}">'
                    f'{_esc(r.category)}</span><span>{_esc(r.case_anonymised)}</span>{link}</div>'
                )
            st.html("".join(rows))
        shown += len(g)


# ──────────────────────────────────────────────────────────────────────────────
def judiciary_page() -> None:
    _inject_jd_css()
    hide_sidebar()

    schedule = fetch_legal_diary_schedule()
    counts = fetch_legal_diary_counts()
    cases = fetch_legal_diary_cases()

    if schedule is None or schedule.empty:
        empty_state(
            "The Legal Diary isn't loaded yet",
            "Run the daily capture (pdf_infra/legal_diary_poller.py) and "
            "extractors/legal_diary_extract.py to populate the judiciary views.",
        )
        return

    days = sorted(schedule["diary_date"].dropna().unique(), reverse=True)
    n_judges = schedule["judge"].nunique()
    n_courts = schedule["court"].nunique()
    hero_banner(
        kicker="COURTS & JUDICIARY",
        title="The Legal Diary",
        dek="Daily court sittings from the Courts Service Legal Diary — which judge sits "
            "on which list, in which court, and how busy each list is.",
        badges=[
            f"{len(days)} day{'s' if len(days) != 1 else ''} captured",
            f"{n_judges} judges",
            f"{n_courts} courts",
            f"latest: {_fmt_day(days[0])}",
        ],
    )
    st.html(
        '<div class="jd-context">We publish the <strong>sitting schedule</strong> and '
        '<strong>anonymised</strong> list entries. Matters heard in private — family law, '
        'childcare, wards of court and cases involving minors — are <strong>excluded</strong>. '
        'People are shown by <strong>initials only</strong>; organisations and the State are '
        'named. Every entry links to the official diary so the public record can be checked. '
        'This page describes the work of the courts; it does not track or comment on any '
        'individual case or judge.</div>'
    )
    glossary_strip([
        ("DPP", "Director of Public Prosecutions — the State's prosecutor in criminal cases"),
        ("For mention", "a short listing to manage a case, not a full hearing"),
        ("Ex parte", "an application made by one side only"),
        ("Judicial review", "a challenge to a decision of the State or a public body"),
    ])

    # day selector — segmented for a handful of days, selectbox once history grows
    labels = {d: _fmt_day(d) for d in days}
    if len(days) <= 7:
        chosen_label = st.segmented_control(
            "Diary day", [labels[d] for d in days], default=labels[days[0]], key="jd_day")
        chosen = next((d for d in days if labels[d] == chosen_label), days[0])
    else:
        chosen = st.selectbox("Diary day", days, index=0, format_func=lambda d: labels[d], key="jd_day")
    st.caption(f"Showing {labels[chosen]}.")

    day_sched = schedule[schedule["diary_date"] == chosen]
    day_counts = counts[counts["diary_date"] == chosen] if not counts.empty else counts
    day_cases = cases[cases["diary_date"] == chosen] if cases is not None and not cases.empty else pd.DataFrame(
        columns=["court", "judge", "list_type", "status", "category", "case_anonymised",
                 "source", "source_url", "source_sha256"])

    if day_sched.empty:
        empty_state("No sittings listed for this day",
                    "Courts may not have sat (vacation or a non-court day).")
        return

    _render_bench(day_sched)
    st.divider()
    _render_busiest(day_counts)
    st.divider()
    _render_cases(day_cases, labels[chosen])

    # provenance footer
    sha = ""
    if day_cases is not None and not day_cases.empty and "source_sha256" in day_cases.columns:
        vals = [s for s in day_cases["source_sha256"].dropna().unique()]
        sha = vals[0] if vals else ""
    st.html(
        '<div class="jd-foot"><strong>Source:</strong> Courts Service Legal Diary '
        '(<a href="https://legaldiary.courts.ie/" target="_blank" rel="noopener">'
        'legaldiary.courts.ie ↗</a>). The official diary shows the current court day only; '
        'earlier days here come from our daily capture. Names are reduced to initials and '
        'private hearings are excluded — see the note above.'
        + (f' Captured file digest: <code>{_esc(sha)}</code>.' if sha else "")
        + "</div>"
    )
