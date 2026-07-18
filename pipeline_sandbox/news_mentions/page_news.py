"""In the News — every member's recent name-matched coverage in one stream.

What this is: a single, most-recent-first feed of recent Irish news articles that NAME a
sitting or former Oireachtas member. It is the cross-member view of the per-member "Recent
media mentions" card on the Member Overview page, reading the same data
(``v_news_mentions_recent`` → ``data/silver/parquet/news_mentions.parquet``, produced by
``extractors/news_mentions_extract.py`` — one Google-News search per member).

NO INFERENCE (logic firewall + feedback_no_inference_in_app / feedback_cite_news_claims):
each row is a NAME MATCH from a public news search, NOT an assertion by this site that the
article is about that politician, and never an endorsement of its content. We show headline +
outlet + date + a link to the publisher only — no snippet, no scoring, no "implicated"
framing. The value is discoverability: coverage someone might hope goes unnoticed is here to
be FOUND, sourced to the publisher. ~83% of matches are in the article body (not the
headline), which is flagged per row so a reader can weigh confidence.

DATA BOUNDARY: the view owns the join (news mentions × member registry for display names);
this page reads it via ``data_access.news_data.fetch_news_feed`` and does presentation
faceting only (outlet tier, headline-only, free-text search). No business logic here.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# news_data moved HERE from utility/data_access/ 2026-07-18 (this sandbox page is
# its only consumer — it was unwired from the production app).
from news_data import fetch_news_feed
from shared_css import inject_css
from ui.components import empty_state, field_label, filter_bar, glossary_strip, hero_banner, hide_sidebar
from ui.entity_links import member_link_html

# Outlet-tier badge palette — mirrors member_overview._NEWS_TIER so the two views read alike.
_NEWS_TIER: dict[str, tuple[str, str]] = {
    "national": ("#1d4ed8", "National"),
    "specialist": ("#0f766e", "Specialist"),
    "local_paper": ("#b45309", "Local paper"),
    "local_radio": ("#7c3aed", "Local radio"),
    "partisan": ("#9f1239", "Partisan"),
}

_TIER_ORDER = ["national", "specialist", "local_paper", "local_radio", "partisan"]

_GLOSSARY = [
    (
        "Mention",
        "A recent news article whose text names a member. It is a name match from a public "
        "search — not an assertion the article is about that person, and not an endorsement.",
    ),
    (
        "In the body",
        "The member's name appears in the article text but not the headline (~83% of matches). "
        "Lower confidence the piece is primarily about them.",
    ),
    (
        "Coverage",
        "Name-matched from a public news search over a recent window; skews to higher-profile "
        "members. An absence here is not proof of no coverage.",
    ),
]


def _h(s: object) -> str:
    return html.escape(str(s if s is not None else ""))


def _render_card(r) -> str:
    colour, label = _NEWS_TIER.get(getattr(r, "outlet_tier", ""), ("#6b7280", "News"))
    dt = getattr(r, "published_at", None)
    date_str = dt.strftime("%d %b %Y") if dt is not None and pd.notna(dt) else ""
    body_note = (
        ""
        if getattr(r, "match_in_title", False)
        else "<span style='color:#9ca3af;font-style:italic'>· named in article body</span>"
    )
    url = _h(str(getattr(r, "article_url", "") or "#"))
    member = member_link_html(
        getattr(r, "unique_member_code", None),
        str(getattr(r, "member_name", "") or ""),
    )
    party = _h(str(getattr(r, "party_name", "") or ""))
    constituency = _h(str(getattr(r, "constituency", "") or ""))
    who_meta = " · ".join(p for p in (party, constituency) if p)
    return (
        "<div style='background:#ffffff;border:1px solid #e7e2d8;border-radius:12px;"
        "padding:14px 16px;margin-bottom:11px'>"
        f"<a href='{url}' target='_blank' rel='noopener' "
        "style='color:#111827;text-decoration:none;font-weight:600;font-size:1.02rem;"
        f"line-height:1.35'>{_h(str(getattr(r, 'article_title', '')))}</a>"
        "<div style='margin-top:8px;font-size:0.85rem;color:#374151'>"
        f"<span style='font-weight:600'>{member}</span>"
        + (f"<span style='color:#6b7280'> · {who_meta}</span>" if who_meta else "")
        + "</div>"
        "<div style='margin-top:6px;font-size:0.8rem;color:#6b7280;display:flex;gap:8px;"
        "align-items:center;flex-wrap:wrap'>"
        f"<span style='color:#fff;border-radius:999px;padding:1px 9px;font-size:0.7rem;"
        f"font-weight:600;background:{colour}'>{label}</span>"
        f"<span>{_h(str(getattr(r, 'outlet', '') or ''))}</span>"
        f"<span>· {date_str}</span>{body_note}</div></div>"
    )


def news_page() -> None:
    inject_css()
    hide_sidebar()

    df = fetch_news_feed(limit=600)

    hero_banner(
        kicker="Members & Parliament",
        title="In the News",
        dek=(
            "Recent Irish news that names a sitting or former Oireachtas member — one stream, "
            "most-recent first. Each item is a name match from a public search and links to the "
            "publisher; a mention is not an assertion by this site."
        ),
    )
    glossary_strip(_GLOSSARY)

    if df is None or df.empty:
        empty_state(
            "No recent mentions available",
            "The news feed is empty — the underlying search hasn't been run recently, or the "
            "data isn't present in this environment.",
        )
        return

    # ── display-only faceting (no business logic) ───────────────────────────────
    tiers_present = [t for t in _TIER_ORDER if t in set(df["outlet_tier"].dropna())]
    tier_labels = {t: _NEWS_TIER.get(t, ("", t.title()))[1] for t in tiers_present}

    with filter_bar([4, 3, 2, 2]) as cols:
        with cols[0]:
            field_label("Search member or headline")
            query = st.text_input(
                "Search member or headline",
                key="news_search",
                placeholder="e.g. housing, a member's name…",
                label_visibility="collapsed",
            ).strip().lower()
        with cols[1]:
            field_label("Outlet type")
            picked_labels = st.multiselect(
                "Outlet type",
                options=list(tier_labels.values()),
                default=[],
                key="news_tier",
                label_visibility="collapsed",
                placeholder="All outlets",
            )
        with cols[2]:
            field_label("Confidence")
            headline_only = st.toggle(
                "Headline only",
                key="news_headline_only",
                help="Show only articles whose headline names the member (higher confidence).",
            )
        with cols[3]:
            field_label("Membership")
            # Default = current members only. Former-member matches are the weakest
            # (common-name collisions with long-out-of-office people), and current
            # members are the civically relevant set — so they're opt-in, not default.
            include_former = st.toggle(
                "Include former members",
                key="news_include_former",
                help="Off by default: name matches against former members are lower-precision.",
            )

    view = df
    if not include_former:
        view = view[view["is_current"] == True]  # noqa: E712 — pandas boolean mask
    if query:
        mask = (
            view["article_title"].fillna("").str.lower().str.contains(query, regex=False)
            | view["member_name"].fillna("").str.lower().str.contains(query, regex=False)
        )
        view = view[mask]
    if picked_labels:
        label_to_tier = {v: k for k, v in tier_labels.items()}
        wanted = {label_to_tier[lbl] for lbl in picked_labels if lbl in label_to_tier}
        view = view[view["outlet_tier"].isin(wanted)]
    if headline_only:
        view = view[view["match_in_title"] == True]  # noqa: E712 — pandas boolean mask

    total = len(df)
    shown = len(view)
    n_members = view["unique_member_code"].nunique() if shown else 0
    st.html(
        "<div style='margin:0.2rem 0 0.9rem;font-size:0.9rem;color:#374151'>"
        f"<strong>{shown:,}</strong> recent mention{'s' if shown != 1 else ''}"
        + (f" of <strong>{n_members:,}</strong> member{'s' if n_members != 1 else ''}" if shown else "")
        + (f" <span style='color:#9ca3af'>(of {total:,} in the feed)</span>" if shown != total else "")
        + "</div>"
    )

    if shown == 0:
        empty_state(
            "No mentions match those filters",
            "Try clearing the search box, widening the outlet types, turning off "
            "headline-only, or including former members.",
        )
        return

    st.html("".join(_render_card(r) for r in view.itertuples(index=False)))
    st.caption(
        "Name-matched from a public news search (one Google-News query per member). A mention "
        "is not an assertion by this site and does not imply involvement; headlines link to the "
        "publisher. Coverage skews to higher-profile members; an absence is not proof of none."
    )
