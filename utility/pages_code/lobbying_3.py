"""Lobbying PoC — lobbying_3.py

Calmer treatment of lobby_2's same IA. Hybrid reference: TheyWorkForYou-style
prose heroes for Stage 2s, Datasette-style tables for returns lists, ranked
cards only for drill-downs (top politicians per org, top orgs per area).

Shared with lobby_2:
- data_access.lobbying_data.fetch_* (identical data layer)
- ui.components helpers (back_button, breadcrumb, empty_state, ...)
- ui.entity_links (member_profile_url, name_join_key, source_link_html)

Owned by this PoC (lp3-* CSS prefix prevents collision with lob-*):
- lp3-hero, lp3-dek, lp3-prose
- lp3-tile (quiet gateway tile)
- lp3-topic-tile (calmer topic card)
- lp3-section-head (quiet H2 with one-line dek)

This file is incrementally built. As of now: landing only. Stage 2s, RD,
Topic, Area, and Area×Politician are stubs that fall back to a "not yet
built in PoC — open in production" callout pointing at /rankings-lobbying.
"""

from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_access.lobbying_data import (
    fetch_all_org_names,
    fetch_all_politician_names,
    fetch_area_contact_detail,
    fetch_clients_for_org,
    fetch_dpo_client_breakdown,
    fetch_dpo_firms,
    fetch_dpo_one,
    fetch_dpo_politicians_targeted,
    fetch_dpo_returns_detail,
    fetch_org_contact_detail,
    fetch_org_index,
    fetch_org_persistence,
    fetch_policy_area_summary,
    fetch_politician_area_returns,
    fetch_politician_index,
    fetch_politicians_for_area,
    fetch_politicians_for_org,
    fetch_recent_returns,
    fetch_return_documents_for_org,
    fetch_revolving_door,
    fetch_revolving_door_summary,
    fetch_sources_for_org,
    fetch_summary,
    fetch_topic_returns,
    fetch_topic_summary,
)
from shared_css import inject_css
from ui.avatars import avatar_data_url, initials as _initials
from ui.components import (
    back_button,
    breadcrumb,
    clean_meta,
    clickable_card_link,
    empty_state,
    member_card_html,
    page_error_boundary,
    pagination_controls,
    pill,
    sidebar_page_header,
)
from ui.entity_links import member_profile_url, name_join_key, source_link_html
from ui.export_controls import export_button
from ui.source_links import render_source_links
from ui.source_pdfs import provenance_expander

# Curated topics — same as lobby_2; presented quieter on landing.
_CURATED_TOPICS: dict[str, dict[str, object]] = {
    "Immigration & asylum": {
        "blurb": "Returns mentioning immigration, asylum, refugees, or direct provision.",
        "keywords": (
            "immigration",
            "immigrant",
            "asylum",
            "refugee",
            "direct provision",
            "international protection",
            "migrant",
        ),
    },
    "Housing crisis": {
        "blurb": "Returns mentioning homelessness, evictions, the rental crisis, or affordable housing.",
        "keywords": (
            "homeless",
            "homelessness",
            "eviction",
            "tenant",
            "rent freeze",
            "rental crisis",
            "affordable housing",
        ),
    },
    "Climate": {
        "blurb": "Returns mentioning climate, emissions, decarbonisation, or net zero.",
        "keywords": (
            "climate",
            "emissions",
            "decarbonis",
            "net zero",
            "carbon",
            "renewable energy",
        ),
    },
}

_QP_KEYS = (
    "lp3_pol",
    "lp3_org",
    "lp3_area",
    "lp3_dpo",
    "lp3_topic",
    "lp3_rd",
    "lp3_orgindex",
)


def _init() -> None:
    for k in (
        "lp3_sidebar_search",
    ):
        st.session_state.setdefault(k, "")


def _clear_lp3_qp() -> None:
    for k in _QP_KEYS:
        st.query_params.pop(k, None)


# ── HTML helpers (calm building blocks) ───────────────────────────────────────


def _quiet_hero(title: str, dek: str = "", *, dek_html: str = "") -> None:
    """Plain H1 + dek paragraph. No kicker, no badges, no border accents.

    ``dek`` is plain text and HTML-escaped on render. ``dek_html`` is raw
    HTML for cases where you want to <strong>-wrap numbers so they pick up
    the navy accent treatment defined in shared_css.py (.lp3-dek strong).
    Callers using ``dek_html`` are responsible for escaping their own
    variable substrings via ``_h()``.
    """
    dek_inner = dek_html if dek_html else _h(dek)
    st.html(
        '<header class="lp3-hero">'
        f'<h1 class="lp3-h1">{_h(title)}</h1>'
        f'<p class="lp3-dek">{dek_inner}</p>'
        "</header>"
    )


def _section_head(label: str, dek: str = "") -> None:
    """Quiet H2 with optional one-line dek. Replaces evidence_heading's
    underlined-uppercase treatment with a plainer h2 + grey dek."""
    dek_html = f'<p class="lp3-section-dek">{_h(dek)}</p>' if dek else ""
    st.html(f'<div class="lp3-section-head"><h2 class="lp3-h2">{_h(label)}</h2>{dek_html}</div>')


def _tile_html(heading: str, body: str) -> str:
    """Quiet gateway tile: heading + single sentence. No icon, no accent
    border-top, no stat block. Just a white card with a thin border and the
    text doing the work."""
    return (
        '<div class="lp3-tile">'
        f'<h3 class="lp3-tile-heading">{_h(heading)}</h3>'
        f'<p class="lp3-tile-body">{_h(body)}</p>'
        "</div>"
    )


def _topic_tile_html(heading: str, body: str) -> str:
    """Calmer topic tile — solid border, no dashed treatment, no icon."""
    return (
        '<div class="lp3-topic-tile">'
        f'<h3 class="lp3-tile-heading">{_h(heading)}</h3>'
        f'<p class="lp3-tile-body">{_h(body)}</p>'
        "</div>"
    )


def _ranked_card_html(
    name: str,
    meta: str,
    pills_list: list[str],
    rank: int,
    avatar_url: str | None = None,
    avatar_initials: str | None = None,
) -> str:
    """Reuses member_card_html so the calm look stays consistent with the
    rest of the app's ranked-list pages (Attendance, Payments). Pills are
    rendered via the canonical pill() helper — escaping is safe."""
    pills_html = "".join(pill(p) for p in pills_list)
    return member_card_html(
        name=name,
        meta=meta,
        rank=rank,
        pills_html=pills_html,
        avatar_url=avatar_url,
        avatar_initials=avatar_initials,
    )


def _provenance_footer(summary: pd.DataFrame) -> None:
    s = summary.iloc[0] if not summary.empty else pd.Series()
    src = s.get("source_summary", "lobbying.ie via lobby_processing.py")
    ts = s.get("latest_fetch_timestamp_utc", "—")
    fp = s.get("first_period", "—")
    lp = s.get("last_period", "—")
    provenance_expander(
        sections=[
            "**Data source:** lobbying.ie — the Irish Register of Lobbying. "
            "Returns are filed quarterly by organisations lobbying Designated Public Officials.",
            f"Source: {src}",
            f"Dataset covers: {fp} → {lp}  ·  Last fetched: {ts}",
        ]
    )


# ── Sidebar ────────────────────────────────────────────────────────────────────


def _render_sidebar() -> None:
    """Search only. The 'Notable targets' and 'Browse by policy area' expanders
    from lobby_2's sidebar are dropped to reduce visual din; users reach areas
    via the gateway tile on landing instead.
    """
    with st.sidebar:
        sidebar_page_header("Lobbying<br>Register · PoC")

        st.html('<p class="lp3-sidebar-label">Search</p>')
        search = st.text_input(
            "Search",
            placeholder="e.g. Ibec, Mary Lou McDonald",
            key="lp3_sidebar_search",
            label_visibility="collapsed",
        )

        pol_names = fetch_all_politician_names()
        org_names = fetch_all_org_names()

        s = search.strip().lower()
        if s:
            pol_filtered = [n for n in pol_names if s in n.lower()]
            org_filtered = [n for n in org_names if s in n.lower()]
        else:
            pol_filtered = pol_names[:200]
            org_filtered = []

        combined = [""] + pol_filtered + [f"[Org] {n}" for n in org_filtered[:50]]
        sel = st.selectbox(
            "Jump to",
            combined,
            label_visibility="collapsed",
        )
        if sel:
            _clear_lp3_qp()
            if sel.startswith("[Org] "):
                st.query_params["lp3_org"] = sel[6:]
            else:
                # Politicians: redirect to canonical member-overview (matches lobby_2 behaviour).
                target = member_profile_url(name_join_key(sel), section="lobbying")
                st.markdown(
                    f'<meta http-equiv="refresh" content="0;url={_h(target)}">',
                    unsafe_allow_html=True,
                )
                st.stop()
            st.rerun()


# ── Landing ────────────────────────────────────────────────────────────────────


def _render_landing(summary: pd.DataFrame) -> None:
    s = summary.iloc[0] if not summary.empty else pd.Series()
    total_returns = int(s.get("total_returns", 0) or 0)
    total_orgs = int(s.get("total_orgs", 0) or 0)
    total_pols = int(s.get("total_politicians", 0) or 0)
    total_areas = int(s.get("total_policy_areas", 0) or 0)
    first_p = str(s.get("first_period", "") or "")
    last_p = str(s.get("last_period", "") or "")

    # Hero — plain H1 + dek. No badges, no kicker. A single sentence puts the
    # counts inline as part of the prose, not as separate chips.
    period_clause_html = (
        f" The dataset covers returns from <strong>{_h(first_p)}</strong> to <strong>{_h(last_p)}</strong>."
        if first_p and last_p and first_p != "None" and last_p != "None"
        else ""
    )
    if total_returns:
        _quiet_hero(
            title="Lobbying register",
            dek_html=(
                f"<strong>{total_returns:,}</strong> returns filed by "
                f"<strong>{total_orgs:,}</strong> organisations targeting "
                f"<strong>{total_pols:,}</strong> politicians across "
                f"<strong>{total_areas}</strong> registered policy areas.{period_clause_html}"
            ),
        )
    else:
        _quiet_hero(
            title="Lobbying register",
            dek="The lobbying.ie register is not yet populated. Run lobby_processing.py to ingest the data.",
        )

    # Gateway — three matching quiet tiles. Identical shape across the trio
    # (heading + one sentence + CTA underneath). Icons dropped; the prose is
    # the affordance.
    _section_head(
        "Where do you want to investigate?",
        "Three entry points into the register. The data underneath is the same; the angle differs.",
    )
    g1, g2, g3 = st.columns(3)
    with g1:
        st.html(
            _tile_html(
                "Follow a politician",
                "See every lobbying return targeting a specific TD, senator, or minister.",
            )
        )
        if st.button("Open politician profile →", key="lp3_gw_pol", width="stretch"):
            idx = fetch_politician_index()
            if not idx.empty:
                # Politicians live on /member-overview (canonical, per lobby_2 redirect).
                m_id = str(idx.iloc[0].get("unique_member_code", "") or "")
                m_name = str(idx.iloc[0].get("member_name", ""))
                target = member_profile_url(m_id or name_join_key(m_name), section="lobbying")
                st.markdown(
                    f'<meta http-equiv="refresh" content="0;url={_h(target)}">',
                    unsafe_allow_html=True,
                )
                st.stop()
    with g2:
        st.html(
            _tile_html(
                "Follow an organisation",
                "See which politicians an organisation has lobbied, on what topics, and over what period.",
            )
        )
        if st.button("Browse organisations →", key="lp3_gw_org", width="stretch"):
            _clear_lp3_qp()
            st.query_params["lp3_orgindex"] = "1"
            st.rerun()
    with g3:
        st.html(
            _tile_html(
                "Browse by policy area",
                "Find every return filed under one of lobbying.ie's 32 registered public policy areas.",
            )
        )
        if st.button("Browse policy areas →", key="lp3_gw_area", width="stretch"):
            areas = fetch_policy_area_summary()
            if not areas.empty:
                _clear_lp3_qp()
                st.query_params["lp3_area"] = str(areas.iloc[0]["public_policy_area"])
                st.rerun()

    # Topics rail — preserved per user instruction. Calmer than lobby_2:
    # solid border (not dashed), no rust icon, one italic caveat instead
    # of a multi-line banner.
    _section_head(
        "By topic",
        "Free-text scans for issues the register does not have an official area for. Results are indicative.",
    )
    t_cols = st.columns(len(_CURATED_TOPICS))
    for col, (topic_name, spec) in zip(t_cols, _CURATED_TOPICS.items(), strict=True):
        with col:
            st.html(_topic_tile_html(topic_name, str(spec["blurb"])))
            if st.button(f"Open {topic_name} →", key=f"lp3_topic_{topic_name[:24]}", width="stretch"):
                _clear_lp3_qp()
                st.query_params["lp3_topic"] = topic_name
                st.rerun()

    # Two ranked card lists side-by-side: most-lobbied politicians, most-active
    # orgs. These preserve the "instantly searchable" feel the user named as
    # lobby_2's best quality — you scan top names, click through.
    lb1, lb2 = st.columns(2)
    with lb1:
        _section_head("Most-lobbied politicians", "Top 10 by total returns targeting them.")
        idx = fetch_politician_index()
        if idx.empty:
            empty_state("No data", "v_lobbying_index has not been populated.")
        else:
            cards: list[str] = []
            for rank, (_, row) in enumerate(idx.head(10).iterrows(), start=1):
                name = str(row.get("member_name", "—"))
                m_id = str(row.get("unique_member_code", "") or "")
                meta = clean_meta(str(row.get("chamber", "") or ""), str(row.get("position", "") or ""))
                pills_list = [
                    f"{int(row.get('return_count', 0) or 0):,} returns",
                    f"{int(row.get('distinct_orgs', 0) or 0):,} orgs",
                ]
                jump = member_profile_url(m_id or name_join_key(name), section="lobbying")
                inner = _ranked_card_html(
                    name, meta, pills_list, rank,
                    avatar_url=avatar_data_url(name),
                    avatar_initials=_initials(name),
                )
                cards.append(
                    clickable_card_link(
                        href=jump,
                        inner_html=inner,
                        aria_label=f"View {name}'s profile",
                    )
                )
            st.html("\n".join(cards))
    with lb2:
        _section_head("Most active organisations", "Top 10 by number of returns filed.")
        orgs = fetch_org_index()
        if orgs.empty:
            empty_state("No data", "v_lobbying_org_index has not been populated.")
        else:
            cards = []
            for rank, (_, row) in enumerate(orgs.head(10).iterrows(), start=1):
                name = str(row.get("lobbyist_name", "—"))
                meta = clean_meta(str(row.get("sector", "") or ""))
                pills_list = [
                    f"{int(row.get('return_count', 0) or 0):,} returns",
                    f"{int(row.get('politicians_targeted', 0) or 0):,} politicians",
                ]
                cards.append(
                    clickable_card_link(
                        href=f"?lp3_org={quote(name)}",
                        inner_html=_ranked_card_html(name, meta, pills_list, rank),
                        aria_label=f"View profile for {name}",
                    )
                )
            st.html("\n".join(cards))

    # Revolving door — replaces lobby_2's amber callout box with a quiet
    # H2 + prose paragraph + a small card list of the top 3.
    rd_summary = fetch_revolving_door_summary()
    dpos = fetch_revolving_door(limit=3)
    if not rd_summary.empty and not dpos.empty:
        rd_row = rd_summary.iloc[0]
        rd_n = int(rd_row.get("individuals", 0) or 0)
        rd_returns = int(rd_row.get("total_returns", 0) or 0)
        _section_head("Revolving door")
        st.html(
            '<p class="lp3-prose">'
            f"<strong>{rd_n:,}</strong> former Designated Public Officials — politicians, ministers, "
            f"senior civil servants — have filed <strong>{rd_returns:,}</strong> lobbying returns "
            "against their former colleagues. Officials are subject to a one-year cooling-off period "
            "before they may lobby; identification is by name-matching against the DPO register and "
            "should be treated as indicative, not a legal finding."
            "</p>"
        )
        cards = []
        for rank, (_, row) in enumerate(dpos.iterrows(), start=1):
            name = str(row.get("individual_name", "—"))
            former_pos = str(row.get("former_position", "") or "")
            meta = clean_meta(f"Former {former_pos}" if former_pos else "Former DPO")
            pills_list = [
                f"{int(row.get('return_count', 0) or 0):,} returns",
                f"{int(row.get('distinct_politicians_targeted', 0) or 0):,} politicians",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lp3_dpo={quote(name)}",
                    inner_html=_ranked_card_html(
                        name, meta, pills_list, rank,
                        avatar_initials=_initials(name),
                    ),
                    aria_label=f"View revolving-door profile for {name}",
                )
            )
        st.html("\n".join(cards))
        if st.button(f"Explore all {rd_n:,} revolving-door cases →", key="lp3_rd_explore"):
            _clear_lp3_qp()
            st.query_params["lp3_rd"] = "1"
            st.rerun()

    # Latest returns — prose entries instead of lobby_2's custom row HTML.
    _section_head("Latest returns", "Most recent filings on the register.")
    recent = fetch_recent_returns()
    if recent.empty:
        empty_state("No data", "v_lobbying_recent_returns is empty.")
    else:
        rows = []
        for _, row in recent.head(8).iterrows():
            period = str(row.get("period_start_date", "") or "")[:7]
            org = str(row.get("lobbyist_name", "") or "—")
            area = str(row.get("public_policy_area", "") or "")
            area_clause = f" under <em>{_h(area)}</em>" if area else ""
            rows.append(
                f'<li class="lp3-recent-item">'
                f'<span class="lp3-recent-period">{_h(period)}</span> '
                f'<span class="lp3-recent-body"><strong>{_h(org)}</strong>{area_clause}</span>'
                "</li>"
            )
        st.html(f'<ul class="lp3-recent-list">{"".join(rows)}</ul>')

    _provenance_footer(summary)


# ── Shared helpers across Stage 2 handlers ────────────────────────────────────


def _back_button() -> None:
    """Return-to-landing button. Clears lp3_* query params and reruns."""
    if back_button("← Back to Lobbying register", key="lp3"):
        _clear_lp3_qp()
        st.rerun()


def _fmt_mmm(value: object) -> str:
    try:
        return pd.to_datetime(value).strftime("%b %Y")
    except Exception:
        return str(value or "—")


def _year_pills(df: pd.DataFrame, key: str) -> tuple[str | None, str | None]:
    """Year filter pills above a returns table. Pushes selection back to SQL
    via the returned (start, end) tuple — pandas does no row masking here."""
    if df.empty or "period_start_date" not in df.columns:
        return None, None
    try:
        years = sorted(
            pd.to_datetime(df["period_start_date"], errors="coerce").dropna().dt.year.unique().tolist(),
            reverse=True,
        )
    except Exception:
        return None, None
    if not years:
        return None, None
    options = ["All"] + [str(y) for y in years]
    chosen = (
        st.segmented_control("Year", options, default=options[0], key=key, label_visibility="collapsed")
        or options[0]
    )
    if chosen == "All":
        return None, None
    return f"{chosen}-01-01", f"{chosen}-12-31"


def _return_card_html(
    *,
    period: str,
    title: str,
    subtitle: str = "",
    area: str = "",
    return_id: str = "",
    snippet: str = "",
    url: str = "",
) -> str:
    """Canonical return-record card for the PoC. Used wherever a list of
    lobbying returns is displayed — topic Stage 2, org / area / DPO / Stage 3.

    Header row: period chip, optional area pill, return-# on the right.
    Body: title in serif (the variable field — politician, org, or firm),
    optional second-line subtitle (client / "on behalf of …"), optional
    snippet of free-text details, styled `↗` source-link in the actions row.
    """
    head_bits = [f'<span class="lp3-return-period">{_h(period or "—")}</span>']
    if area:
        head_bits.append(f'<span class="lp3-return-area">{_h(area)}</span>')
    if return_id:
        head_bits.append(f'<span class="lp3-return-id">Return #{_h(return_id)}</span>')
    head_html = "".join(head_bits)

    link_html = (
        source_link_html(url, "View on lobbying.ie", aria_label="Open this return on lobbying.ie")
        if url and url.startswith("http")
        else ""
    )
    sub_html = f'<p class="lp3-return-sub">{_h(subtitle)}</p>' if subtitle else ""
    snippet_html = f'<p class="lp3-return-snippet">{_h(snippet)}</p>' if snippet else ""
    actions_html = f'<div class="lp3-return-actions">{link_html}</div>' if link_html else ""

    return (
        '<article class="lp3-return-card">'
        f'<header class="lp3-return-head">{head_html}</header>'
        f'<p class="lp3-return-org">{_h(title)}</p>'
        f"{sub_html}{snippet_html}{actions_html}"
        "</article>"
    )


def _datasette_table(detail: pd.DataFrame, columns: dict[str, str], height: int | None = None) -> None:
    """Plain Datasette-tone dataframe. DateColumn for any 'Period' / 'First
    filing' / 'Last filing' column, LinkColumn for any 'Return URL' column.
    Everything else is plain TextColumn or NumberColumn.
    """
    if not columns:
        return
    keep = [c for c in columns if c in detail.columns]
    display = detail[keep].rename(columns=columns)
    col_config: dict[str, object] = {}
    for orig, new in columns.items():
        if orig not in detail.columns:
            continue
        if new in ("Period", "First filing", "Last filing"):
            col_config[new] = st.column_config.DateColumn(new, format="MMM YYYY")
        elif new == "Return URL":
            col_config[new] = st.column_config.LinkColumn(
                new, display_text=r"https://www\.lobbying\.ie/return/(\d+)"
            )
    kwargs: dict[str, object] = {
        "width": "stretch",
        "hide_index": True,
        "column_config": col_config,
    }
    if height is not None:
        kwargs["height"] = height
    st.dataframe(display, **kwargs)


# ── Organisation index ────────────────────────────────────────────────────────


_FUNDING_LABELS = {
    "state_funded": "State-funded",
    "mostly_donations": "Mostly donations",
    "mostly_trading": "Mostly trading",
    "mixed": "Mixed funding",
}
_FUNDING_FILTER = {label: key for key, label in _FUNDING_LABELS.items()}


def _render_org_index(summary: pd.DataFrame) -> None:
    _back_button()
    breadcrumb(["Lobbying", "Organisations"], key_prefix="lp3_org_idx")

    show_state = st.toggle(
        "Include state-funded public bodies (HSE, hospitals…)",
        value=False,
        key="lp3_org_idx_show_state",
    )
    orgs = fetch_org_index(exclude_state_adjacent=not show_state)

    _quiet_hero(
        title="Browse lobbying organisations",
        dek=(
            f"{len(orgs):,} organisations on the register, enriched where possible with their "
            "Companies Registration Office and Charities Regulator record. Filter by funding "
            "profile or income trend, or search by name."
        ),
    )

    if orgs.empty:
        empty_state("No organisations", "The lobbying organisation index is empty.")
        _provenance_footer(summary)
        return

    # Single search + two single-selectbox filters. Drops lobby_2's
    # segmented_control rows; one primitive per facet.
    search = st.text_input(
        "Search by name",
        key="lp3_org_idx_search",
        placeholder="Search organisations…",
    )
    f1, f2 = st.columns(2)
    funding_choice = f1.selectbox(
        "Funding profile",
        ["All", *_FUNDING_LABELS.values()],
        key="lp3_org_idx_funding",
    )
    trend_choice = f2.selectbox(
        "Income trend",
        ["All", "Growing", "Flat", "Shrinking"],
        key="lp3_org_idx_trend",
    )

    filtered = orgs
    if search and search.strip():
        filtered = filtered[
            filtered["lobbyist_name"].astype(str).str.contains(
                search.strip(), case=False, na=False, regex=False
            )
        ]
    if funding_choice and funding_choice != "All":
        filtered = filtered[filtered["funding_profile"] == _FUNDING_FILTER[funding_choice]]
    if trend_choice and trend_choice != "All":
        filtered = filtered[filtered["income_trend"] == trend_choice.lower()]

    st.caption(f"Showing {len(filtered):,} of {len(orgs):,} organisations.")
    if filtered.empty:
        empty_state("No matches", "No organisations match the current filters.")
        _provenance_footer(summary)
        return

    _section_head("Organisations", "Click any card to open its lobbying profile.")
    page_size, page_idx = pagination_controls(
        total=len(filtered), key_prefix="lp3_org_idx", label="organisations"
    )
    page_slice = filtered.iloc[page_idx * page_size : (page_idx + 1) * page_size]
    rank_offset = page_idx * page_size
    cards: list[str] = []
    for i, (_, row) in enumerate(page_slice.iterrows(), start=1):
        name = str(row.get("lobbyist_name", "—"))
        meta = clean_meta(str(row.get("sector", "") or ""))
        pills_list = [
            f"{int(row.get('return_count', 0) or 0):,} returns",
            f"{int(row.get('politicians_targeted', 0) or 0):,} politicians",
        ]
        cards.append(
            clickable_card_link(
                href=f"?lp3_org={quote(name)}",
                inner_html=_ranked_card_html(name, meta, pills_list, rank=rank_offset + i),
                aria_label=f"View lobbying profile for {name}",
            )
        )
    st.html("\n".join(cards))

    _provenance_footer(summary)


# ── Organisation Stage 2 ──────────────────────────────────────────────────────


def _render_org(org_name: str, summary: pd.DataFrame) -> None:
    _back_button()

    # Validate the URL parameter — silent fallback to the first-alphabetical
    # org is exactly the bug we flagged in the audit.
    all_orgs = fetch_all_org_names()
    if all_orgs and org_name not in all_orgs:
        empty_state(
            "Organisation not found",
            f"No register entry on record for '{org_name}'. The URL may be a typo.",
        )
        _provenance_footer(summary)
        return

    org_idx = fetch_org_index()
    org_row = pd.Series()
    if not org_idx.empty and "lobbyist_name" in org_idx.columns:
        m = org_idx[org_idx["lobbyist_name"] == org_name]
        if not m.empty:
            org_row = m.iloc[0]

    sector = clean_meta(str(org_row.get("sector", "") or ""))
    website = str(org_row.get("website", "") or "")
    ret_cnt = int(org_row.get("return_count", 0) or 0)
    pol_cnt = int(org_row.get("politicians_targeted", 0) or 0)
    area_cnt = int(org_row.get("distinct_policy_areas", 0) or 0)
    first_p = str(org_row.get("first_period", "") or "")
    last_p = str(org_row.get("last_period", "") or "")

    sector_clause_html = f" {_h(sector)}." if sector else ""
    period_clause_html = (
        f" Active <strong>{_h(first_p)}</strong> to <strong>{_h(last_p)}</strong>."
        if first_p and last_p and first_p != "None" and last_p != "None"
        else ""
    )
    if ret_cnt:
        _quiet_hero(
            title=org_name,
            dek_html=(
                f"Filed <strong>{ret_cnt:,}</strong> lobbying returns targeting "
                f"<strong>{pol_cnt:,}</strong> politicians across "
                f"<strong>{area_cnt}</strong> policy areas.{sector_clause_html}{period_clause_html}"
            ),
        )
    else:
        _quiet_hero(title=org_name, dek="No lobbying returns on record for this organisation.")

    if website and website.startswith("http"):
        st.html(
            f'<p class="lp3-prose">Website: {source_link_html(website, website, aria_label=f"Open {org_name} website")}</p>'
        )

    # Switch organisation (sits below hero, narrow column)
    if all_orgs:
        st.html(
            "<style>"
            ".st-key-lp3_org_switcher .stSelectbox > div > div,"
            '.st-key-lp3_org_switcher [data-baseweb="select"] > div'
            "{background:#ffffff !important;}"
            "</style>"
        )
        sw_col, _ = st.columns([1, 2])
        with sw_col:
            picked = st.selectbox(
                "Switch organisation",
                all_orgs,
                index=all_orgs.index(org_name) if org_name in all_orgs else 0,
                key="lp3_org_switcher",
            )
        if picked and picked != org_name:
            _clear_lp3_qp()
            st.query_params["lp3_org"] = picked
            st.rerun()

    # Politicians targeted — ranked cards (preserves "instant searchability"
    # for cross-reference).
    _section_head("Politicians targeted", "Click any card to open the politician's full profile.")
    pol_intensity = fetch_politicians_for_org(org_name)
    if pol_intensity.empty:
        empty_state("No data", "No politicians found targeted by this organisation.")
    else:
        cards: list[str] = []
        for rank, (_, row) in enumerate(pol_intensity.head(20).iterrows(), start=1):
            pol_name = str(row.get("member_name", "—"))
            member_id = str(row.get("unique_member_code", "") or "")
            chamber = str(row.get("chamber", "") or "")
            first_c = str(row.get("first_contact", "") or "")[:7]
            last_c = str(row.get("last_contact", "") or "")[:7]
            meta = clean_meta(chamber, first_c, last_c)
            pills_list = [
                f"{int(row.get('returns_in_relationship', 0) or 0):,} returns",
                f"{int(row.get('distinct_policy_areas', 0) or 0):,} policy areas",
            ]
            jump = member_profile_url(member_id or name_join_key(pol_name), section="lobbying")
            cards.append(
                clickable_card_link(
                    href=jump,
                    inner_html=_ranked_card_html(
                        pol_name, meta, pills_list, rank,
                        avatar_url=avatar_data_url(pol_name),
                        avatar_initials=_initials(pol_name),
                    ),
                    aria_label=f"View {pol_name}'s profile",
                )
            )
        st.html("\n".join(cards))
        if len(pol_intensity) > 20:
            st.caption(f"Showing top 20 of {len(pol_intensity):,} politicians targeted.")

    # Clients table (Datasette-tone)
    clients = fetch_clients_for_org(org_name)
    if not clients.empty:
        _section_head("Clients represented")
        _datasette_table(
            clients,
            columns={
                "client_name": "Client",
                "period_start_date": "Period",
                "policy_areas": "Policy areas",
                "politicians_count": "Politicians",
                "source_url": "Return URL",
            },
        )

    # Returns — card list. The organisation is the page subject, so the
    # politician is the variable field that earns the serif heading.
    detail_all = fetch_org_contact_detail(org_name)
    _section_head(
        "All lobbying returns",
        "Each card links to the original filing on lobbying.ie.",
    )
    if detail_all.empty:
        empty_state("No returns", "No contact detail on record for this organisation.")
    else:
        start, end = _year_pills(detail_all, "lp3_year_org")
        detail = fetch_org_contact_detail(org_name, start, end) if start else detail_all
        page_size, page_idx = pagination_controls(
            total=len(detail), key_prefix="lp3_org_returns", label="returns"
        )
        page_slice = detail.iloc[page_idx * page_size : (page_idx + 1) * page_size]
        cards = [
            _return_card_html(
                period=_fmt_mmm(row.get("period_start_date")),
                title=str(row.get("member_name", "") or "—"),
                area=str(row.get("public_policy_area", "") or ""),
                url=str(row.get("source_url", "") or ""),
            )
            for _, row in page_slice.iterrows()
        ]
        st.html("\n".join(cards))
        export_button(
            detail,
            "Export CSV",
            f"{org_name[:40].replace(' ', '_')}_lobbying.csv",
            "lp3_export_org_detail",
        )

    # Attached references — third-party PDFs cited inside the org's returns.
    _render_org_attached_references(org_name)

    # Official source links — keep lobby_2's render_source_links helper.
    _section_head("Official source links")
    render_source_links(fetch_sources_for_org(org_name))

    _provenance_footer(summary)


# ── Attached references (third-party PDFs in return free-text) ─────────────────


_SOURCE_FIELD_LABELS: dict[str, str] = {
    "lobbying_activities": "Lobbying activities",
    "intended_results": "Intended results",
    "specific_details": "Specific details",
    "grassroots_directive": "Grassroots directive",
}


def _render_org_attached_references(org_name: str) -> None:
    """Plain list of external PDFs the organisation cited inside its returns.

    Calmer than lobby_2's bordered "EXTERNAL" card variant: serif H2,
    one-line dek, then a <ul> of items. Each item names the host, the
    cited field, and exposes the styled .dt-source-link chips (Open PDF +
    View return) so the hyperlink styling the user prefers stays visible.
    """
    docs = fetch_return_documents_for_org(org_name)
    if docs.empty:
        return

    _section_head(
        f"Attached references ({len(docs)})",
        "PDFs cited by this organisation inside its own lobbying return text. These are "
        "external sources hosted by the lobbyist or third parties — they may move or be removed.",
    )

    rows: list[str] = []
    for _, r in docs.iterrows():
        url = str(r.get("pdf_url") or "")
        host = str(r.get("host") or "external")
        ret_id = str(r.get("return_id") or "—")
        lobby_url = str(r.get("lobby_url") or "")
        field = str(r.get("source_field") or "")
        field_lbl = _SOURCE_FIELD_LABELS.get(field, field or "—")
        area = str(r.get("public_policy_area") or "")

        meta_bits: list[str] = [f"Return #{ret_id}", f"Cited in: {field_lbl}"]
        if area:
            meta_bits.append(area)
        meta_html = " · ".join(_h(b) for b in meta_bits)

        return_link = (
            source_link_html(
                lobby_url,
                "View return",
                aria_label=f"Open lobbying.ie return {ret_id} in a new tab",
            )
            if lobby_url.startswith("http")
            else ""
        )
        pdf_link = source_link_html(
            url,
            f"Open PDF on {host}",
            aria_label=f"Open attached PDF from {host} in a new tab",
        )

        sep = ' <span class="lp3-recent-period" style="min-width:auto;">·</span> ' if return_link else ""
        rows.append(
            '<li class="lp3-recent-item">'
            f'<span class="lp3-recent-body"><strong>{pdf_link}</strong>{sep}{return_link}'
            f'<div style="font-size:0.78rem;color:var(--text-meta);margin-top:0.15rem;">{meta_html}</div>'
            "</span>"
            "</li>"
        )
    st.html(f'<ul class="lp3-recent-list">{"".join(rows)}</ul>')


# ── Area Stage 2 ──────────────────────────────────────────────────────────────


def _render_area(area: str, summary: pd.DataFrame) -> None:
    _back_button()

    areas_df = fetch_policy_area_summary()
    all_area_names = (
        areas_df["public_policy_area"].dropna().tolist()
        if not areas_df.empty and "public_policy_area" in areas_df.columns
        else []
    )

    if all_area_names and area not in all_area_names:
        empty_state(
            "Policy area not found",
            f"'{area}' is not one of lobbying.ie's 32 registered policy areas.",
        )
        _provenance_footer(summary)
        return

    area_row = pd.Series()
    if not areas_df.empty:
        m = areas_df[areas_df["public_policy_area"] == area]
        if not m.empty:
            area_row = m.iloc[0]

    ret_cnt = int(area_row.get("return_count", 0) or 0)
    org_cnt = int(area_row.get("distinct_orgs", 0) or 0)
    pol_cnt = int(area_row.get("distinct_politicians", 0) or 0)

    if ret_cnt:
        _quiet_hero(
            title=area,
            dek_html=(
                f"<strong>{ret_cnt:,}</strong> returns filed by "
                f"<strong>{org_cnt}</strong> organisations targeting "
                f"<strong>{pol_cnt}</strong> politicians on this policy area."
            ),
        )
    else:
        _quiet_hero(title=area, dek="No lobbying returns on record for this policy area.")

    # Switch policy area
    if all_area_names:
        st.html(
            "<style>"
            ".st-key-lp3_area_switcher .stSelectbox > div > div,"
            '.st-key-lp3_area_switcher [data-baseweb="select"] > div'
            "{background:#ffffff !important;}"
            "</style>"
        )
        sw_col, _ = st.columns([1, 2])
        with sw_col:
            picked = st.selectbox(
                "Switch policy area",
                all_area_names,
                index=all_area_names.index(area) if area in all_area_names else 0,
                key="lp3_area_switcher",
            )
        if picked and picked != area:
            _clear_lp3_qp()
            st.query_params["lp3_area"] = picked
            st.rerun()

    # Most-targeted politicians — ranked cards, each linking to the area×politician Stage 3.
    _section_head(
        "Most-targeted politicians",
        "Click any card to see every return filed against that politician under this area.",
    )
    area_pols = fetch_politicians_for_area(area)
    if area_pols.empty:
        empty_state("No data", "No politicians found for this policy area.")
    else:
        cards: list[str] = []
        for rank, (_, row) in enumerate(area_pols.head(20).iterrows(), start=1):
            pol_name = str(row.get("member_name", "—"))
            chamber = str(row.get("chamber", "") or "")
            pills_list = [
                f"{int(row.get('returns_targeting', 0) or 0):,} returns",
                f"{int(row.get('distinct_lobbyists', 0) or 0):,} orgs",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lp3_area={quote(area)}&lp3_result_pol={quote(pol_name)}",
                    inner_html=_ranked_card_html(
                        pol_name, chamber, pills_list, rank,
                        avatar_url=avatar_data_url(pol_name),
                        avatar_initials=_initials(pol_name),
                    ),
                    aria_label=f"View every return targeting {pol_name} under {area}",
                )
            )
        st.html("\n".join(cards))
        if len(area_pols) > 20:
            st.caption(f"Showing top 20 of {len(area_pols):,} politicians.")

    # Returns — card list. Politician is the title; org goes on the subtitle.
    detail_all = fetch_area_contact_detail(area)
    _section_head(
        "Lobbying returns for this policy area",
        "Each card links to the original filing on lobbying.ie.",
    )
    if detail_all.empty:
        empty_state("No returns", "No lobbying contact detail on record for this policy area.")
    else:
        start, end = _year_pills(detail_all, "lp3_year_area")
        detail = fetch_area_contact_detail(area, start, end) if start else detail_all
        page_size, page_idx = pagination_controls(
            total=len(detail), key_prefix="lp3_area_returns", label="returns"
        )
        page_slice = detail.iloc[page_idx * page_size : (page_idx + 1) * page_size]
        cards = []
        for _, row in page_slice.iterrows():
            org = str(row.get("lobbyist_name", "") or "")
            pol = str(row.get("member_name", "") or "—")
            cards.append(
                _return_card_html(
                    period=_fmt_mmm(row.get("period_start_date")),
                    title=pol,
                    subtitle=f"lobbied by {org}" if org else "",
                    url=str(row.get("source_url", "") or ""),
                )
            )
        st.html("\n".join(cards))
        export_button(
            detail,
            "Export CSV",
            f"{area[:40].replace(' ', '_')}_lobbying.csv",
            "lp3_export_area_detail",
        )

    _provenance_footer(summary)


# ── Topic Stage 2 (curated free-text scan) ────────────────────────────────────


def _render_topic(topic_name: str, summary: pd.DataFrame) -> None:
    _back_button()

    spec = _CURATED_TOPICS.get(topic_name)
    if not spec:
        empty_state("Unknown topic", f"No curated keyword set defined for '{topic_name}'.")
        _provenance_footer(summary)
        return

    keywords: tuple[str, ...] = tuple(spec["keywords"])  # type: ignore[arg-type]
    blurb = str(spec["blurb"])

    topic_summary = fetch_topic_summary(keywords)
    if topic_summary.empty:
        total = orgs_n = areas_n = 0
        first_p = last_p = "—"
    else:
        s_row = topic_summary.iloc[0]
        total = int(s_row.get("total_returns", 0) or 0)
        orgs_n = int(s_row.get("distinct_orgs", 0) or 0)
        areas_n = int(s_row.get("distinct_areas", 0) or 0)
        first_p = str(s_row.get("first_period", "") or "—")[:10] or "—"
        last_p = str(s_row.get("last_period", "") or "—")[:10] or "—"

    period_clause_html = (
        f" Period covered: <strong>{_h(first_p[:7])}</strong> – <strong>{_h(last_p[:7])}</strong>."
        if first_p != "—"
        else ""
    )
    if total:
        _quiet_hero(
            title=topic_name,
            dek_html=(
                f"{_h(blurb)} <strong>{total:,}</strong> returns match across "
                f"<strong>{orgs_n}</strong> organisations and <strong>{areas_n}</strong> of "
                f"lobbying.ie's official policy areas.{period_clause_html}"
            ),
        )
    else:
        _quiet_hero(title=topic_name, dek=f"{blurb} No matches found in the current dataset.")

    # Single quiet caveat line — one sentence in italic, not a multi-section box.
    keyword_words = ", ".join(keywords)
    st.html(
        '<p class="lp3-prose" style="font-style:italic;color:var(--text-meta);">'
        f"Free-text scan over the <em>relevant matter</em>, <em>specific details</em> and "
        f"<em>intended results</em> fields. Keywords: {_h(keyword_words)}. False positives are "
        "possible — open a return's source link to verify."
        "</p>"
    )

    if total == 0:
        _provenance_footer(summary)
        return

    detail_all = fetch_topic_returns(keywords)
    start, end = _year_pills(detail_all, "lp3_year_topic")
    detail = fetch_topic_returns(keywords, start, end) if start else detail_all

    _section_head(
        "Matching returns",
        "Each card links straight to the original filing on lobbying.ie — open it to read what was lobbied for.",
    )
    page_size, page_idx = pagination_controls(
        total=len(detail), key_prefix=f"lp3_topic_{topic_name}", label="returns"
    )
    page_slice = detail.iloc[page_idx * page_size : (page_idx + 1) * page_size]
    cards: list[str] = []
    for _, row in page_slice.iterrows():
        details = str(row.get("specific_details", "") or "")
        snippet = (details[:260] + "…") if len(details) > 260 else details
        cards.append(
            _return_card_html(
                period=_fmt_mmm(row.get("period_start_date")),
                title=str(row.get("lobbyist_name", "") or "—"),
                area=str(row.get("public_policy_area", "") or ""),
                return_id=str(row.get("return_id", "") or ""),
                snippet=snippet,
                url=str(row.get("source_url", "") or ""),
            )
        )
    st.html("\n".join(cards))

    safe_topic = "".join(c if c.isalnum() else "_" for c in topic_name)[:60]
    export_button(
        detail,
        "Export every matching return as CSV",
        f"topic_{safe_topic}.csv",
        "lp3_export_topic",
    )

    _provenance_footer(summary)


# ── Revolving Door index ──────────────────────────────────────────────────────


def _bucket(label: str) -> str:
    s = label.lower()
    if "dáil" in s or "dail" in s:
        return "Dáil"
    if "seanad" in s:
        return "Seanad"
    if s.startswith("department of") or "department " in s:
        return "Department"
    return "Other body"


def _render_rd_index(summary: pd.DataFrame) -> None:
    _back_button()
    breadcrumb(["Lobbying", "Revolving Door"], key_prefix="lp3_rd_idx")

    rd_summary = fetch_revolving_door_summary()
    rd_n = int(rd_summary.iloc[0].get("individuals", 0) or 0) if not rd_summary.empty else 0
    rd_returns = int(rd_summary.iloc[0].get("total_returns", 0) or 0) if not rd_summary.empty else 0

    _quiet_hero(
        title="Revolving door",
        dek_html=(
            f"<strong>{rd_n:,}</strong> former Designated Public Officials have filed "
            f"<strong>{rd_returns:,}</strong> lobbying returns against their former colleagues. "
            "Identification is by name-matching against the DPO register; treat as indicative, "
            "not a legal finding."
        ),
    )

    all_dpos = fetch_revolving_door(limit=None)
    if all_dpos.empty:
        empty_state("No data", "revolving_door_dpos.parquet has not been produced yet.")
        _provenance_footer(summary)
        return

    # Single selectbox for chamber filter — replaces the 40+ chip strip in lobby_2.
    chambers_present = (
        all_dpos["chamber_display"].dropna().astype(str).replace({"": pd.NA}).dropna().unique().tolist()
        if "chamber_display" in all_dpos.columns
        else []
    )
    bucket_counts: dict[str, int] = {}
    for c in chambers_present:
        bucket_counts[_bucket(c)] = bucket_counts.get(_bucket(c), 0) + 1

    options = [f"All chambers ({len(all_dpos):,})"]
    for bucket in ("Dáil", "Seanad", "Department", "Other body"):
        if bucket_counts.get(bucket):
            options.append(f"{bucket} ({bucket_counts[bucket]:,})")
    options += [f"  {c}" for c in sorted(chambers_present)]

    sw_col, _ = st.columns([1, 2])
    with sw_col:
        choice = st.selectbox("Filter by former chamber", options, key="lp3_rd_chamber_filter")

    if not choice or choice.startswith("All"):
        filtered = all_dpos
    elif choice.startswith(("Dáil ", "Seanad ", "Department ", "Other body ")):
        bucket = choice.split(" (")[0]
        mask = all_dpos["chamber_display"].astype(str).map(_bucket) == bucket
        filtered = all_dpos[mask]
    else:
        filtered = all_dpos[all_dpos["chamber_display"].astype(str) == choice.strip()]

    st.caption(f"Showing {len(filtered):,} of {len(all_dpos):,} individuals.")

    _section_head("All revolving-door individuals")
    if filtered.empty:
        empty_state("No matches", "No DPOs match the current filter.")
    else:
        cards: list[str] = []
        for rank, (_, row) in enumerate(filtered.iterrows(), start=1):
            name = str(row.get("individual_name", "—"))
            position = str(row.get("former_position", "") or "")
            chamber = str(row.get("chamber_display", "") or "")
            former = f"Former {position}" if position else "Former DPO"
            meta = clean_meta(former, chamber)
            pills_list = [
                f"{int(row.get('return_count', 0) or 0):,} returns",
                f"{int(row.get('distinct_firms', 0) or 0):,} firms",
                f"{int(row.get('distinct_politicians_targeted', 0) or 0):,} politicians",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lp3_dpo={quote(name)}",
                    inner_html=_ranked_card_html(
                        name, meta, pills_list, rank,
                        avatar_initials=_initials(name),
                    ),
                    aria_label=f"View revolving-door profile for {name}",
                )
            )
        st.html("\n".join(cards))

    _provenance_footer(summary)


# ── Revolving Door individual ─────────────────────────────────────────────────


def _render_dpo_individual(individual_name: str, summary: pd.DataFrame) -> None:
    _back_button()
    breadcrumb(["Lobbying", "Revolving Door", individual_name], key_prefix="lp3_dpo")

    dpo_df = fetch_dpo_one(individual_name)
    if dpo_df.empty:
        empty_state(
            "Individual not found",
            f"No revolving-door entry on record for '{individual_name}'.",
        )
        _provenance_footer(summary)
        return

    dpo_row = dpo_df.iloc[0]
    position = str(dpo_row.get("former_position", "") or "")
    chamber = str(dpo_row.get("chamber_display", "") or "")
    ret_cnt = int(dpo_row.get("return_count", 0) or 0)
    firm_cnt = int(dpo_row.get("distinct_firms", 0) or 0)
    pol_cnt = int(dpo_row.get("distinct_politicians_targeted", 0) or 0)
    area_cnt = int(dpo_row.get("distinct_policy_areas", 0) or 0)
    former = f"Former {position}" if position else "Former DPO"
    chamber_clause_html = f" in the {_h(chamber)}" if chamber else ""

    if ret_cnt:
        _quiet_hero(
            title=individual_name,
            dek_html=(
                f"{_h(former)}{chamber_clause_html}. Filed <strong>{ret_cnt:,}</strong> "
                f"lobbying returns across <strong>{firm_cnt:,}</strong> firms, targeting "
                f"<strong>{pol_cnt:,}</strong> politicians on <strong>{area_cnt:,}</strong> "
                "policy areas."
            ),
        )
    else:
        _quiet_hero(
            title=individual_name,
            dek=f"{former}. No lobbying returns on record for this individual.",
        )

    # Firms represented — Datasette table
    firms_df = fetch_dpo_firms(individual_name)
    if not firms_df.empty:
        _section_head("Firms represented")
        _datasette_table(
            firms_df,
            columns={
                "lobbyist_name": "Firm",
                "return_count": "Returns",
                "first_period": "First filing",
                "last_period": "Last filing",
            },
        )

    # Clients represented — Datasette table
    clients_df = fetch_dpo_client_breakdown(individual_name)
    if not clients_df.empty:
        _section_head("Clients represented")
        _datasette_table(
            clients_df,
            columns={
                "client_name": "Client",
                "return_count": "Returns",
                "first_period": "First filing",
                "last_period": "Last filing",
            },
        )

    # Politicians targeted — ranked card list (cross-link to canonical /member-overview)
    pols_df = fetch_dpo_politicians_targeted(individual_name)
    if not pols_df.empty:
        _section_head("Politicians targeted")
        pol_known = set(fetch_all_politician_names())
        cards: list[str] = []
        for rank, (_, prow) in enumerate(pols_df.head(20).iterrows(), start=1):
            pname = str(prow.get("member_name", "—"))
            pchm = str(prow.get("chamber", "") or "")
            pcnt = int(prow.get("return_count", 0) or 0)
            pills_list = [f"{pcnt:,} returns"]
            inner = _ranked_card_html(
                pname, pchm, pills_list, rank,
                avatar_url=avatar_data_url(pname),
                avatar_initials=_initials(pname),
            )
            if pname in pol_known:
                cards.append(
                    clickable_card_link(
                        href=member_profile_url(name_join_key(pname), section="lobbying"),
                        inner_html=inner,
                        aria_label=f"Open profile for {pname}",
                    )
                )
            else:
                cards.append(inner)
        st.html("\n".join(cards))
        if len(pols_df) > 20:
            st.caption(f"Showing top 20 of {len(pols_df):,} politicians.")

    # Lobbying returns — card list. Firm = title, client on subtitle.
    returns_df = fetch_dpo_returns_detail(individual_name)
    if not returns_df.empty:
        _section_head(
            "Lobbying returns",
            "Each card links to the original filing on lobbying.ie.",
        )
        safe_name = "".join(c if c.isalnum() else "_" for c in individual_name)[:60]
        page_size, page_idx = pagination_controls(
            total=len(returns_df), key_prefix="lp3_rd_returns", label="returns"
        )
        page_slice = returns_df.iloc[page_idx * page_size : (page_idx + 1) * page_size]
        cards = []
        for _, row in page_slice.iterrows():
            client = str(row.get("client_name", "") or "")
            cards.append(
                _return_card_html(
                    period=_fmt_mmm(row.get("period_start_date")),
                    title=str(row.get("lobbyist_name", "") or "—"),
                    subtitle=f"on behalf of {client}" if client else "",
                    area=str(row.get("public_policy_area", "") or ""),
                    url=str(row.get("source_url", "") or ""),
                )
            )
        st.html("\n".join(cards))
        export_button(
            returns_df,
            "Export returns CSV",
            f"{safe_name}_revolving_door_returns.csv",
            "lp3_rd_export_returns",
        )

    _provenance_footer(summary)


# ── Area × Politician (Stage 3) ───────────────────────────────────────────────


def _render_results(area: str, politician: str, summary: pd.DataFrame) -> None:
    # Custom back goes to the area page, not the landing.
    if back_button(f"← Back to {area}", key="lp3_results"):
        st.query_params.pop("lp3_result_pol", None)
        st.rerun()

    detail_all = fetch_politician_area_returns(politician, area)
    if not detail_all.empty:
        _quiet_hero(
            title=f"{politician} on {area}",
            dek_html=(
                f"<strong>{len(detail_all):,}</strong> returns filed against "
                f"{_h(politician)} under the policy area '{_h(area)}'. Each card links to the "
                "original lobbying.ie return — open it to read exactly what was lobbied for."
            ),
        )
    else:
        _quiet_hero(
            title=f"{politician} on {area}",
            dek=f"No returns on record for {politician} under '{area}'.",
        )
    if detail_all.empty:
        _provenance_footer(summary)
        return

    start, end = _year_pills(detail_all, "lp3_year_results")
    detail = (
        fetch_politician_area_returns(politician, area, start, end) if start else detail_all
    )

    _section_head(
        "Every return",
        "Each card links to the original filing on lobbying.ie.",
    )
    page_size, page_idx = pagination_controls(
        total=len(detail), key_prefix="lp3_results_page", label="returns"
    )
    page_slice = detail.iloc[page_idx * page_size : (page_idx + 1) * page_size]
    cards = [
        _return_card_html(
            period=_fmt_mmm(row.get("period_start_date")),
            title=str(row.get("lobbyist_name", "") or "—"),
            return_id=str(row.get("return_id", "") or ""),
            url=str(row.get("source_url", "") or ""),
        )
        for _, row in page_slice.iterrows()
    ]
    st.html("\n".join(cards))
    export_button(
        detail,
        "Export every return as CSV",
        f"{politician.replace(' ', '_')}_{area[:30].replace(' ', '_')}_returns.csv",
        "lp3_export_results",
    )

    _provenance_footer(summary)


# ── Entry point ────────────────────────────────────────────────────────────────


@page_error_boundary
def lobbying_poc_page() -> None:
    _init()
    inject_css()

    qp = st.query_params
    _render_sidebar()

    summary = fetch_summary()

    # Dispatch by URL parameter. lp3_area + lp3_result_pol = Stage 3.
    if "lp3_dpo" in qp:
        _render_dpo_individual(qp["lp3_dpo"], summary)
    elif "lp3_rd" in qp:
        _render_rd_index(summary)
    elif "lp3_orgindex" in qp:
        _render_org_index(summary)
    elif "lp3_topic" in qp:
        _render_topic(qp["lp3_topic"], summary)
    elif "lp3_org" in qp:
        _render_org(qp["lp3_org"], summary)
    elif "lp3_area" in qp:
        if "lp3_result_pol" in qp:
            _render_results(qp["lp3_area"], qp["lp3_result_pol"], summary)
        else:
            _render_area(qp["lp3_area"], summary)
    else:
        _render_landing(summary)


if __name__ == "__main__":
    lobbying_poc_page()
