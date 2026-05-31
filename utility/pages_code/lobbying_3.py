"""Lobbying — lobbying_3.py

The lobbying register page (served at /rankings-lobbying). Calm, hybrid
treatment: TheyWorkForYou-style prose heroes for Stage 2s, Datasette-style
tables for returns lists, ranked cards only for drill-downs (top politicians
per org, top orgs per area).

Data layer:
- data_access.lobbying_data.fetch_* (DuckDB registered views; no raw reads here)
- ui.components helpers (back_button, breadcrumb, empty_state, ...)
- ui.entity_links (member_profile_url, name_join_key, source_link_html)

CSS namespace (lp3-* prefix):
- lp3-hero, lp3-dek, lp3-prose
- lp3-tile (quiet gateway tile)
- lp3-topic-tile (calmer topic card)
- lp3-section-head (quiet H2 with one-line dek)

Stages: landing gateway → org / area / topic / revolving-door / DPO /
politician → Stage 3 (org×politician, area×politician, DPO×politician).
Stage 3 sub-routes keep the lobbyist context: the politician is only
relevant because a specific org / area / DPO targeted them, so the cards
filter the returns instead of dropping the user into the generic
/member-overview profile. A secondary CTA at the bottom of each Stage 3
page still points at the full accountability profile.

The per-politician body (render_member_lobbying) is embedded on
/member-overview (show_header=False) and also reached standalone via
?lp3_pol=X from the landing's most-lobbied list.
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
    fetch_contact_detail,
    fetch_dpo_client_breakdown,
    fetch_dpo_firms,
    fetch_dpo_one,
    fetch_dpo_politician_returns,
    fetch_dpo_politicians_targeted,
    fetch_dpo_returns_detail,
    fetch_org_contact_detail,
    fetch_org_index,
    fetch_org_persistence,
    fetch_org_politician_returns,
    fetch_orgs_for_politician,
    fetch_policy_area_summary,
    fetch_policy_exposure_for_politician,
    fetch_politician_area_returns,
    fetch_politician_index,
    fetch_politicians_for_area,
    fetch_politicians_for_org,
    fetch_recent_returns,
    fetch_return_documents_for_org,
    fetch_revolving_door,
    fetch_revolving_door_summary,
    fetch_sources_for_org,
    fetch_sources_for_politician,
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
    evidence_heading,
    field_label,
    filter_bar,
    hero_banner,
    hide_sidebar,
    page_error_boundary,
    pagination_controls,
    period_year_pills as _year_pills,
    pill,
    ranked_member_card,
    totals_strip,
)
from data_access.identity_resolver import resolve_member_code
from ui.entity_links import entity_cta_html, member_profile_url, name_join_key, source_link_html
from ui.export_controls import export_button
from ui.source_links import render_source_links


def _resolve_or_join(name: str) -> str:
    """Prefer the canonical registry code; fall back to the deprecated
    sorted-letters join key only when the name isn't in v_member_registry.
    Matches the post-round-3 contract used by every other dimension page.
    """
    return resolve_member_code(name) or name_join_key(name)


def _p(n: int, singular: str, plural: str | None = None) -> str:
    """Pluralise a count with thousand-separators. `_p(1, 'firm')` → '1 firm'."""
    word = singular if n == 1 else (plural or singular + "s")
    return f"{n:,} {word}"


def _fmt_period(value: object) -> str:
    """Friendly month-year format for ISO date / period strings.
    `2025-09-01` → `Sep 2025`; `2025-09` → `Sep 2025`; falsy → '—'.
    """
    if value is None or value == "" or value == "None":
        return "—"
    try:
        return pd.to_datetime(value).strftime("%b %Y")
    except Exception:
        return str(value)
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
    """Reuses ``ranked_member_card`` so the calm look stays consistent with
    the rest of the app's ranked-list pages (Attendance, Payments). Pills
    rendered via the canonical ``pill()`` helper — escaping is safe."""
    return ranked_member_card(
        name=name,
        meta=meta,
        rank=rank,
        pills_html="".join(pill(p) for p in pills_list),
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
            f"Dataset covers: {_fmt_period(fp)} → {_fmt_period(lp)}  ·  Last fetched: {ts}",
        ]
    )


# ── Sidebar ────────────────────────────────────────────────────────────────────


def _render_search_bar() -> None:
    """Main-panel typeahead. One control: a Streamlit selectbox whose built-in
    type-to-filter behaviour replaces the previous two-input pattern (text
    input + adjacent dropdown) where Enter on the text input did nothing —
    confusing reporters who typed "Ibec" or "greyhound" and saw no response.

    Picking a politician routes to their canonical /member-overview; picking
    an org sets ?lp3_org=<name> on this page.
    """
    pol_names = fetch_all_politician_names()
    org_names = fetch_all_org_names()
    # Politicians first, then orgs (prefixed so the routing branch can
    # distinguish without a second lookup).
    combined = [""] + pol_names + [f"[Org] {n}" for n in org_names]

    with filter_bar([6, 6]) as cols:
        with cols[0]:
            field_label("Search the register")
            sel = st.selectbox(
                "Search",
                combined,
                index=0,
                label_visibility="collapsed",
                placeholder="e.g. Ibec, Mary Lou McDonald",
                key="lp3_jump",
            )

    if sel:
        _clear_lp3_qp()
        if sel.startswith("[Org] "):
            st.query_params["lp3_org"] = sel[6:]
        else:
            target = member_profile_url(_resolve_or_join(sel), section="lobbying")
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
        f" The dataset covers returns from <strong>{_h(_fmt_period(first_p))}</strong> "
        f"to <strong>{_h(_fmt_period(last_p))}</strong>."
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

    # Search + jump (was the sidebar) — sits under the hero on the landing.
    _render_search_bar()

    # Gateway — three matching quiet tiles. Identical shape across the trio
    # (heading + one sentence). Whole tile is the click target via the
    # clickable_card_link stretched-link pattern — no separate buttons,
    # matches the rest of the app's ranked-card affordance contract.
    _section_head(
        "Where do you want to investigate?",
        "Three entry points into the register. The data underneath is the same; the angle differs.",
    )

    # Pre-compute URLs so each tile becomes a real <a href> at render-time.
    pol_idx = fetch_politician_index()
    if not pol_idx.empty:
        m_id = str(pol_idx.iloc[0].get("unique_member_code", "") or "")
        m_name = str(pol_idx.iloc[0].get("member_name", ""))
        pol_gateway_href = member_profile_url(m_id or _resolve_or_join(m_name), section="lobbying")
    else:
        pol_gateway_href = "#"
    org_gateway_href = f"?lp3_orgindex=1"
    area_summary = fetch_policy_area_summary()
    if not area_summary.empty:
        first_area = str(area_summary.iloc[0]["public_policy_area"])
        area_gateway_href = f"?lp3_area={quote(first_area)}"
    else:
        area_gateway_href = "#"

    g1, g2, g3 = st.columns(3)
    with g1:
        st.html(
            clickable_card_link(
                href=pol_gateway_href,
                inner_html=_tile_html(
                    "Follow a politician",
                    "See every lobbying return targeting a specific TD, senator, or minister.",
                ),
                aria_label="Open a politician profile",
            )
        )
    with g2:
        st.html(
            clickable_card_link(
                href=org_gateway_href,
                inner_html=_tile_html(
                    "Follow an organisation",
                    "See which politicians an organisation has lobbied, on what topics, and over what period.",
                ),
                aria_label="Browse all lobbying organisations",
            )
        )
    with g3:
        st.html(
            clickable_card_link(
                href=area_gateway_href,
                inner_html=_tile_html(
                    "Browse by policy area",
                    "Find every return filed under one of lobbying.ie's 32 registered public policy areas.",
                ),
                aria_label="Browse lobbying by policy area",
            )
        )

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
            st.html(
                clickable_card_link(
                    href=f"?lp3_topic={quote(topic_name)}",
                    inner_html=_topic_tile_html(topic_name, str(spec["blurb"])),
                    aria_label=f"Open {topic_name} returns",
                )
            )

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
                meta = clean_meta(str(row.get("chamber", "") or ""), str(row.get("position", "") or ""))
                pills_list = [
                    _p(int(row.get("return_count", 0) or 0), "return"),
                    _p(int(row.get("distinct_orgs", 0) or 0), "org"),
                ]
                inner = _ranked_card_html(
                    name, meta, pills_list, rank,
                    avatar_url=avatar_data_url(name),
                    avatar_initials=_initials(name),
                )
                cards.append(
                    clickable_card_link(
                        href=f"?lp3_pol={quote(name)}",
                        inner_html=inner,
                        aria_label=f"View lobbying record for {name}",
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
                    _p(int(row.get("return_count", 0) or 0), "return"),
                    _p(int(row.get("politicians_targeted", 0) or 0), "politician"),
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
                _p(int(row.get("return_count", 0) or 0), "return"),
                _p(int(row.get("distinct_politicians_targeted", 0) or 0), "politician"),
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
            url = str(row.get("source_url", "") or "")
            area_clause = f" under <em>{_h(area)}</em>" if area else ""
            inner = f"<strong>{_h(org)}</strong>{area_clause}"
            if url.startswith("http"):
                body = (
                    f'<a class="lp3-recent-link" href="{_h(url)}" target="_blank" '
                    f'rel="noopener" aria-label="Open the {_h(org)} return on lobbying.ie">'
                    f"{inner}</a>"
                )
            else:
                body = f'<span class="lp3-recent-body">{inner}</span>'
            rows.append(
                f'<li class="lp3-recent-item">'
                f'<span class="lp3-recent-period">{_h(period)}</span> '
                f"{body}"
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


def _return_card_html(
    *,
    period: str,
    title: str,
    subtitle: str = "",
    area: str = "",
    return_id: str = "",
    snippet: str = "",
    url: str = "",
    filed_by: str = "",
) -> str:
    """Canonical return-record card. Used wherever a list of
    lobbying returns is displayed — topic Stage 2, org / area / DPO / Stage 3.

    Header row: period chip, optional area pill, return-# on the right.
    Body: title in serif (the variable field — politician, org, or firm),
    optional second-line subtitle (client / "on behalf of …"), optional
    "Filed by …" meta line (person_primarily_responsible from the lobbying.ie
    return), optional snippet of free-text details, styled `↗` source-link
    in the actions row.
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
    filed_by_clean = (filed_by or "").strip()
    # The free-text source has rare paragraph-length entries (max seen: 334
    # chars). Cap so card height stays predictable when paginating; the full
    # string is one click away on lobbying.ie via the "View" link below.
    if len(filed_by_clean) > 80:
        filed_by_clean = filed_by_clean[:80].rstrip(" ,.;:-") + "…"
    filed_html = (
        f'<p class="lp3-return-filed-by"><strong>Filed by</strong> {_h(filed_by_clean)}</p>'
        if filed_by_clean
        else ""
    )
    snippet_html = f'<p class="lp3-return-snippet">{_h(snippet)}</p>' if snippet else ""
    actions_html = f'<div class="lp3-return-actions">{link_html}</div>' if link_html else ""

    return (
        '<article class="lp3-return-card">'
        f'<header class="lp3-return-head">{head_html}</header>'
        f'<p class="lp3-return-org">{_h(title)}</p>'
        f"{sub_html}{filed_html}{snippet_html}{actions_html}"
        "</article>"
    )


def _datasette_table(detail: pd.DataFrame, columns: dict[str, str], height: int | None = None) -> None:
    """Plain Datasette-tone dataframe. DateColumn for any 'Period' / 'First
    filing' / 'Last filing' column, LinkColumn for any 'Return URL' column.
    Everything else is plain TextColumn or NumberColumn.

    Defensive against schema drift: when the source view has been updated
    and none of the expected columns are present, emit an empty_state
    callout instead of rendering a silent grey rectangle (audit P0-1).
    """
    if not columns:
        return
    keep = [c for c in columns if c in detail.columns]
    if not keep or detail.empty:
        empty_state(
            "No data available",
            "Records exist but the expected columns are missing — the view "
            "shape may have drifted. Run the pipeline to refresh.",
        )
        return
    display = detail[keep].rename(columns=columns)
    if display.empty:
        empty_state("No data available", "No matching records to display.")
        return
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
            _p(int(row.get("return_count", 0) or 0), "return"),
            _p(int(row.get("politicians_targeted", 0) or 0), "politician"),
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
        f" Active <strong>{_h(_fmt_period(first_p))}</strong> "
        f"to <strong>{_h(_fmt_period(last_p))}</strong>."
        if first_p and last_p and first_p != "None" and last_p != "None"
        else ""
    )
    if ret_cnt:
        _quiet_hero(
            title=org_name,
            dek_html=(
                f"Filed <strong>{ret_cnt:,}</strong> lobbying returns targeting "
                f"<strong>{pol_cnt:,}</strong> politicians across "
                f"<strong>{area_cnt:,}</strong> policy areas.{sector_clause_html}{period_clause_html}"
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
            chamber = str(row.get("chamber", "") or "")
            first_c = str(row.get("first_contact", "") or "")[:7]
            last_c = str(row.get("last_contact", "") or "")[:7]
            meta = clean_meta(chamber, first_c, last_c)
            pills_list = [
                _p(int(row.get("returns_in_relationship", 0) or 0), "return"),
                _p(int(row.get("distinct_policy_areas", 0) or 0), "policy area"),
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lp3_org={quote(org_name)}&lp3_result_pol={quote(pol_name)}",
                    inner_html=_ranked_card_html(
                        pol_name, meta, pills_list, rank,
                        avatar_url=avatar_data_url(pol_name),
                        avatar_initials=_initials(pol_name),
                    ),
                    aria_label=f"View every return from {org_name} targeting {pol_name}",
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
        cards = []
        for _, row in page_slice.iterrows():
            wanted = str(row.get("intended_results", "") or "").strip()
            snippet = (wanted[:260] + "…") if len(wanted) > 260 else wanted
            cards.append(
                _return_card_html(
                    period=_fmt_mmm(row.get("period_start_date")),
                    title=str(row.get("member_name", "") or "—"),
                    area=str(row.get("public_policy_area", "") or ""),
                    snippet=snippet,
                    url=str(row.get("source_url", "") or ""),
                    filed_by=str(row.get("person_primarily_responsible", "") or ""),
                )
            )
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
                f"<strong>{org_cnt:,}</strong> organisations targeting "
                f"<strong>{pol_cnt:,}</strong> politicians on this policy area."
            ),
        )
    else:
        _quiet_hero(title=area, dek="No lobbying returns on record for this policy area.")

    # Switch policy area
    if all_area_names:
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
                _p(int(row.get("returns_targeting", 0) or 0), "return"),
                _p(int(row.get("distinct_lobbyists", 0) or 0), "org"),
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
                    filed_by=str(row.get("person_primarily_responsible", "") or ""),
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
                filed_by=str(row.get("person_primarily_responsible", "") or ""),
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
                _p(int(row.get("return_count", 0) or 0), "return"),
                _p(int(row.get("distinct_firms", 0) or 0), "firm"),
                _p(int(row.get("distinct_politicians_targeted", 0) or 0), "politician"),
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
        _section_head(
            "Politicians targeted",
            "Click any card to see every return this individual filed targeting that politician.",
        )
        cards: list[str] = []
        for rank, (_, prow) in enumerate(pols_df.head(20).iterrows(), start=1):
            pname = str(prow.get("member_name", "—"))
            pchm = str(prow.get("chamber", "") or "")
            pcnt = int(prow.get("return_count", 0) or 0)
            pills_list = [_p(pcnt, "return")]
            inner = _ranked_card_html(
                pname, pchm, pills_list, rank,
                avatar_url=avatar_data_url(pname),
                avatar_initials=_initials(pname),
            )
            cards.append(
                clickable_card_link(
                    href=f"?lp3_dpo={quote(individual_name)}&lp3_result_pol={quote(pname)}",
                    inner_html=inner,
                    aria_label=f"View every return from {individual_name} targeting {pname}",
                )
            )
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
            filed_by=str(row.get("person_primarily_responsible", "") or ""),
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


# ── Org × Politician (Stage 3) ────────────────────────────────────────────────


def _render_org_results(org_name: str, politician: str, summary: pd.DataFrame) -> None:
    """Every return filed by ``org_name`` targeting ``politician``.

    Mirrors the area×politician Stage 3 shape so users staying inside the
    lobbying page see exactly what the org lobbied this politician on,
    instead of the generic /member-overview profile (the lobbyist context
    would otherwise be lost the moment they click).
    """
    if back_button(f"← Back to {org_name}", key="lp3_org_results"):
        st.query_params.pop("lp3_result_pol", None)
        st.rerun()

    detail_all = fetch_org_politician_returns(org_name, politician)
    if not detail_all.empty:
        _quiet_hero(
            title=f"{politician} lobbied by {org_name}",
            dek_html=(
                f"<strong>{len(detail_all):,}</strong> returns filed by "
                f"{_h(org_name)} targeting {_h(politician)}. Each card links to the "
                "original filing on lobbying.ie."
            ),
        )
    else:
        _quiet_hero(
            title=f"{politician} lobbied by {org_name}",
            dek=f"No returns on record from {org_name} targeting {politician}.",
        )
    if detail_all.empty:
        _provenance_footer(summary)
        return

    start, end = _year_pills(detail_all, "lp3_year_org_results")
    detail = (
        fetch_org_politician_returns(org_name, politician, start, end) if start else detail_all
    )

    _section_head(
        "Every return",
        "Each card links to the original filing on lobbying.ie.",
    )
    page_size, page_idx = pagination_controls(
        total=len(detail), key_prefix="lp3_org_results_page", label="returns"
    )
    page_slice = detail.iloc[page_idx * page_size : (page_idx + 1) * page_size]
    cards = [
        _return_card_html(
            period=_fmt_mmm(row.get("period_start_date")),
            title=str(row.get("lobbyist_name", "") or "—"),
            area=str(row.get("public_policy_area", "") or ""),
            return_id=str(row.get("return_id", "") or ""),
            url=str(row.get("source_url", "") or ""),
            filed_by=str(row.get("person_primarily_responsible", "") or ""),
        )
        for _, row in page_slice.iterrows()
    ]
    st.html("\n".join(cards))
    export_button(
        detail,
        "Export every return as CSV",
        f"{politician.replace(' ', '_')}_x_{org_name[:30].replace(' ', '_')}_returns.csv",
        "lp3_export_org_results",
    )

    # Secondary CTA — full accountability profile is still one click away.
    st.html(
        entity_cta_html(
            member_profile_url(_resolve_or_join(politician)),
            f"View {politician}'s full accountability profile →",
        )
    )

    _provenance_footer(summary)


# ── DPO × Politician (Stage 3) ────────────────────────────────────────────────


def _render_dpo_results(individual_name: str, politician: str, summary: pd.DataFrame) -> None:
    """Every return filed by former-DPO ``individual_name`` targeting ``politician``."""
    if back_button(f"← Back to {individual_name}", key="lp3_dpo_results"):
        st.query_params.pop("lp3_result_pol", None)
        st.rerun()

    detail_all = fetch_dpo_politician_returns(individual_name, politician)
    if not detail_all.empty:
        _quiet_hero(
            title=f"{politician} lobbied by {individual_name}",
            dek_html=(
                f"<strong>{len(detail_all):,}</strong> returns filed by former DPO "
                f"{_h(individual_name)} targeting {_h(politician)}. Each card links to the "
                "original filing on lobbying.ie."
            ),
        )
    else:
        _quiet_hero(
            title=f"{politician} lobbied by {individual_name}",
            dek=f"No returns on record from {individual_name} targeting {politician}.",
        )
    if detail_all.empty:
        _provenance_footer(summary)
        return

    start, end = _year_pills(detail_all, "lp3_year_dpo_results")
    detail = (
        fetch_dpo_politician_returns(individual_name, politician, start, end)
        if start
        else detail_all
    )

    _section_head(
        "Every return",
        "Each card links to the original filing on lobbying.ie.",
    )
    page_size, page_idx = pagination_controls(
        total=len(detail), key_prefix="lp3_dpo_results_page", label="returns"
    )
    page_slice = detail.iloc[page_idx * page_size : (page_idx + 1) * page_size]
    cards = []
    for _, row in page_slice.iterrows():
        client = str(row.get("client_name", "") or "")
        cards.append(
            _return_card_html(
                period=_fmt_mmm(row.get("period_start_date")),
                title=str(row.get("lobbyist_name", "") or "—"),
                subtitle=f"on behalf of {client}" if client else "",
                area=str(row.get("public_policy_area", "") or ""),
                return_id=str(row.get("return_id", "") or ""),
                url=str(row.get("source_url", "") or ""),
            )
        )
    st.html("\n".join(cards))
    safe_ind = "".join(c if c.isalnum() else "_" for c in individual_name)[:60]
    export_button(
        detail,
        "Export every return as CSV",
        f"{politician.replace(' ', '_')}_x_{safe_ind}_returns.csv",
        "lp3_export_dpo_results",
    )

    # Secondary CTA — full accountability profile is still one click away.
    st.html(
        entity_cta_html(
            member_profile_url(_resolve_or_join(politician)),
            f"View {politician}'s full accountability profile →",
        )
    )

    _provenance_footer(summary)


# ── Per-politician body (embedded on /member-overview) ──────────────────────────
#
# Migrated verbatim from the retired lobbying_2.py. member_overview.py embeds
# this with show_header=False; the standalone-page branches (back button, hero,
# CTA, provenance footer) reuse this module's own _back_button/_provenance_footer.


def _lob_card_html(
    name: str,
    meta: str,
    pills: list[str],
    *,
    rank: int | None = None,
    profile_href: str = "",
) -> str:
    """Lobbying ranked-list card body — pill row with optional "Profile ↗"
    link to the canonical /member-overview profile.
    """
    return ranked_member_card(
        name=name,
        meta=meta,
        rank=rank,
        pills_html="".join(pill(p) for p in pills),
        profile_href=profile_href,
    )


def render_member_lobbying(
    name: str,
    summary: pd.DataFrame | None = None,
    *,
    show_header: bool = True,
    year_pill_key: str = "lob_year_pol",
) -> None:
    """Render the per-politician lobbying body.

    Public so :mod:`pages_code.member_overview` can embed it inside the
    Lobbying expander. When ``show_header=False``, the back button, lobbying-
    specific hero, "View full accountability profile" CTA and provenance
    footer are all skipped (the embedding page provides those).

    ``year_pill_key`` is overridable so the embedded copy can use a key that
    doesn't collide with the stand-alone lobbying-page state.
    """
    if show_header:
        _back_button()

    if summary is None:
        summary = fetch_summary()

    idx = fetch_politician_index()
    pol_row = pd.Series()
    if not idx.empty and "member_name" in idx.columns:
        m = idx[idx["member_name"] == name]
        if not m.empty:
            pol_row = m.iloc[0]

    chamber = str(pol_row.get("chamber", "") or "")
    position = str(pol_row.get("position", "") or "")
    member_id = str(pol_row.get("unique_member_code", "") or "")
    ret_cnt = int(pol_row.get("return_count", 0) or 0)
    org_cnt = int(pol_row.get("distinct_orgs", 0) or 0)
    area_cnt = int(pol_row.get("distinct_policy_areas", 0) or 0)
    first_p = str(pol_row.get("first_period", "") or "")
    last_p = str(pol_row.get("last_period", "") or "")

    if show_header:
        meta_badges = [b for b in [chamber, position] if b]
        hero_banner(
            kicker="LOBBYING PROFILE · POLITICIAN",
            title=name,
            dek=(
                f"Lobbied across {area_cnt} policy area(s) by {org_cnt} organisation(s). "
                f"Returns span {first_p} → {last_p}."
                if ret_cnt
                else "No lobbying returns on record for this politician."
            ),
            badges=meta_badges or None,
        )

    totals_strip(
        [
            (f"{ret_cnt:,}", "Returns targeting them"),
            (f"{org_cnt:,}", "Distinct organisations"),
            (f"{area_cnt:,}", "Policy areas"),
        ]
    )

    # Cross-page CTA only in stand-alone mode — embedded copy already lives
    # on the canonical profile page, so the link would point at itself.
    if show_header and member_id:
        st.html(
            entity_cta_html(
                member_profile_url(member_id),
                "View full accountability profile →",
            )
        )

    # ── Orgs by intensity — ranked cards (primary view) ───────────────────
    evidence_heading("Organisations lobbying this politician")
    intensity = fetch_orgs_for_politician(name)
    if intensity.empty:
        empty_state("No intensity data", "No organisations found lobbying this politician.")
    else:
        cards: list[str] = []
        for rank, (_, row) in enumerate(intensity.iterrows(), start=1):
            org_name = str(row.get("lobbyist_name", "—"))
            first_c = str(row.get("first_contact", "") or "")[:7]
            last_c = str(row.get("last_contact", "") or "")[:7]
            meta = clean_meta(first_c, last_c)
            pills = [
                f"{int(row.get('returns_in_relationship', 0) or 0):,} returns",
                f"{int(row.get('distinct_policy_areas', 0) or 0):,} policy areas",
                f"{int(row.get('distinct_periods', 0) or 0):,} periods",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lp3_org={quote(org_name)}",
                    inner_html=_lob_card_html(org_name, meta, pills, rank=rank),
                    aria_label=f"View lobbying profile for {org_name}",
                )
            )
        st.html("\n".join(cards))

    # ── Policy exposure ───────────────────────────────────────────────────
    evidence_heading("Policy areas lobbied on")
    exposure = fetch_policy_exposure_for_politician(name)
    if not exposure.empty:
        disp2 = exposure.rename(
            columns={
                "public_policy_area": "Policy area",
                "returns_targeting": "Returns",
                "distinct_lobbyists": "Organisations",
            }
        )
        max_ret = int(disp2["Returns"].max()) if not disp2.empty else 1
        st.dataframe(
            disp2,
            width="stretch",
            hide_index=True,
            column_config={
                "Policy area": st.column_config.TextColumn("Policy area"),
                "Returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_ret),
                "Organisations": st.column_config.NumberColumn("Organisations"),
            },
        )

    # ── Lobbying returns ──────────────────────────────────────────────────
    detail_all = fetch_contact_detail(name)
    evidence_heading("Lobbying returns")
    if detail_all.empty:
        empty_state("No lobbying returns", "No contact detail on record for this politician.")
    else:
        start, end = _year_pills(detail_all, year_pill_key)
        detail = fetch_contact_detail(name, start, end) if start else detail_all
        display = detail[
            [
                c
                for c in ["period_start_date", "lobbyist_name", "public_policy_area", "source_url"]
                if c in detail.columns
            ]
        ].rename(
            columns={
                "period_start_date": "Period",
                "lobbyist_name": "Organisation",
                "public_policy_area": "Policy area",
                "source_url": "Return URL",
            }
        )
        st.dataframe(
            display,
            column_config={
                "Return URL": st.column_config.LinkColumn(
                    "Return URL", display_text=r"https://www\.lobbying\.ie/return/(\d+)"
                )
            },
            width="stretch",
            hide_index=True,
        )
        export_button(detail, "Export CSV", f"{name.replace(' ', '_')}_lobbying.csv", "lob_export_pol_detail")

    # ── Official source links ─────────────────────────────────────────────
    evidence_heading("Official source links")
    render_source_links(fetch_sources_for_politician(name))

    if show_header:
        _provenance_footer(summary)


# ── Entry point ────────────────────────────────────────────────────────────────


@page_error_boundary
def lobbying_poc_page() -> None:
    _init()
    inject_css()

    qp = st.query_params
    # Sidebar→filter-bar migration: the search/jump moved into a main-panel
    # bar under the landing hero (see _render_search_bar in _render_landing).
    hide_sidebar()

    summary = fetch_summary()

    # Dispatch by URL parameter. *_pol + lp3_result_pol = Stage 3.
    if "lp3_dpo" in qp:
        if "lp3_result_pol" in qp:
            _render_dpo_results(qp["lp3_dpo"], qp["lp3_result_pol"], summary)
        else:
            _render_dpo_individual(qp["lp3_dpo"], summary)
    elif "lp3_rd" in qp:
        _render_rd_index(summary)
    elif "lp3_orgindex" in qp:
        _render_org_index(summary)
    elif "lp3_topic" in qp:
        _render_topic(qp["lp3_topic"], summary)
    elif "lp3_org" in qp:
        if "lp3_result_pol" in qp:
            _render_org_results(qp["lp3_org"], qp["lp3_result_pol"], summary)
        else:
            _render_org(qp["lp3_org"], summary)
    elif "lp3_area" in qp:
        if "lp3_result_pol" in qp:
            _render_results(qp["lp3_area"], qp["lp3_result_pol"], summary)
        else:
            _render_area(qp["lp3_area"], summary)
    elif "lp3_pol" in qp:
        render_member_lobbying(qp["lp3_pol"], summary, show_header=True, year_pill_key="lp3_year_pol")
    else:
        _render_landing(summary)


if __name__ == "__main__":
    lobbying_poc_page()
