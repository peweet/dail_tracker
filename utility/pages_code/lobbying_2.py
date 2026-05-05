"""
Lobbying — lobbying_2.py  (bold redesign)

Investigative lookup tool with three-path gateway.
Entry points: follow a politician · follow an organisation · browse by policy area.

Architecture:
- All data via DuckDB registered views (sql_views/lobbying_*.sql)
- No raw CSV/Parquet reads in this file
- No joins, groupby, or pivot in this file
- Two-stage flow: landing gateway → Stage 2 profile

TODO_PIPELINE_VIEW_REQUIRED: unique_member_code on the following lobbying views
so politician names rendered from them can link to /member-overview?member=…:
  - v_lobbying_org_intensity      (politicians targeted by an org)
  - v_lobbying_policy_exposure    (politicians for a policy area)
  - v_lobbying_recent_returns     (recent returns ticker)
  - v_lobbying_contact_detail     (per-return rows)
  - v_lobbying_revolving_door     (DPO ↔ politician overlap)
Until these carry the ID, only the Stage 2 politician profile (sourced from
v_lobbying_index) gets a cross-page CTA. Name-based fallback URLs are not
acceptable — they break silently on name normalisation.
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
    fetch_dpo_politicians_targeted,
    fetch_dpo_return_map,
    fetch_dpo_returns_detail,
    fetch_org_contact_detail,
    fetch_org_index,
    fetch_org_persistence,
    fetch_orgs_for_politician,
    fetch_policy_area_summary,
    fetch_policy_exposure_for_politician,
    fetch_politician_area_returns,
    fetch_politician_index,
    fetch_politicians_for_area,
    fetch_politicians_for_org,
    fetch_recent_returns,
    fetch_revolving_door,
    fetch_revolving_door_summary,
    fetch_sources_for_org,
    fetch_sources_for_politician,
    fetch_summary,
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
    hero_banner,
    member_card_html,
    pagination_controls,
    sidebar_page_header,
    todo_callout,
)
from ui.entity_links import entity_cta_html, member_profile_url, source_link_html
from ui.export_controls import export_button
from ui.source_links import render_source_links
from ui.source_pdfs import provenance_expander

# ── State helpers ──────────────────────────────────────────────────────────────

def _init() -> None:
    for k, v in {
        "lob_selected_politician":   None,
        "lob_selected_org":          None,
        "lob_selected_area":         None,
        "lob_selected_dpo":          None,
        "lob_view_revolving_door":   False,
        "lob_results_pol":           None,
        "lob_sidebar_search":        "",
        "lob_date_start":            None,
        "lob_date_end":              None,
    }.items():
        st.session_state.setdefault(k, v)


def _clear_profile() -> None:
    st.session_state.lob_selected_politician = None
    st.session_state.lob_selected_org        = None
    st.session_state.lob_selected_area       = None
    st.session_state.lob_selected_dpo        = None
    st.session_state.lob_view_revolving_door = False
    st.session_state.lob_results_pol         = None


_NAV_KEYS: dict[str, str] = {
    "pol":  "lob_selected_politician",
    "org":  "lob_selected_org",
    "area": "lob_selected_area",
    "dpo":  "lob_selected_dpo",
}

# Query param key for each _NAV_KEYS kind. Card links write these directly;
# session-state-only triggers (sidebar, gateway buttons, breadcrumbs) call
# _nav()/_open_rd_index() and rely on the param sync below to keep the URL
# in step.
_NAV_QP: dict[str, str] = {
    "pol":  "lob_pol",
    "org":  "lob_org",
    "area": "lob_area",
    "dpo":  "lob_dpo",
}

_LOB_QP_ALL = ("lob_pol", "lob_org", "lob_area", "lob_dpo", "lob_rd", "lob_result_pol")


def _clear_lob_qp() -> None:
    for k in _LOB_QP_ALL:
        st.query_params.pop(k, None)


def _nav(kind: str, value: object = True) -> None:
    _clear_profile()
    setattr(st.session_state, _NAV_KEYS[kind], value)
    _clear_lob_qp()
    if isinstance(value, str):
        st.query_params[_NAV_QP[kind]] = value


def _open_rd_index() -> None:
    """Navigate to the Stage 2a revolving-door index."""
    _clear_profile()
    st.session_state.lob_view_revolving_door = True
    _clear_lob_qp()
    st.query_params["lob_rd"] = "1"


# ── HTML helpers ───────────────────────────────────────────────────────────────

def _path_card_html(symbol: str, heading: str, body: str, stat: str, stat_lbl: str) -> str:
    return (
        f'<div class="lob-path-card">'
        f'<div class="lob-path-icon">'
        f'<span class="material-symbols-outlined">{_h(symbol)}</span>'
        f'</div>'
        f'<p class="lob-path-heading">{_h(heading)}</p>'
        f'<p class="lob-path-body">{_h(body)}</p>'
        f'<div class="lob-path-stat">'
        f'<span class="lob-path-stat-num">{_h(stat)}</span>'
        f'<span class="lob-path-stat-lbl">&nbsp;{_h(stat_lbl)}</span>'
        f'</div></div>'
    )


def _lob_card_html(
    name: str,
    meta: str,
    pills: list[str],
    *,
    rank: int | None = None,
    profile_href: str = "",
) -> str:
    """Lobbying ranked-list card body (no link wrap, no arrow).

    Mirrors the layout that ``rank_card_row`` produced before the migration:
    a ``member_card_html`` with pill row and an optional cross-page
    "Profile ↗" link to the canonical /member-overview profile.
    """
    pills_html = "".join(f'<span class="int-stat-pill">{p}</span>' for p in pills)
    if profile_href:
        pills_html += (
            f'<a class="dt-member-link int-stat-pill-link" href="{_h(profile_href)}" '
            f'target="_self" aria-label="View profile of {_h(name)}">Profile ↗</a>'
        )
    return member_card_html(
        name=name, meta=meta, rank=rank, pills_html=pills_html,
        avatar_url=avatar_data_url(name),
        avatar_initials=_initials(name),
    )


def _activity_row_html(period: str, org: str, area: str) -> str:
    return (
        f'<div class="lob-activity-row">'
        f'<div class="lob-activity-period">{_h(period) or "—"}</div>'
        f'<div class="lob-activity-body">'
        f'<div class="lob-activity-org">{_h(org) or "—"}</div>'
        f'<div class="lob-activity-area">{_h(area)}</div>'
        f'</div></div>'
    )


def _back_button() -> None:
    if back_button("← Back to Lobbying", key="lob"):
        _clear_profile()
        _clear_lob_qp()
        st.rerun()


def _year_selector(df: pd.DataFrame, key: str) -> tuple[str | None, str | None]:
    """Render year pills from already-fetched data; return SQL-ready (start, end) or (None, None).

    Filtering is pushed to SQL via the returned params — no pandas row-masking here.
    The pd.to_datetime call is display-only: extracting unique years for the pill control.
    """
    if df.empty or "period_start_date" not in df.columns:
        return None, None
    try:
        years = sorted(
            pd.to_datetime(df["period_start_date"], errors="coerce")
            .dropna().dt.year.unique().tolist(),
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


def _provenance_footer(summary: pd.DataFrame) -> None:
    s   = summary.iloc[0] if not summary.empty else pd.Series()
    src = s.get("source_summary", "lobbying.ie via lobby_processing.py")
    ts  = s.get("latest_fetch_timestamp_utc", "—")
    fp  = s.get("first_period", "—")
    lp  = s.get("last_period",  "—")
    provenance_expander(
        sections=[
            "**Data source:** lobbying.ie — the Irish Register of Lobbying. "
            "Returns are filed quarterly by organisations lobbying Designated Public Officials (DPOs). "
            "Data is extracted via the lobbying.ie API and processed by the Dáil Tracker pipeline.",
            f"Source: {src}",
            f"Dataset covers: {fp} → {lp}  ·  Last fetched: {ts}",
        ]
    )


# ── Sidebar ────────────────────────────────────────────────────────────────────

def _render_sidebar() -> None:
    with st.sidebar:
        sidebar_page_header("Lobbying<br>Register")
        st.html('<p class="lob-sidebar-label">Search</p>')
        search = st.text_input(
            "Search",
            placeholder="e.g. Ibec, Mary Lou McDonald",
            key="lob_sidebar_search",
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

        combined_labels = (
            [""] +
            pol_filtered +
            [f"[Org] {n}" for n in org_filtered[:50]]
        )

        sel = st.selectbox(
            "Browse all members",
            combined_labels,
            label_visibility="collapsed",
        )
        if sel:
            if sel.startswith("[Org] "):
                _nav("org", sel[6:])
            else:
                _nav("pol", sel)
            st.rerun()

        st.divider()

        st.html('<p class="lob-sidebar-label">Notable targets</p>')
        notable = ["An Taoiseach", "Minister for Finance", "Tánaiste", "Minister for Health"]
        chip_cols = st.columns(2)
        for i, chip in enumerate(notable):
            if chip_cols[i % 2].button(chip, key=f"lob_chip_{i}", width="stretch"):
                idx = fetch_politician_index()
                if not idx.empty and "position" in idx.columns:
                    m = idx[idx["position"].str.contains(chip, case=False, na=False)]
                    if not m.empty:
                        _nav("pol", m.iloc[0]["member_name"])
                        st.rerun()
                    else:
                        st.caption(f"No politician found with position matching '{chip}'.")

        areas = fetch_policy_area_summary()
        if not areas.empty and "public_policy_area" in areas.columns:
            st.divider()
            st.html('<p class="lob-sidebar-label">Browse by policy area</p>')
            top_areas = areas["public_policy_area"].dropna().head(10).tolist()
            for area in top_areas:
                safe_key = f"lob_area_{area[:25].replace(' ', '_')}"
                if st.button(area, key=safe_key, width="stretch"):
                    _nav("area", area)
                    st.rerun()


# ── Landing page ───────────────────────────────────────────────────────────────

def _render_landing(summary: pd.DataFrame) -> None:
    s = summary.iloc[0] if not summary.empty else pd.Series()

    total_returns  = int(s.get("total_returns",      0) or 0)
    total_orgs     = int(s.get("total_orgs",         0) or 0)
    total_pols     = int(s.get("total_politicians",  0) or 0)
    total_areas    = int(s.get("total_policy_areas", 0) or 0)
    first_p        = s.get("first_period", "—")
    last_p         = s.get("last_period",  "—")

    hero_banner(
        kicker="LOBBYING REGISTER · IRELAND",
        title="Who is lobbying Irish politicians?",
        dek=(
            "Explore the official register of lobbying returns. "
            "Follow a politician to see who is lobbying them, "
            "follow an organisation to see who they target, "
            "or browse by policy area to understand where influence is concentrated."
        ),
        badges=[
            f"{total_returns:,} returns",
            f"{total_orgs:,} organisations",
            f"{total_pols:,} politicians",
            f"Data: {first_p} → {last_p}",
        ] if total_returns else [],
    )

    # Date range filter (global temporal control)
    with st.expander("Filter by date range", expanded=False):
        date_cols = st.columns(2)
        lob_start = date_cols[0].date_input(
            "From",
            value=st.session_state.lob_date_start,
            key="lob_date_start_input",
        )
        lob_end = date_cols[1].date_input(
            "To",
            value=st.session_state.lob_date_end,
            key="lob_date_end_input",
        )
        if date_cols[0].button("Apply", key="lob_apply_date"):
            st.session_state.lob_date_start = lob_start
            st.session_state.lob_date_end   = lob_end
            st.rerun()
        if date_cols[1].button("Clear", key="lob_clear_date"):
            st.session_state.lob_date_start = None
            st.session_state.lob_date_end   = None
            st.rerun()

    if summary.empty:
        todo_callout("Enrichment pipeline not yet run — populate lobbying data to enable this page.")

    # ── Three-path gateway ────────────────────────────────────────────────
    evidence_heading("Where do you want to investigate?")
    g1, g2, g3 = st.columns(3)

    with g1:
        st.html(
            _path_card_html(
                "person",
                "Follow a politician",
                "See every lobbying return targeting a specific politician or senator.",
                f"{total_pols:,}",
                "politicians on record",
            )
        )
        if st.button("Browse politicians →", key="lob_gw_pol", width="stretch"):
            idx = fetch_politician_index()
            if not idx.empty:
                _nav("pol", idx.iloc[0]["member_name"])
                st.rerun()

    with g2:
        st.html(
            _path_card_html(
                "business",
                "Follow an organisation",
                "See which politicians an organisation has lobbied and on what topics.",
                f"{total_orgs:,}",
                "organisations on record",
            )
        )
        if st.button("Browse organisations →", key="lob_gw_org", width="stretch"):
            orgs = fetch_org_index()
            if not orgs.empty:
                _nav("org", orgs.iloc[0]["lobbyist_name"])
                st.rerun()

    with g3:
        st.html(
            _path_card_html(
                "policy",
                "Browse by policy area",
                "Find all lobbying returns filed under a specific public policy area.",
                f"{total_areas:,}",
                "policy areas",
            )
        )
        if st.button("Browse policy areas →", key="lob_gw_area", width="stretch"):
            areas = fetch_policy_area_summary()
            if not areas.empty:
                _nav("area", areas.iloc[0]["public_policy_area"])
                st.rerun()

    # ── Dual leaderboards ─────────────────────────────────────────────────
    lb1, lb2 = st.columns(2)

    with lb1:
        evidence_heading("Most-lobbied politicians")
        idx = fetch_politician_index()
        if idx.empty:
            todo_callout(
                "v_lobbying_index — re-run lobby_processing.py to regenerate "
                "data/gold/parquet/most_lobbied_politicians.parquet"
            )
        else:
            cards: list[str] = []
            for rank, (_, row) in enumerate(idx.head(10).iterrows(), start=1):
                name      = str(row.get("member_name", "—"))
                member_id = str(row.get("unique_member_code", "") or "")
                meta      = clean_meta(
                    str(row.get("chamber",  "") or ""),
                    str(row.get("position", "") or ""),
                )
                pills = [
                    f"{int(row.get('return_count', 0) or 0):,} returns",
                    f"{int(row.get('distinct_orgs', 0) or 0):,} orgs",
                ]
                cards.append(
                    clickable_card_link(
                        href=f"?lob_pol={quote(name)}",
                        inner_html=_lob_card_html(
                            name, meta, pills,
                            rank=rank,
                            profile_href=member_profile_url(member_id) if member_id else "",
                        ),
                        aria_label=f"View lobbying profile for {name}",
                    )
                )
            st.html("\n".join(cards))

    with lb2:
        evidence_heading("Most active organisations")
        orgs = fetch_org_index()
        if orgs.empty:
            todo_callout(
                "v_lobbying_org_index — re-run lobby_processing.py to regenerate "
                "data/gold/parquet/top_lobbyist_organisations.parquet"
            )
        else:
            cards: list[str] = []
            for rank, (_, row) in enumerate(orgs.head(10).iterrows(), start=1):
                name  = str(row.get("lobbyist_name", "—"))
                meta  = str(row.get("sector", "") or "")
                pills = [
                    f"{int(row.get('return_count', 0) or 0):,} returns",
                    f"{int(row.get('politicians_targeted', 0) or 0):,} politicians",
                ]
                cards.append(
                    clickable_card_link(
                        href=f"?lob_org={quote(name)}",
                        inner_html=_lob_card_html(name, meta, pills, rank=rank),
                        aria_label=f"View lobbying profile for {name}",
                    )
                )
            st.html("\n".join(cards))

    # ── Revolving door promoted callout ───────────────────────────────────
    rd_summary = fetch_revolving_door_summary()
    dpos       = fetch_revolving_door(limit=5)
    if rd_summary.empty or dpos.empty:
        st.html(
            '<div class="lob-revolving-callout">'
            '<p class="lob-revolving-heading">Revolving door</p>'
            '<p class="lob-revolving-explain">No DPO data available yet.</p>'
            '</div>'
        )
        todo_callout("v_lobbying_revolving_door — re-run lobby_processing.py to regenerate data/gold/parquet/revolving_door_dpos.parquet")
    else:
        rd_row     = rd_summary.iloc[0]
        rd_n       = int(rd_row.get("individuals",         0) or 0)
        rd_returns = int(rd_row.get("total_returns",       0) or 0)
        rows_html = ""
        for rank, (_, row) in enumerate(dpos.iterrows(), start=1):
            name_     = _h(str(row.get("individual_name", "—")))
            position  = _h(str(row.get("former_position", "") or ""))
            ret_cnt_  = int(row.get("return_count",                  0) or 0)
            pol_cnt_  = int(row.get("distinct_politicians_targeted", 0) or 0)
            former    = f"Former {position}" if position else "Former DPO"
            rows_html += (
                f'<div class="lob-revolving-row">'
                f'<span class="lob-revolving-row-rank">#{rank}</span>'
                f'<span class="lob-revolving-row-name">{name_}</span>'
                f'<span class="lob-revolving-row-meta">{_h(former)} · {ret_cnt_:,} returns · {pol_cnt_:,} politicians</span>'
                f'</div>'
            )
        st.html(
            '<div class="lob-revolving-callout">'
            '<p class="lob-revolving-heading">Revolving door</p>'
            f'<p class="lob-revolving-headline">{rd_n:,} former DPOs filed {rd_returns:,} returns lobbying current officials.</p>'
            '<p class="lob-revolving-explain">'
            'Former Designated Public Officials — politicians, ministers, and senior civil servants — are subject '
            'to a one-year cooling-off period before they may lobby former colleagues. Identification is by '
            'name-matching against the DPO register; treat as indicative, not a legal finding.'
            '</p>'
            f'<div class="lob-revolving-list">{rows_html}</div>'
            '</div>'
        )
        cta_col, _spacer = st.columns([1, 2])
        with cta_col:
            if st.button(
                f"Explore all {rd_n:,} revolving door cases →",
                key="dt_cta_rd_explore",
                width="stretch",
                help="Open the full revolving door index",
            ):
                _open_rd_index()
                st.rerun()

    # ── Latest activity feed ──────────────────────────────────────────────
    evidence_heading("Latest returns")
    recent = fetch_recent_returns()
    if recent.empty:
        todo_callout(
            "v_lobbying_recent_returns — re-run lobby_processing.py to regenerate "
            "data/silver/lobbying/parquet/returns_master.parquet"
        )
    else:
        activity_html = ""
        for _, row in recent.iterrows():
            period = str(row.get("period_start_date", "") or "")[:7]
            org    = str(row.get("lobbyist_name",     "") or "")
            area   = str(row.get("public_policy_area", "") or "")
            activity_html += _activity_row_html(period, org, area)
        st.html(activity_html)
        todo_callout(
            "member_name on v_lobbying_recent_returns — returns_master.csv is "
            "return-level, not politician-level; wire in once pipeline enrichment joins them"
        )

    _provenance_footer(summary)


# ── Revolving Door Stage 2a — index ────────────────────────────────────────────

def _render_dpo_index(summary: pd.DataFrame) -> None:
    crumb = breadcrumb(["Lobbying", "Revolving Door"], key_prefix="rd_idx")
    if crumb == 0:
        _clear_profile()
        _clear_lob_qp()
        st.rerun()

    rd_summary = fetch_revolving_door_summary()
    rd_n       = int(rd_summary.iloc[0].get("individuals",   0) or 0) if not rd_summary.empty else 0
    rd_returns = int(rd_summary.iloc[0].get("total_returns", 0) or 0) if not rd_summary.empty else 0

    hero_banner(
        kicker="TRANSPARENCY · REVOLVING DOOR",
        title="Former Designated Public Officials on lobbying returns",
        dek=(
            "Politicians, ministers, and senior civil servants are subject to a one-year "
            "cooling-off period before they may lobby former colleagues. Identification is "
            "by name-matching against the DPO register; treat as indicative, not a legal finding."
        ),
        badges=[f"{rd_n:,} individuals", f"{rd_returns:,} returns"] if rd_n else None,
    )

    all_dpos = fetch_revolving_door(limit=None)
    if all_dpos.empty:
        empty_state("No revolving door data", "Pipeline has not produced revolving_door_dpos.parquet yet.")
        _provenance_footer(summary)
        return

    # ── Prominent cases sub-callout — top 3 by return count ───────────────
    prominent = all_dpos.head(3)
    pills_html = ""
    for _, row in prominent.iterrows():
        pname = _h(str(row.get("individual_name", "—")))
        pcnt  = int(row.get("return_count", 0) or 0)
        pills_html += (
            f'<span class="lob-rd-prominent-pill">'
            f'<strong>{pname}</strong> · {pcnt:,} returns'
            f'</span>'
        )
    st.html(
        '<div class="lob-rd-prominent">'
        '<p class="lob-rd-prominent-heading">Most-active filers</p>'
        f'<div class="lob-rd-prominent-grid">{pills_html}</div>'
        '</div>'
    )

    # ── Chamber filter pills ──────────────────────────────────────────────
    chambers_present = (
        all_dpos["chamber_display"].dropna().astype(str).replace({"": pd.NA}).dropna().unique().tolist()
        if "chamber_display" in all_dpos.columns else []
    )
    chamber_options = ["All"] + sorted(chambers_present)
    chosen = st.segmented_control(
        "Filter by former chamber",
        options=chamber_options,
        default="All",
        key="rd_chamber_filter",
    ) or "All"

    if chosen != "All":
        filtered = all_dpos[all_dpos["chamber_display"].astype(str) == chosen]
    else:
        filtered = all_dpos

    st.caption(f"Showing {len(filtered):,} of {len(all_dpos):,} individuals.")

    # ── Ranked card list ──────────────────────────────────────────────────
    evidence_heading("All revolving door individuals")
    if filtered.empty:
        empty_state("No matches", f"No DPOs in chamber '{chosen}'.")
    else:
        cards: list[str] = []
        for rank, (_, row) in enumerate(filtered.iterrows(), start=1):
            name     = str(row.get("individual_name", "—"))
            position = str(row.get("former_position",  "") or "")
            chamber  = str(row.get("chamber_display",  "") or "")
            former   = f"Former {position}" if position else "Former DPO"
            meta     = clean_meta(former, chamber)
            pills    = [
                f"{int(row.get('return_count',                  0) or 0):,} returns",
                f"{int(row.get('distinct_firms',                0) or 0):,} firms",
                f"{int(row.get('distinct_politicians_targeted', 0) or 0):,} politicians",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lob_dpo={quote(name)}",
                    inner_html=_lob_card_html(name, meta, pills, rank=rank),
                    aria_label=f"View revolving door profile for {name}",
                )
            )
        st.html("\n".join(cards))

    _provenance_footer(summary)


# ── Revolving Door Stage 2b — individual profile ──────────────────────────────

def _render_dpo_individual(individual_name: str, summary: pd.DataFrame) -> None:
    crumb = breadcrumb(
        ["Lobbying", "Revolving Door", individual_name],
        key_prefix="rd_indiv",
    )
    if crumb == 0:
        _clear_profile()
        _clear_lob_qp()
        st.rerun()
    elif crumb == 1:
        _open_rd_index()
        st.rerun()

    dpo_df = fetch_dpo_one(individual_name)
    if dpo_df.empty:
        empty_state(
            "Individual not found",
            f"No revolving door entry on record for '{individual_name}'.",
        )
        _provenance_footer(summary)
        return

    dpo_row  = dpo_df.iloc[0]
    position = str(dpo_row.get("former_position",  "") or "")
    chamber  = str(dpo_row.get("chamber_display",  "") or "")
    ret_cnt  = int(dpo_row.get("return_count",                  0) or 0)
    firm_cnt = int(dpo_row.get("distinct_firms",                0) or 0)
    pol_cnt  = int(dpo_row.get("distinct_politicians_targeted", 0) or 0)
    area_cnt = int(dpo_row.get("distinct_policy_areas",         0) or 0)
    former   = f"Former {position}" if position else "Former DPO"

    badges = [b for b in [former, chamber] if b]
    hero_banner(
        kicker="REVOLVING DOOR PROFILE",
        title=individual_name,
        dek=(
            f"{ret_cnt:,} returns filed across {firm_cnt:,} firm(s), targeting "
            f"{pol_cnt:,} politicians on {area_cnt:,} policy area(s)."
        ) if ret_cnt else "No lobbying returns on record for this individual.",
        badges=badges or None,
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Returns",            f"{ret_cnt:,}")
    m2.metric("Firms",              f"{firm_cnt:,}")
    m3.metric("Politicians",        f"{pol_cnt:,}")
    m4.metric("Policy areas",       f"{area_cnt:,}")

    # ── Firms represented ─────────────────────────────────────────────────
    evidence_heading("Firms represented")
    firms_df = fetch_dpo_firms(individual_name)
    if firms_df.empty:
        empty_state("No firms", "No firm associations recorded for this individual.")
    else:
        firms_display = firms_df.rename(columns={
            "lobbyist_name": "Firm",
            "return_count":  "Returns",
            "first_period":  "First filing",
            "last_period":   "Last filing",
        })
        max_firm = int(firms_display["Returns"].max()) if not firms_display.empty else 1
        st.dataframe(
            firms_display,
            width="stretch",
            hide_index=True,
            column_config={
                "Firm":         st.column_config.TextColumn("Firm"),
                "Returns":      st.column_config.ProgressColumn(
                    "Returns", format="%d", min_value=0, max_value=max_firm,
                ),
                "First filing": st.column_config.DateColumn("First filing", format="MMM YYYY"),
                "Last filing":  st.column_config.DateColumn("Last filing",  format="MMM YYYY"),
            },
        )

    # ── Clients represented ───────────────────────────────────────────────
    evidence_heading("Clients represented")
    clients_df = fetch_dpo_client_breakdown(individual_name)
    if clients_df.empty:
        empty_state("No clients", "No client companies recorded for this individual.")
    else:
        clients_display = clients_df.rename(columns={
            "client_name":   "Client",
            "return_count":  "Returns",
            "first_period":  "First filing",
            "last_period":   "Last filing",
        })
        max_client = int(clients_display["Returns"].max()) if not clients_display.empty else 1
        st.dataframe(
            clients_display,
            width="stretch",
            hide_index=True,
            column_config={
                "Client":       st.column_config.TextColumn("Client"),
                "Returns":      st.column_config.ProgressColumn(
                    "Returns", format="%d", min_value=0, max_value=max_client,
                ),
                "First filing": st.column_config.DateColumn("First filing", format="MMM YYYY"),
                "Last filing":  st.column_config.DateColumn("Last filing",  format="MMM YYYY"),
            },
        )

    # ── Politicians targeted (with cross-link to politician Stage 2) ──────
    evidence_heading("Politicians targeted")
    pols_df = fetch_dpo_politicians_targeted(individual_name)
    if pols_df.empty:
        empty_state("No politicians", "No politicians matched on the joined returns.")
    else:
        pol_known = set(fetch_all_politician_names())
        cards: list[str] = []
        for rank, (_, prow) in enumerate(pols_df.head(20).iterrows(), start=1):
            pname = str(prow.get("member_name", "—"))
            pchm  = str(prow.get("chamber",     "") or "")
            pcnt  = int(prow.get("return_count", 0) or 0)
            pills = [f"{pcnt:,} returns"]
            inner = _lob_card_html(pname, pchm, pills, rank=rank)
            if pname in pol_known:
                cards.append(
                    clickable_card_link(
                        href=f"?lob_pol={quote(pname)}",
                        inner_html=inner,
                        aria_label=f"Open politician profile for {pname}",
                    )
                )
            else:
                # Politician not in the politician_index — render as non-clickable card.
                cards.append(inner)
        st.html("\n".join(cards))
        if len(pols_df) > 20:
            st.caption(f"Showing top 20 of {len(pols_df):,} politicians.")

    # ── Lobbying returns + CSV export ─────────────────────────────────────
    evidence_heading("Lobbying returns")
    returns_df = fetch_dpo_returns_detail(individual_name)
    if returns_df.empty:
        empty_state("No returns", "No individual returns recorded for this person.")
    else:
        display = returns_df[
            [c for c in ["period_start_date", "lobbyist_name", "client_name",
                         "public_policy_area", "source_url"]
             if c in returns_df.columns]
        ].rename(columns={
            "period_start_date":  "Period",
            "lobbyist_name":      "Firm",
            "client_name":        "Client",
            "public_policy_area": "Policy area",
            "source_url":         "Return URL",
        })
        st.dataframe(
            display,
            width="stretch",
            hide_index=True,
            column_config={
                "Return URL": st.column_config.LinkColumn(
                    "Return URL",
                    display_text=r"https://www\.lobbying\.ie/return/(\d+)",
                ),
            },
        )
        first_p = str(returns_df["period_start_date"].min())[:7] if "period_start_date" in returns_df.columns else ""
        last_p  = str(returns_df["period_start_date"].max())[:7] if "period_start_date" in returns_df.columns else ""
        period_span = f"{first_p}_{last_p}" if first_p and last_p else "all"
        safe_name = "".join(c if c.isalnum() else "_" for c in individual_name)[:60]
        st.caption(
            f"Showing {len(returns_df):,} returns · {first_p} – {last_p}. "
            "Click any row's link to view that filing on lobbying.ie."
        )
        export_button(
            display,
            "Export returns CSV",
            f"{safe_name}_revolving_door_returns_{period_span}.csv",
            "rd_export_returns",
        )

    # ── Official source links ─────────────────────────────────────────────
    # Sources are keyed by member_name or lobbyist_name; for an individual
    # there is no canonical sources view, so this slot is intentionally
    # empty here. The per-row Return URL above already exposes the
    # lobbying.ie source for every filing.

    _provenance_footer(summary)


# ── Politician Stage 2 ─────────────────────────────────────────────────────────

def _render_politician(name: str, summary: pd.DataFrame) -> None:
    _back_button()

    idx = fetch_politician_index()
    pol_row = pd.Series()
    if not idx.empty and "member_name" in idx.columns:
        m = idx[idx["member_name"] == name]
        if not m.empty:
            pol_row = m.iloc[0]

    chamber   = str(pol_row.get("chamber",  "") or "")
    position  = str(pol_row.get("position", "") or "")
    member_id = str(pol_row.get("unique_member_code", "") or "")
    ret_cnt   = int(pol_row.get("return_count",          0) or 0)
    org_cnt   = int(pol_row.get("distinct_orgs",         0) or 0)
    area_cnt  = int(pol_row.get("distinct_policy_areas", 0) or 0)
    first_p   = str(pol_row.get("first_period", "") or "")
    last_p    = str(pol_row.get("last_period",  "") or "")

    meta_badges = [b for b in [chamber, position] if b]
    hero_banner(
        kicker="LOBBYING PROFILE · POLITICIAN",
        title=name,
        dek=(
            f"Lobbied across {area_cnt} policy area(s) by {org_cnt} organisation(s). "
            f"Returns span {first_p} → {last_p}." if ret_cnt else
            "No lobbying returns on record for this politician."
        ),
        badges=meta_badges or None,
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Returns targeting them", f"{ret_cnt:,}")
    c2.metric("Distinct organisations", f"{org_cnt:,}")
    c3.metric("Policy areas",           f"{area_cnt:,}")

    # Cross-page jump to the full accountability profile. unique_member_code
    # comes from v_lobbying_index — see fetch_politician_index().
    if member_id:
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
            first_c  = str(row.get("first_contact", "") or "")[:7]
            last_c   = str(row.get("last_contact",  "") or "")[:7]
            meta     = clean_meta(first_c, last_c)
            pills    = [
                f"{int(row.get('returns_in_relationship', 0) or 0):,} returns",
                f"{int(row.get('distinct_policy_areas',   0) or 0):,} policy areas",
                f"{int(row.get('distinct_periods',        0) or 0):,} periods",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lob_org={quote(org_name)}",
                    inner_html=_lob_card_html(org_name, meta, pills, rank=rank),
                    aria_label=f"View lobbying profile for {org_name}",
                )
            )
        st.html("\n".join(cards))

    # ── Policy exposure ───────────────────────────────────────────────────
    evidence_heading("Policy areas lobbied on")
    exposure = fetch_policy_exposure_for_politician(name)
    if not exposure.empty:
        disp2 = exposure.rename(columns={
            "public_policy_area": "Policy area",
            "returns_targeting":  "Returns",
            "distinct_lobbyists": "Organisations",
        })
        max_ret = int(disp2["Returns"].max()) if not disp2.empty else 1
        st.dataframe(
            disp2,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Policy area":   st.column_config.TextColumn("Policy area"),
                "Returns":       st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_ret),
                "Organisations": st.column_config.NumberColumn("Organisations"),
            },
        )

    # ── Lobbying returns ──────────────────────────────────────────────────
    detail_all = fetch_contact_detail(name)
    evidence_heading("Lobbying returns")
    if detail_all.empty:
        empty_state("No lobbying returns", "No contact detail on record for this politician.")
    else:
        start, end = _year_selector(detail_all, "lob_year_pol")
        detail = fetch_contact_detail(name, start, end) if start else detail_all
        display = detail[
            [c for c in ["period_start_date", "lobbyist_name",
                         "public_policy_area", "source_url"]
             if c in detail.columns]
        ].rename(columns={
            "period_start_date":  "Period",
            "lobbyist_name":      "Organisation",
            "public_policy_area": "Policy area",
            "source_url":         "Return URL",
        })
        st.dataframe(
            display,
            column_config={"Return URL": st.column_config.LinkColumn(
                "Return URL",
                display_text=r"https://www\.lobbying\.ie/return/(\d+)")},
            use_container_width=True,
            hide_index=True,
        )
        export_button(detail, "Export CSV", f"{name.replace(' ', '_')}_lobbying.csv",
                      "lob_export_pol_detail")

    # ── Official source links ─────────────────────────────────────────────
    evidence_heading("Official source links")
    render_source_links(fetch_sources_for_politician(name))

    _provenance_footer(summary)


# ── Org Stage 2 ────────────────────────────────────────────────────────────────

def _render_org(org_name: str, summary: pd.DataFrame) -> None:
    _back_button()

    all_org_names = fetch_all_org_names()
    if all_org_names:
        st.html(
            "<style>"
            ".st-key-lob_org_switcher .stSelectbox > div > div,"
            ".st-key-lob_org_switcher [data-baseweb=\"select\"] > div"
            "{background:#ffffff !important;}"
            "</style>"
        )
        switch_col, _ = st.columns([1, 2])
        with switch_col:
            picked = st.selectbox(
                "Switch organisation",
                all_org_names,
                index=all_org_names.index(org_name) if org_name in all_org_names else 0,
                key="lob_org_switcher",
            )
        if picked != org_name:
            _nav("org", picked)
            st.rerun()

    org_idx = fetch_org_index()
    org_row = pd.Series()
    if not org_idx.empty and "lobbyist_name" in org_idx.columns:
        m = org_idx[org_idx["lobbyist_name"] == org_name]
        if not m.empty:
            org_row = m.iloc[0]

    sector   = str(org_row.get("sector",               "") or "")
    website  = str(org_row.get("website",              "") or "")
    ret_cnt  = int(org_row.get("return_count",          0) or 0)
    pol_cnt  = int(org_row.get("politicians_targeted",  0) or 0)
    area_cnt = int(org_row.get("distinct_policy_areas", 0) or 0)
    first_p  = str(org_row.get("first_period", "") or "")
    last_p   = str(org_row.get("last_period",  "") or "")

    badges = [b for b in [sector] if b]
    if website and website.startswith("http"):
        badges.append(source_link_html(website, website,
                                       aria_label=f"Open {org_name} website"))

    hero_banner(
        kicker="LOBBYING PROFILE · ORGANISATION",
        title=org_name,
        dek=(
            f"Filed {ret_cnt:,} lobbying return(s) targeting {pol_cnt} politician(s) "
            f"across {area_cnt} policy area(s). Active {first_p} → {last_p}." if ret_cnt else
            "No lobbying returns on record for this organisation."
        ),
        badges=badges or None,
    )

    # ── Persistence stats ─────────────────────────────────────────────────
    persist = fetch_org_persistence(org_name)
    if not persist.empty:
        pr = persist.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Returns filed",        f"{ret_cnt:,}")
        c2.metric("Politicians targeted",  f"{pol_cnt:,}")
        c3.metric("Periods filed",         str(pr.get("distinct_periods_filed", "—") or "—"))
        c4.metric("Active span (days)",    str(pr.get("active_span_days", "—") or "—"))
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Returns filed",        f"{ret_cnt:,}")
        c2.metric("Politicians targeted", f"{pol_cnt:,}")
        c3.metric("Policy areas",         f"{area_cnt:,}")

    # ── Politicians targeted — ranked cards (primary view) ────────────────
    evidence_heading("Politicians targeted")
    pol_intensity = fetch_politicians_for_org(org_name)
    if pol_intensity.empty:
        empty_state("No intensity data", "No politicians found targeted by this organisation.")
    else:
        cards: list[str] = []
        for rank, (_, row) in enumerate(pol_intensity.iterrows(), start=1):
            pol_name  = str(row.get("member_name", "—"))
            member_id = str(row.get("unique_member_code", "") or "")
            chamber   = str(row.get("chamber", "") or "")
            first_c   = str(row.get("first_contact", "") or "")[:7]
            last_c    = str(row.get("last_contact",  "") or "")[:7]
            meta      = clean_meta(chamber, first_c, last_c)
            pills     = [
                f"{int(row.get('returns_in_relationship', 0) or 0):,} returns",
                f"{int(row.get('distinct_policy_areas',   0) or 0):,} policy areas",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lob_pol={quote(pol_name)}",
                    inner_html=_lob_card_html(
                        pol_name, meta, pills,
                        rank=rank,
                        profile_href=member_profile_url(member_id) if member_id else "",
                    ),
                    aria_label=f"View lobbying profile for {pol_name}",
                )
            )
        st.html("\n".join(cards))

    # ── Clients ───────────────────────────────────────────────────────────
    clients = fetch_clients_for_org(org_name)
    if not clients.empty:
        evidence_heading("Clients represented")
        disp_c = clients.rename(columns={
            "client_name":       "Client",
            "period_start_date": "Period",
            "policy_areas":      "Policy areas",
            "politicians_count": "Politicians",
            "source_url":        "Return URL",
        })
        st.dataframe(
            disp_c,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Client":       st.column_config.TextColumn("Client"),
                "Period":       st.column_config.TextColumn("Period"),
                "Policy areas": st.column_config.TextColumn("Policy areas"),
                "Politicians":  st.column_config.NumberColumn("Politicians"),
                "Return URL":   st.column_config.LinkColumn(
                    "Return URL",
                    display_text=r"https://www\.lobbying\.ie/return/(\d+)",
                ),
            },
        )

    # ── All lobbying returns ──────────────────────────────────────────────
    detail_all = fetch_org_contact_detail(org_name)
    evidence_heading("All lobbying returns")
    if detail_all.empty:
        empty_state("No lobbying returns", "No contact detail on record for this organisation.")
    else:
        start, end = _year_selector(detail_all, "lob_year_org")
        detail = fetch_org_contact_detail(org_name, start, end) if start else detail_all
        display = detail[
            [c for c in ["period_start_date", "lobbyist_name", "member_name",
                         "public_policy_area", "source_url"]
             if c in detail.columns]
        ].rename(columns={
            "period_start_date":  "Period",
            "lobbyist_name":      "Organisation",
            "member_name":        "Politician",
            "public_policy_area": "Policy area",
            "source_url":         "Return URL",
        })
        st.dataframe(
            display,
            column_config={"Return URL": st.column_config.LinkColumn(
                "Return URL",
                display_text=r"https://www\.lobbying\.ie/return/(\d+)")},
            use_container_width=True,
            hide_index=True,
        )
        export_button(detail, "Export CSV",
                      f"{org_name[:40].replace(' ', '_')}_lobbying.csv",
                      "lob_export_org_detail")

    # ── Official source links ─────────────────────────────────────────────
    evidence_heading("Official source links")
    render_source_links(fetch_sources_for_org(org_name))

    _provenance_footer(summary)


# ── Area Stage 2 ────────────────────────────────────────────────────────────────

def _render_area(area: str, summary: pd.DataFrame) -> None:
    _back_button()

    areas_df = fetch_policy_area_summary()
    all_area_names = (
        areas_df["public_policy_area"].dropna().tolist()
        if not areas_df.empty and "public_policy_area" in areas_df.columns
        else []
    )
    if all_area_names:
        st.html(
            "<style>"
            ".st-key-lob_area_switcher .stSelectbox > div > div,"
            ".st-key-lob_area_switcher [data-baseweb=\"select\"] > div"
            "{background:#ffffff !important;}"
            "</style>"
        )
        switch_col, _ = st.columns([1, 2])
        with switch_col:
            picked = st.selectbox(
                "Switch policy area",
                all_area_names,
                index=all_area_names.index(area) if area in all_area_names else 0,
                key="lob_area_switcher",
            )
        if picked != area:
            _nav("area", picked)
            st.rerun()

    area_row = pd.Series()
    if not areas_df.empty:
        m = areas_df[areas_df["public_policy_area"] == area]
        if not m.empty:
            area_row = m.iloc[0]

    ret_cnt = int(area_row.get("return_count",        0) or 0)
    org_cnt = int(area_row.get("distinct_orgs",       0) or 0)
    pol_cnt = int(area_row.get("distinct_politicians", 0) or 0)

    hero_banner(
        kicker="LOBBYING PROFILE · POLICY AREA",
        title=area,
        dek=(
            f"{ret_cnt:,} returns filed by {org_cnt} organisation(s) "
            f"targeting {pol_cnt} politician(s) on this topic." if ret_cnt else
            "No lobbying returns on record for this policy area."
        ),
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Returns",       f"{ret_cnt:,}")
    c2.metric("Organisations", f"{org_cnt:,}")
    c3.metric("Politicians",   f"{pol_cnt:,}")

    # ── Most-exposed politicians — ranked cards (primary view) ─────────────
    evidence_heading("Most-targeted politicians")
    st.caption("Click → on a politician to see every return filed against them under this policy area.")
    area_pols = fetch_politicians_for_area(area)
    if area_pols.empty:
        empty_state("No data", "No politicians found for this policy area.")
    else:
        cards: list[str] = []
        for rank, (_, row) in enumerate(area_pols.iterrows(), start=1):
            pol_name  = str(row.get("member_name", "—"))
            member_id = str(row.get("unique_member_code", "") or "")
            chamber   = str(row.get("chamber", "") or "")
            pills     = [
                f"{int(row.get('returns_targeting',  0) or 0):,} returns",
                f"{int(row.get('distinct_lobbyists', 0) or 0):,} orgs",
            ]
            cards.append(
                clickable_card_link(
                    href=f"?lob_area={quote(area)}&lob_result_pol={quote(pol_name)}",
                    inner_html=_lob_card_html(
                        pol_name, chamber, pills,
                        rank=rank,
                        profile_href=member_profile_url(member_id) if member_id else "",
                    ),
                    aria_label=f"View every return targeting {pol_name} under {area}",
                )
            )
        st.html("\n".join(cards))

    # ── Lobbying returns ──────────────────────────────────────────────────
    detail_all = fetch_area_contact_detail(area)
    evidence_heading("Lobbying returns for this policy area")
    if detail_all.empty:
        empty_state("No returns", "No lobbying contact detail on record for this policy area.")
    else:
        start, end = _year_selector(detail_all, "lob_year_area")
        detail = fetch_area_contact_detail(area, start, end) if start else detail_all
        display = detail[
            [c for c in ["period_start_date", "lobbyist_name", "member_name", "source_url"]
             if c in detail.columns]
        ].rename(columns={
            "period_start_date": "Period",
            "lobbyist_name":     "Organisation",
            "member_name":       "Politician",
            "source_url":        "Return URL",
        })
        st.dataframe(
            display,
            column_config={"Return URL": st.column_config.LinkColumn(
                "Return URL",
                display_text=r"https://www\.lobbying\.ie/return/(\d+)")},
            use_container_width=True,
            hide_index=True,
        )
        export_button(detail, "Export CSV",
                      f"{area[:40].replace(' ', '_')}_lobbying.csv",
                      "lob_export_area_detail")

    # ── Source links note ─────────────────────────────────────────────────
    evidence_heading("Official source links")
    st.caption(
        "Return URLs are in the Return URL column above. "
        "Area-level source aggregation requires pipeline support "
        "(v_lobbying_sources area filter — TODO_PIPELINE_VIEW_REQUIRED)."
    )

    _provenance_footer(summary)


# ── Results Stage 3 (politician × area) ────────────────────────────────────────

def _render_results(area: str, politician: str, summary: pd.DataFrame) -> None:
    # Custom back button — return to the area page, not the landing page
    if back_button(f"← Back to {area}", key="lob_results"):
        st.session_state.lob_results_pol = None
        st.query_params.pop("lob_result_pol", None)
        st.rerun()

    st.html(
        f'<p class="lob-sidebar-label" style="margin:0.5rem 0 0.5rem;">'
        f'Lobbying › {_h(area)} › <strong>{_h(politician)}</strong></p>'
    )

    detail_all = fetch_politician_area_returns(politician, area)

    hero_banner(
        kicker="LOBBYING RESULTS · POLITICIAN × POLICY AREA",
        title=f"{politician} on {area}",
        dek=(
            f"{len(detail_all):,} return(s) filed against {politician} "
            f"under the public policy area '{area}'. Each card below links to "
            f"the original lobbying.ie return — open it to read exactly what was lobbied for."
        ) if not detail_all.empty else
        f"No returns on record for {politician} under '{area}'.",
    )

    if detail_all.empty:
        empty_state("No returns", f"No lobbying returns found for {politician} under {area}.")
        _provenance_footer(summary)
        return

    # ── Filters + export row ──────────────────────────────────────────────
    start, end = _year_selector(detail_all, "lob_year_results")
    detail = (
        fetch_politician_area_returns(politician, area, start, end)
        if start else detail_all
    )

    # Build org-detail lookup (sector, website) without a pandas merge
    org_idx = fetch_org_index()
    org_lookup: dict[str, dict[str, str]] = {}
    if not org_idx.empty and "lobbyist_name" in org_idx.columns:
        for _, r in org_idx.iterrows():
            org_lookup[str(r["lobbyist_name"])] = {
                "sector":  str(r.get("sector",  "") or ""),
                "website": str(r.get("website", "") or ""),
            }

    # Build DPO map: return_id -> sorted list of individual names
    dpo_df = fetch_dpo_return_map()
    dpo_by_return: dict[str, list[str]] = {}
    if not dpo_df.empty and {"return_id", "individual_name"}.issubset(dpo_df.columns):
        for _, r in dpo_df.iterrows():
            rid = str(r["return_id"])
            dpo_by_return.setdefault(rid, []).append(str(r["individual_name"]))

    dpo_count = sum(1 for _, r in detail.iterrows() if str(r.get("return_id", "")) in dpo_by_return)

    c1, c2, c3 = st.columns(3)
    c1.metric("Returns shown",      f"{len(detail):,}")
    c2.metric("Distinct organisations",
              f"{detail['lobbyist_name'].nunique():,}" if "lobbyist_name" in detail.columns else "—")
    c3.metric("DPO involvement",    f"{dpo_count:,}",
              help="Returns where a former Designated Public Officer carried out the lobbying.")

    csv_export = detail.copy()
    csv_export["dpo_individuals"] = csv_export["return_id"].astype(str).map(
        lambda rid: "; ".join(dpo_by_return.get(rid, []))
    )
    export_button(
        csv_export, "Export every return as CSV",
        f"{politician.replace(' ', '_')}_{area[:30].replace(' ', '_')}_returns.csv",
        "lob_export_results",
    )

    # ── Return cards ──────────────────────────────────────────────────────
    evidence_heading("Every return")
    page_size, page_idx = pagination_controls(
        total=len(detail),
        key_prefix=f"lob_results_{politician}_{area}",
        label="returns",
    )
    page_slice = detail.iloc[page_idx * page_size : (page_idx + 1) * page_size]
    rank_offset = page_idx * page_size
    cards: list[str] = []
    for i, (_, row) in enumerate(page_slice.iterrows(), start=1):
        rank = rank_offset + i
        org_name  = str(row.get("lobbyist_name", "—"))
        return_id = str(row.get("return_id", "—"))
        period    = str(row.get("period_start_date", "") or "")[:10]
        url       = str(row.get("source_url", "") or "")
        org_meta  = org_lookup.get(org_name, {})
        sector    = org_meta.get("sector",  "")
        website   = org_meta.get("website", "")

        dpo_names = dpo_by_return.get(return_id, [])
        meta_bits: list[str] = []
        if sector:
            meta_bits.append(sector)
        if website and website.startswith("http"):
            meta_bits.append(website)
        meta = " · ".join(meta_bits) if meta_bits else "Sector not on file"
        if dpo_names:
            meta += f"  ⚠ Former DPO involved: {', '.join(dpo_names)}"

        pills = [
            f"Period {period}" if period else "Period —",
            f"Return #{return_id}",
        ]
        if url and url.startswith("http"):
            pills.append(source_link_html(url, "View on lobbying.ie",
                                          aria_label="Open this return on lobbying.ie"))

        cards.append(
            clickable_card_link(
                href=f"?lob_org={quote(org_name)}",
                inner_html=_lob_card_html(org_name, meta, pills, rank=rank),
                aria_label=f"Open lobbying profile for {org_name}",
            )
        )
    st.html("\n".join(cards))

    _provenance_footer(summary)


# ── Entry point ────────────────────────────────────────────────────────────────

def lobbying_page() -> None:
    _init()
    inject_css()

    # Seed session state from URL query params so cards (full-page links) drill
    # straight into the correct stage on first load. Each card writes the URL
    # via clickable_card_link; this read closes the loop.
    qp = st.query_params
    if "lob_dpo" in qp:
        _clear_profile()
        st.session_state.lob_selected_dpo = qp["lob_dpo"]
    elif "lob_rd" in qp:
        _clear_profile()
        st.session_state.lob_view_revolving_door = True
    elif "lob_pol" in qp:
        _clear_profile()
        st.session_state.lob_selected_politician = qp["lob_pol"]
    elif "lob_org" in qp:
        _clear_profile()
        st.session_state.lob_selected_org = qp["lob_org"]
    elif "lob_area" in qp:
        _clear_profile()
        st.session_state.lob_selected_area = qp["lob_area"]
        if "lob_result_pol" in qp:
            st.session_state.lob_results_pol = qp["lob_result_pol"]

    _render_sidebar()

    summary        = fetch_summary()
    sel_pol        = st.session_state.lob_selected_politician
    sel_org        = st.session_state.lob_selected_org
    sel_area       = st.session_state.lob_selected_area
    sel_dpo        = st.session_state.lob_selected_dpo
    view_rd        = st.session_state.lob_view_revolving_door
    sel_result_pol = st.session_state.lob_results_pol

    if sel_dpo:
        _render_dpo_individual(sel_dpo, summary)
    elif view_rd:
        _render_dpo_index(summary)
    elif sel_pol:
        _render_politician(sel_pol, summary)
    elif sel_org:
        _render_org(sel_org, summary)
    elif sel_area and sel_result_pol:
        _render_results(sel_area, sel_result_pol, summary)
    elif sel_area:
        _render_area(sel_area, summary)
    else:
        _render_landing(summary)


if __name__ == "__main__":
    lobbying_page()
