"""
Lobbying — lobbying_2.py  (bold redesign)

Investigative lookup tool with three-path gateway.
Entry points: follow a politician · follow an organisation · browse by policy area.

Architecture:
- All data via DuckDB registered views (sql_views/lobbying_*.sql)
- No raw CSV/Parquet reads in this file
- No joins, groupby, or pivot in this file
- Two-stage flow: landing gateway → Stage 2 profile
"""
from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_access.lobbying_data import (
    fetch_all_org_names,
    fetch_all_politician_names,
    fetch_area_contact_detail,
    fetch_clients_for_org,
    fetch_contact_detail,
    fetch_org_contact_detail,
    fetch_org_index,
    fetch_org_persistence,
    fetch_orgs_for_politician,
    fetch_policy_area_summary,
    fetch_policy_exposure_for_politician,
    fetch_politician_index,
    fetch_politicians_for_area,
    fetch_politicians_for_org,
    fetch_recent_returns,
    fetch_revolving_door,
    fetch_sources_for_org,
    fetch_sources_for_politician,
    fetch_summary,
)
from shared_css import inject_css
from ui.components import (
    clean_meta,
    empty_state,
    evidence_heading,
    hero_banner,
    rank_card_row,
    sidebar_page_header,
    todo_callout,
)
from ui.export_controls import export_button
from ui.source_links import render_source_links
from ui.source_pdfs import provenance_expander

# ── State helpers ──────────────────────────────────────────────────────────────

def _init() -> None:
    for k, v in {
        "lob_selected_politician": None,
        "lob_selected_org":        None,
        "lob_selected_area":       None,
        "lob_sidebar_search":      "",
        "lob_date_start":          None,
        "lob_date_end":            None,
    }.items():
        st.session_state.setdefault(k, v)


def _clear_profile() -> None:
    st.session_state.lob_selected_politician = None
    st.session_state.lob_selected_org        = None
    st.session_state.lob_selected_area       = None


_NAV_KEYS: dict[str, str] = {
    "pol":  "lob_selected_politician",
    "org":  "lob_selected_org",
    "area": "lob_selected_area",
}


def _nav(kind: str, value: object = True) -> None:
    _clear_profile()
    setattr(st.session_state, _NAV_KEYS[kind], value)


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
    if st.button("← Back to Lobbying", key="lob_back"):
        _clear_profile()
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
                "v_lobbying_index — run lobbying_enrichment.py to generate "
                "lobbying_politician_index.parquet"
            )
        else:
            for rank, (_, row) in enumerate(idx.head(10).iterrows(), start=1):
                name  = str(row.get("member_name", "—"))
                meta  = clean_meta(
                    str(row.get("chamber",  "") or ""),
                    str(row.get("position", "") or ""),
                )
                pills = [
                    f"{int(row.get('return_count', 0) or 0):,} returns",
                    f"{int(row.get('distinct_orgs', 0) or 0):,} orgs",
                ]
                if rank_card_row(name, meta, pills, btn_key=f"lob_pol_{rank}", rank=rank):
                    _nav("pol", name)
                    st.rerun()

    with lb2:
        evidence_heading("Most active organisations")
        orgs = fetch_org_index()
        if orgs.empty:
            todo_callout(
                "v_lobbying_org_index — run lobbying_enrichment.py to generate "
                "lobbying_org_index.parquet"
            )
        else:
            for rank, (_, row) in enumerate(orgs.head(10).iterrows(), start=1):
                name  = str(row.get("lobbyist_name", "—"))
                meta  = str(row.get("sector", "") or "")
                pills = [
                    f"{int(row.get('return_count', 0) or 0):,} returns",
                    f"{int(row.get('politicians_targeted', 0) or 0):,} politicians",
                ]
                if rank_card_row(name, meta, pills, btn_key=f"lob_org_{rank}", rank=rank):
                    _nav("org", name)
                    st.rerun()

    # ── Revolving door amber callout ──────────────────────────────────────
    dpos = fetch_revolving_door()
    dpo_n = len(dpos) if not dpos.empty else 0
    st.html(
        f'<div class="lob-revolving-callout">'
        f'<p class="lob-revolving-heading" style="margin:0 0 0.3rem">Revolving Door</p>'
        f'<p style="font-size:0.85rem;color:#78350f;margin:0">'
        f'Former Designated Public Officials (DPOs) — politicians, ministers, and senior officials '
        f'— are subject to a one-year cooling-off period before they may lobby former colleagues. '
        f'{dpo_n:,} individuals identified on lobbying.ie returns by name-matching. '
        f'Treat as indicative — no legal findings are implied.'
        f'</p></div>'
    )
    with st.expander("Show revolving door individuals", expanded=False):
        if dpos.empty:
            todo_callout("v_lobbying_revolving_door — run lobbying_enrichment.py")
        else:
            rows_html = ""
            for _, row in dpos.head(15).iterrows():
                name_     = _h(str(row.get("individual_name", "—")))
                position  = _h(str(row.get("former_position", "") or ""))
                ret_cnt_  = int(row.get("return_count",    0) or 0)
                firm_cnt_ = int(row.get("distinct_firms",  0) or 0)
                former    = f"Former {position}" if position else "Former DPO"
                rows_html += (
                    f'<div class="lob-activity-row">'
                    f'<div class="lob-activity-body">'
                    f'<div class="lob-activity-org">{name_}</div>'
                    f'<div class="lob-activity-area">'
                    f'{_h(former)} · {ret_cnt_:,} returns · {firm_cnt_:,} firms'
                    f'</div></div></div>'
                )
            st.html(rows_html)

    # ── Latest activity feed ──────────────────────────────────────────────
    evidence_heading("Latest returns")
    recent = fetch_recent_returns()
    if recent.empty:
        todo_callout(
            "v_lobbying_recent_returns — run lobbying_enrichment.py to generate "
            "lobbying_recent_returns.parquet"
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


# ── Politician Stage 2 ─────────────────────────────────────────────────────────

def _render_politician(name: str, summary: pd.DataFrame) -> None:
    _back_button()

    idx = fetch_politician_index()
    pol_row = pd.Series()
    if not idx.empty and "member_name" in idx.columns:
        m = idx[idx["member_name"] == name]
        if not m.empty:
            pol_row = m.iloc[0]

    chamber  = str(pol_row.get("chamber",  "") or "")
    position = str(pol_row.get("position", "") or "")
    ret_cnt  = int(pol_row.get("return_count",          0) or 0)
    org_cnt  = int(pol_row.get("distinct_orgs",         0) or 0)
    area_cnt = int(pol_row.get("distinct_policy_areas", 0) or 0)
    first_p  = str(pol_row.get("first_period", "") or "")
    last_p   = str(pol_row.get("last_period",  "") or "")

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

    # ── Orgs by intensity — ranked cards (primary view) ───────────────────
    evidence_heading("Organisations lobbying this politician")
    intensity = fetch_orgs_for_politician(name)
    if intensity.empty:
        empty_state("No intensity data", "No organisations found lobbying this politician.")
    else:
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
            if rank_card_row(org_name, meta, pills, btn_key=f"lob_pol_org_{rank}", rank=rank):
                _nav("org", org_name)
                st.rerun()

    # ── Policy exposure ───────────────────────────────────────────────────
    evidence_heading("Policy areas lobbied on")
    exposure = fetch_policy_exposure_for_politician(name)
    if not exposure.empty:
        disp2 = exposure.rename(columns={
            "public_policy_area": "Policy area",
            "returns_targeting":  "Returns",
            "distinct_lobbyists": "Organisations",
        })
        st.dataframe(disp2, use_container_width=True, hide_index=True)

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
        h_web = _h(website)
        badges.append(
            f'<a href="{h_web}" target="_blank" rel="noopener noreferrer" '
            f'style="color:var(--accent)">{h_web}</a>'
        )

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
        for rank, (_, row) in enumerate(pol_intensity.iterrows(), start=1):
            pol_name = str(row.get("member_name", "—"))
            chamber  = str(row.get("chamber", "") or "")
            first_c  = str(row.get("first_contact", "") or "")[:7]
            last_c   = str(row.get("last_contact",  "") or "")[:7]
            meta     = clean_meta(chamber, first_c, last_c)
            pills    = [
                f"{int(row.get('returns_in_relationship', 0) or 0):,} returns",
                f"{int(row.get('distinct_policy_areas',   0) or 0):,} policy areas",
            ]
            if rank_card_row(pol_name, meta, pills, btn_key=f"lob_org_pol_{rank}", rank=rank):
                _nav("pol", pol_name)
                st.rerun()

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
        st.dataframe(disp_c, use_container_width=True, hide_index=True)

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
    area_pols = fetch_politicians_for_area(area)
    if area_pols.empty:
        empty_state("No data", "No politicians found for this policy area.")
    else:
        for rank, (_, row) in enumerate(area_pols.iterrows(), start=1):
            pol_name = str(row.get("member_name", "—"))
            chamber  = str(row.get("chamber", "") or "")
            pills    = [
                f"{int(row.get('returns_targeting',  0) or 0):,} returns",
                f"{int(row.get('distinct_lobbyists', 0) or 0):,} orgs",
            ]
            if rank_card_row(pol_name, chamber, pills, btn_key=f"lob_area_pol_{rank}", rank=rank):
                _nav("pol", pol_name)
                st.rerun()

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


# ── Entry point ────────────────────────────────────────────────────────────────

def lobbying_page() -> None:
    _init()
    inject_css()

    _render_sidebar()

    summary  = fetch_summary()
    sel_pol  = st.session_state.lob_selected_politician
    sel_org  = st.session_state.lob_selected_org
    sel_area = st.session_state.lob_selected_area

    if sel_pol:
        _render_politician(sel_pol, summary)
    elif sel_org:
        _render_org(sel_org, summary)
    elif sel_area:
        _render_area(sel_area, summary)
    else:
        _render_landing(summary)


if __name__ == "__main__":
    lobbying_page()
