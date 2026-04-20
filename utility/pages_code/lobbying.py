
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent
_OUT = _ROOT / "lobbyist" / "output"

_VIEWS = [
    "Overview",
    "Politician Profile",
    "Lobbyist Profile",
    "Browse Returns",
    "Revolving Door",
    "Transparency",
]


# ── data ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load(filename: str) -> pd.DataFrame:
    p = _OUT / filename
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, low_memory=False)


# ── helpers ───────────────────────────────────────────────────────────

def _export(df: pd.DataFrame, filename: str, key: str, labefl: str = "Export CSV") -> None:
    st.download_button(
        labefl, df.to_csv(index=False).encode("utf-8"), filename, "text/csv", key=key,
    )


def _link() -> st.column_config.LinkColumn:
    return st.column_config.LinkColumn("Link", display_text="Open ↗", width="small")


def _stat(num, label: str) -> str:
    return (
        f"<div>"
        f'<div class="stat-num">{num}</div>'
        f'<div class="stat-lbl">{label}</div>'
        f"</div>"
    )


def _section(text: str) -> None:
    st.markdown(f'<p class="section-heading">{text}</p>', unsafe_allow_html=True)


def _parse_dates(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df = df.copy()
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ── views ─────────────────────────────────────────────────────────────

def _overview() -> None:
    returns      = _load("returns_master.csv")
    most_lobbied = _load("most_lobbied_politicians.csv")
    lobby_count  = _load("lobby_count_details.csv")
    policy       = _load("experimental_policy_area_breakdown.csv")
    quarterly    = _load("experimental_quarterly_trend.csv")
    clients      = _load("experimental_top_client_companies.csv")
    bilateral    = _load("experimental_bilateral_relationships.csv")

    # ── stat strip ────────────────────────────────────────────────────
    if not returns.empty:
        returns = _parse_dates(returns, "lobbying_period_start_date")
        min_y = returns["lobbying_period_start_date"].min()
        max_y = returns["lobbying_period_start_date"].max()
        date_range = (
            f"{int(min_y.year)}–{int(max_y.year)}"
            if pd.notna(min_y) and pd.notna(max_y) else "—"
        )
    else:
        date_range = "—"

    total_returns = len(returns)
    total_orgs    = lobby_count["lobbyist_name"].nunique() if not lobby_count.empty else 0
    total_pols    = most_lobbied["full_name"].nunique() if not most_lobbied.empty else 0
    total_areas   = policy["public_policy_area"].nunique() if not policy.empty else 0

    st.markdown(
        """
        <div style="background:#1d3557;padding:0.7rem 0 0.5rem 0;margin-bottom:1.2rem;border-radius:6px 6px 0 0;">
            <h1 style="color:#fff;font-family:Epilogue,sans-serif;font-size:2.1rem;font-weight:700;margin:0 0 0 1.2rem;letter-spacing:-0.03em;">Oireachtas Explorer</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        '<h1 class="page-title">Lobbying in Ireland</h1>', unsafe_allow_html=True)
    st.markdown(
        """
        <div style="font-size:1.1rem;line-height:1.6;margin-bottom:0.7em">
        <strong>Who is lobbying whom?</strong> This dashboard brings together all registered lobbying returns in Ireland since 2015. Explore which organisations are lobbying, which politicians are targeted, and what policy areas are most active.
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("What is this data? (Click for details)", expanded=False):
        st.markdown(
            """
            **About the lobbying register**  
            This data is sourced from the official [Register of Lobbying](https://www.lobbying.ie/) maintained by the Standards in Public Office Commission (SIPO). Organisations and individuals who lobby designated public officials must file returns describing their lobbying activities, targets, and policy areas.
            
            **Data sources:**
            - Official lobbying returns (lobbyist/output/returns_master.csv and related files)
            - Aggregated and cleaned in the 'gold' layer for analysis
            
            **Caveats:**
            - Not all lobbying is captured (e.g., informal or unregistered activity)
            - Some returns may be missing or misclassified
            - Policy area and target names are as reported by filers
            """
        )
    st.markdown(
        '<div class="stat-strip">'
        + _stat(f"{total_returns:,}", "Returns")
        + _stat(f"{total_orgs:,}", "Lobbying orgs")
        + _stat(f"{total_pols:,}", "Politicians targeted")
        + _stat(total_areas, "Policy areas")
        + _stat(date_range, "Period covered")
        + "</div>",
        unsafe_allow_html=True,
    )

    # ── most lobbied | most prolific ──────────────────────────────────
    col_l, col_r = st.columns(2)

    with col_l:
        _section("Most lobbied politicians")
        if not most_lobbied.empty:
            top_ml = (
                most_lobbied
                .groupby("full_name", as_index=False)
                .agg(total_returns=("total_returns", "max"), distinct_orgs=("distinct_orgs", "max"))
                .sort_values("total_returns", ascending=False)
                .head(20)
                .reset_index(drop=True)
            )
            max_r = int(top_ml["total_returns"].max()) or 1
            st.dataframe(
                top_ml,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "full_name":       st.column_config.TextColumn("Politician"),
                    "total_returns":   st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_r),
                    "distinct_orgs":   st.column_config.NumberColumn("Orgs"),
                },
            )
            _export(top_ml, "most_lobbied_politicians.csv", "ov_ml_exp")

    with col_r:
        _section("Most prolific lobbying organisations")
        if not lobby_count.empty:
            top_lc = (
                lobby_count
                .groupby("lobbyist_name", as_index=False)
                .agg(returns=("lobby_requests_count", "first"))
                .sort_values("returns", ascending=False)
                .head(20)
                .reset_index(drop=True)
            )
            max_lc = int(top_lc["returns"].max()) or 1
            st.dataframe(
                top_lc,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "lobbyist_name": st.column_config.TextColumn("Organisation", width="large"),
                    "returns":       st.column_config.ProgressColumn("Returns filed", format="%d", min_value=0, max_value=max_lc),
                },
            )
            _export(top_lc, "most_prolific_lobbyists.csv", "ov_lc_exp")

    # ── quarterly trend ───────────────────────────────────────────────
    if not quarterly.empty and "year_quarter" in quarterly.columns:
        _section("Returns filed per quarter")
        chart_df = quarterly.sort_values("year_quarter").set_index("year_quarter")[["return_count"]]
        st.bar_chart(chart_df, use_container_width=True)
        _export(quarterly.sort_values("year_quarter"), "quarterly_trend.csv", "ov_qt_exp")

    # ── policy area breakdown ─────────────────────────────────────────
    if not policy.empty:
        _section("Returns by policy area")
        policy_s = policy.sort_values("return_count", ascending=False).reset_index(drop=True)
        max_pa = int(policy_s["return_count"].max()) or 1
        st.dataframe(
            policy_s,
            hide_index=True,
            use_container_width=True,
            column_config={
                "public_policy_area": st.column_config.TextColumn("Policy area", width="large"),
                "return_count":       st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_pa),
                "distinct_lobbyists": st.column_config.NumberColumn("Distinct orgs"),
            },
        )
        _export(policy_s, "policy_area_breakdown.csv", "ov_pa_exp")

    # ── top client companies ──────────────────────────────────────────
    if not clients.empty:
        _section("Top client companies")
        st.caption("Companies that hired third-party lobbying firms to lobby on their behalf.")
        top_cl = clients.sort_values("return_count", ascending=False).head(20).reset_index(drop=True)
        max_cl = int(top_cl["return_count"].max()) or 1
        st.dataframe(
            top_cl,
            hide_index=True,
            use_container_width=True,
            column_config={
                "client_name":                   st.column_config.TextColumn("Client company", width="large"),
                "return_count":                  st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_cl),
                "distinct_lobbyist_firms":        st.column_config.NumberColumn("Firms hired"),
                "distinct_politicians_targeted":  st.column_config.NumberColumn("Politicians targeted"),
                "distinct_policy_areas":          st.column_config.NumberColumn("Policy areas"),
                "distinct_chambers":              st.column_config.NumberColumn("Chambers"),
            },
        )
        _export(clients, "top_client_companies.csv", "ov_cc_exp")

    # ── bilateral relationships ───────────────────────────────────────
    if not bilateral.empty:
        _section("Most persistent lobbying relationships")
        st.caption("Same organisation targeting the same politician across multiple filing periods.")
        top_bl = bilateral.sort_values("returns_in_relationship", ascending=False).head(20).reset_index(drop=True)
        max_bl = int(top_bl["returns_in_relationship"].max()) or 1
        st.dataframe(
            top_bl,
            hide_index=True,
            use_container_width=True,
            column_config={
                "lobbyist_name":          st.column_config.TextColumn("Organisation", width="large"),
                "full_name":              st.column_config.TextColumn("Politician"),
                "chamber":                st.column_config.TextColumn("Chamber", width="small"),
                "returns_in_relationship":st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_bl),
                "distinct_periods":       st.column_config.NumberColumn("Periods active"),
                "distinct_policy_areas":  st.column_config.NumberColumn("Policy areas"),
            },
        )
        _export(bilateral, "bilateral_relationships.csv", "ov_bl_exp")


def _politician_profile() -> None:
    pol_returns  = _load("politician_returns_detail.csv")
    distinct_orgs = _load("experimental_distinct_orgs_per_politician.csv")

    if pol_returns.empty:
        st.info("No politician returns data found. Run the pipeline first.")
        return

    pol_returns = _parse_dates(pol_returns, "lobbying_period_start_date")
    all_names   = sorted(pol_returns["full_name"].dropna().unique())

    # ── search + select ───────────────────────────────────────────────
    search = st.text_input(
        "Search politician", placeholder="Type a name…",
        key="pol_search", label_visibility="collapsed",
    )
    query          = search.strip().lower()
    filtered_names = [n for n in all_names if query in n.lower()] if query else all_names
    current        = st.session_state.get("pol_selected")
    default_idx    = filtered_names.index(current) if current in filtered_names else None

    chosen = st.selectbox(
        "Select politician", filtered_names,
        index=default_idx, placeholder="Select a politician…",
        label_visibility="collapsed", key="pol_selectbox",
    )
    if chosen and chosen != current:
        st.session_state["pol_selected"] = chosen

    td = st.session_state.get("pol_selected")
    if not td:
        return

    person = pol_returns[pol_returns["full_name"] == td].copy()
    if person.empty:
        st.warning(f"No data for {td}.")
        return

    chamber  = person["chamber"].dropna().iloc[0] if "chamber" in person.columns and not person["chamber"].dropna().empty else "—"
    position = person["position"].dropna().iloc[0] if "position" in person.columns and not person["position"].dropna().empty else "—"
    total_r  = person["primary_key"].nunique()
    total_o  = person["lobbyist_name"].nunique() if "lobbyist_name" in person.columns else 0
    total_a  = person["public_policy_area"].nunique() if "public_policy_area" in person.columns else 0
    first_d  = person["lobbying_period_start_date"].min()
    last_d   = person["lobbying_period_start_date"].max()
    span     = f"{int(first_d.year)}–{int(last_d.year)}" if pd.notna(first_d) and pd.notna(last_d) else "—"

    st.markdown(
        '<hr class="section-rule">'
        f'<div style="padding:0 0 1rem 0;">'
        f'<div class="td-name">{td}</div>'
        f'<div class="td-meta">{position} · {chamber}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="stat-strip">'
        + _stat(total_r, "Returns targeting them")
        + _stat(total_o, "Distinct orgs")
        + _stat(total_a, "Policy areas")
        + _stat(span, "Period")
        + "</div>",
        unsafe_allow_html=True,
    )

    # ── filters ───────────────────────────────────────────────────────
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        area_opts   = ["All areas"] + sorted(person["public_policy_area"].dropna().unique().tolist()) if "public_policy_area" in person.columns else ["All areas"]
        area_filter = st.selectbox("Policy area", area_opts, key="pol_area")
    with col_f2:
        years       = sorted(person["lobbying_period_start_date"].dropna().dt.year.unique().astype(int).tolist())
        year_filter = st.selectbox("Year", ["All years"] + [str(y) for y in years], key="pol_year")

    filtered = person.copy()
    if area_filter != "All areas":
        filtered = filtered[filtered["public_policy_area"] == area_filter]
    if year_filter != "All years":
        filtered = filtered[filtered["lobbying_period_start_date"].dt.year == int(year_filter)]

    # ── returns table ─────────────────────────────────────────────────
    _section(f"Returns targeting {td} ({len(filtered)})")
    view_cols = [c for c in ["lobbyist_name", "lobby_url", "public_policy_area", "lobbying_period_start_date"] if c in filtered.columns]
    st.dataframe(
        filtered[view_cols].sort_values("lobbying_period_start_date", ascending=False).reset_index(drop=True),
        hide_index=True,
        use_container_width=True,
        column_config={
            "lobbyist_name":              st.column_config.TextColumn("Organisation", width="large"),
            "lobby_url":                  _link(),
            "public_policy_area":         st.column_config.TextColumn("Policy area"),
            "lobbying_period_start_date": st.column_config.DateColumn("Period start", format="YYYY-MM-DD"),
        },
    )
    _export(filtered[view_cols], f"{td.replace(' ', '_')}_lobby_returns.csv", "pol_ret_exp")

    # ── breakdown ─────────────────────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        _section("By policy area")
        if "public_policy_area" in filtered.columns:
            pa = (
                filtered.groupby("public_policy_area")["primary_key"]
                .nunique().reset_index()
                .rename(columns={"primary_key": "returns"})
                .sort_values("returns", ascending=False)
            )
            max_pa = int(pa["returns"].max()) or 1
            st.dataframe(
                pa, hide_index=True, use_container_width=True,
                column_config={
                    "public_policy_area": st.column_config.TextColumn("Policy area"),
                    "returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_pa),
                },
            )
            _export(pa, f"{td.replace(' ', '_')}_policy_areas.csv", "pol_pa_exp")

    with col_b:
        _section("By organisation")
        if "lobbyist_name" in filtered.columns:
            ob = (
                filtered.groupby("lobbyist_name")["primary_key"]
                .nunique().reset_index()
                .rename(columns={"primary_key": "returns"})
                .sort_values("returns", ascending=False)
            )
            max_ob = int(ob["returns"].max()) or 1
            st.dataframe(
                ob, hide_index=True, use_container_width=True,
                column_config={
                    "lobbyist_name": st.column_config.TextColumn("Organisation"),
                    "returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_ob),
                },
            )
            _export(ob, f"{td.replace(' ', '_')}_orgs.csv", "pol_org_exp")


def _lobbyist_profile() -> None:
    lob_returns = _load("lobbyist_returns_detail.csv")
    persistence = _load("experimental_lobbyist_persistence.csv")
    reach       = _load("experimental_reach_by_lobbyist.csv")

    if lob_returns.empty:
        st.info("No lobbyist returns data found. Run the pipeline first.")
        return

    lob_returns = _parse_dates(lob_returns, "lobbying_period_start_date")
    all_orgs    = sorted(lob_returns["lobbyist_name"].dropna().unique())

    # ── search + select ───────────────────────────────────────────────
    search = st.text_input(
        "Search organisation", placeholder="Type a name…",
        key="lob_search", label_visibility="collapsed",
    )
    query        = search.strip().lower()
    filtered_orgs = [o for o in all_orgs if query in o.lower()] if query else all_orgs
    current      = st.session_state.get("lob_selected")
    default_idx  = filtered_orgs.index(current) if current in filtered_orgs else None

    chosen = st.selectbox(
        "Select organisation", filtered_orgs,
        index=default_idx, placeholder="Select an organisation…",
        label_visibility="collapsed", key="lob_selectbox",
    )
    if chosen and chosen != current:
        st.session_state["lob_selected"] = chosen

    org = st.session_state.get("lob_selected")
    if not org:
        return

    org_returns = lob_returns[lob_returns["lobbyist_name"] == org].copy()
    if org_returns.empty:
        st.warning(f"No data for {org}.")
        return

    pers_row  = persistence[persistence["lobbyist_name"] == org].iloc[0] if not persistence.empty and (persistence["lobbyist_name"] == org).any() else None
    reach_row = reach[reach["lobbyist_name"] == org].iloc[0] if not reach.empty and (reach["lobbyist_name"] == org).any() else None

    total_r    = org_returns["primary_key"].nunique()
    total_a    = org_returns["public_policy_area"].nunique() if "public_policy_area" in org_returns.columns else 0
    active_span = f"{pers_row['active_span_days'] / 365.25:.0f}y" if pers_row is not None and pd.notna(pers_row.get("active_span_days")) else "—"
    periods    = int(pers_row["distinct_periods_filed"]) if pers_row is not None and pd.notna(pers_row.get("distinct_periods_filed")) else "—"
    est_reach  = f"{int(reach_row['total_reach_estimate']):,}" if reach_row is not None and pd.notna(reach_row.get("total_reach_estimate")) else "—"

    st.markdown(
        '<hr class="section-rule">'
        f'<div style="padding:0 0 1rem 0;">'
        f'<div class="td-name">{org}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="stat-strip">'
        + _stat(total_r, "Returns filed")
        + _stat(total_a, "Policy areas")
        + _stat(periods, "Periods active")
        + _stat(active_span, "Active span")
        + _stat(est_reach, "Est. total reach")
        + "</div>",
        unsafe_allow_html=True,
    )

    # ── filters ───────────────────────────────────────────────────────
    area_opts   = ["All areas"] + sorted(org_returns["public_policy_area"].dropna().unique().tolist()) if "public_policy_area" in org_returns.columns else ["All areas"]
    area_filter = st.selectbox("Filter by policy area", area_opts, key="lob_area")

    filtered = org_returns.copy()
    if area_filter != "All areas":
        filtered = filtered[filtered["public_policy_area"] == area_filter]

    # ── returns table ─────────────────────────────────────────────────
    _section(f"Returns filed by {org} ({len(filtered)})")
    view_cols = [c for c in ["primary_key", "lobby_url", "public_policy_area", "relevant_matter", "lobbying_period_start_date"] if c in filtered.columns]
    st.dataframe(
        filtered[view_cols].sort_values("lobbying_period_start_date", ascending=False).reset_index(drop=True),
        hide_index=True,
        use_container_width=True,
        column_config={
            "primary_key":                st.column_config.NumberColumn("ID", width="small"),
            "lobby_url":                  _link(),
            "public_policy_area":         st.column_config.TextColumn("Policy area"),
            "relevant_matter":            st.column_config.TextColumn("Matter"),
            "lobbying_period_start_date": st.column_config.DateColumn("Period start", format="YYYY-MM-DD"),
        },
    )
    _export(filtered[view_cols], f"{org[:40].replace(' ', '_')}_returns.csv", "lob_ret_exp")

    # ── policy area breakdown ─────────────────────────────────────────
    if "public_policy_area" in filtered.columns:
        _section("Policy area breakdown")
        pa = (
            filtered.groupby("public_policy_area")["primary_key"]
            .nunique().reset_index()
            .rename(columns={"primary_key": "returns"})
            .sort_values("returns", ascending=False)
        )
        max_pa = int(pa["returns"].max()) or 1
        st.dataframe(
            pa, hide_index=True, use_container_width=True,
            column_config={
                "public_policy_area": st.column_config.TextColumn("Policy area"),
                "returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_pa),
            },
        )
        _export(pa, f"{org[:40].replace(' ', '_')}_policy_areas.csv", "lob_pa_exp")


def _browse_returns() -> None:
    returns = _load("returns_master.csv")

    if returns.empty:
        st.info("No returns data found. Run the pipeline first.")
        return

    returns = _parse_dates(returns, "lobbying_period_start_date")
    returns = _parse_dates(returns, "lobbying_period_end_date")

    # ── filters ───────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        org_search = st.text_input("Organisation", placeholder="Filter by org name…", key="br_org")
    with col2:
        area_opts   = ["All areas"] + sorted(returns["public_policy_area"].dropna().unique().tolist()) if "public_policy_area" in returns.columns else ["All areas"]
        area_filter = st.selectbox("Policy area", area_opts, key="br_area")
    with col3:
        years       = sorted(returns["lobbying_period_start_date"].dropna().dt.year.unique().astype(int).tolist())
        year_filter = st.selectbox("Year", ["All years"] + [str(y) for y in years], key="br_year")

    col4, col5 = st.columns(2)
    with col4:
        grass_filter  = st.selectbox("Grassroots campaign", ["All", "Yes", "No"], key="br_grass")
    with col5:
        client_filter = st.selectbox("On behalf of client", ["All", "Yes", "No"], key="br_client")

    # ── apply ─────────────────────────────────────────────────────────
    filtered = returns.copy()
    if org_search.strip():
        filtered = filtered[filtered["lobbyist_name"].str.contains(org_search.strip(), case=False, na=False)]
    if area_filter != "All areas":
        filtered = filtered[filtered["public_policy_area"] == area_filter]
    if year_filter != "All years":
        filtered = filtered[filtered["lobbying_period_start_date"].dt.year == int(year_filter)]
    if grass_filter != "All" and "was_this_a_grassroots_campaign" in filtered.columns:
        filtered = filtered[filtered["was_this_a_grassroots_campaign"].astype(str).str.lower() == grass_filter.lower()]
    if client_filter != "All" and "was_this_lobbying_done_on_behalf_of_a_client" in filtered.columns:
        filtered = filtered[filtered["was_this_lobbying_done_on_behalf_of_a_client"].astype(str).str.lower() == client_filter.lower()]

    _section(f"{len(filtered):,} returns")

    view_cols = [c for c in [
        "lobbyist_name", "lobby_url", "public_policy_area", "relevant_matter",
        "person_primarily_responsible", "was_this_a_grassroots_campaign",
        "was_this_lobbying_done_on_behalf_of_a_client",
        "lobbying_period_start_date", "lobbying_period_end_date",
    ] if c in filtered.columns]

    st.dataframe(
        filtered[view_cols].sort_values("lobbying_period_start_date", ascending=False).reset_index(drop=True),
        hide_index=True,
        use_container_width=True,
        column_config={
            "lobbyist_name":                             st.column_config.TextColumn("Organisation", width="large"),
            "lobby_url":                                 _link(),
            "public_policy_area":                        st.column_config.TextColumn("Policy area"),
            "relevant_matter":                           st.column_config.TextColumn("Matter"),
            "person_primarily_responsible":              st.column_config.TextColumn("Responsible"),
            "was_this_a_grassroots_campaign":            st.column_config.TextColumn("Grassroots", width="small"),
            "was_this_lobbying_done_on_behalf_of_a_client": st.column_config.TextColumn("For client", width="small"),
            "lobbying_period_start_date":                st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
            "lobbying_period_end_date":                  st.column_config.DateColumn("End", format="YYYY-MM-DD"),
        },
    )
    _export(filtered[view_cols], "lobbying_returns_filtered.csv", "br_exp")


def _revolving_door() -> None:
    summary   = _load("experimental_revolving_door_dpos.csv")
    detail    = _load("revolving_door_returns_detail.csv")
    name_col  = "dpos_or_former_dpos_who_carried_out_lobbying_name"
    # Load returns_master for Client(s) field
    returns_master = _load("returns_master.csv")

    if summary.empty:
        st.info("No revolving door data found. Run the pipeline first.")
        return

    total_ind = summary[name_col].nunique() if name_col in summary.columns else 0
    total_ret = int(summary["returns_involved_in"].sum()) if "returns_involved_in" in summary.columns else 0

    st.markdown(
        '<div class="stat-strip">'
        + _stat(total_ind, "Former officials now lobbying")
        + _stat(f"{total_ret:,}", "Returns they appeared on")
        + "</div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Current or former Designated Public Officials who personally carried out lobbying "
        "on behalf of a lobbying organisation — the classic revolving door signal."
    )

    _section("Ranked by lobbying activity")
    max_r = int(summary["returns_involved_in"].max()) or 1
    st.dataframe(
        summary.sort_values("returns_involved_in", ascending=False).reset_index(drop=True),
        hide_index=True,
        use_container_width=True,
        column_config={
            name_col:                            st.column_config.TextColumn("Name", width="large"),
            "current_or_former_dpos_position":   st.column_config.TextColumn("Former position"),
            "current_or_former_dpos_chamber":    st.column_config.TextColumn("Chamber", width="small"),
            "returns_involved_in":               st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_r),
            "distinct_lobbyist_firms":           st.column_config.NumberColumn("Firms"),
            "distinct_policy_areas":             st.column_config.NumberColumn("Policy areas"),
            "distinct_politicians_targeted":     st.column_config.NumberColumn("Politicians targeted"),
        },
    )
    _export(summary, "revolving_door_summary.csv", "rd_sum_exp")

    if not detail.empty and name_col in summary.columns:
        st.markdown("---")
        _section("Drill into an individual")
        names  = sorted(summary[name_col].dropna().unique())
        chosen = st.selectbox("Select individual", names, label_visibility="collapsed", key="rd_chosen")
        if chosen:
            person_detail = _parse_dates(detail[detail[name_col] == chosen].copy(), "lobbying_period_start_date")
            # Join company name, address, and specific details from returns_master on primary_key
            if not person_detail.empty and "primary_key" in person_detail.columns and not returns_master.empty:
                person_detail["primary_key"] = person_detail["primary_key"].astype(str)
                returns_master["primary_key"] = returns_master["primary_key"].astype(str)
                merged = person_detail.merge(
                    returns_master[[
                        "primary_key",
                        "lobbyist_name",
                        "specific_details",
                        "intended_results"
                    ]],
                    on="primary_key", how="left"
                )
            else:
                merged = person_detail.copy()
            # Prepare columns for display and export
            dcols = [c for c in [
                name_col,
                "lobby_url",
                "lobbyist_name",
                "public_policy_area",
                "lobbying_period_start_date",
                "specific_details",
                "intended_results"
            ] if c in merged.columns]
            st.dataframe(
                merged[dcols].sort_values("lobbying_period_start_date", ascending=False).reset_index(drop=True),
                hide_index=True,
                use_container_width=True,
                column_config={
                    name_col:                     st.column_config.TextColumn("Name"),
                    "lobby_url":                  _link(),
                    "lobbyist_name":              st.column_config.TextColumn("Client/Company", width="large"),
                    "public_policy_area":         st.column_config.TextColumn("Policy area"),
                    "lobbying_period_start_date": st.column_config.DateColumn("Period start", format="YYYY-MM-DD"),
                    "specific_details":           st.column_config.TextColumn("Specific Details", width="large"),
                    "intended_results":           st.column_config.TextColumn("Intended Results", width="large"),
                    "display_client_name":        st.column_config.TextColumn("Client/Company", width="large"),
                    "display_client_address":     st.column_config.TextColumn("Client Address", width="large"),
                    "display_specific_details":   st.column_config.TextColumn("Specific Details", width="large"),
                },
            )
            _export(merged[dcols], f"{chosen.replace(' ', '_')}_revolving_door.csv", "rd_det_exp")


def _transparency() -> None:
    late_filers  = _load("experimental_time_to_publish.csv")
    desc_lengths = _load("experimental_return_description_lengths.csv")

    col_l, col_r = st.columns(2)

    with col_l:
        _section("Late filers")
        st.caption("Ranked by median days between lobbying period end and submission. Higher = later.")
        if not late_filers.empty:
            lf = late_filers.sort_values("median_days_to_publish", ascending=False).head(30).reset_index(drop=True)
            max_days = int(lf["median_days_to_publish"].max()) or 1
            st.dataframe(
                lf, hide_index=True, use_container_width=True,
                column_config={
                    "lobbyist_name":          st.column_config.TextColumn("Organisation", width="large"),
                    "median_days_to_publish": st.column_config.ProgressColumn("Median days", format="%d", min_value=0, max_value=max_days),
                    "max_days_to_publish":    st.column_config.NumberColumn("Max days"),
                    "returns_filed":          st.column_config.NumberColumn("Returns"),
                },
            )
            _export(late_filers, "late_filers.csv", "tr_late_exp")

    with col_r:
        _section("Shortest return descriptions")
        st.caption("Returns with least content in specific details + intended results — a minimal-compliance proxy.")
        if not desc_lengths.empty:
            shortest = desc_lengths.sort_values("total_desc_len").head(30).reset_index(drop=True)
            median_len = int(desc_lengths["total_desc_len"].median()) or 1
            st.dataframe(
                shortest, hide_index=True, use_container_width=True,
                column_config={
                    "lobbyist_name":          st.column_config.TextColumn("Organisation"),
                    "lobby_url":              _link(),
                    "total_desc_len":         st.column_config.ProgressColumn("Total chars", format="%d", min_value=0, max_value=median_len),
                    "specific_details_len":   st.column_config.NumberColumn("Specific details"),
                    "intended_results_len":   st.column_config.NumberColumn("Intended results"),
                    "lobbying_period_start_date": st.column_config.TextColumn("Period"),
                },
            )
            _export(shortest, "shortest_descriptions.csv", "tr_desc_exp")

    # ── transparency scorecard ────────────────────────────────────────
    if not late_filers.empty and not desc_lengths.empty:
        _section("Transparency scorecard — per organisation")
        st.caption("Filing latency combined with average description length per organisation.")
        avg_desc = (
            desc_lengths.groupby("lobbyist_name")["total_desc_len"]
            .mean().reset_index()
            .rename(columns={"total_desc_len": "avg_desc_len"})
        )
        scorecard = (
            late_filers
            .merge(avg_desc, on="lobbyist_name", how="outer")
            .sort_values("median_days_to_publish", ascending=False)
            .reset_index(drop=True)
        )
        max_sc = int(scorecard["median_days_to_publish"].max()) or 1
        st.dataframe(
            scorecard, hide_index=True, use_container_width=True,
            column_config={
                "lobbyist_name":          st.column_config.TextColumn("Organisation", width="large"),
                "median_days_to_publish": st.column_config.ProgressColumn("Median days late", format="%d", min_value=0, max_value=max_sc),
                "returns_filed":          st.column_config.NumberColumn("Returns"),
                "avg_desc_len":           st.column_config.NumberColumn("Avg description length"),
            },
        )
        _export(scorecard, "transparency_scorecard.csv", "tr_score_exp")


# ── entry point ───────────────────────────────────────────────────────
def lobbying_page() -> None:
    inject_css()

    returns      = _load("returns_master.csv")
    total_returns = len(returns)

    with st.sidebar:
        st.markdown('<div class="page-kicker">Dáil Tracker</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-title">Lobbying<br>Register</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="page-subtitle">{total_returns:,} returns · '
            f'Source: lobbying.ie</div>',
            unsafe_allow_html=True,
        )

        st.markdown('<p class="sidebar-label">View</p>', unsafe_allow_html=True)
        view = st.radio(
            "View", _VIEWS,
            label_visibility="collapsed",
            key="lobby_view",
        )

    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    if view == "Overview":
        _overview()
    elif view == "Politician Profile":
        _politician_profile()
    elif view == "Lobbyist Profile":
        _lobbyist_profile()
    elif view == "Browse Returns":
        _browse_returns()
    elif view == "Revolving Door":
        _revolving_door()
    elif view == "Transparency":
        _transparency()
