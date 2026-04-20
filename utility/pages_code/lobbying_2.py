# Accurate division vote count utility
def get_division_vote_counts(df, debate_title, vote_id, date):
    """
    Return counts of Yes, No, Abstained for a specific division (debate_title, vote_id, date).
    Avoids cross-debate contamination when vote_id is reused.
    """
    filtered = df[(df['debate_title'] == debate_title) &
                  (df['vote_id'] == vote_id) &
                  (df['date'] == date)]
    return {
        'yes': (filtered['vote_type'] == 'Voted Yes').sum(),
        'no': (filtered['vote_type'] == 'Voted No').sum(),
        'abstained': (filtered['vote_type'] == 'Abstained').sum(),
        'total': len(filtered)
    }
import sys
from pathlib import Path

import pandas as pd
import streamlit as st


sys.path.insert(0, str(Path(__file__).parent.parent))
from shared_css import inject_css


# ── paths / config ────────────────────────────────────────────────────

_ROOT = Path(__file__).parent.parent.parent
_OUT = _ROOT / "lobbyist" / "output"

_VIEWS = [
    "Overview",
    "Politician Profile",
    "Lobbyist Profile",
    "Browse Returns",
    "Organisations",
    "Revolving Door",
    "Transparency",
]

_ORG_CSV = Path(__file__).parent.parent.parent / "lobbyist" / "lobby_orgs.csv"

MAX_ROWS_DISPLAY = 2000

DATE_COLUMNS = {
    "returns_master.csv": ["lobbying_period_start_date", "lobbying_period_end_date"],
    "politician_returns_detail.csv": ["lobbying_period_start_date"],
    "lobbyist_returns_detail.csv": ["lobbying_period_start_date"],
    "revolving_door_returns_detail.csv": ["lobbying_period_start_date"],
}

DTYPES = {
    "returns_master.csv": {
        "primary_key": "string",
        "lobbyist_name": "string",
        "lobby_url": "string",
        "public_policy_area": "string",
        "relevant_matter": "string",
        "person_primarily_responsible": "string",
        "was_this_a_grassroots_campaign": "string",
        "was_this_lobbying_done_on_behalf_of_a_client": "string",
        "specific_details": "string",
        "intended_results": "string",
    },
    "politician_returns_detail.csv": {
        "primary_key": "string",
        "full_name": "string",
        "chamber": "string",
        "position": "string",
        "lobbyist_name": "string",
        "lobby_url": "string",
        "public_policy_area": "string",
    },
    "lobbyist_returns_detail.csv": {
        "primary_key": "string",
        "lobbyist_name": "string",
        "lobby_url": "string",
        "public_policy_area": "string",
        "relevant_matter": "string",
    },
    "revolving_door_returns_detail.csv": {
        "primary_key": "string",
        "dpos_or_former_dpos_who_carried_out_lobbying_name": "string",
        "lobby_url": "string",
        "public_policy_area": "string",
    },
    "experimental_revolving_door_dpos.csv": {
        "dpos_or_former_dpos_who_carried_out_lobbying_name": "string",
        "current_or_former_dpos_position": "string",
        "current_or_former_dpos_chamber": "string",
    },
    "lobby_count_details.csv": {
        "lobbyist_name": "string",
    },
    "most_lobbied_politicians.csv": {
        "full_name": "string",
    },
    "experimental_policy_area_breakdown.csv": {
        "public_policy_area": "string",
    },
    "experimental_top_client_companies.csv": {
        "client_name": "string",
    },
    "experimental_bilateral_relationships.csv": {
        "lobbyist_name": "string",
        "full_name": "string",
        "chamber": "string",
    },
    "experimental_time_to_publish.csv": {
        "lobbyist_name": "string",
    },
    "experimental_return_description_lengths.csv": {
        "lobbyist_name": "string",
        "lobby_url": "string",
        "lobbying_period_start_date": "string",
    },
    "experimental_lobbyist_persistence.csv": {
        "lobbyist_name": "string",
    },
    "experimental_reach_by_lobbyist.csv": {
        "lobbyist_name": "string",
    },
}


# ── data loading / caching ────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load(filename: str) -> pd.DataFrame:
    p = _OUT / filename
    if not p.exists():
        return pd.DataFrame()

    return pd.read_csv(
        p,
        low_memory=False,
        dtype=DTYPES.get(filename),
        parse_dates=DATE_COLUMNS.get(filename, []),
    )


@st.cache_data(show_spinner=False)
def _load_returns_master() -> pd.DataFrame:
    df = _load("returns_master.csv")
    if df.empty:
        return df

    df = df.copy()

    if "lobbying_period_start_date" in df.columns:
        df["lobbying_year"] = df["lobbying_period_start_date"].dt.year

    if "was_this_a_grassroots_campaign" in df.columns:
        df["grassroots_norm"] = df["was_this_a_grassroots_campaign"].astype("string").str.lower()

    if "was_this_lobbying_done_on_behalf_of_a_client" in df.columns:
        df["client_norm"] = df["was_this_lobbying_done_on_behalf_of_a_client"].astype("string").str.lower()

    return df


@st.cache_data(show_spinner=False)
def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


@st.cache_data(show_spinner=False)
def _sorted_unique_values(df: pd.DataFrame, col: str) -> list[str]:
    if df.empty or col not in df.columns:
        return []
    return sorted(df[col].dropna().astype(str).unique().tolist())


@st.cache_data(show_spinner=False)
def _distinct_years(df: pd.DataFrame, col: str) -> list[int]:
    if df.empty or col not in df.columns:
        return []
    s = df[col].dropna()
    if not pd.api.types.is_datetime64_any_dtype(s):
        return []
    return sorted(s.dt.year.dropna().astype(int).unique().tolist())


# ── helpers ───────────────────────────────────────────────────────────
# REFACTOR OPPORTUNITY: the helpers below are render-level utilities (buttons, HTML).
# The missing piece is a *data-level* guard — a function that checks required columns
# are present and returns None + a user-visible message if not. Every view function
# currently inlines this check with `if {...}.issubset(df.columns)`, coupling the
# "does the data have what I need?" question to the render logic.
# See: docs/lobbying_refactor.md

def _export(df: pd.DataFrame, filename: str, key: str, label: str = "Export CSV") -> None:
    st.download_button(
        label=label,
        data=_to_csv_bytes(df),
        file_name=filename,
        mime="text/csv",
        key=key,
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


def _limit_display(df: pd.DataFrame, limit: int = MAX_ROWS_DISPLAY) -> pd.DataFrame:
    if len(df) <= limit:
        return df
    return df.head(limit)


def _safe_selectbox(label: str, options: list[str], key: str, current_value=None, placeholder=None):
    if not options:
        st.info("No options available.")
        return None

    index = 0
    if current_value in options:
        index = options.index(current_value)

    return st.selectbox(
        label,
        options,
        index=index,
        key=key,
        label_visibility="collapsed",
        placeholder=placeholder,
    )


# ── views ─────────────────────────────────────────────────────────────

def _overview() -> None:
    returns = _load_returns_master()
    most_lobbied = _load("most_lobbied_politicians.csv")
    lobby_count = _load("lobby_count_details.csv")
    policy = _load("experimental_policy_area_breakdown.csv")
    quarterly = _load("experimental_quarterly_trend.csv")
    clients = _load("experimental_top_client_companies.csv")
    bilateral = _load("experimental_bilateral_relationships.csv")

    if not returns.empty and "lobbying_period_start_date" in returns.columns:
        min_y = returns["lobbying_period_start_date"].min()
        max_y = returns["lobbying_period_start_date"].max()
        date_range = f"{int(min_y.year)}–{int(max_y.year)}" if pd.notna(min_y) and pd.notna(max_y) else "—"
    else:
        date_range = "—"

    total_returns = len(returns)
    total_orgs = lobby_count["lobbyist_name"].nunique() if not lobby_count.empty and "lobbyist_name" in lobby_count.columns else 0
    total_pols = most_lobbied["full_name"].nunique() if not most_lobbied.empty and "full_name" in most_lobbied.columns else 0
    total_areas = policy["public_policy_area"].nunique() if not policy.empty and "public_policy_area" in policy.columns else 0

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

    col_l, col_r = st.columns(2)

    with col_l:
        _section("Most lobbied politicians")
        if not most_lobbied.empty and {"full_name", "total_returns", "distinct_orgs"}.issubset(most_lobbied.columns):
            top_ml = (
                most_lobbied.groupby("full_name", as_index=False)
                .agg(total_returns=("total_returns", "max"), distinct_orgs=("distinct_orgs", "max"))
                .sort_values("total_returns", ascending=False)
                .head(20)
                .reset_index(drop=True)
            )
            max_r = int(top_ml["total_returns"].max()) if not top_ml.empty else 1
            st.dataframe(
                top_ml,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "full_name": st.column_config.TextColumn("Politician"),
                    "total_returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_r or 1),
                    "distinct_orgs": st.column_config.NumberColumn("Orgs"),
                },
            )
            _export(top_ml, "most_lobbied_politicians.csv", "ov_ml_exp")

    with col_r:
        _section("Most prolific lobbying organisations")
        if not lobby_count.empty and {"lobbyist_name", "lobby_requests_count"}.issubset(lobby_count.columns):
            top_lc = (
                lobby_count.groupby("lobbyist_name", as_index=False)
                .agg(returns=("lobby_requests_count", "first"))
                .sort_values("returns", ascending=False)
                .head(20)
                .reset_index(drop=True)
            )
            max_lc = int(top_lc["returns"].max()) if not top_lc.empty else 1
            st.dataframe(
                top_lc,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "lobbyist_name": st.column_config.TextColumn("Organisation", width="large"),
                    "returns": st.column_config.ProgressColumn("Returns filed", format="%d", min_value=0, max_value=max_lc or 1),
                },
            )
            _export(top_lc, "most_prolific_lobbyists.csv", "ov_lc_exp")

    if not quarterly.empty and {"year_quarter", "return_count"}.issubset(quarterly.columns):
        _section("Returns filed per quarter")
        chart_df = quarterly.sort_values("year_quarter").set_index("year_quarter")[["return_count"]]
        st.bar_chart(chart_df, use_container_width=True)
        _export(quarterly.sort_values("year_quarter"), "quarterly_trend.csv", "ov_qt_exp")

    if not policy.empty and {"public_policy_area", "return_count"}.issubset(policy.columns):
        _section("Returns by policy area")
        policy_s = policy.sort_values("return_count", ascending=False).reset_index(drop=True)
        max_pa = int(policy_s["return_count"].max()) if not policy_s.empty else 1
        st.dataframe(
            policy_s,
            hide_index=True,
            use_container_width=True,
            column_config={
                "public_policy_area": st.column_config.TextColumn("Policy area", width="large"),
                "return_count": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_pa or 1),
                "distinct_lobbyists": st.column_config.NumberColumn("Distinct orgs"),
            },
        )
        _export(policy_s, "policy_area_breakdown.csv", "ov_pa_exp")

    if not clients.empty and {"client_name", "return_count"}.issubset(clients.columns):
        _section("Top client companies")
        st.caption("Companies that hired third-party lobbying firms to lobby on their behalf.")
        top_cl = clients.sort_values("return_count", ascending=False).head(20).reset_index(drop=True)
        max_cl = int(top_cl["return_count"].max()) if not top_cl.empty else 1
        st.dataframe(
            top_cl,
            hide_index=True,
            use_container_width=True,
            column_config={
                "client_name": st.column_config.TextColumn("Client company", width="large"),
                "return_count": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_cl or 1),
                "distinct_lobbyist_firms": st.column_config.NumberColumn("Firms hired"),
                "distinct_politicians_targeted": st.column_config.NumberColumn("Politicians targeted"),
                "distinct_policy_areas": st.column_config.NumberColumn("Policy areas"),
                "distinct_chambers": st.column_config.NumberColumn("Chambers"),
            },
        )
        _export(top_cl, "top_client_companies.csv", "ov_cc_exp")

    if not bilateral.empty and {"returns_in_relationship", "lobbyist_name", "full_name"}.issubset(bilateral.columns):
        _section("Most persistent lobbying relationships")
        st.caption("Same organisation targeting the same politician across multiple filing periods.")
        top_bl = bilateral.sort_values("returns_in_relationship", ascending=False).head(20).reset_index(drop=True)
        max_bl = int(top_bl["returns_in_relationship"].max()) if not top_bl.empty else 1
        st.dataframe(
            top_bl,
            hide_index=True,
            use_container_width=True,
            column_config={
                "lobbyist_name": st.column_config.TextColumn("Organisation", width="large"),
                "full_name": st.column_config.TextColumn("Politician"),
                "chamber": st.column_config.TextColumn("Chamber", width="small"),
                "returns_in_relationship": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_bl or 1),
                "distinct_periods": st.column_config.NumberColumn("Periods active"),
                "distinct_policy_areas": st.column_config.NumberColumn("Policy areas"),
            },
        )
        _export(top_bl, "bilateral_relationships.csv", "ov_bl_exp")


def _politician_profile() -> None:
    pol_returns = _load("politician_returns_detail.csv")
    distinct_orgs = _load("experimental_distinct_orgs_per_politician.csv")  # kept if needed later

    if pol_returns.empty:
        st.info("No politician returns data found. Run the pipeline first.")
        return

    all_names = _sorted_unique_values(pol_returns, "full_name")

    search = st.text_input(
        "Search politician",
        placeholder="Type a name…",
        key="pol_search",
        label_visibility="collapsed",
    )
    query = search.strip().lower()
    filtered_names = [n for n in all_names if query in n.lower()] if query else all_names

    current = st.session_state.get("pol_selected")
    chosen = _safe_selectbox(
        "Select politician",
        filtered_names,
        key="pol_selectbox",
        current_value=current,
        placeholder="Select a politician…",
    )

    if chosen and chosen != current:
        st.session_state["pol_selected"] = chosen

    td = st.session_state.get("pol_selected")
    if not td:
        return

    person = pol_returns.loc[pol_returns["full_name"] == td]
    if person.empty:
        st.warning(f"No data for {td}.")
        return

    chamber = person["chamber"].dropna().iloc[0] if "chamber" in person.columns and not person["chamber"].dropna().empty else "—"
    position = person["position"].dropna().iloc[0] if "position" in person.columns and not person["position"].dropna().empty else "—"
    total_r = person["primary_key"].nunique() if "primary_key" in person.columns else len(person)
    total_o = person["lobbyist_name"].nunique() if "lobbyist_name" in person.columns else 0
    total_a = person["public_policy_area"].nunique() if "public_policy_area" in person.columns else 0

    first_d = person["lobbying_period_start_date"].min() if "lobbying_period_start_date" in person.columns else pd.NaT
    last_d = person["lobbying_period_start_date"].max() if "lobbying_period_start_date" in person.columns else pd.NaT
    span = f"{int(first_d.year)}–{int(last_d.year)}" if pd.notna(first_d) and pd.notna(last_d) else "—"

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

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        area_opts = ["All areas"] + _sorted_unique_values(person, "public_policy_area")
        area_filter = st.selectbox("Policy area", area_opts, key="pol_area")
    with col_f2:
        years = _distinct_years(person, "lobbying_period_start_date")
        year_filter = st.selectbox("Year", ["All years"] + [str(y) for y in years], key="pol_year")

    mask = pd.Series(True, index=person.index)
    if area_filter != "All areas" and "public_policy_area" in person.columns:
        mask &= person["public_policy_area"].eq(area_filter)
    if year_filter != "All years" and "lobbying_period_start_date" in person.columns:
        mask &= person["lobbying_period_start_date"].dt.year.eq(int(year_filter))

    filtered = person.loc[mask]

    _section(f"Returns targeting {td} ({len(filtered)})")
    view_cols = [c for c in ["lobbyist_name", "lobby_url", "public_policy_area", "lobbying_period_start_date"] if c in filtered.columns]

    display_df = (
        filtered.loc[:, view_cols]
        .sort_values("lobbying_period_start_date", ascending=False)
        .reset_index(drop=True)
    )
    if len(display_df) > MAX_ROWS_DISPLAY:
        st.caption(f"Showing first {MAX_ROWS_DISPLAY:,} of {len(display_df):,} rows.")
        display_df = _limit_display(display_df)

    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "lobbyist_name": st.column_config.TextColumn("Organisation", width="large"),
            "lobby_url": _link(),
            "public_policy_area": st.column_config.TextColumn("Policy area"),
            "lobbying_period_start_date": st.column_config.DateColumn("Period start", format="YYYY-MM-DD"),
        },
    )
    _export(filtered.loc[:, view_cols], f"{td.replace(' ', '_')}_lobby_returns.csv", "pol_ret_exp")

    col_a, col_b = st.columns(2)

    with col_a:
        _section("By policy area")
        if {"public_policy_area", "primary_key"}.issubset(filtered.columns):
            # REFACTOR TARGET: this groupby-count-rename-sort block appears ~4 times.
            # A generic _group_count(df, group_col, count_col) would remove the duplication
            # and make each call site declare its intent rather than its mechanics.
            pa = (
                filtered.groupby("public_policy_area", dropna=True)["primary_key"]
                .nunique()
                .reset_index()
                .rename(columns={"primary_key": "returns"})
                .sort_values("returns", ascending=False)
            )
            max_pa = int(pa["returns"].max()) if not pa.empty else 1
            st.dataframe(
                pa,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "public_policy_area": st.column_config.TextColumn("Policy area"),
                    "returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_pa or 1),
                },
            )
            _export(pa, f"{td.replace(' ', '_')}_policy_areas.csv", "pol_pa_exp")

    with col_b:
        _section("By organisation")
        if {"lobbyist_name", "primary_key"}.issubset(filtered.columns):
            ob = (
                filtered.groupby("lobbyist_name", dropna=True)["primary_key"]
                .nunique()
                .reset_index()
                .rename(columns={"primary_key": "returns"})
                .sort_values("returns", ascending=False)
            )
            max_ob = int(ob["returns"].max()) if not ob.empty else 1
            st.dataframe(
                ob,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "lobbyist_name": st.column_config.TextColumn("Organisation"),
                    "returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_ob or 1),
                },
            )
            _export(ob, f"{td.replace(' ', '_')}_orgs.csv", "pol_org_exp")


def _lobbyist_profile() -> None:
    lob_returns = _load("lobbyist_returns_detail.csv")
    persistence = _load("experimental_lobbyist_persistence.csv")
    reach = _load("experimental_reach_by_lobbyist.csv")

    if lob_returns.empty:
        st.info("No lobbyist returns data found. Run the pipeline first.")
        return

    all_orgs = _sorted_unique_values(lob_returns, "lobbyist_name")

    search = st.text_input(
        "Search organisation",
        placeholder="Type a name…",
        key="lob_search",
        label_visibility="collapsed",
    )
    query = search.strip().lower()
    filtered_orgs = [o for o in all_orgs if query in o.lower()] if query else all_orgs

    current = st.session_state.get("lob_selected")
    chosen = _safe_selectbox(
        "Select organisation",
        filtered_orgs,
        key="lob_selectbox",
        current_value=current,
        placeholder="Select an organisation…",
    )

    if chosen and chosen != current:
        st.session_state["lob_selected"] = chosen

    org = st.session_state.get("lob_selected")
    if not org:
        return

    org_returns = lob_returns.loc[lob_returns["lobbyist_name"] == org]
    if org_returns.empty:
        st.warning(f"No data for {org}.")
        return

    pers_row = None
    if not persistence.empty and "lobbyist_name" in persistence.columns:
        m = persistence["lobbyist_name"] == org
        if m.any():
            pers_row = persistence.loc[m].iloc[0]

    reach_row = None
    if not reach.empty and "lobbyist_name" in reach.columns:
        m = reach["lobbyist_name"] == org
        if m.any():
            reach_row = reach.loc[m].iloc[0]

    total_r = org_returns["primary_key"].nunique() if "primary_key" in org_returns.columns else len(org_returns)
    total_a = org_returns["public_policy_area"].nunique() if "public_policy_area" in org_returns.columns else 0
    active_span = f"{pers_row['active_span_days'] / 365.25:.0f}y" if pers_row is not None and pd.notna(pers_row.get("active_span_days")) else "—"
    periods = int(pers_row["distinct_periods_filed"]) if pers_row is not None and pd.notna(pers_row.get("distinct_periods_filed")) else "—"
    est_reach = f"{int(reach_row['total_reach_estimate']):,}" if reach_row is not None and pd.notna(reach_row.get("total_reach_estimate")) else "—"

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

    area_opts = ["All areas"] + _sorted_unique_values(org_returns, "public_policy_area")
    area_filter = st.selectbox("Filter by policy area", area_opts, key="lob_area")

    filtered = (
        org_returns if area_filter == "All areas"
        else org_returns.loc[org_returns["public_policy_area"] == area_filter]
    )

    _section(f"Returns filed by {org} ({len(filtered)})")
    view_cols = [c for c in ["primary_key", "lobby_url", "public_policy_area", "relevant_matter", "lobbying_period_start_date"] if c in filtered.columns]

    display_df = (
        filtered.loc[:, view_cols]
        .sort_values("lobbying_period_start_date", ascending=False)
        .reset_index(drop=True)
    )
    if len(display_df) > MAX_ROWS_DISPLAY:
        st.caption(f"Showing first {MAX_ROWS_DISPLAY:,} of {len(display_df):,} rows.")
        display_df = _limit_display(display_df)

    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "primary_key": st.column_config.NumberColumn("ID", width="small"),
            "lobby_url": _link(),
            "public_policy_area": st.column_config.TextColumn("Policy area"),
            "relevant_matter": st.column_config.TextColumn("Matter"),
            "lobbying_period_start_date": st.column_config.DateColumn("Period start", format="YYYY-MM-DD"),
        },
    )
    _export(filtered.loc[:, view_cols], f"{org[:40].replace(' ', '_')}_returns.csv", "lob_ret_exp")

    if {"public_policy_area", "primary_key"}.issubset(filtered.columns):
        _section("Policy area breakdown")
        pa = (
            filtered.groupby("public_policy_area", dropna=True)["primary_key"]
            .nunique()
            .reset_index()
            .rename(columns={"primary_key": "returns"})
            .sort_values("returns", ascending=False)
        )
        max_pa = int(pa["returns"].max()) if not pa.empty else 1
        st.dataframe(
            pa,
            hide_index=True,
            use_container_width=True,
            column_config={
                "public_policy_area": st.column_config.TextColumn("Policy area"),
                "returns": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_pa or 1),
            },
        )
        _export(pa, f"{org[:40].replace(' ', '_')}_policy_areas.csv", "lob_pa_exp")


def _browse_returns() -> None:
    returns = _load_returns_master()

    if returns.empty:
        st.info("No returns data found. Run the pipeline first.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        org_search = st.text_input("Organisation", placeholder="Filter by org name…", key="br_org")
    with col2:
        area_opts = ["All areas"] + _sorted_unique_values(returns, "public_policy_area")
        area_filter = st.selectbox("Policy area", area_opts, key="br_area")
    with col3:
        years = _distinct_years(returns, "lobbying_period_start_date")
        year_filter = st.selectbox("Year", ["All years"] + [str(y) for y in years], key="br_year")

    col4, col5 = st.columns(2)
    with col4:
        grass_filter = st.selectbox("Grassroots campaign", ["All", "Yes", "No"], key="br_grass")
    with col5:
        client_filter = st.selectbox("On behalf of client", ["All", "Yes", "No"], key="br_client")

    mask = pd.Series(True, index=returns.index)

    if org_search.strip() and "lobbyist_name" in returns.columns:
        mask &= returns["lobbyist_name"].str.contains(org_search.strip(), case=False, na=False)

    if area_filter != "All areas" and "public_policy_area" in returns.columns:
        mask &= returns["public_policy_area"].eq(area_filter)

    if year_filter != "All years" and "lobbying_year" in returns.columns:
        mask &= returns["lobbying_year"].eq(int(year_filter))

    if grass_filter != "All" and "grassroots_norm" in returns.columns:
        mask &= returns["grassroots_norm"].eq(grass_filter.lower())

    if client_filter != "All" and "client_norm" in returns.columns:
        mask &= returns["client_norm"].eq(client_filter.lower())

    filtered = returns.loc[mask]

    _section(f"{len(filtered):,} returns")

    view_cols = [c for c in [
        "lobbyist_name",
        "lobby_url",
        "public_policy_area",
        "relevant_matter",
        "person_primarily_responsible",
        "was_this_a_grassroots_campaign",
        "was_this_lobbying_done_on_behalf_of_a_client",
        "lobbying_period_start_date",
        "lobbying_period_end_date",
    ] if c in filtered.columns]

    display_df = (
        filtered.loc[:, view_cols]
        .sort_values("lobbying_period_start_date", ascending=False)
        .reset_index(drop=True)
    )
    if len(display_df) > MAX_ROWS_DISPLAY:
        st.caption(f"Showing first {MAX_ROWS_DISPLAY:,} of {len(display_df):,} rows.")
        display_df = _limit_display(display_df)

    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "lobbyist_name": st.column_config.TextColumn("Organisation", width="large"),
            "lobby_url": _link(),
            "public_policy_area": st.column_config.TextColumn("Policy area"),
            "relevant_matter": st.column_config.TextColumn("Matter"),
            "person_primarily_responsible": st.column_config.TextColumn("Responsible"),
            "was_this_a_grassroots_campaign": st.column_config.TextColumn("Grassroots", width="small"),
            "was_this_lobbying_done_on_behalf_of_a_client": st.column_config.TextColumn("For client", width="small"),
            "lobbying_period_start_date": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
            "lobbying_period_end_date": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
        },
    )
    _export(filtered.loc[:, view_cols], "lobbying_returns_filtered.csv", "br_exp")


def _revolving_door() -> None:
    summary = _load("experimental_revolving_door_dpos.csv")
    detail = _load("revolving_door_returns_detail.csv")
    returns_master = _load_returns_master()
    name_col = "dpos_or_former_dpos_who_carried_out_lobbying_name"

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
    max_r = int(summary["returns_involved_in"].max()) if not summary.empty and "returns_involved_in" in summary.columns else 1
    st.dataframe(
        summary.sort_values("returns_involved_in", ascending=False).reset_index(drop=True),
        hide_index=True,
        use_container_width=True,
        column_config={
            name_col: st.column_config.TextColumn("Name", width="large"),
            "current_or_former_dpos_position": st.column_config.TextColumn("Former position"),
            "current_or_former_dpos_chamber": st.column_config.TextColumn("Chamber", width="small"),
            "returns_involved_in": st.column_config.ProgressColumn("Returns", format="%d", min_value=0, max_value=max_r or 1),
            "distinct_lobbyist_firms": st.column_config.NumberColumn("Firms"),
            "distinct_policy_areas": st.column_config.NumberColumn("Policy areas"),
            "distinct_politicians_targeted": st.column_config.NumberColumn("Politicians targeted"),
        },
    )
    _export(summary, "revolving_door_summary.csv", "rd_sum_exp")

    if not detail.empty and name_col in summary.columns:
        st.markdown("---")
        _section("Drill into an individual")
        names = _sorted_unique_values(summary, name_col)
        chosen = _safe_selectbox("Select individual", names, key="rd_chosen", placeholder="Select an individual…")

        if chosen:
            person_detail = detail.loc[detail[name_col] == chosen]

            if (
                not person_detail.empty
                and "primary_key" in person_detail.columns
                and not returns_master.empty
                and "primary_key" in returns_master.columns
            ):
                merged = person_detail.merge(
                    returns_master[["primary_key", "lobbyist_name", "specific_details", "intended_results"]],
                    on="primary_key",
                    how="left",
                    suffixes=("", "_from_master"),
                )
            else:
                merged = person_detail

            dcols = [c for c in [
                name_col,
                "lobby_url",
                "lobbyist_name",
                "public_policy_area",
                "lobbying_period_start_date",
                "specific_details",
                "intended_results",
            ] if c in merged.columns]

            display_df = (
                merged.loc[:, dcols]
                .sort_values("lobbying_period_start_date", ascending=False)
                .reset_index(drop=True)
            )
            if len(display_df) > MAX_ROWS_DISPLAY:
                st.caption(f"Showing first {MAX_ROWS_DISPLAY:,} of {len(display_df):,} rows.")
                display_df = _limit_display(display_df)

            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    name_col: st.column_config.TextColumn("Name"),
                    "lobby_url": _link(),
                    "lobbyist_name": st.column_config.TextColumn("Client/Company", width="large"),
                    "public_policy_area": st.column_config.TextColumn("Policy area"),
                    "lobbying_period_start_date": st.column_config.DateColumn("Period start", format="YYYY-MM-DD"),
                    "specific_details": st.column_config.TextColumn("Specific Details", width="large"),
                    "intended_results": st.column_config.TextColumn("Intended Results", width="large"),
                },
            )
            _export(merged.loc[:, dcols], f"{chosen.replace(' ', '_')}_revolving_door.csv", "rd_det_exp")


def _transparency() -> None:
    late_filers = _load("experimental_time_to_publish.csv")
    desc_lengths = _load("experimental_return_description_lengths.csv")

    col_l, col_r = st.columns(2)

    with col_l:
        _section("Late filers")
        st.caption("Ranked by median days between lobbying period end and submission. Higher = later.")
        if not late_filers.empty and "median_days_to_publish" in late_filers.columns:
            lf = late_filers.sort_values("median_days_to_publish", ascending=False).head(30).reset_index(drop=True)
            max_days = int(lf["median_days_to_publish"].max()) if not lf.empty else 1
            st.dataframe(
                lf,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "lobbyist_name": st.column_config.TextColumn("Organisation", width="large"),
                    "median_days_to_publish": st.column_config.ProgressColumn("Median days", format="%d", min_value=0, max_value=max_days or 1),
                    "max_days_to_publish": st.column_config.NumberColumn("Max days"),
                    "returns_filed": st.column_config.NumberColumn("Returns"),
                },
            )
            _export(lf, "late_filers.csv", "tr_late_exp")

    with col_r:
        _section("Shortest return descriptions")
        st.caption("Returns with least content in specific details + intended results — a minimal-compliance proxy.")
        if not desc_lengths.empty and "total_desc_len" in desc_lengths.columns:
            shortest = desc_lengths.sort_values("total_desc_len").head(30).reset_index(drop=True)
            median_len = int(desc_lengths["total_desc_len"].median()) if not desc_lengths.empty else 1
            st.dataframe(
                shortest,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "lobbyist_name": st.column_config.TextColumn("Organisation"),
                    "lobby_url": _link(),
                    "total_desc_len": st.column_config.ProgressColumn("Total chars", format="%d", min_value=0, max_value=median_len or 1),
                    "specific_details_len": st.column_config.NumberColumn("Specific details"),
                    "intended_results_len": st.column_config.NumberColumn("Intended results"),
                    "lobbying_period_start_date": st.column_config.TextColumn("Period"),
                },
            )
            _export(shortest, "shortest_descriptions.csv", "tr_desc_exp")

    if not late_filers.empty and not desc_lengths.empty and "lobbyist_name" in late_filers.columns and "lobbyist_name" in desc_lengths.columns:
        _section("Transparency scorecard — per organisation")
        st.caption("Filing latency combined with average description length per organisation.")
        avg_desc = (
            desc_lengths.groupby("lobbyist_name", dropna=True)["total_desc_len"]
            .mean()
            .reset_index()
            .rename(columns={"total_desc_len": "avg_desc_len"})
        )
        scorecard = (
            late_filers.merge(avg_desc, on="lobbyist_name", how="outer")
            .sort_values("median_days_to_publish", ascending=False, na_position="last")
            .reset_index(drop=True)
        )
        max_sc = int(scorecard["median_days_to_publish"].max()) if "median_days_to_publish" in scorecard.columns and scorecard["median_days_to_publish"].notna().any() else 1
        st.dataframe(
            scorecard,
            hide_index=True,
            use_container_width=True,
            column_config={
                "lobbyist_name": st.column_config.TextColumn("Organisation", width="large"),
                "median_days_to_publish": st.column_config.ProgressColumn("Median days late", format="%d", min_value=0, max_value=max_sc or 1),
                "returns_filed": st.column_config.NumberColumn("Returns"),
                "avg_desc_len": st.column_config.NumberColumn("Avg description length"),
            },
        )
        _export(scorecard, "transparency_scorecard.csv", "tr_score_exp")


# ── organisations view ────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load_orgs() -> pd.DataFrame:
    """
    Merge lobby_orgs.csv (sector) with lobby_count_details.csv (returns, website,
    politicians targeted) on lobbyist_name. Deduplicates to one row per org.
    """
    if not _ORG_CSV.exists():
        return pd.DataFrame()

    orgs = pd.read_csv(_ORG_CSV, dtype=str)

    counts = _load("lobby_count_details.csv")
    if counts.empty:
        return orgs

    counts_deduped = (
        counts.sort_values("lobby_requests_count", ascending=False)
        .drop_duplicates(subset=["lobbyist_name"])
        [["lobbyist_name", "lobby_requests_count", "politicians_involved_count",
          "main_activities_of_organisation", "website",
          "company_registration_number", "company_registered_name", "lobby_org_link"]]
    )
    counts_deduped["lobby_requests_count"] = pd.to_numeric(
        counts_deduped["lobby_requests_count"], errors="coerce"
    )
    counts_deduped["politicians_involved_count"] = pd.to_numeric(
        counts_deduped["politicians_involved_count"], errors="coerce"
    )

    merged = orgs.merge(counts_deduped, on="lobbyist_name", how="left", suffixes=("_orgs", ""))

    # prefer counts table values; fall back to orgs columns
    merged["sector"] = merged["main_activities_of_organisation"].fillna(merged["lobby_activities"])
    merged["crn"] = merged["company_registration_number"].fillna(merged["company_registration_number_orgs"])
    merged["registered_name"] = merged["company_registered_name"].fillna(merged["company_registered_name_orgs"])
    merged["profile_url"] = merged["lobby_org_link"].fillna(merged["lobby_org_link_orgs"])
    merged["website"] = merged["website"].fillna("")

    keep = ["lobbyist_name", "sector", "registered_name", "crn", "website",
            "profile_url", "lobby_requests_count", "politicians_involved_count"]
    return merged[keep].drop_duplicates(subset=["lobbyist_name"]).reset_index(drop=True)


def _organisations() -> None:
    df = _load_orgs()

    if df.empty:
        st.info("Organisation data not found.")
        return

    with_counts = df[df["lobby_requests_count"].notna()]
    total_orgs    = len(df)
    total_active  = len(with_counts)
    total_sectors = df["sector"].nunique()
    top_returns   = int(with_counts["lobby_requests_count"].max()) if not with_counts.empty else 0

    st.markdown(
        '<div class="stat-strip">'
        + _stat(f"{total_orgs:,}",    "Registered orgs")
        + _stat(f"{total_active:,}",  "With filed returns")
        + _stat(total_sectors,        "Sectors")
        + _stat(f"{top_returns:,}",   "Most returns by one org")
        + "</div>",
        unsafe_allow_html=True,
    )

    # ── filters ───────────────────────────────────────────────────────
    col_s, col_n = st.columns([1, 2])
    with col_s:
        st.markdown('<p class="sidebar-label">Sector</p>', unsafe_allow_html=True)
        sectors = ["All sectors"] + sorted(df["sector"].dropna().unique().tolist())
        sector_filter = st.selectbox("Sector", sectors, key="org_sector", label_visibility="collapsed")
    with col_n:
        st.markdown('<p class="sidebar-label">Search organisation</p>', unsafe_allow_html=True)
        name_filter = st.text_input("Organisation name", placeholder="e.g. Ibec, IFA, Google",
                                    key="org_name", label_visibility="collapsed")

    view = df.copy()
    if sector_filter != "All sectors":
        view = view[view["sector"] == sector_filter]
    if name_filter.strip():
        view = view[view["lobbyist_name"].str.contains(name_filter.strip(), case=False, na=False)]

    # ── org profile card when one org matches ─────────────────────────
    if name_filter.strip() and len(view) == 1:
        row = view.iloc[0]
        _org_card(row)
        _org_returns(row["lobbyist_name"])
        return

    # ── power index table ─────────────────────────────────────────────
    _section("Lobbying power index")
    st.caption("Ranked by total returns filed. Sector · website · lobbying.ie profile included.")

    ranked = (
        view[view["lobby_requests_count"].notna()]
        .sort_values("lobby_requests_count", ascending=False)
        .reset_index(drop=True)
    )

    if ranked.empty:
        st.info("No organisations with return data match the current filter.")
    else:
        max_r = int(ranked["lobby_requests_count"].max()) if not ranked.empty else 1

        display = ranked[[
            "lobbyist_name", "sector", "lobby_requests_count",
            "politicians_involved_count", "website", "profile_url",
        ]].copy()
        display["website"] = display["website"].apply(
            lambda w: f"https://{w}" if w and not str(w).startswith("http") else w
        )

        st.dataframe(
            display,
            hide_index=True,
            use_container_width=True,
            column_config={
                "lobbyist_name":            st.column_config.TextColumn("Organisation", width="large"),
                "sector":                   st.column_config.TextColumn("Sector"),
                "lobby_requests_count":     st.column_config.ProgressColumn(
                                                "Returns", format="%d",
                                                min_value=0, max_value=max_r),
                "politicians_involved_count": st.column_config.NumberColumn("Politicians targeted"),
                "website":                  st.column_config.LinkColumn("Website", display_text="Visit ↗"),
                "profile_url":              st.column_config.LinkColumn("Lobbying.ie", display_text="Profile ↗"),
            },
        )
        _export(display, "lobbying_organisations.csv", "org_exp")

    # ── sector breakdown ──────────────────────────────────────────────
    _section("Returns by sector")
    sector_summary = (
        view[view["lobby_requests_count"].notna()]
        .groupby("sector", dropna=True)
        .agg(orgs=("lobbyist_name", "count"),
             total_returns=("lobby_requests_count", "sum"))
        .reset_index()
        .sort_values("total_returns", ascending=False)
        .reset_index(drop=True)
    )
    if not sector_summary.empty:
        max_sec = int(sector_summary["total_returns"].max())
        st.dataframe(
            sector_summary,
            hide_index=True,
            use_container_width=True,
            column_config={
                "sector":        st.column_config.TextColumn("Sector"),
                "orgs":          st.column_config.NumberColumn("Orgs"),
                "total_returns": st.column_config.ProgressColumn(
                                     "Total returns", format="%d",
                                     min_value=0, max_value=max_sec),
            },
        )


def _org_card(row: pd.Series) -> None:
    """Full profile card for a single organisation."""
    name     = row["lobbyist_name"]
    sector   = row["sector"] or "—"
    reg_name = row["registered_name"] or ""
    crn      = row["crn"] or ""
    website  = row["website"] or ""
    url      = row["profile_url"] or ""
    returns  = int(row["lobby_requests_count"]) if pd.notna(row["lobby_requests_count"]) else "—"
    pols     = int(row["politicians_involved_count"]) if pd.notna(row["politicians_involved_count"]) else "—"

    if website and not str(website).startswith("http"):
        website = f"https://{website}"

    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)
    st.markdown(f'<h2 class="td-name">{name}</h2>', unsafe_allow_html=True)

    badges = f'<span class="signal signal-neutral">{sector}</span>'
    if reg_name and reg_name != name:
        badges += f'&nbsp;<span class="signal signal-neutral" style="font-weight:400;text-transform:none">{reg_name}</span>'
    st.markdown(badges, unsafe_allow_html=True)

    st.markdown(
        '<div class="stat-strip">'
        + _stat(returns, "Returns filed")
        + _stat(pols,    "Politicians targeted")
        + "</div>",
        unsafe_allow_html=True,
    )

    link_cols = st.columns(3)
    if website:
        link_cols[0].link_button("Website ↗", website)
    if url:
        link_cols[1].link_button("Lobbying.ie profile ↗", url)
    if crn:
        cro_url = f"https://core.cro.ie/company/{crn}/details"
        link_cols[2].link_button(f"CRO · {crn} ↗", cro_url)


def _org_returns(org_name: str) -> None:
    """Recent returns for the selected organisation."""
    returns = _load_returns_master()
    if returns.empty:
        return

    org_returns = returns[returns["lobbyist_name"].str.fullmatch(org_name, case=False, na=False)]
    if org_returns.empty:
        # fallback to contains match
        org_returns = returns[returns["lobbyist_name"].str.contains(org_name, case=False, na=False)]

    if org_returns.empty:
        st.info("No returns found for this organisation in the master dataset.")
        return

    _section(f"Returns ({len(org_returns):,} total)")

    display_cols = [c for c in [
        "lobby_url", "relevant_matter", "public_policy_area",
        "lobbying_period_start_date", "specific_details",
    ] if c in org_returns.columns]

    display = (
        org_returns[display_cols]
        .sort_values("lobbying_period_start_date", ascending=False)
        .reset_index(drop=True)
    )
    st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        column_config={
            "lobby_url":                    _link(),
            "relevant_matter":              st.column_config.TextColumn("Matter", width="medium"),
            "public_policy_area":           st.column_config.TextColumn("Policy area"),
            "lobbying_period_start_date":   st.column_config.DateColumn("Period", format="YYYY-MM-DD"),
            "specific_details":             st.column_config.TextColumn("Details", width="large"),
        },
    )
    _export(display, f"{org_name[:40].replace(' ','_')}_returns.csv", "org_ret_exp")


# ── entry point ───────────────────────────────────────────────────────

def lobbying_page() -> None:
    inject_css()

    returns = _load_returns_master()
    total_returns = len(returns)

    with st.sidebar:
        st.markdown('<div class="page-kicker">Dáil Tracker</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-title">Lobbying<br>Register</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="page-subtitle">{total_returns:,} returns · Source: lobbying.ie</div>',
            unsafe_allow_html=True,
        )

        st.markdown('<p class="sidebar-label">View</p>', unsafe_allow_html=True)
        view = st.radio(
            "View",
            _VIEWS,
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
    elif view == "Organisations":
        _organisations()
    elif view == "Revolving Door":
        _revolving_door()
    elif view == "Transparency":
        _transparency()