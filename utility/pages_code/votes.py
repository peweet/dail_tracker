
from pathlib import Path
import sys
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from shared_css import inject_css
import re
import os

sys.path.insert(0, str(Path(__file__).parent.parent))

_ROOT = Path(__file__).parent.parent.parent
_CSV  = _ROOT / "data" / "gold" / "current_dail_vote_history.csv"

_VOTE_COLOURS = {
    "Voted Yes": "#2d6a4f",
    "Voted No":  "#c1121f",
    "Abstained": "#adb5bd",
}

# Curated themes: label → list of regex patterns to match debate_title
_THEMES = {
    "Housing & Rent":        [r"housing", r"rent(?!al\s+sector\s+aid)", r"landlord", r"affordable housing", r"residential tenancies"],
    "Carbon & Climate":      [r"carbon tax", r"climate action", r"low carbon", r"natural gas carbon"],
    "Confidence Votes":      [r"confidence in"],
    "Taxation":              [r"finance bill", r"financial resolution", r"local property tax", r"budget"],
    "Health & Abortion":     [r"termination of pregnancy", r"abortion", r"health \(waiting", r"health \(regulation"],
    "Asylum & Migration":    [r"international protection"],
    "Energy Costs":          [r"energy costs", r"fuel costs", r"soaring energy"],
    "Defective Blocks":      [r"defective concrete", r"mica"],
    "Irish Neutrality":      [r"neutrality"],
    "Water Charges":         [r"water charge", r"water services.*repeal"],
}



def _get_csv_mtime():
    try:
        return os.path.getmtime(_CSV)
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def _load(csv_mtime=None) -> pd.DataFrame:
    df = pd.read_csv(_CSV, low_memory=False)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year.astype("Int64")
    # restrict to 34th Dáil members; nulls are former members not in current Dáil
    df = df[df["party"].notna()]
    return df



def _tag_themes(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'theme' column to a debates-level frame (vectorized)."""
    titles = df["debate_title"].fillna("")
    theme_arr = pd.Series("Other", index=df.index)
    for theme, patterns in _THEMES.items():
        pattern = "|".join(patterns)
        mask = titles.str.contains(pattern, case=False, regex=True, na=False)
        theme_arr[mask] = theme
    df = df.copy()
    df["theme"] = theme_arr
    return df


@st.cache_data
def _debates_summary(df_hash: int, year: int, td_filter: str, party_filter: str) -> pd.DataFrame:
    # NOTE: df_hash is unused — it exists only to bust the cache when the underlying
    # data changes, since st.cache_data can't hash a DataFrame directly here.
    return pd.DataFrame()  # computed inline; this function is a placeholder



def _build_debates(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse vote-level rows to one row per (debate_title, vote_id) division,
    then aggregate to one row per debate_title with division count + outcomes."""
    if df.empty:
        return pd.DataFrame()
    divisions = (
        df.groupby(["debate_title", "vote_id", "date", "vote_outcome"], dropna=False)
        .agg(
            yes=("vote_type", lambda x: (x == "Voted Yes").sum()),
            no=("vote_type",  lambda x: (x == "Voted No").sum()),
            abstained=("vote_type", lambda x: (x == "Abstained").sum()),
        )
        .reset_index()
    )
    debates = (
        divisions.groupby("debate_title", dropna=False)
        .agg(
            divisions=("vote_id", "nunique"),
            first_date=("date", "min"),
            last_date=("date",  "max"),
            carried=("vote_outcome", lambda x: (x == "Carried").sum()),
            lost=("vote_outcome",    lambda x: (x == "Lost").sum()),
            total_yes=("yes",        "sum"),
            total_no=("no",          "sum"),
        )
        .reset_index()
        .sort_values("last_date", ascending=False)
    )
    return _tag_themes(debates)


# ── Charts ────────────────────────────────────────────────────────────────────

def _party_chart(df: pd.DataFrame) -> go.Figure:
    order = ["Voted Yes", "Voted No", "Abstained"]
    pivot = (
        df.groupby(["party", "vote_type"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=order, fill_value=0)
    )
    pivot["total"] = pivot.sum(axis=1)
    for col in order:
        pivot[f"{col}_pct"] = (pivot[col] / pivot["total"] * 100).round(1)
    pivot = pivot.sort_values("Voted Yes_pct")

    fig = go.Figure()
    for vtype in order:
        fig.add_trace(go.Bar(
            name=vtype,
            y=pivot.index,
            x=pivot[vtype],
            orientation="h",
            marker_color=_VOTE_COLOURS[vtype],
            text=[f"{p:.0f}%" for p in pivot[f"{vtype}_pct"]],
            textposition="inside",
            insidetextanchor="middle",
            hovertemplate=f"<b>%{{y}}</b><br>{vtype}: %{{x}} (%{{text}})<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack",
        height=max(260, len(pivot) * 36),
        margin=dict(l=0, r=20, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Epilogue, sans-serif", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False),
    )
    return fig


def _td_year_chart(df: pd.DataFrame) -> go.Figure:
    order = ["Voted Yes", "Voted No", "Abstained"]
    pivot = (
        df.groupby(["year", "vote_type"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=order, fill_value=0)
    )
    fig = go.Figure()
    for vtype in order:
        fig.add_trace(go.Bar(
            name=vtype,
            x=pivot.index.astype(int),
            y=pivot[vtype],
            marker_color=_VOTE_COLOURS[vtype],
        ))
    fig.update_layout(
        barmode="stack", height=240,
        margin=dict(l=0, r=0, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Epilogue, sans-serif", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(showgrid=False, tickmode="linear", dtick=1),
        yaxis=dict(showgrid=True, gridcolor="#e9ecef"),
    )
    return fig


# ── Page ──────────────────────────────────────────────────────────────────────


def votes_page() -> None:

    inject_css()
    csv_mtime = _get_csv_mtime()
    df = _load(csv_mtime)

    # ── Oireachtas Explorer banner ───────────────────────────────────
    st.markdown(
        """
        <div style="background:#1d3557;padding:0.7rem 0 0.5rem 0;margin-bottom:1.2rem;border-radius:6px 6px 0 0;">
            <h1 style="color:#fff;font-family:Epilogue,sans-serif;font-size:2.1rem;font-weight:700;margin:0 0 0 1.2rem;letter-spacing:-0.03em;">Oireachtas Explorer</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )
    # ── Page blurb and explainer ─────────────────────────────────────
    st.markdown('<h1 class="page-title">Dáil Divisions</h1>', unsafe_allow_html=True)
    st.markdown(
        """
        <div style="font-size:1.1rem;line-height:1.6;margin-bottom:0.7em">
        <strong>How did the Dáil divide?</strong> Explore every division (vote) in the Irish parliament since 2015. Browse by year, debate, or TD. See party breakdowns, margins, and download the full record.
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("What is this data? (Click for details)", expanded=False):
        st.markdown(
            """
            **About Dáil Divisions**  
            This page shows every official division (vote) held in the Dáil since 2015, as published by the Oireachtas. Each division records how every TD present voted: Yes, No, or Abstained. Debates often have multiple divisions (e.g., on amendments).
            
            **Data sources:**
            - Official Oireachtas division records (current_dail_vote_history.csv)
            - Member and party metadata (silver layer)
            
            **Caveats:**
            - Some votes may be missing if not published or if members were absent.
            - "Abstained" means present but did not vote Yes/No; absences are not shown.
            - Debate titles and vote IDs are not always unique—results are grouped by (debate, vote_id, date) for accuracy.
            """
        )

    # ── Sidebar ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<p class="sidebar-label">Filter by TD</p>', unsafe_allow_html=True)
        td_search = st.text_input(
            "TD name", placeholder="e.g. McDonald — leave blank for all",
            label_visibility="collapsed", key="v_td",
        )
        st.markdown('<p class="sidebar-label">Filter by party</p>', unsafe_allow_html=True)
        parties = ["All"] + sorted(df["party"].dropna().unique())
        sel_party = st.selectbox("Party", parties, label_visibility="collapsed", key="v_party")
        st.markdown('<p class="sidebar-label">Filter by theme</p>', unsafe_allow_html=True)
        themes = ["All themes"] + list(_THEMES.keys()) + ["Other"]
        sel_theme = st.selectbox("Theme", themes, label_visibility="collapsed", key="v_theme")

    # ── Apply sidebar filters efficiently ─────────────────────────────
    mask = pd.Series(True, index=df.index)
    if td_search.strip():
        mask &= df["full_name"].str.contains(td_search.strip(), case=False, na=False)
    if sel_party != "All":
        mask &= (df["party"] == sel_party)
    view = df[mask]

    # ── Header ────────────────────────────────────────────────────────
    st.markdown('<p class="page-kicker">Oireachtas Tracker</p>', unsafe_allow_html=True)
    st.markdown('<h1 class="page-title">Dáil Divisions</h1>', unsafe_allow_html=True)
    subtitle = "How the Dáil divided — browse by year, explore debates, see who voted what."
    if td_search.strip() and view["full_name"].nunique() == 1:
        name = view["full_name"].iloc[0]
        party = view["party"].dropna().mode().iloc[0] if not view["party"].dropna().empty else ""
        subtitle = f"Filtered to <strong>{name}</strong> ({party})"
    st.markdown(f'<p class="page-subtitle">{subtitle}</p>', unsafe_allow_html=True)
    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    if view.empty:
        st.info("No records match the current filters.")
        return

    # ── Year selector (clickable radio, not slider) ───────────────────
    years_with_data = sorted(view["year"].dropna().astype(int).unique(), reverse=True)
    st.markdown('<p class="section-heading">Select year</p>', unsafe_allow_html=True)
    sel_year = st.radio(
        "Year",
        years_with_data,
        index=0,
        horizontal=True,
        label_visibility="collapsed",
        key="v_year",
        format_func=str,
    )

    year_view = view[view["year"] == sel_year]

    # ── TD profile (when filtered to one person) ──────────────────────
    if td_search.strip() and view["full_name"].nunique() == 1:
        _td_profile_strip(view, year_view, sel_year)
        st.markdown("---")

    # ── Build debate summary for selected year ────────────────────────
    debates = _build_debates(year_view)

    # apply theme filter
    if sel_theme != "All themes":
        debates = debates[debates["theme"] == sel_theme]

    # ── Notable debates for this year ─────────────────────────────────
    notable = debates[debates["theme"] != "Other"]
    if not notable.empty:
        st.markdown('<p class="section-heading">Significant debates</p>', unsafe_allow_html=True)
        st.caption("Debates matching tracked themes — housing, carbon, confidence, taxation and more.")
        _debates_table(notable, year_view, key_prefix=f"notable_{sel_year}")

    # ── All debates that year ─────────────────────────────────────────
    st.markdown(f'<p class="section-heading">All debates · {sel_year}</p>', unsafe_allow_html=True)
    st.caption(f"{len(debates)} debates · {debates['divisions'].sum()} divisions · select a debate to drill in.")
    _debates_table(debates, year_view, key_prefix=f"all_{sel_year}")


def _td_profile_strip(all_view: pd.DataFrame, year_view: pd.DataFrame, sel_year: int) -> None:
    """Compact profile bar shown when filtered to one TD."""
    name       = all_view["full_name"].iloc[0]
    yes_yr     = (year_view["vote_type"] == "Voted Yes").sum()
    no_yr      = (year_view["vote_type"] == "Voted No").sum()
    abs_yr     = (year_view["vote_type"] == "Abstained").sum()
    total_all  = len(all_view)
    divs_all   = all_view["vote_id"].nunique()

    col_l, col_r = st.columns([1, 2])
    with col_l:
        st.markdown(
            f"""
            <div class="stat-strip" style="padding:0.6rem 0;gap:1.5rem">
                <div><div class="stat-num" style="font-size:1.2rem;color:#2d6a4f">{yes_yr}</div><div class="stat-lbl">{sel_year} Yes</div></div>
                <div><div class="stat-num" style="font-size:1.2rem;color:#c1121f">{no_yr}</div><div class="stat-lbl">{sel_year} No</div></div>
                <div><div class="stat-num" style="font-size:1.2rem">{abs_yr}</div><div class="stat-lbl">{sel_year} Abstained</div></div>
                <div><div class="stat-num" style="font-size:1.2rem">{total_all:,}</div><div class="stat-lbl">All-time votes</div></div>
                <div><div class="stat-num" style="font-size:1.2rem">{divs_all}</div><div class="stat-lbl">Divisions</div></div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_r:
        if len(all_view["year"].dropna().unique()) > 1:
            st.plotly_chart(_td_year_chart(all_view), use_container_width=True)


def _debates_table(debates: pd.DataFrame, year_view: pd.DataFrame, key_prefix: str) -> None:
    """
    Show debates as a table. A selectbox below lets the user drill into one debate.
    """
    if debates.empty:
        st.info("No debates found.")
        return

    display = debates[[
        "debate_title", "theme", "divisions", "carried", "lost",
        "total_yes", "total_no", "last_date",
    ]].copy()
    display["last_date"] = pd.to_datetime(display["last_date"]).dt.strftime("%d %b %Y")
    display = display.rename(columns={
        "debate_title": "Debate",
        "theme":        "Theme",
        "divisions":    "Divisions",
        "carried":      "Carried",
        "lost":         "Lost",
        "total_yes":    "Total Yes",
        "total_no":     "Total No",
        "last_date":    "Latest date",
    })

    st.dataframe(display, use_container_width=True, hide_index=True)

    # ── drill-in selectbox ────────────────────────────────────────────
    debate_titles = debates["debate_title"].tolist()
    sel = st.selectbox(
        "Drill into a debate",
        ["— select to expand —"] + debate_titles,
        key=f"{key_prefix}_sel",
        label_visibility="visible",
    )
    if sel != "— select to expand —":
        _debate_panel(sel, year_view, key_prefix)


def _debate_panel(debate_title: str, year_view: pd.DataFrame, key_prefix: str) -> None:
    """Full breakdown for a single selected debate."""
    debate_df = year_view[year_view["debate_title"] == debate_title]

    if debate_df.empty:
        st.info("No data for this debate in the current view.")
        return

    # ── division selector (if multiple) ──────────────────────────────
    divisions_in_debate = (
        debate_df.groupby(["vote_id", "date", "vote_outcome", "subject"])
        .size()
        .reset_index()
        .sort_values("date")
    )

    if len(divisions_in_debate) > 1:
        st.markdown(
            f'<p class="section-heading">Divisions in: {debate_title[:80]}</p>',
            unsafe_allow_html=True,
        )
        opts = {}
        for _, row in divisions_in_debate.iterrows():
            date_str = pd.to_datetime(row["date"]).strftime("%d %b %Y") if pd.notna(row["date"]) else "—"
            subj = str(row["subject"])[:70] if row["subject"] else row["vote_id"]
            label = f"{date_str} · {subj}"
            opts[label] = row["vote_id"]

        chosen_label = st.selectbox(
            "Division",
            list(opts.keys()),
            key=f"{key_prefix}_div",
            label_visibility="visible",
        )
        div_df = debate_df[debate_df["vote_id"] == opts[chosen_label]]
    else:
        div_df = debate_df

    # ── stats ─────────────────────────────────────────────────────────
    outcome   = div_df["vote_outcome"].mode().iloc[0] if not div_df.empty else "—"
    date_val  = div_df["date"].dropna().iloc[0] if not div_df["date"].dropna().empty else None
    subject   = div_df["subject"].dropna().iloc[0][:100] if not div_df["subject"].dropna().empty else ""
    yes_n     = (div_df["vote_type"] == "Voted Yes").sum()
    no_n      = (div_df["vote_type"] == "Voted No").sum()
    abs_n     = (div_df["vote_type"] == "Abstained").sum()
    margin    = abs(yes_n - no_n)
    date_str  = date_val.strftime("%d %b %Y") if date_val else "—"

    outcome_col = "#2d6a4f" if outcome == "Carried" else "#c1121f" if outcome == "Lost" else "#6c757d"

    st.markdown(
        f'<p style="font-size:0.75rem;font-weight:700;letter-spacing:0.07em;'
        f'text-transform:uppercase;color:{outcome_col};margin:1rem 0 0.2rem">'
        f'{outcome} · {date_str}</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<p style="font-size:0.85rem;color:var(--text-meta);margin-bottom:0.8rem">{subject}</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class="stat-strip">
            <div><div class="stat-num" style="color:#2d6a4f">{yes_n}</div><div class="stat-lbl">Yes</div></div>
            <div><div class="stat-num" style="color:#c1121f">{no_n}</div><div class="stat-lbl">No</div></div>
            <div><div class="stat-num">{abs_n}</div><div class="stat-lbl">Abstained</div></div>
            <div><div class="stat-num">{margin}</div><div class="stat-lbl">Margin</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── party breakdown chart ─────────────────────────────────────────
    st.markdown('<p class="section-heading">Party breakdown</p>', unsafe_allow_html=True)
    st.plotly_chart(_party_chart(div_df), use_container_width=True)

    # ── member votes table ────────────────────────────────────────────
    st.markdown('<p class="section-heading">Individual votes</p>', unsafe_allow_html=True)
    vote_table = (
        div_df[["full_name", "party", "constituency_name", "vote_type"]]
        .sort_values(["vote_type", "party", "full_name"])
        .rename(columns={
            "full_name":         "Member",
            "party":             "Party",
            "constituency_name": "Constituency",
            "vote_type":         "Vote",
        })
    )
    st.dataframe(vote_table, use_container_width=True, hide_index=True)
    st.download_button(
        "Download CSV",
        data=vote_table.to_csv(index=False).encode(),
        file_name=f"{debate_title[:40].replace(' ','_')}.csv",
        mime="text/csv",
        key=f"{key_prefix}_dl",
    )
