import os
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent
_CSV = _ROOT / "data" / "gold" / "current_dail_vote_history.csv"
_SPONSORS_CSV = _ROOT / "data" / "silver" / "sponsors.csv"

_VOTE_COLOURS = {
    "Voted Yes": "#2d6a4f",
    "Voted No": "#c1121f",
    "Abstained": "#adb5bd",
}

_THEMES = {
    "Housing & Rent": [
        r"housing",
        r"rent(?!al\s+sector\s+aid)",
        r"landlord",
        r"affordable housing",
        r"residential tenancies",
    ],
    "Carbon & Climate": [r"carbon tax", r"climate action", r"low carbon", r"natural gas carbon"],
    "Confidence Votes": [r"confidence in"],
    "Taxation": [r"finance bill", r"financial resolution", r"local property tax", r"budget"],
    "Health & Abortion": [r"termination of pregnancy", r"abortion", r"health \(waiting", r"health \(regulation"],
    "Asylum & Migration": [r"international protection"],
    "Energy Costs": [r"energy costs", r"fuel costs", r"soaring energy"],
    "Defective Blocks": [r"defective concrete", r"mica"],
    "Irish Neutrality": [r"neutrality"],
    "Water Charges": [r"water charge", r"water services.*repeal"],
}

# ── Bill URL note ─────────────────────────────────────────────────────────────
# Oireachtas bill URLs follow: https://www.oireachtas.ie/en/bills/bill/{year}/{bill_no}/
# The bill year can be extracted from debate titles (e.g. "… Bill 2025") but the bill
# number requires a join to data/silver/stages.csv — see the commented-out enrichment
# block at the bottom of this file and in enrich.py.
# _extract_bill_year is defined there as part of the exercise.


def _get_csv_mtime():
    try:
        return os.path.getmtime(_CSV)
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def _load_sponsors() -> pd.DataFrame:
    if not _SPONSORS_CSV.exists():
        return pd.DataFrame()
    sp = pd.read_csv(_SPONSORS_CSV, low_memory=False)
    sp["unique_member_code"] = sp["sponsor.by.uri"].str.split("/id/").str[-1]
    sp["bill_year"] = pd.to_numeric(sp["bill.billYear"], errors="coerce").astype("Int64")
    sp["bill_no"] = pd.to_numeric(sp["bill.billNo"], errors="coerce").astype("Int64")
    sp["url"] = (
        "https://www.oireachtas.ie/en/bills/bill/" + sp["bill_year"].astype(str) + "/" + sp["bill_no"].astype(str) + "/"
    )
    return sp[
        [
            "unique_member_code",
            "bill_no",
            "bill_year",
            "bill.shortTitleEn",
            "bill.status",
            "bill.source",
            "sponsor.isPrimary",
            "bill.mostRecentStage.event.showAs",
            "url",
        ]
    ].rename(
        columns={
            "bill.shortTitleEn": "title",
            "bill.status": "status",
            "bill.source": "source",
            "sponsor.isPrimary": "is_primary",
            "bill.mostRecentStage.event.showAs": "current_stage",
        }
    )


@st.cache_data(show_spinner=False)
def _load(_csv_mtime=None) -> pd.DataFrame:
    df = pd.read_csv(_CSV, low_memory=False)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year.astype("Int64")
    df = df[df["party"].notna()]
    return df


def _tag_themes(df: pd.DataFrame) -> pd.DataFrame:
    titles = df["debate_title"].fillna("")
    theme_arr = pd.Series("Other", index=df.index)
    for theme, patterns in _THEMES.items():
        mask = titles.str.contains("|".join(patterns), case=False, regex=True, na=False)
        theme_arr[mask] = theme
    df = df.copy()
    df["theme"] = theme_arr
    return df


def _build_debates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    divisions = (
        df.groupby(["debate_title", "vote_id", "date", "vote_outcome"], dropna=False)
        .agg(
            yes=("vote_type", lambda x: (x == "Voted Yes").sum()),
            no=("vote_type", lambda x: (x == "Voted No").sum()),
            abstained=("vote_type", lambda x: (x == "Abstained").sum()),
        )
        .reset_index()
    )
    debates = (
        divisions.groupby("debate_title", dropna=False)
        .agg(
            divisions=("vote_id", "nunique"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            carried=("vote_outcome", lambda x: (x == "Carried").sum()),
            lost=("vote_outcome", lambda x: (x == "Lost").sum()),
            total_yes=("yes", "sum"),
            total_no=("no", "sum"),
        )
        .reset_index()
        .sort_values("last_date", ascending=False)
    )
    return _tag_themes(debates)


# ── Charts ────────────────────────────────────────────────────────────────────


def _party_chart(df: pd.DataFrame) -> go.Figure:
    order = ["Voted Yes", "Voted No", "Abstained"]
    pivot = df.groupby(["party", "vote_type"]).size().unstack(fill_value=0).reindex(columns=order, fill_value=0)
    pivot["total"] = pivot.sum(axis=1)
    for col in order:
        pivot[f"{col}_pct"] = (pivot[col] / pivot["total"] * 100).round(1)
    pivot = pivot.sort_values("Voted Yes_pct")

    fig = go.Figure()
    for vtype in order:
        fig.add_trace(
            go.Bar(
                name=vtype,
                y=pivot.index,
                x=pivot[vtype],
                orientation="h",
                marker_color=_VOTE_COLOURS[vtype],
                text=[f"{p:.0f}%" for p in pivot[f"{vtype}_pct"]],
                textposition="inside",
                insidetextanchor="middle",
                hovertemplate=f"<b>%{{y}}</b><br>{vtype}: %{{x}} (%{{text}})<extra></extra>",
            )
        )
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
    pivot = df.groupby(["year", "vote_type"]).size().unstack(fill_value=0).reindex(columns=order, fill_value=0)
    fig = go.Figure()
    for vtype in order:
        fig.add_trace(
            go.Bar(
                name=vtype,
                x=pivot.index.astype(int),
                y=pivot[vtype],
                marker_color=_VOTE_COLOURS[vtype],
            )
        )
    fig.update_layout(
        barmode="stack",
        height=220,
        margin=dict(l=0, r=0, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Epilogue, sans-serif", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(showgrid=False, tickmode="linear", dtick=1),
        yaxis=dict(showgrid=True, gridcolor="#e9ecef"),
    )
    return fig


# ── TD vote record HTML table ─────────────────────────────────────────────────

_VOTE_CSS = """
<style>
.vr-table { width:100%; border-collapse:collapse; font-family:'Epilogue',sans-serif; }
.vr-table th {
    font-size:0.70rem; font-weight:700; letter-spacing:0.08em; text-transform:uppercase;
    color:#888; border-bottom:2px solid #111827; padding:0.4rem 0.6rem; text-align:left;
}
.vr-table td { padding:0.45rem 0.6rem; border-bottom:1px solid #e9ecef; vertical-align:middle; }
.vr-table tr:hover td { background:#f8f7f5; }
.v-yes  { color:#2d6a4f; font-size:1.45rem; font-weight:900; line-height:1; display:block; text-align:center; }
.v-no   { color:#c1121f; font-size:1.45rem; font-weight:900; line-height:1; display:block; text-align:center; }
.v-abs  { color:#adb5bd; font-size:1.1rem;  display:block; text-align:center; }
.vr-date  { white-space:nowrap; color:#888; font-size:0.80rem; width:95px; }
.vr-badge { width:52px; text-align:center; }
.vr-out   { font-size:0.78rem; font-weight:700; width:80px; }
.out-c { color:#2d6a4f; } .out-l { color:#c1121f; }
.vr-title { font-size:0.86rem; line-height:1.35; }
.vr-theme { display:inline-block; font-size:0.68rem; font-weight:700; letter-spacing:0.05em;
            text-transform:uppercase; background:#f0ede8; color:#888; border-radius:2px;
            padding:0.1rem 0.4rem; margin-left:0.4rem; vertical-align:middle; }
</style>
"""


def _vote_badge(vote_type: str) -> str:
    if vote_type == "Voted Yes":
        return '<span class="v-yes">✓</span>'
    if vote_type == "Voted No":
        return '<span class="v-no">✗</span>'
    return '<span class="v-abs">—</span>'


def _td_vote_html(df: pd.DataFrame) -> str:
    df = _tag_themes(df.sort_values("date", ascending=False))
    rows = []
    for _, r in df.iterrows():
        date_s = pd.to_datetime(r["date"]).strftime("%d %b %Y") if pd.notna(r["date"]) else "—"
        title = str(r["debate_title"])
        short = (title[:140] + "…") if len(title) > 140 else title
        theme = r.get("theme", "Other")
        badge = _vote_badge(r["vote_type"])
        outcome = str(r.get("vote_outcome", ""))
        out_cls = "out-c" if outcome == "Carried" else ("out-l" if outcome == "Lost" else "")
        tag = f'<span class="vr-theme">{theme}</span>' if theme != "Other" else ""
        rows.append(
            f"<tr>"
            f'<td class="vr-date">{date_s}</td>'
            f'<td class="vr-badge">{badge}</td>'
            f'<td class="vr-title">{short}{tag}</td>'
            f'<td class="vr-out {out_cls}">{outcome}</td>'
            f"</tr>"
        )
    body = (
        "".join(rows)
        if rows
        else "<tr><td colspan='4' style='color:#888;padding:1rem'>No votes in this period.</td></tr>"
    )
    return (
        _VOTE_CSS
        + "<table class='vr-table'>"
        + "<thead><tr>"
        + "<th>Date</th><th>Vote</th><th>Debate</th><th>Outcome</th>"
        + "</tr></thead>"
        + f"<tbody>{body}</tbody></table>"
    )


# ── TD Record view ────────────────────────────────────────────────────────────


def _td_record(df: pd.DataFrame, td_name: str, years: list[int]) -> None:
    td_df = df[df["full_name"] == td_name]
    if td_df.empty:
        st.info("No vote records found for this TD.")
        return

    party = td_df["party"].dropna().mode().iloc[0] if not td_df["party"].dropna().empty else "—"
    const = td_df["constituency_name"].dropna().iloc[0] if not td_df["constituency_name"].dropna().empty else "—"
    total_yes = (td_df["vote_type"] == "Voted Yes").sum()
    total_no = (td_df["vote_type"] == "Voted No").sum()
    total_divs = td_df["vote_id"].nunique()

    st.markdown(
        f'<div class="td-name">{td_name}</div><div class="td-meta">{party} · {const}</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""<div class="stat-strip">
            <div><div class="stat-num" style="color:#2d6a4f">{total_yes:,}</div><div class="stat-lbl">All-time Yes</div></div>
            <div><div class="stat-num" style="color:#c1121f">{total_no:,}</div><div class="stat-lbl">All-time No</div></div>
            <div><div class="stat-num">{total_divs:,}</div><div class="stat-lbl">Divisions</div></div>
        </div>""",
        unsafe_allow_html=True,
    )

    if len(td_df["year"].dropna().unique()) > 1:
        st.plotly_chart(_td_year_chart(td_df), use_container_width=True)

    st.markdown('<p class="section-heading">Select year</p>', unsafe_allow_html=True)
    sel_year = st.radio(
        "Year", years, index=0, horizontal=True, label_visibility="collapsed", key="v_year", format_func=str
    )

    year_df = td_df[td_df["year"] == sel_year]
    yes_yr = (year_df["vote_type"] == "Voted Yes").sum()
    no_yr = (year_df["vote_type"] == "Voted No").sum()

    st.markdown(
        f"""<div style="display:flex;gap:2rem;margin:0.5rem 0 1rem 0;align-items:center">
            <span style="font-size:0.78rem;font-weight:700;letter-spacing:0.07em;
                  text-transform:uppercase;color:#888">{sel_year}</span>
            <span style="color:#2d6a4f;font-weight:700">{yes_yr} ✓ Yes</span>
            <span style="color:#c1121f;font-weight:700">{no_yr} ✗ No</span>
            <span style="color:#888;font-size:0.85rem">{len(year_df)} votes · {year_df["vote_id"].nunique()} divisions</span>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown(_td_vote_html(year_df), unsafe_allow_html=True)

    st.markdown("&nbsp;", unsafe_allow_html=True)
    st.download_button(
        "Export votes CSV",
        year_df[["date", "debate_title", "vote_type", "vote_outcome", "subject"]].to_csv(index=False).encode(),
        file_name=f"{td_name.replace(' ', '_')}_{sel_year}_votes.csv",
        mime="text/csv",
        key="td_dl",
    )

    # ── Sponsored bills ───────────────────────────────────────────────
    member_code = (
        td_df["unique_member_code"].dropna().iloc[0] if not td_df["unique_member_code"].dropna().empty else None
    )
    if member_code:
        sponsors = _load_sponsors()
        td_bills = sponsors[sponsors["unique_member_code"] == member_code].copy()
        if not td_bills.empty:
            st.markdown("---")
            st.markdown('<p class="section-heading">Bills sponsored by this TD</p>', unsafe_allow_html=True)
            st.caption(
                f"{len(td_bills)} bill{'s' if len(td_bills) != 1 else ''} · "
                f"{td_bills['is_primary'].sum()} as primary sponsor · "
                "links open on oireachtas.ie"
            )
            st.dataframe(
                td_bills[["title", "bill_year", "status", "current_stage", "is_primary", "url"]].sort_values(
                    "bill_year", ascending=False
                ),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "title": st.column_config.TextColumn("Bill", width="large"),
                    "bill_year": st.column_config.NumberColumn("Year", format="%d", width="small"),
                    "status": st.column_config.TextColumn("Status"),
                    "current_stage": st.column_config.TextColumn("Current stage"),
                    "is_primary": st.column_config.CheckboxColumn("Primary sponsor", width="small"),
                    "url": st.column_config.LinkColumn("Oireachtas link", display_text="Open ↗", width="small"),
                },
            )


# ── Divisions view ────────────────────────────────────────────────────────────


def _debates_table(debates: pd.DataFrame, year_view: pd.DataFrame, key_prefix: str) -> None:
    if debates.empty:
        st.info("No debates found.")
        return

    display = debates[
        [
            "debate_title",
            "theme",
            "divisions",
            "total_yes",
            "total_no",
            "carried",
            "lost",
            "last_date",
        ]
    ].copy()
    display["last_date"] = pd.to_datetime(display["last_date"]).dt.strftime("%d %b %Y")
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "debate_title": st.column_config.TextColumn("Debate", width="large"),
            "theme": st.column_config.TextColumn("Theme"),
            "divisions": st.column_config.NumberColumn("Divs", format="%d"),
            "total_yes": st.column_config.NumberColumn("Yes ✓", format="%d"),
            "total_no": st.column_config.NumberColumn("No ✗", format="%d"),
            "carried": st.column_config.NumberColumn("Carried", format="%d"),
            "lost": st.column_config.NumberColumn("Lost", format="%d"),
            "last_date": st.column_config.TextColumn("Date"),
        },
    )

    sel = st.selectbox(
        "Drill into a debate",
        ["— select —"] + debates["debate_title"].tolist(),
        key=f"{key_prefix}_sel",
        label_visibility="visible",
    )
    if sel != "— select —":
        _debate_panel(sel, year_view, key_prefix)


def _debate_panel(debate_title: str, year_view: pd.DataFrame, key_prefix: str) -> None:
    debate_df = year_view[year_view["debate_title"] == debate_title]
    if debate_df.empty:
        st.info("No data for this debate in the current view.")
        return

    divisions_in_debate = (
        debate_df.groupby(["vote_id", "date", "vote_outcome", "subject"]).size().reset_index().sort_values("date")
    )

    if len(divisions_in_debate) > 1:
        opts = {}
        for _, row in divisions_in_debate.iterrows():
            date_str = pd.to_datetime(row["date"]).strftime("%d %b %Y") if pd.notna(row["date"]) else "—"
            subj = str(row["subject"])[:70] if row["subject"] else row["vote_id"]
            opts[f"{date_str} · {subj}"] = row["vote_id"]
        chosen = st.selectbox("Division", list(opts.keys()), key=f"{key_prefix}_div", label_visibility="visible")
        div_df = debate_df[debate_df["vote_id"] == opts[chosen]]
    else:
        div_df = debate_df

    outcome = div_df["vote_outcome"].mode().iloc[0] if not div_df.empty else "—"
    date_val = div_df["date"].dropna().iloc[0] if not div_df["date"].dropna().empty else None
    subject = div_df["subject"].dropna().iloc[0][:100] if not div_df["subject"].dropna().empty else ""
    yes_n = (div_df["vote_type"] == "Voted Yes").sum()
    no_n = (div_df["vote_type"] == "Voted No").sum()
    abs_n = (div_df["vote_type"] == "Abstained").sum()
    margin = abs(yes_n - no_n)
    date_str = date_val.strftime("%d %b %Y") if date_val else "—"

    out_col = "#2d6a4f" if outcome == "Carried" else "#c1121f" if outcome == "Lost" else "#6c757d"
    st.markdown(
        f'<p style="font-size:0.75rem;font-weight:700;letter-spacing:0.07em;'
        f'text-transform:uppercase;color:{out_col};margin:1rem 0 0.2rem">'
        f"{outcome} · {date_str}</p>"
        f'<p style="font-size:0.85rem;color:var(--text-meta);margin-bottom:0.8rem">{subject}</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""<div class="stat-strip">
            <div><div class="stat-num" style="color:#2d6a4f">{yes_n}</div><div class="stat-lbl">Yes ✓</div></div>
            <div><div class="stat-num" style="color:#c1121f">{no_n}</div><div class="stat-lbl">No ✗</div></div>
            <div><div class="stat-num">{abs_n}</div><div class="stat-lbl">Abstained</div></div>
            <div><div class="stat-num">{margin}</div><div class="stat-lbl">Margin</div></div>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown('<p class="section-heading">Party breakdown</p>', unsafe_allow_html=True)
    st.plotly_chart(_party_chart(div_df), use_container_width=True)

    st.markdown('<p class="section-heading">Individual votes</p>', unsafe_allow_html=True)
    st.markdown(
        _td_vote_html(
            div_df[["date", "debate_title", "vote_type", "vote_outcome"]].assign(debate_title=subject or debate_title)
        ),
        unsafe_allow_html=True,
    )

    st.download_button(
        "Download CSV",
        data=div_df[["full_name", "party", "constituency_name", "vote_type"]].to_csv(index=False).encode(),
        file_name=f"{debate_title[:40].replace(' ', '_')}.csv",
        mime="text/csv",
        key=f"{key_prefix}_dl",
    )


def _divisions_view(df: pd.DataFrame, years: list[int]) -> None:
    st.markdown('<p class="section-heading">Select year</p>', unsafe_allow_html=True)
    sel_year = st.radio(
        "Year", years, index=0, horizontal=True, label_visibility="collapsed", key="v_year_d", format_func=str
    )

    year_view = df[df["year"] == sel_year]
    debates = _build_debates(year_view)

    col1, col2 = st.columns(2)
    with col1:
        theme_opts = ["All themes"] + list(_THEMES.keys()) + ["Other"]
        sel_theme = st.selectbox("Theme", theme_opts, key="v_theme_d", label_visibility="visible")
    with col2:
        st.metric("Debates", len(debates))

    if sel_theme != "All themes":
        debates = debates[debates["theme"] == sel_theme]

    notable = debates[debates["theme"] != "Other"]
    if not notable.empty:
        st.markdown('<p class="section-heading">Significant debates</p>', unsafe_allow_html=True)
        st.caption("Debates matching tracked themes — housing, carbon, confidence, taxation and more.")
        _debates_table(notable, year_view, key_prefix=f"notable_{sel_year}")
        st.markdown("---")

    st.markdown(f'<p class="section-heading">All debates · {sel_year}</p>', unsafe_allow_html=True)
    st.caption(f"{len(debates)} debates · {debates['divisions'].sum()} divisions · select a debate to drill in.")
    _debates_table(debates, year_view, key_prefix=f"all_{sel_year}")


# ── TD landing (no search yet) ───────────────────────────────────────────────


def _td_landing(df: pd.DataFrame, years: list[int]) -> None:
    """Browse all TDs by party with summary stats — shown before a TD is searched."""

    # Build per-TD summary across all years
    summary = (
        df.groupby(["full_name", "party", "constituency_name"])
        .agg(
            yes=("vote_type", lambda x: (x == "Voted Yes").sum()),
            no=("vote_type", lambda x: (x == "Voted No").sum()),
            divisions=("vote_id", "nunique"),
        )
        .reset_index()
    )
    summary["yes_pct"] = (summary["yes"] / (summary["yes"] + summary["no"]).replace(0, 1) * 100).round(0).astype(int)

    st.markdown('<p class="section-heading">Select a TD to view their voting record</p>', unsafe_allow_html=True)

    col1, col2 = st.columns([1, 2])
    with col1:
        party_opts = ["All parties"] + sorted(summary["party"].dropna().unique())
        party_pick = st.selectbox("Filter by party", party_opts, key="landing_party", label_visibility="visible")
    with col2:
        name_search = st.text_input(
            "Search by name", placeholder="e.g. McDonald, Harris…", key="landing_search", label_visibility="visible"
        )

    filtered = summary.copy()
    if party_pick != "All parties":
        filtered = filtered[filtered["party"] == party_pick]
    if name_search.strip():
        filtered = filtered[filtered["full_name"].str.contains(name_search.strip(), case=False, na=False)]

    filtered = filtered.sort_values("full_name")

    st.caption(f"{len(filtered)} TDs · click a name to load their full record")

    st.dataframe(
        filtered[["full_name", "party", "constituency_name", "divisions", "yes", "no", "yes_pct"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "full_name": st.column_config.TextColumn("TD"),
            "party": st.column_config.TextColumn("Party"),
            "constituency_name": st.column_config.TextColumn("Constituency"),
            "divisions": st.column_config.NumberColumn("Divisions", format="%d"),
            "yes": st.column_config.NumberColumn("Yes ✓", format="%d"),
            "no": st.column_config.NumberColumn("No ✗", format="%d"),
            "yes_pct": st.column_config.ProgressColumn("% Yes", format="%d%%", min_value=0, max_value=100),
        },
    )

    st.markdown('<p class="section-heading">Or pick directly</p>', unsafe_allow_html=True)
    names = filtered["full_name"].tolist()
    chosen = st.selectbox("Select TD", ["— select —"] + names, key="landing_pick", label_visibility="collapsed")
    if chosen != "— select —":
        _td_record(df, chosen, years)


# ── Entry point ───────────────────────────────────────────────────────────────


def votes_page() -> None:
    inject_css()
    csv_mtime = _get_csv_mtime()
    df = _load(csv_mtime)

    years = sorted(df["year"].dropna().astype(int).unique(), reverse=True)

    with st.sidebar:
        st.markdown('<div class="page-kicker">Dáil Tracker</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-title">Dáil<br>Divisions</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="page-subtitle">{df["vote_id"].nunique():,} divisions · {df["full_name"].nunique()} TDs</div>',
            unsafe_allow_html=True,
        )

        st.markdown('<p class="sidebar-label">View</p>', unsafe_allow_html=True)
        view = st.radio("View", ["TD Record", "Divisions"], label_visibility="collapsed", key="v_mode")

        if view == "TD Record":
            st.markdown('<p class="sidebar-label">Search TD</p>', unsafe_allow_html=True)
            td_search = st.text_input("TD name", placeholder="e.g. McDonald", label_visibility="collapsed", key="v_td")
        else:
            td_search = ""
            st.markdown('<p class="sidebar-label">Filter by party</p>', unsafe_allow_html=True)
            parties = ["All"] + sorted(df["party"].dropna().unique())
            sel_party = st.selectbox("Party", parties, label_visibility="collapsed", key="v_party")
            if sel_party != "All":
                df = df[df["party"] == sel_party]

    with st.expander("What is this data? (Click for details)", expanded=False):
        st.markdown(
            """
            **About Dáil Divisions**

            Every time TDs formally divide in the chamber, each member's vote — Yes, No,
            or Abstained — is recorded and published by the Oireachtas.

            **TD Record** shows one TD's complete voting history: every debate they voted on,
            how they voted, and whether their side won. Green ✓ = voted Yes, red ✗ = voted No.

            **Divisions** shows the debates themselves — how the full chamber divided, Yes/No
            totals, outcomes, and a drill-in to see the party breakdown and individual votes.

            **Caveats:** Absences are not recorded — only members present and voting appear.
            Debate titles are as published; they do not always map cleanly to a single bill.
            """
        )

    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    if view == "TD Record":
        if not td_search.strip():
            _td_landing(df, years)
            return

        matches = sorted(
            df[df["full_name"].str.contains(td_search.strip(), case=False, na=False)]["full_name"].unique()
        )
        if not matches:
            st.info("No TD found matching that name.")
            return

        td_name = st.selectbox("Select TD", matches, key="v_td_sel") if len(matches) > 1 else matches[0]
        _td_record(df, td_name, years)

    else:
        _divisions_view(df, years)


# ── Bill URL enrichment (exercise — do not uncomment until stages join is ready) ──
#
# The Oireachtas bill URL format is:
#   https://www.oireachtas.ie/en/bills/bill/{bill_year}/{bill_no}/
# Example: Bill 75 of 2025 → https://www.oireachtas.ie/en/bills/bill/2025/75/
#
# The vote data contains debate_title (e.g. "… Bill 2025") but NOT the bill number.
# To construct URLs you need to join vote history with data/silver/stages.csv on title.
#
# import polars as pl
# from config import DATA_DIR
#
# stages = pl.read_csv(DATA_DIR / "silver" / "stages.csv")
#
# bills = (
#     stages
#     .select([
#         pl.col("bill.billNo").alias("bill_no"),
#         pl.col("bill.billYear").alias("bill_year").cast(pl.Int32),
#         pl.col("bill.shortTitleEn").alias("short_title"),
#     ])
#     .unique(subset=["bill_no", "bill_year"])
#     .with_columns(
#         pl.format(
#             "https://www.oireachtas.ie/en/bills/bill/{}/{}",
#             pl.col("bill_year"),
#             pl.col("bill_no"),
#         ).alias("oireachtas_url")
#     )
# )
#
# # NOTE: debate_title in votes ≠ short_title in bills exactly.
# # Titles in votes often include stage text ("… Second Stage [Resumed]").
# # A str.contains join is more reliable than an exact match:
# #
# # votes = pl.read_csv(DATA_DIR / "gold" / "current_dail_vote_history.csv")
# #
# # For each vote row, check if any bill short_title appears in debate_title.
# # This is easier to do in pandas with a cross-merge + str.contains than in Polars.
# # See enrich.py for the join pattern to add to the pipeline.
