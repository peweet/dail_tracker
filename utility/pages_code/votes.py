from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent
_CSV  = _ROOT / "data" / "gold" / "current_dail_vote_history.csv"


@st.cache_data
def _load() -> pd.DataFrame:
    df = pd.read_csv(_CSV, low_memory=False)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year
    df["voted_with_outcome"] = (
        ((df["vote_type"] == "Voted Yes")  & (df["vote_outcome"] == "Carried")) |
        ((df["vote_type"] == "Voted No")   & (df["vote_outcome"] == "Lost"))
    )
    return df


def _rebellion_rate(df: pd.DataFrame) -> float:
    eligible = df[df["vote_type"].isin(["Voted Yes", "Voted No"])]
    if len(eligible) == 0:
        return 0.0
    rebels = (~eligible["voted_with_outcome"]).sum()
    return rebels / len(eligible) * 100


def votes_page() -> None:
    inject_css()

    df = _load()

    # ── Sidebar filters ──────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<p class="sidebar-label">Member</p>', unsafe_allow_html=True)
        search = st.text_input("Search by name", placeholder="e.g. McDonald", label_visibility="collapsed")

        st.markdown('<p class="sidebar-label">Vote type</p>', unsafe_allow_html=True)
        vote_types = ["All"] + sorted(df["vote_type"].dropna().unique().tolist())
        selected_type = st.selectbox("Vote type", vote_types, label_visibility="collapsed")

        st.markdown('<p class="sidebar-label">Date range</p>', unsafe_allow_html=True)
        min_y = int(df["year"].min()) if not df["year"].isna().all() else 2020
        max_y = int(df["year"].max()) if not df["year"].isna().all() else 2026
        year_range = st.slider("Year", min_y, max_y, (min_y, max_y), label_visibility="collapsed")

        st.markdown('<p class="sidebar-label">Dáil term</p>', unsafe_allow_html=True)
        terms = ["All"] + sorted(df["dail_term"].dropna().unique().tolist())
        selected_term = st.selectbox("Dáil term", terms, label_visibility="collapsed")

    # ── Apply filters ─────────────────────────────────────────────────
    view = df.copy()

    if search.strip():
        mask = view["full_name"].str.contains(search.strip(), case=False, na=False)
        view = view[mask]

    if selected_type != "All":
        view = view[view["vote_type"] == selected_type]

    view = view[view["year"].between(year_range[0], year_range[1])]

    if selected_term != "All":
        view = view[view["dail_term"] == selected_term]

    # ── Page header ───────────────────────────────────────────────────
    st.markdown('<p class="page-kicker">Oireachtas Tracker</p>', unsafe_allow_html=True)
    st.markdown('<h1 class="page-title">Vote History</h1>', unsafe_allow_html=True)
    st.markdown(
        '<p class="page-subtitle">Division records for the current Dáil. '
        'Each row is one member\'s vote on one division.</p>',
        unsafe_allow_html=True,
    )
    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    if view.empty:
        st.info("No records match the current filters.")
        return

    # ── Summary metrics ───────────────────────────────────────────────
    total_votes  = len(view)
    total_members = view["full_name"].nunique()
    total_divs   = view["vote_id"].nunique()
    reb_rate     = _rebellion_rate(view)

    yes_pct = (view["vote_type"] == "Voted Yes").sum() / total_votes * 100
    no_pct  = (view["vote_type"] == "Voted No").sum()  / total_votes * 100
    abs_pct = (view["vote_type"] == "Abstained").sum() / total_votes * 100

    st.markdown(
        f"""
        <div class="stat-strip">
            <div><div class="stat-num">{total_votes:,}</div><div class="stat-lbl">Total votes cast</div></div>
            <div><div class="stat-num">{total_divs:,}</div><div class="stat-lbl">Divisions</div></div>
            <div><div class="stat-num">{total_members:,}</div><div class="stat-lbl">Members</div></div>
            <div><div class="stat-num">{yes_pct:.0f}%</div><div class="stat-lbl">Voted Yes</div></div>
            <div><div class="stat-num">{no_pct:.0f}%</div><div class="stat-lbl">Voted No</div></div>
            <div><div class="stat-num">{reb_rate:.1f}%</div><div class="stat-lbl">Rebellion rate</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Member spotlight (when filtered to one person) ─────────────────
    if search.strip() and total_members == 1:
        name = view["full_name"].iloc[0]
        dail = view["dail_term"].dropna().mode()
        dail_label = dail.iloc[0] if not dail.empty else "—"

        st.markdown(f'<h2 class="td-name">{name}</h2>', unsafe_allow_html=True)
        st.markdown(f'<p class="td-meta">{dail_label}</p>', unsafe_allow_html=True)

        yes_n = (view["vote_type"] == "Voted Yes").sum()
        no_n  = (view["vote_type"] == "Voted No").sum()
        abs_n = (view["vote_type"] == "Abstained").sum()
        reb_n = (~view[view["vote_type"].isin(["Voted Yes","Voted No"])]["voted_with_outcome"]).sum() if total_votes else 0

        st.markdown(
            f"""
            <div class="stat-strip">
                <div><div class="stat-num">{yes_n:,}</div><div class="stat-lbl">Voted Yes</div></div>
                <div><div class="stat-num">{no_n:,}</div><div class="stat-lbl">Voted No</div></div>
                <div><div class="stat-num">{abs_n:,}</div><div class="stat-lbl">Abstained</div></div>
                <div><div class="stat-num">{reb_rate:.1f}%</div><div class="stat-lbl">Rebellion rate</div></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ── Top 10 most rebellious TDs ─────────────────────────────────────
    st.markdown('<p class="section-heading">Most rebellious TDs</p>', unsafe_allow_html=True)

    eligible = view[view["vote_type"].isin(["Voted Yes", "Voted No"])].copy()
    if not eligible.empty:
        rebellion = (
            eligible.groupby("full_name")
            .agg(
                total_div_votes=("vote_id", "count"),
                rebellions=("voted_with_outcome", lambda x: (~x).sum()),
            )
            .reset_index()
        )
        rebellion["rebellion_rate"] = rebellion["rebellions"] / rebellion["total_div_votes"] * 100
        rebellion = rebellion[rebellion["total_div_votes"] >= 10].sort_values("rebellion_rate", ascending=False).head(10)
        rebellion = rebellion.rename(columns={
            "full_name": "Member",
            "total_div_votes": "Divisions voted",
            "rebellions": "Rebellions",
            "rebellion_rate": "Rebellion rate %",
        })
        rebellion["Rebellion rate %"] = rebellion["Rebellion rate %"].round(1)
        st.dataframe(rebellion, use_container_width=True, hide_index=True)

    # ── Most contested divisions ───────────────────────────────────────
    st.markdown('<p class="section-heading">Most contested divisions</p>', unsafe_allow_html=True)

    contested = (
        view.groupby(["vote_id", "debate_title", "date", "vote_outcome"])
        .agg(
            yes_votes=("vote_type", lambda x: (x == "Voted Yes").sum()),
            no_votes=("vote_type",  lambda x: (x == "Voted No").sum()),
        )
        .reset_index()
    )
    contested["margin"] = (contested["yes_votes"] - contested["no_votes"]).abs()
    contested = contested.sort_values("margin").head(10)
    contested = contested.rename(columns={
        "vote_id": "Division",
        "debate_title": "Debate",
        "date": "Date",
        "vote_outcome": "Outcome",
        "yes_votes": "Yes",
        "no_votes": "No",
        "margin": "Margin",
    })
    st.dataframe(contested, use_container_width=True, hide_index=True)

    # ── Full table ─────────────────────────────────────────────────────
    st.markdown('<p class="section-heading">All records</p>', unsafe_allow_html=True)

    display_cols = ["full_name", "vote_type", "vote_outcome", "date", "debate_title", "subject", "dail_term"]
    display = view[display_cols].rename(columns={
        "full_name": "Member",
        "vote_type": "Vote",
        "vote_outcome": "Outcome",
        "date": "Date",
        "debate_title": "Debate",
        "subject": "Subject",
        "dail_term": "Dáil term",
    }).sort_values("Date", ascending=False)

    st.dataframe(display, use_container_width=True, hide_index=True)

    st.download_button(
        "Download CSV",
        data=display.to_csv(index=False).encode(),
        file_name="vote_history.csv",
        mime="text/csv",
    )
