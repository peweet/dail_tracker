import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent
_SILVER = _ROOT / "data" / "silver"

_SPONSORS_CSV = _SILVER / "sponsors.csv"
_STAGES_CSV = _SILVER / "stages.csv"
_DEBATES_CSV = _SILVER / "debates.csv"

_STAGE_ORDER = [
    "First Stage",
    "Second Stage",
    "Committee Stage",
    "Report Stage",
    "Fifth Stage",
    "Passed",
    "Signed",
]


@st.cache_data(show_spinner=False)
def _load_sponsors() -> pd.DataFrame:
    if not _SPONSORS_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(_SPONSORS_CSV, low_memory=False)
    df = df.rename(
        columns={
            "sponsor.by.showAs": "td_name",
            "sponsor.isPrimary": "is_primary",
            "bill.billNo": "bill_no",
            "bill.billYear": "bill_year",
            "bill.shortTitleEn": "title",
            "bill.status": "status",
            "bill.source": "source",
            "bill.method": "method",
            "bill.mostRecentStage.event.showAs": "current_stage",
            "bill.mostRecentStage.event.house.showAs": "house",
            "bill.lastUpdated": "last_updated",
        }
    )
    df["bill_year"] = pd.to_numeric(df["bill_year"], errors="coerce")
    df["is_primary"] = df["is_primary"].astype(str).str.lower().isin(["true", "1", "yes"])
    return df


@st.cache_data(show_spinner=False)
def _load_stages() -> pd.DataFrame:
    if not _STAGES_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(_STAGES_CSV, low_memory=False)
    df = df.rename(
        columns={
            "event.showAs": "stage",
            "event.progressStage": "stage_no",
            "event.stageCompleted": "completed",
            "event.stageOutcome": "outcome",
            "event.house.showAs": "house",
            "bill.billNo": "bill_no",
            "bill.billYear": "bill_year",
            "bill.shortTitleEn": "title",
            "bill.status": "bill_status",
            "bill.source": "source",
            "bill.lastUpdated": "last_updated",
        }
    )
    df["bill_year"] = pd.to_numeric(df["bill_year"], errors="coerce")
    df["stage_no"] = pd.to_numeric(df["stage_no"], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def _load_debates() -> pd.DataFrame:
    if not _DEBATES_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(_DEBATES_CSV, low_memory=False)
    df = df.rename(
        columns={
            "date": "date",
            "showAs": "debate_title",
            "chamber.showAs": "chamber",
            "bill.billNo": "bill_no",
            "bill.billYear": "bill_year",
            "bill.shortTitleEn": "title",
            "bill.status": "bill_status",
            "bill.source": "source",
            "bill.lastUpdated": "last_updated",
            "bill.mostRecentStage.event.showAs": "current_stage",
        }
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["bill_year"] = pd.to_numeric(df["bill_year"], errors="coerce")
    return df


def _section(text: str) -> None:
    st.markdown(f'<p class="section-heading">{text}</p>', unsafe_allow_html=True)


def _export(df: pd.DataFrame, filename: str, key: str) -> None:
    st.download_button("Export CSV", df.to_csv(index=False).encode("utf-8"), filename, "text/csv", key=key)


# ── Sponsors view ─────────────────────────────────────────────────────


def _sponsors_view() -> None:
    df = _load_sponsors()
    if df.empty:
        st.info("Sponsors data not found. Run the pipeline first.")
        return

    st.caption(
        "Every bill a TD has sponsored or co-sponsored. Primary sponsors introduced the bill; "
        "co-sponsors formally backed it. Filter to see a TD's full legislative agenda."
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        td_search = st.text_input("TD name", placeholder="Filter by TD…", key="sp_td")
    with col2:
        years = sorted(df["bill_year"].dropna().unique().astype(int), reverse=True)
        year_filter = st.selectbox("Year", ["All years"] + [str(y) for y in years], key="sp_year")
    with col3:
        status_opts = ["All statuses"] + sorted(df["status"].dropna().unique())
        status_filter = st.selectbox("Status", status_opts, key="sp_status")

    primary_only = st.checkbox("Primary sponsors only", value=False, key="sp_primary")

    filtered = df.copy()
    if td_search.strip():
        filtered = filtered[filtered["td_name"].str.contains(td_search.strip(), case=False, na=False)]
    if year_filter != "All years":
        filtered = filtered[filtered["bill_year"] == int(year_filter)]
    if status_filter != "All statuses":
        filtered = filtered[filtered["status"] == status_filter]
    if primary_only:
        filtered = filtered[filtered["is_primary"]]

    _section(f"{len(filtered):,} sponsorships")
    st.dataframe(
        filtered[["td_name", "title", "bill_year", "status", "current_stage", "house", "is_primary"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "td_name": st.column_config.TextColumn("TD"),
            "title": st.column_config.TextColumn("Bill", width="large"),
            "bill_year": st.column_config.NumberColumn("Year", format="%d"),
            "status": st.column_config.TextColumn("Status"),
            "current_stage": st.column_config.TextColumn("Current stage"),
            "house": st.column_config.TextColumn("House"),
            "is_primary": st.column_config.CheckboxColumn("Primary"),
        },
    )
    _export(filtered, "sponsors.csv", "sp_exp")

    if td_search.strip():
        td_bills = filtered.groupby("td_name")["title"].count().reset_index()
        td_bills.columns = ["TD", "Bills"]
        if len(td_bills) == 1:
            st.markdown("---")
            _section("Bills by status")
            st.bar_chart(filtered["status"].value_counts().rename("Count"))


# ── Stages / timeline view ────────────────────────────────────────────


def _stages_view() -> None:
    df = _load_stages()
    if df.empty:
        st.info("Stages data not found. Run the pipeline first.")
        return

    st.caption(
        "The legislative journey of every bill — from First Stage introduction through to "
        "passing or lapsing. Each row is one stage reached by one bill."
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        title_search = st.text_input("Bill title", placeholder="Search bills…", key="st_title")
    with col2:
        years = sorted(df["bill_year"].dropna().unique().astype(int), reverse=True)
        year_filter = st.selectbox("Year introduced", ["All years"] + [str(y) for y in years], key="st_year")
    with col3:
        status_opts = ["All statuses"] + sorted(df["bill_status"].dropna().unique())
        status_filter = st.selectbox("Bill status", status_opts, key="st_status")

    source_opts = ["All sources"] + sorted(df["source"].dropna().unique())
    source_filter = st.selectbox("Source", source_opts, key="st_source")

    filtered = df.copy()
    if title_search.strip():
        filtered = filtered[filtered["title"].str.contains(title_search.strip(), case=False, na=False)]
    if year_filter != "All years":
        filtered = filtered[filtered["bill_year"] == int(year_filter)]
    if status_filter != "All statuses":
        filtered = filtered[filtered["bill_status"] == status_filter]
    if source_filter != "All sources":
        filtered = filtered[filtered["source"] == source_filter]

    # Summary: bills by status
    bill_summary = (
        filtered.groupby(["bill_no", "bill_year", "title", "bill_status", "source"])
        .agg(stages_reached=("stage_no", "max"))
        .reset_index()
        .sort_values(["bill_year", "bill_no"], ascending=[False, True])
    )

    with st.expander(f"Bill summary — {len(bill_summary):,} bills", expanded=True):
        st.dataframe(
            bill_summary,
            hide_index=True,
            use_container_width=True,
            column_config={
                "bill_no": st.column_config.NumberColumn("No.", format="%d", width="small"),
                "bill_year": st.column_config.NumberColumn("Year", format="%d", width="small"),
                "title": st.column_config.TextColumn("Title", width="large"),
                "bill_status": st.column_config.TextColumn("Status"),
                "source": st.column_config.TextColumn("Source"),
                "stages_reached": st.column_config.ProgressColumn(
                    "Stages reached", format="%d", min_value=0, max_value=7
                ),
            },
        )
        _export(bill_summary, "bill_summary.csv", "st_sum_exp")

    with st.expander("Full stage-by-stage detail", expanded=False):
        display_cols = [
            c
            for c in ["title", "bill_year", "stage", "completed", "outcome", "house", "bill_status"]
            if c in filtered.columns
        ]
        st.dataframe(
            filtered[display_cols].sort_values(["bill_year", "bill_no", "stage_no"], ascending=[False, True, True]),
            hide_index=True,
            use_container_width=True,
            column_config={
                "title": st.column_config.TextColumn("Bill", width="large"),
                "bill_year": st.column_config.NumberColumn("Year", format="%d", width="small"),
                "stage": st.column_config.TextColumn("Stage"),
                "completed": st.column_config.CheckboxColumn("Done"),
                "outcome": st.column_config.TextColumn("Outcome"),
                "house": st.column_config.TextColumn("House"),
                "bill_status": st.column_config.TextColumn("Bill status"),
            },
        )
        _export(filtered[display_cols], "stages_detail.csv", "st_det_exp")

    # Status breakdown chart
    _section("Bills by status")
    st.bar_chart(bill_summary["bill_status"].value_counts().rename("Bills"))


# ── Debates view ──────────────────────────────────────────────────────


def _debates_view() -> None:
    df = _load_debates()
    if df.empty:
        st.info("Debates data not found. Run the pipeline first.")
        return

    st.caption(
        "Each row is a debate section in the Dáil or Seanad that mentioned a specific bill. "
        "This shows which bills were actually discussed on the floor, and when — a measure of "
        "parliamentary engagement beyond formal stage progression."
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        title_search = st.text_input("Bill title", placeholder="Search bills…", key="db_title")
    with col2:
        years = sorted(df["bill_year"].dropna().unique().astype(int), reverse=True)
        year_filter = st.selectbox("Bill year", ["All years"] + [str(y) for y in years], key="db_year")
    with col3:
        chamber_opts = ["All chambers"] + sorted(df["chamber"].dropna().unique())
        chamber_filter = st.selectbox("Chamber", chamber_opts, key="db_chamber")

    date_range = st.date_input(
        "Debate date range",
        value=(df["date"].min().date(), df["date"].max().date()),
        key="db_dates",
    )

    filtered = df.copy()
    if title_search.strip():
        filtered = filtered[filtered["title"].str.contains(title_search.strip(), case=False, na=False)]
    if year_filter != "All years":
        filtered = filtered[filtered["bill_year"] == int(year_filter)]
    if chamber_filter != "All chambers":
        filtered = filtered[filtered["chamber"] == chamber_filter]
    if len(date_range) == 2:
        filtered = filtered[(filtered["date"].dt.date >= date_range[0]) & (filtered["date"].dt.date <= date_range[1])]

    _section(f"{len(filtered):,} debate sections")

    # Most debated bills
    most_debated = (
        filtered.groupby(["title", "bill_year", "bill_status"])
        .size()
        .reset_index(name="debate_count")
        .sort_values("debate_count", ascending=False)
        .head(20)
    )

    col_l, col_r = st.columns([2, 1])
    with col_l:
        _section("Most debated bills")
        max_d = int(most_debated["debate_count"].max()) if not most_debated.empty else 1
        st.dataframe(
            most_debated,
            hide_index=True,
            use_container_width=True,
            column_config={
                "title": st.column_config.TextColumn("Bill", width="large"),
                "bill_year": st.column_config.NumberColumn("Year", format="%d", width="small"),
                "bill_status": st.column_config.TextColumn("Status"),
                "debate_count": st.column_config.ProgressColumn("Debates", format="%d", min_value=0, max_value=max_d),
            },
        )

    with col_r:
        _section("By chamber")
        st.bar_chart(filtered["chamber"].value_counts().rename("Debates"))

    _section("All debate records")
    display_cols = [
        c for c in ["date", "debate_title", "chamber", "title", "bill_year", "bill_status"] if c in filtered.columns
    ]
    st.dataframe(
        filtered[display_cols].sort_values("date", ascending=False),
        hide_index=True,
        use_container_width=True,
        column_config={
            "date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "debate_title": st.column_config.TextColumn("Debate section", width="large"),
            "chamber": st.column_config.TextColumn("Chamber"),
            "title": st.column_config.TextColumn("Bill", width="large"),
            "bill_year": st.column_config.NumberColumn("Year", format="%d", width="small"),
            "bill_status": st.column_config.TextColumn("Status"),
        },
    )
    _export(filtered[display_cols], "debates.csv", "db_exp")


# ── Entry point ───────────────────────────────────────────────────────


def legislation_page() -> None:
    inject_css()

    sponsors = _load_sponsors()
    stages = _load_stages()
    debates = _load_debates()

    total_bills = stages["title"].nunique() if not stages.empty else 0
    total_sponsors = sponsors["td_name"].nunique() if not sponsors.empty else 0
    total_debates = len(debates)

    with st.sidebar:
        st.markdown('<div class="page-kicker">Dáil Tracker</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-title">Legislation</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="page-subtitle">{total_bills:,} bills · {total_sponsors:,} TD sponsors</div>',
            unsafe_allow_html=True,
        )
        st.markdown('<p class="sidebar-label">View</p>', unsafe_allow_html=True)
        view = st.radio(
            "View",
            ["Sponsors", "Stages", "Debates"],
            label_visibility="collapsed",
            key="leg_view",
        )

    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    with st.expander("About this data — three windows on the same agenda", expanded=False):
        st.markdown(
            """
            Irish legislation can be explored from three angles, each giving a different window
            on what TDs actually do in office:

            **Sponsors** — *Who proposed what?*
            Every bill must be formally sponsored by one or more TDs. The primary sponsor
            introduced the bill; co-sponsors backed it. This dataset lets you see a TD's full
            legislative agenda across their career — what issues they pushed for, how far each
            bill got, and whether they tend to sponsor government or private member bills.

            **Stages** — *What happened to each bill?*
            Every bill moves through a sequence of stages (First Stage introduction → Second Stage
            debate → Committee Stage amendment → Report Stage → passing or lapsing). This dataset
            is the historical record of every bill's journey — a timeline of Irish legislation
            since the data begins. Use it to see what proportion of bills make it through, which
            lapse, and which sources (Government vs. Private Member) have the best success rates.

            **Debates** — *Was it actually discussed?*
            Being introduced is not the same as being debated. This dataset records every time a
            bill appeared as a formal debate section in the Dáil or Seanad chamber. A bill with
            many debate records was genuinely contested; one that never appears here was introduced
            but largely ignored. This is a measure of parliamentary engagement.

            *Note: vote data is tracked separately and will be joined to legislation in a future update.*
            """
        )

    if view == "Sponsors":
        _sponsors_view()
    elif view == "Stages":
        _stages_view()
    elif view == "Debates":
        _debates_view()
