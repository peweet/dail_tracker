from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent
_CSV  = _ROOT / "data" / "silver" / "aggregated_td_tables.csv"

_DISCLAIMER = """
**Note on attendance figures**

Plenary attendance records the days a TD was present in the full chamber on scheduled sitting days.
It does not capture the full extent of a TD's parliamentary activity. Members with ministerial
responsibilities, committee assignments, or committee leadership roles frequently conduct substantial
parliamentary work outside plenary hours — including committee hearings, departmental scrutiny, and
constituency casework. The Oireachtas itself notes that committee membership carries a
[responsibility to attend and participate](https://www.oireachtas.ie/en/committees/about-committees/)
in meetings that often run concurrently with, or instead of, plenary business.

Lower plenary attendance figures should not, therefore, be interpreted as a complete or definitive
measure of a member's parliamentary engagement.
""".strip()


@st.cache_data
def _load() -> pd.DataFrame:
    df = pd.read_csv(_CSV, low_memory=False)
    df["sitting_date"] = pd.to_datetime(df["iso_sitting_days_attendance"], errors="coerce")
    df["full_name"]    = df["first_name"] + " " + df["last_name"]
    return df


def _calendar_heatmap(dates: pd.Series, year: int) -> go.Figure:
    """GitHub-style calendar heatmap for a single TD's plenary sitting days in one year."""
    start    = pd.Timestamp(f"{year}-01-01")
    year_end = pd.Timestamp(f"{year}-12-31")
    today    = pd.Timestamp.today().normalize()

    attended_set = set(pd.to_datetime(dates).dt.normalize())

    records = []
    for d in pd.date_range(start, year_end, freq="D"):
        week    = (d.dayofyear - 1) // 7
        weekday = d.weekday()
        if d > today:
            val = np.nan  # future — leave blank
        elif d in attended_set:
            val = 1
        else:
            val = 0
        records.append({"date": d, "week": week, "weekday": weekday, "val": val,
                         "label": d.strftime("%d %b %Y")})

    df_cal     = pd.DataFrame(records)
    pivot_z    = df_cal.pivot_table(index="weekday", columns="week", values="val",    aggfunc="first")
    pivot_text = df_cal.pivot_table(index="weekday", columns="week", values="label",  aggfunc="first")

    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Month tick positions on x-axis
    month_df   = df_cal.groupby(df_cal["date"].dt.month)["week"].min()
    month_text = [pd.Timestamp(f"{year}-{m:02d}-01").strftime("%b") for m in month_df.index]

    fig = go.Figure(go.Heatmap(
        z=pivot_z.values,
        x=pivot_z.columns.tolist(),
        y=[day_labels[i] for i in pivot_z.index],
        text=pivot_text.values,
        colorscale=[[0, "#e5e7eb"], [1, "#1d4ed8"]],
        zmin=0, zmax=1,
        showscale=False,
        hovertemplate="%{text}<extra></extra>",
        xgap=3, ygap=3,
    ))
    fig.update_layout(
        title=dict(text=str(year), font=dict(size=13)),
        height=165,
        margin=dict(l=40, r=10, t=30, b=30),
        xaxis=dict(tickvals=month_df.tolist(), ticktext=month_text, showgrid=False, zeroline=False),
        yaxis=dict(autorange="reversed", showgrid=False, zeroline=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig


def _yearly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Per-TD per-year sitting and other day counts, taken directly from the source data."""
    return (
        df.groupby(["full_name", "year"], sort=True)
        .agg(
            sitting_days=("sitting_days_count", "first"),
            other_days  =("other_days_count",   "first"),
        )
        .reset_index()
        .sort_values(["full_name", "year"])
    )


def attendance_page() -> None:
    inject_css()
    st.title("TD Attendance")
    st.caption("Plenary sitting day and other day attendance — 2024 to 2026")

    with st.expander("About these figures", expanded=False):
        st.markdown(_DISCLAIMER)

    df = _load()
    all_names = sorted(df["full_name"].unique())
    all_years = sorted(df["year"].dropna().unique().astype(int))

    # ── Sidebar ──────────────────────────────────────────────────────────────
    st.sidebar.header("Controls")

    selected_years = st.sidebar.multiselect(
        "Year",
        all_years,
        default=all_years,
    )

    selected_tds = st.sidebar.multiselect(
        "Filter to specific TDs (leave blank for all)",
        all_names,
    )

    view_mode = st.sidebar.radio(
        "View mode",
        ["Table", "Timeline", "Individual TD"],
        index=0,
    )

    # ── Filter ────────────────────────────────────────────────────────────────
    filtered = df[df["year"].isin(selected_years)]
    if selected_tds:
        filtered = filtered[filtered["full_name"].isin(selected_tds)]

    yearly = _yearly_summary(filtered)

    # ── Top-level metrics ─────────────────────────────────────────────────────
    totals_per_td = yearly.groupby("full_name")[["sitting_days", "other_days"]].sum()
    c1, c2, c3 = st.columns(3)
    c1.metric("TDs in view",                totals_per_td.index.nunique())
    c2.metric("Avg plenary sitting days",   f"{totals_per_td['sitting_days'].mean():.0f}")
    c3.metric("Avg other days",             f"{totals_per_td['other_days'].mean():.0f}")

    st.divider()

    # ── Table view ────────────────────────────────────────────────────────────
    if view_mode == "Table":
        st.subheader("Attendance by TD and year")
        st.caption(
            "Sitting days = plenary chamber days attended. "
            "Other days = committee, delegated, and other parliamentary business."
        )

        if len(selected_years) == 1:
            display = yearly[["full_name", "sitting_days", "other_days"]].sort_values(
                "sitting_days", ascending=False
            )
            st.dataframe(
                display,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "full_name":    st.column_config.TextColumn("TD"),
                    "sitting_days": st.column_config.NumberColumn("Plenary sitting days", format="%d"),
                    "other_days":   st.column_config.NumberColumn("Other days", format="%d"),
                },
            )
        else:
            # Pivot so each year gets its own column pair
            pivot = yearly.pivot_table(
                index="full_name",
                columns="year",
                values=["sitting_days", "other_days"],
                fill_value=0,
            )
            pivot.columns = [f"{col[0].replace('_',' ').title()} {col[1]}" for col in pivot.columns]
            pivot = pivot.reset_index().sort_values(
                [c for c in pivot.reset_index().columns if "Sitting" in c][-1],
                ascending=False,
            )
            st.dataframe(pivot, hide_index=True, use_container_width=True)

        st.download_button(
            "Download CSV",
            yearly.to_csv(index=False),
            file_name="td_attendance_by_year.csv",
            mime="text/csv",
        )

    # ── Timeline ──────────────────────────────────────────────────────────────
    elif view_mode == "Timeline":
        st.subheader("Plenary sitting days per year")

        if not selected_tds:
            st.info(
                "Showing all TDs aggregated. Select specific TDs in the sidebar to compare individuals."
            )
            agg = yearly.groupby("year")[["sitting_days", "other_days"]].sum()
            st.bar_chart(agg)
        else:
            pivot = yearly.pivot_table(
                index="year", columns="full_name", values="sitting_days", fill_value=0
            )
            st.subheader("Plenary sitting days")
            st.bar_chart(pivot)

            pivot_other = yearly.pivot_table(
                index="year", columns="full_name", values="other_days", fill_value=0
            )
            st.subheader("Other days")
            st.bar_chart(pivot_other)

    # ── Individual TD ─────────────────────────────────────────────────────────
    elif view_mode == "Individual TD":
        st.subheader("Individual TD")
        td_name   = st.selectbox("Select TD", all_names)
        td_yearly = yearly[yearly["full_name"] == td_name]
        td_raw    = filtered[filtered["full_name"] == td_name].copy()

        if td_yearly.empty:
            st.warning("No data for this TD in the selected years.")
        else:
            total_sit   = int(td_yearly["sitting_days"].sum())
            total_other = int(td_yearly["other_days"].sum())

            m1, m2 = st.columns(2)
            m1.metric("Total plenary sitting days", total_sit)
            m2.metric("Total other days",           total_other)

            st.subheader("Year-by-year breakdown")
            st.dataframe(
                td_yearly[["year", "sitting_days", "other_days"]].rename(columns={
                    "year":         "Year",
                    "sitting_days": "Plenary sitting days",
                    "other_days":   "Other days",
                }),
                hide_index=True,
                use_container_width=True,
            )

            # ── ORIGINAL: date table (commented out — replaced by calendar heatmap below)
            # st.subheader("Plenary sitting day dates")
            # td_sit = (
            #     td_raw[td_raw["sitting_date"].notna()]
            #     .sort_values("sitting_date")[["sitting_date", "year"]]
            #     .copy()
            # )
            # td_sit["date"]    = td_sit["sitting_date"].dt.strftime("%d %b %Y")
            # td_sit["weekday"] = td_sit["sitting_date"].dt.day_name()
            # st.dataframe(
            #     td_sit[["date", "weekday", "year"]].rename(columns={
            #         "date":    "Date",
            #         "weekday": "Day",
            #         "year":    "Year",
            #     }),
            #     hide_index=True,
            #     use_container_width=True,
            # )
            # ── END ORIGINAL ──────────────────────────────────────────────────

            # ── NEW: calendar heatmap ─────────────────────────────────────────
            st.subheader("Plenary attendance calendar")
            st.caption("Blue = plenary sitting day attended. Grey = not recorded. Blank = future.")
            td_sit_dates = td_raw[td_raw["sitting_date"].notna()]["sitting_date"]
            for yr in sorted(td_yearly["year"].unique()):
                yr_dates = td_sit_dates[td_raw["year"] == yr]
                st.plotly_chart(
                    _calendar_heatmap(yr_dates, int(yr)),
                    use_container_width=True,
                    config={"displayModeBar": False},
                )
            # ── END NEW ───────────────────────────────────────────────────────

            st.download_button(
                f"Download {td_name} CSV",
                td_yearly.to_csv(index=False),
                file_name=f"{td_name.replace(' ', '_')}_attendance.csv",
                mime="text/csv",
            )
