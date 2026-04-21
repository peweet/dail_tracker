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
            val = np.nan
        elif d in attended_set:
            val = 1
        else:
            val = 0
        records.append({"date": d, "week": week, "weekday": weekday, "val": val,
                         "label": d.strftime("%d %b %Y")})

    df_cal     = pd.DataFrame(records)
    pivot_z    = df_cal.pivot_table(index="weekday", columns="week", values="val",   aggfunc="first")
    pivot_text = df_cal.pivot_table(index="weekday", columns="week", values="label", aggfunc="first")

    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    month_df   = df_cal.groupby(df_cal["date"].dt.month)["week"].min()
    month_text = [pd.Timestamp(f"{year}-{m:02d}-01").strftime("%b") for m in month_df.index]

    fig = go.Figure(go.Heatmap(
        z=pivot_z.values,
        x=pivot_z.columns.tolist(),
        y=[day_labels[i] for i in pivot_z.index],
        text=pivot_text.values,
        colorscale=[[0, "#e5e7eb"], [1, "oklch(51% 0.130 62)"]],
        zmin=0, zmax=1,
        showscale=False,
        hovertemplate="%{text}<extra></extra>",
        xgap=3, ygap=3,
    ))
    fig.update_layout(
        title=dict(text=str(year), font=dict(size=13, family="Zilla Slab, Georgia, serif")),
        height=165,
        margin=dict(l=40, r=10, t=30, b=30),
        xaxis=dict(tickvals=month_df.tolist(), ticktext=month_text, showgrid=False, zeroline=False),
        yaxis=dict(autorange="reversed", showgrid=False, zeroline=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig


def _yearly_summary(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["full_name", "year"], sort=True)
        .agg(
            sitting_days=("sitting_days_count", "first"),
            other_days  =("other_days_count",   "first"),
        )
        .reset_index()
        .sort_values(["full_name", "year"])
    )


def _totals(yearly: pd.DataFrame) -> pd.DataFrame:
    t = (
        yearly.groupby("full_name")[["sitting_days", "other_days"]]
        .sum()
        .reset_index()
    )
    t["total_days"] = t["sitting_days"] + t["other_days"]
    return t


def _rank_table(subset: pd.DataFrame, max_days: int, ascending: bool) -> None:
    display = (
        subset[["full_name", "sitting_days", "other_days", "total_days"]]
        .sort_values("sitting_days", ascending=ascending)
        .reset_index(drop=True)
    )
    display.index = display.index + 1
    st.dataframe(
        display,
        use_container_width=True,
        column_config={
            "full_name":    st.column_config.TextColumn("TD"),
            "sitting_days": st.column_config.ProgressColumn(
                "Plenary days", min_value=0, max_value=max_days, format="%d"
            ),
            "other_days":   st.column_config.NumberColumn("Other days", format="%d"),
            "total_days":   st.column_config.NumberColumn("Total", format="%d"),
        },
    )


def attendance_page() -> None:
    inject_css()

    st.markdown(
        """
        <div class="page-kicker">Oireachtas</div>
        <div class="page-title">TD Attendance</div>
        <div class="page-subtitle">
          Plenary sitting days and other parliamentary activity, 2024–2026.
          Filter by year or TD in the sidebar. &mdash;
          <a href="https://www.oireachtas.ie/en/members/salaries-and-allowances/parliamentary-standard-allowances/"
             target="_blank" style="color:var(--accent);text-decoration:none;">
            Parliamentary Standard Allowances &rarr;
          </a>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("About these figures", expanded=False):
        st.markdown(_DISCLAIMER)

    df        = _load()
    all_names = sorted(df["full_name"].unique())
    all_years = sorted(df["year"].dropna().unique().astype(int))

    # ── Sidebar ───────────────────────────────────────────────────────────────
    st.sidebar.markdown('<div class="sidebar-label">Filters</div>', unsafe_allow_html=True)

    selected_years = st.sidebar.multiselect("Year", all_years, default=all_years)
    selected_tds   = st.sidebar.multiselect(
        "Filter to specific TDs (leave blank for all)", all_names
    )
    view_mode = st.sidebar.radio(
        "View", ["Rankings", "Table", "Timeline", "Individual TD"], index=0
    )

    # ── Filter ────────────────────────────────────────────────────────────────
    filtered = df[df["year"].isin(selected_years)]
    if selected_tds:
        filtered = filtered[filtered["full_name"].isin(selected_tds)]

    yearly  = _yearly_summary(filtered)
    totals  = _totals(yearly)

    if totals.empty:
        st.warning("No data for the current selection.")
        return

    max_sit   = int(totals["sitting_days"].max())
    best_row  = totals.loc[totals["sitting_days"].idxmax()]
    worst_row = totals.loc[totals["sitting_days"].idxmin()]
    avg_sit   = totals["sitting_days"].mean()
    avg_other = totals["other_days"].mean()
    n_tds     = totals["full_name"].nunique()
    pct_low   = (totals["sitting_days"] < 50).mean() * 100

    # ── Stat strip ────────────────────────────────────────────────────────────
    st.markdown(
        f"""
        <div class="stat-strip">
          <div>
            <div class="stat-num">{n_tds}</div>
            <div class="stat-lbl">TDs in view</div>
          </div>
          <div>
            <div class="stat-num">{avg_sit:.0f}</div>
            <div class="stat-lbl">Avg plenary days</div>
          </div>
          <div>
            <div class="stat-num">{avg_other:.0f}</div>
            <div class="stat-lbl">Avg other days</div>
          </div>
          <div>
            <div class="stat-num">{int(best_row['sitting_days'])}</div>
            <div class="stat-lbl">Most present &mdash; {best_row['full_name']}</div>
          </div>
          <div>
            <div class="stat-num">{int(worst_row['sitting_days'])}</div>
            <div class="stat-lbl">Least present &mdash; {worst_row['full_name']}</div>
          </div>
          <div>
            <div class="stat-num">{pct_low:.0f}%</div>
            <div class="stat-lbl">TDs under 50 plenary days</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Rankings view ─────────────────────────────────────────────────────────
    if view_mode == "Rankings":
        st.markdown('<div class="section-heading">Most present</div>', unsafe_allow_html=True)
        st.caption("Top 20 TDs by total plenary sitting days in the selected period.")
        _rank_table(totals.nlargest(20, "sitting_days"), max_sit, ascending=False)

        st.markdown('<div class="section-heading">Least present</div>', unsafe_allow_html=True)
        st.caption("Bottom 20 TDs by total plenary sitting days in the selected period.")
        _rank_table(totals.nsmallest(20, "sitting_days"), max_sit, ascending=True)

        st.download_button(
            "Download full CSV",
            totals.sort_values("sitting_days", ascending=False).to_csv(index=False),
            file_name="td_attendance_ranked.csv",
            mime="text/csv",
        )

    # ── Table view ────────────────────────────────────────────────────────────
    elif view_mode == "Table":
        st.markdown('<div class="section-heading">Attendance by TD and year</div>', unsafe_allow_html=True)
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
                    "sitting_days": st.column_config.ProgressColumn(
                        "Plenary sitting days", min_value=0, max_value=max_sit, format="%d"
                    ),
                    "other_days":   st.column_config.NumberColumn("Other days", format="%d"),
                },
            )
        else:
            pivot = yearly.pivot_table(
                index="full_name",
                columns="year",
                values=["sitting_days", "other_days"],
                fill_value=0,
            )
            pivot.columns = [f"{col[0].replace('_', ' ').title()} {col[1]}" for col in pivot.columns]
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

    # ── Timeline view ─────────────────────────────────────────────────────────
    elif view_mode == "Timeline":
        st.markdown('<div class="section-heading">Plenary sitting days per year</div>', unsafe_allow_html=True)

        if not selected_tds:
            st.info("Showing all TDs aggregated. Select specific TDs in the sidebar to compare individuals.")
            agg = yearly.groupby("year")[["sitting_days", "other_days"]].sum()
            st.bar_chart(agg)
        else:
            pivot = yearly.pivot_table(
                index="year", columns="full_name", values="sitting_days", fill_value=0
            )
            st.caption("Plenary sitting days")
            st.bar_chart(pivot)

            pivot_other = yearly.pivot_table(
                index="year", columns="full_name", values="other_days", fill_value=0
            )
            st.caption("Other days")
            st.bar_chart(pivot_other)

    # ── Individual TD view ────────────────────────────────────────────────────
    elif view_mode == "Individual TD":
        st.markdown('<div class="section-heading">Individual TD</div>', unsafe_allow_html=True)
        td_name   = st.selectbox("Select TD", all_names)
        td_yearly = yearly[yearly["full_name"] == td_name]
        td_raw    = filtered[filtered["full_name"] == td_name].copy()
        if td_yearly.empty:
            st.warning("No data for this TD in the selected years.")
            return

        total_sit   = int(td_yearly["sitting_days"].sum())
        total_other = int(td_yearly["other_days"].sum())
        overall_rank = totals["sitting_days"].rank(ascending=False, method="min")
        rank_val = int(overall_rank[totals["full_name"] == td_name].values[0])

        st.markdown(
            f"""
            <div class="td-name">{td_name}</div>
            <div class="td-meta">
              <span class="signal signal-accent">{total_sit} plenary days</span>
              <span class="signal signal-neutral">{total_other} other days</span>
              <span class="signal signal-dark">Rank #{rank_val} of {n_tds} TDs</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="section-heading">Year-by-year breakdown</div>', unsafe_allow_html=True)
        st.dataframe(
            td_yearly[["year", "sitting_days", "other_days"]].rename(columns={
                "year":         "Year",
                "sitting_days": "Plenary sitting days",
                "other_days":   "Other days",
            }),
            hide_index=True,
            use_container_width=True,
        )

        st.markdown('<div class="section-heading">Plenary attendance calendar</div>', unsafe_allow_html=True)
        st.caption("Amber = plenary sitting day attended. Grey = not recorded. Blank = future.")
        td_sit_dates = td_raw[td_raw["sitting_date"].notna()]["sitting_date"]
        for yr in sorted(td_yearly["year"].unique()):
            yr_dates = td_sit_dates[td_raw["year"] == yr]
            st.plotly_chart(
                _calendar_heatmap(yr_dates, int(yr)),
                use_container_width=True,
                config={"displayModeBar": False},
            )

        st.download_button(
            f"Download {td_name} CSV",
            td_yearly.to_csv(index=False),
            file_name=f"{td_name.replace(' ', '_')}_attendance.csv",
            mime="text/csv",
        )
