import streamlit as st
import pandas as pd
from datetime import date

st.set_page_config(page_title="TD Attendance", layout="wide")
st.title("📅 TD Attendance Tracker")
st.caption("Dáil sitting day and other day attendance — 2024 to 2026")

# ── Load ─────────────────────────────────────────────────────────────────────
@st.cache_data
def load():
    df = pd.read_csv(
        "C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\data\\silver\\aggregated_td_tables.csv",
        low_memory=False
    )
    df["sitting_date"] = pd.to_datetime(df["iso_sitting_days_attendance"], errors="coerce")
    df["other_date"]   = pd.to_datetime(df["iso_other_days_attendance"],   errors="coerce")
    df["full_name"]    = df["first_name"] + " " + df["last_name"]
    return df

df = load()

min_date = df["sitting_date"].min().date()
max_date = df["sitting_date"].max().date()

# ── Sidebar controls ──────────────────────────────────────────────────────────
st.sidebar.header("Controls")

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# TD multi-select
all_names = sorted(df["full_name"].unique())
selected_tds = st.sidebar.multiselect(
    "Filter to specific TDs (leave blank for all)",
    all_names
)

view_mode = st.sidebar.radio(
    "View mode",
    ["📊 Rankings", "📈 Timeline", "🔍 Individual TD"],
    index=0
)

# ── Filter ────────────────────────────────────────────────────────────────────
mask = (
    (df["sitting_date"].dt.date >= start_date) &
    (df["sitting_date"].dt.date <= end_date)
)
filtered = df[mask]

if selected_tds:
    filtered = filtered[filtered["full_name"].isin(selected_tds)]

# ── Summary stats per TD (within date window) ─────────────────────────────────
@st.cache_data
def summarise(df_hash):
    return (
        df_hash.groupby(["identifier", "full_name"])
        .agg(
            sitting_attended=("sitting_date", "count"),
            sitting_total   =("sitting_total_days", "first"),
            other_attended  =("other_days_count",   "first"),
        )
        .reset_index()
    )

summary = summarise(filtered)
summary["attendance_rate"] = (
    summary["sitting_attended"] / summary["sitting_total"].replace(0, pd.NA) * 100
).round(1)
summary = summary.sort_values("attendance_rate", ascending=False).reset_index(drop=True)

# ── Top-level metrics ─────────────────────────────────────────────────────────
total_tds    = summary["full_name"].nunique()
avg_rate     = summary["attendance_rate"].mean()
top_attender = summary.iloc[0]["full_name"] if len(summary) else "—"
low_attender = summary.iloc[-1]["full_name"] if len(summary) else "—"

c1, c2, c3, c4 = st.columns(4)
c1.metric("TDs in view",       total_tds)
c2.metric("Avg attendance",    f"{avg_rate:.1f}%")
c3.metric("Top attender",      top_attender)
c4.metric("Lowest attender",   low_attender)

st.divider()

# ════════════════════════════════════════════════════════════════════════════
# VIEW: RANKINGS
# ════════════════════════════════════════════════════════════════════════════
if view_mode == "📊 Rankings":
    st.subheader("Attendance rankings")

    col_l, col_r = st.columns([2, 1])

    with col_l:
        st.dataframe(
            summary[["full_name", "sitting_attended", "sitting_total", "attendance_rate"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "full_name":        st.column_config.TextColumn("TD"),
                "sitting_attended": st.column_config.NumberColumn("Days attended", format="%d"),
                "sitting_total":    st.column_config.NumberColumn("Total sitting days", format="%d"),
                "attendance_rate":  st.column_config.ProgressColumn(
                    "Attendance %",
                    format="%.1f%%",
                    min_value=0,
                    max_value=100,
                ),
            }
        )

    with col_r:
        st.subheader("Distribution")
        bins = pd.cut(
            summary["attendance_rate"].dropna(),
            bins=[0, 25, 50, 75, 90, 100],
            labels=["0-25%", "25-50%", "50-75%", "75-90%", "90-100%"]
        ).value_counts().sort_index()
        st.bar_chart(bins)

    st.download_button(
        "⬇️ Export rankings CSV",
        summary.to_csv(index=False),
        file_name=f"td_attendance_{start_date}_to_{end_date}.csv",
        mime="text/csv"
    )

# ════════════════════════════════════════════════════════════════════════════
# VIEW: TIMELINE
# ════════════════════════════════════════════════════════════════════════════
elif view_mode == "📈 Timeline":
    st.subheader("Attendance over time")

    if not selected_tds:
        st.info("Select specific TDs in the sidebar to compare timelines — showing top 5 by attendance rate instead.")
        top5 = summary.head(5)["full_name"].tolist()
        timeline_df = filtered[filtered["full_name"].isin(top5)]
    else:
        timeline_df = filtered

    # Daily attendance count across all selected TDs
    daily = (
        timeline_df.groupby("sitting_date")["full_name"]
        .count()
        .reset_index()
        .rename(columns={"full_name": "TDs attending"})
        .set_index("sitting_date")
    )

    st.subheader("Daily sitting attendance (all selected TDs)")
    st.line_chart(daily)

    # Per-TD monthly attendance
    st.subheader("Monthly attendance per TD")
    timeline_df = timeline_df.copy()
    timeline_df["month"] = timeline_df["sitting_date"].dt.to_period("M").astype(str)
    monthly = (
        timeline_df.groupby(["month", "full_name"])["sitting_date"]
        .count()
        .reset_index()
        .rename(columns={"sitting_date": "days"})
        .pivot(index="month", columns="full_name", values="days")
        .fillna(0)
    )
    st.line_chart(monthly)

# ════════════════════════════════════════════════════════════════════════════
# VIEW: INDIVIDUAL TD
# ════════════════════════════════════════════════════════════════════════════
elif view_mode == "🔍 Individual TD":
    st.subheader("Individual TD deep-dive")

    td_name = st.selectbox("Select TD", all_names)
    td_df   = filtered[filtered["full_name"] == td_name].copy()

    if td_df.empty:
        st.warning("No data for this TD in the selected date range.")
    else:
        row = summary[summary["full_name"] == td_name]
        rate = row["attendance_rate"].values[0] if len(row) else 0
        attended = row["sitting_attended"].values[0] if len(row) else 0
        total    = row["sitting_total"].values[0] if len(row) else 0

        m1, m2, m3 = st.columns(3)
        m1.metric("Sitting days attended", int(attended))
        m2.metric("Total sitting days",    int(total))
        m3.metric("Attendance rate",       f"{rate:.1f}%")

        # Calendar-style daily view
        st.subheader("Sitting days attended")
        td_df = td_df.sort_values("sitting_date")
        td_df["week"]    = td_df["sitting_date"].dt.isocalendar().week.astype(str)
        td_df["weekday"] = td_df["sitting_date"].dt.day_name()
        td_df["label"]   = td_df["sitting_date"].dt.strftime("%d %b %Y")

        st.dataframe(
            td_df[["label", "weekday", "sitting_days_count", "other_days_count"]]
            .rename(columns={
                "label":             "Date",
                "weekday":           "Day",
                "sitting_days_count":"Sitting days (total)",
                "other_days_count":  "Other days (total)"
            }),
            hide_index=True,
            use_container_width=True,
        )

        # Monthly rollup line chart
        st.subheader("Monthly attendance")
        td_df["month"] = td_df["sitting_date"].dt.to_period("M").astype(str)
        monthly_td = td_df.groupby("month")["sitting_date"].count().rename("Days attended")
        st.line_chart(monthly_td)

        st.download_button(
            f"⬇️ Export {td_name} CSV",
            td_df.to_csv(index=False),
            file_name=f"{td_name.replace(' ','_')}_attendance.csv",
            mime="text/csv"
        )
