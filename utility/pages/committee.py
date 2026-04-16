import streamlit as st
import pandas as pd
import re
import altair as alt

st.set_page_config(layout="wide")

# =========================
# LOAD + TRANSFORM
# =========================
@st.cache_data
def load_and_transform(path):
    df = pd.read_csv(r"C:\Users\pglyn\PycharmProjects\dail_extractor\data\silver\flattened_members.csv")

    def is_empty(val):
        return (
            pd.isna(val)
            or str(val).strip() in ["", "[]", "null", "None"]
        )

    # -------------------------
    # GROUP COMMITTEE COLUMNS
    # -------------------------
    committee_groups = {}

    for col in df.columns:
        match = re.match(r'(committee_\d+)_', col)
        if match:
            prefix = match.group(1)
            committee_groups.setdefault(prefix, []).append(col)

    records = []

    for _, row in df.iterrows():
        base = {
            "name": f"{row.get('first_name','')} {row.get('last_name','')}".strip(),
            "party": row.get("party"),
        }

        for prefix in committee_groups.keys():

            # ✅ EXPLICIT COLUMN TARGETING (FIXES YOUR ISSUE)
            name = row.get(f"{prefix}_name_en")
            role = row.get(f"{prefix}_role_title")

            # prefer role dates (more accurate)
            start = row.get(f"{prefix}_role_start_date")
            end = row.get(f"{prefix}_role_end_date")

            # fallback if missing
            if is_empty(start):
                start = row.get(f"{prefix}_member_start_date")

            if is_empty(end):
                end = row.get(f"{prefix}_member_end_date")

            # 🚨 KEY FILTER
            if is_empty(name):
                continue

            records.append({
                **base,
                "committee": name,
                "role": role if not is_empty(role) else "Member",
                "start": pd.to_datetime(start, errors="coerce"),
                "end": pd.to_datetime(end, errors="coerce"),
            })

    if not records:
        st.error("❌ No committee data extracted — check CSV path")
        st.stop()

    df_long = pd.DataFrame(records)

    # -------------------------
    # CLEANING
    # -------------------------
    df_long["party"] = df_long["party"].fillna("Unknown")

    df_long["status"] = df_long["end"].isna().map({
        True: "Active",
        False: "Ended"
    })

    df_long["end_filled"] = df_long["end"].fillna(pd.Timestamp.today())

    return df_long


df = load_and_transform("flattened_members.csv")

# =========================
# SIDEBAR
# =========================
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Overview", "Explorer", "Profile"]
)

# =========================
# OVERVIEW
# =========================
if page == "Overview":
    st.title("📊 Committee Overview")

    col1, col2, col3 = st.columns(3)
    col1.metric("Politicians", df["name"].nunique())
    col2.metric("Committees", df["committee"].nunique())
    col3.metric("Active Roles", (df["status"] == "Active").sum())

    st.divider()

    st.subheader("Top Committees")
    st.bar_chart(df["committee"].value_counts().head(15))

    st.subheader("Party Breakdown")
    st.bar_chart(df["party"].value_counts())

    st.subheader("Status")
    st.bar_chart(df["status"].value_counts())


# =========================
# EXPLORER
# =========================
elif page == "Explorer":
    st.title("🔍 Committee Explorer")

    party = st.multiselect("Party", sorted(df["party"].unique()))
    committee = st.multiselect("Committee", sorted(df["committee"].unique()))
    status = st.selectbox("Status", ["All", "Active", "Ended"])
    search = st.text_input("Search politician")

    filtered = df.copy()

    if party:
        filtered = filtered[filtered["party"].isin(party)]

    if committee:
        filtered = filtered[filtered["committee"].isin(committee)]

    if status != "All":
        filtered = filtered[filtered["status"] == status]

    if search:
        filtered = filtered[
            filtered["name"].str.contains(search, case=False, na=False)
        ]

    st.dataframe(
        filtered.sort_values("start", ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "name": st.column_config.TextColumn("Politician"),
            "committee": st.column_config.TextColumn("Committee", width="large"),
            "role": st.column_config.TextColumn("Role"),
            "start": st.column_config.DateColumn("Start"),
            "end": st.column_config.DateColumn("End"),
        }
    )


# =========================
# PROFILE + GANTT
# =========================
elif page == "Profile":
    st.title("👤 Politician Profile")

    selected = st.selectbox("Select Politician", sorted(df["name"].unique()))
    person = df[df["name"] == selected]

    st.subheader(selected)

    col1, col2 = st.columns(2)
    col1.metric("Committees", person["committee"].nunique())
    col2.metric("Active Roles", (person["status"] == "Active").sum())

    st.divider()

    # ✅ GANTT (works great in Streamlit)
    st.subheader("📅 Timeline")

    chart = alt.Chart(person).mark_bar().encode(
        x="start:T",
        x2="end_filled:T",
        y=alt.Y("committee:N", sort="-x"),
        color="status:N",
        tooltip=["committee", "role", "start", "end"]
    ).properties(height=400)

    st.altair_chart(chart, use_container_width=True)

    st.dataframe(person, use_container_width=True, hide_index=True)