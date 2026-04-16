
import streamlit as st
import pandas as pd
import re
#CHATGPT WRITTEN - NOT TESTED - MAY CONTAIN BUGS
#DOES NOT WORK YET - NEEDS ADJUSTING TO FIT THE STRUCTURE OF THE COMMITTEE DATA
#check flattened_members.csv to see how the committee data is structured, and adjust the code below to dynamically detect the committee columns and extract the relevant information (e.g. committee name, role, start date, end date) for each committee a member has served on. The current code assumes a fixed structure which may not match the actual data.
st.set_page_config(layout="wide")

# =========================
# LOAD + TRANSFORM + CLEAN
# =========================
@st.cache_data
def load_and_transform(path):
    df = pd.read_csv(path)

    records = []

    # detect committee indices dynamically
    committee_cols = [col for col in df.columns if "committee_" in col]
    indices = sorted(set(re.findall(r'committee_(\d+)_', " ".join(committee_cols))))

    for _, row in df.iterrows():
        base = {
            "name": (
                str(row.get("first_name", "")) + " " +
                str(row.get("last_name", ""))
            ).strip(),
            "party": row.get("party"),
        }

        for i in indices:
            name = row.get(f"committee_{i}_name_english")
            role = row.get(f"committee_{i}_role_title_copilot")
            start = row.get(f"committee_{i}_role_start_date_copilot")
            end = row.get(f"committee_{i}_role_end_date_copilot")

            # Only keep real committee entries
            if pd.notna(name):
                records.append({
                    **base,
                    "committee": name,
                    "role": role,
                    "start": pd.to_datetime(start, errors="coerce"),
                    "end": pd.to_datetime(end, errors="coerce"),
                })

    df_long = pd.DataFrame(records)

    # =========================
    # CLEANING
    # =========================
    df_long = df_long.dropna(subset=["name", "committee"])

    df_long["role"] = df_long["role"].fillna("Member")
    df_long["party"] = df_long["party"].fillna("Unknown")

    # Derived status column
    df_long["status"] = df_long["end"].isna().map({
        True: "Active",
        False: "Ended"
    })

    return df_long


df = load_and_transform("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\flattened_members.csv")


# =========================
# SIDEBAR NAVIGATION
# =========================
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Overview", "Committee Explorer", "Politician Profile"]
)

# =========================
# OVERVIEW
# =========================
if page == "Overview":
    st.title("📊 Committee Activity Overview")

    col1, col2, col3 = st.columns(3)

    col1.metric("Politicians", df["name"].nunique())
    col2.metric("Committees", df["committee"].nunique())
    col3.metric("Active Roles", (df["status"] == "Active").sum())

    st.divider()

    st.subheader("Committee Distribution")
    st.bar_chart(df["committee"].value_counts())

    st.subheader("Party Distribution")
    st.bar_chart(df["party"].value_counts())

    st.subheader("Status Breakdown")
    st.bar_chart(df["status"].value_counts())


# =========================
# COMMITTEE EXPLORER
# =========================
elif page == "Committee Explorer":
    st.title("🔍 Committee Explorer")

    col1, col2, col3 = st.columns(3)

    with col1:
        party_filter = st.multiselect(
            "Party",
            sorted(df["party"].unique())
        )

    with col2:
        committee_filter = st.multiselect(
            "Committee",
            sorted(df["committee"].unique())
        )

    with col3:
        status_filter = st.selectbox(
            "Status",
            ["All", "Active", "Ended"]
        )

    search = st.text_input("Search politician")

    filtered = df.copy()

    if party_filter:
        filtered = filtered[filtered["party"].isin(party_filter)]

    if committee_filter:
        filtered = filtered[filtered["committee"].isin(committee_filter)]

    if status_filter != "All":
        filtered = filtered[filtered["status"] == status_filter]

    if search:
        filtered = filtered[
            filtered["name"].str.contains(search, case=False, na=False)
        ]

    st.dataframe(
        filtered.sort_values("start", ascending=False),
        use_container_width=True,
        column_config={
            "name": st.column_config.TextColumn("Politician", width="medium"),
            "party": st.column_config.TextColumn("Party"),
            "committee": st.column_config.TextColumn("Committee", width="large"),
            "role": st.column_config.TextColumn("Role"),
            "status": st.column_config.TextColumn("Status"),
            "start": st.column_config.DateColumn("Start Date"),
            "end": st.column_config.DateColumn("End Date"),
        },
        hide_index=True,
    )


# =========================
# POLITICIAN PROFILE
# =========================
elif page == "Politician Profile":
    st.title("👤 Politician Profile")

    names = sorted(df["name"].unique())
    selected = st.selectbox("Select Politician", names)

    person = df[df["name"] == selected]

    st.subheader(selected)

    col1, col2 = st.columns(2)
    col1.metric("Committees Served", person["committee"].nunique())
    col2.metric("Active Roles", (person["status"] == "Active").sum())

    st.divider()

    # Tabs for better UX
    tab1, tab2 = st.tabs(["Active Roles", "Past Roles"])

    with tab1:
        active = person[person["status"] == "Active"]
        st.dataframe(
            active.sort_values("start", ascending=False),
            use_container_width=True,
            column_config={
                "committee": st.column_config.TextColumn("Committee", width="large"),
                "role": st.column_config.TextColumn("Role"),
                "start": st.column_config.DateColumn("Start"),
            },
            hide_index=True,
        )

    with tab2:
        past = person[person["status"] == "Ended"]
        st.dataframe(
            past.sort_values("start", ascending=False),
            use_container_width=True,
            column_config={
                "committee": st.column_config.TextColumn("Committee", width="large"),
                "role": st.column_config.TextColumn("Role"),
                "start": st.column_config.DateColumn("Start"),
                "end": st.column_config.DateColumn("End"),
            },
            hide_index=True,
        )