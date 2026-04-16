

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Oireachtas Interests Register", layout="wide")

st.title("🏛️ Oireachtas Interests Register")

@st.cache_data
def load():
    return pd.read_csv(
         "C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\member_interests_combined.csv",
        low_memory=False
    )

df = load()
df.columns = df.columns.str.strip()

# ── Suggestions ──────────────────────────────────────────────────────────────
suggestions = [
    "Mary Lou McDonald", "Michael Healy-Rae", "Micheál Martin",
    "Simon Harris", "Gillian Toole", "Michael Lowry",
]

st.write("**Quick select:**")
cols = st.columns(len(suggestions))
for i, name in enumerate(suggestions):
    if cols[i].button(name, use_container_width=True):
        st.session_state["search"] = name

# ── Search bar ───────────────────────────────────────────────────────────────
search = st.text_input(
    "🔍 Search member name",
    placeholder="e.g. Mary Lou McDonald",
    value=st.session_state.get("search", ""),
    key="search",
)

if not search:
    st.info("Type a name above to get started.")
    st.stop()

matches = df[df["full_name"].str.contains(search, case=False, na=False)]["full_name"].unique()

if len(matches) == 0:
    st.warning(f"No members found matching **{search}**")
    st.stop()

if len(matches) > 1:
    selected_name = st.selectbox("Select member", sorted(matches))
else:
    selected_name = matches[0]

member_df = df[df["full_name"] == selected_name]
info = member_df.iloc[0]

# ── Member header ─────────────────────────────────────────────────────────────
st.subheader(selected_name)

# Compute landlord/property across ALL rows — true if any row is true
is_landlord   = member_df["is_landlord"].any()
is_prop_owner = member_df["is_property_owner"].any()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Party",          info.get("party", "—"))
c2.metric("Constituency",   info.get("constituency", "—"))
c3.metric("Year elected",   int(info["year_elected"]) if pd.notna(info.get("year_elected")) else "—")
c4.metric("Landlord",       "✅ Yes" if is_landlord   else "❌ No")
c5.metric("Property owner", "✅ Yes" if is_prop_owner else "❌ No")

st.divider()

# ── Year filter ───────────────────────────────────────────────────────────────
years = sorted(member_df["year_declared"].dropna().unique(), reverse=True)
selected_year = st.radio("Year declared", years, horizontal=True)

year_df = member_df[member_df["year_declared"] == selected_year]

# ── Interest categories as expandable sections ────────────────────────────────
st.subheader(f"Declared interests — {int(selected_year)}")

categories = sorted(year_df["interest_category"].dropna().unique())

for cat in categories:
    cat_df = year_df[year_df["interest_category"] == cat][["interest_description_cleaned"]].copy()
    cat_df = cat_df[cat_df["interest_description_cleaned"] != "No interests declared"]

    has_interests = len(cat_df) > 0
    label = f"{'🔴' if has_interests else '⚪'} {cat}"

    with st.expander(label, expanded=has_interests):
        if has_interests:
            for _, row in cat_df.iterrows():
                st.markdown(f"- {row['interest_description_cleaned']}")
        else:
            st.caption("No interests declared")

st.divider()

export_df = year_df[[
    "full_name", "party", "constituency",
    "interest_category", "interest_description_cleaned",
    "is_landlord", "is_property_owner",
    "ministerial_office_filled", "year_elected", "year_declared"
]].copy()

st.download_button(
    label="⬇️ Export as CSV",
    data=export_df.to_csv(index=False),
    file_name=f"{selected_name.replace(' ', '_')}_{int(selected_year)}_interests.csv",
    mime="text/csv",
)
