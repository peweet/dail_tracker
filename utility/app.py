import streamlit as st

from pages_code.interests import interests_page
from pages_code.committees import committees_page

st.set_page_config(
    page_title="Oireachtas Explorer",
    page_icon="🏛️",
    layout="wide",
)

pg = st.navigation([
    st.Page(interests_page, title="Interests", icon="💼", default=True),
    st.Page(committees_page, title="Committees", icon="🏛️"),
])
pg.run()