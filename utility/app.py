import streamlit as st
from pages_code.attendance import attendance_page
from pages_code.committees import committees_page
from pages_code.interests import interests_page
# from pages_code.legislation import legislation_page
from pages_code.lobbying_2 import lobbying_page
from pages_code.payments import payments_page
from pages_code.votes import votes_page

st.set_page_config(
    page_title="Oireachtas Explorer",
    page_icon="🏛️",
    layout="wide",
)

pg = st.navigation(
    [
        st.Page(interests_page, title="Interests", icon="💼", default=True),
        st.Page(committees_page, title="Committees", icon="🏛️"),
        st.Page(attendance_page, title="Attendance", icon="📅"),
        st.Page(payments_page, title="Payments", icon="💶"),
        st.Page(lobbying_page, title="Lobbying", icon="📋"),
        # st.Page(legislation_page, title="Legislation", icon="📜"),
        st.Page(votes_page, title="Votes", icon="🗳️"),
    ]
)
pg.run()