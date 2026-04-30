import streamlit as st
from pages_code.attendance import attendance_page
from pages_code.committees import committees_page
from pages_code.interests import interests_page
from pages_code.legislation import legislation_page
from pages_code.lobbying_2 import lobbying_page
from pages_code.member_overview import member_overview_page
from pages_code.payments import payments_page
from pages_code.votes import votes_page

st.set_page_config(
    page_title="Oireachtas Explorer",
    page_icon=":material/account_balance:",
    layout="wide",
)

pg = st.navigation(
    [
        st.Page(attendance_page, title="Attendance", icon=":material/calendar_today:", default=True),
        st.Page(member_overview_page, title="Member Overview", icon=":material/person:"),
        st.Page(votes_page, title="Votes", icon=":material/how_to_vote:"),
        st.Page(interests_page, title="Interests", icon=":material/interests:"),
        st.Page(payments_page, title="Payments", icon=":material/payments:"),
        st.Page(lobbying_page, title="Lobbying", icon=":material/groups:"),
        st.Page(legislation_page, title="Legislation", icon=":material/gavel:"),
        st.Page(committees_page, title="Committees", icon=":material/account_balance:"),
    ]
)
pg.run()
