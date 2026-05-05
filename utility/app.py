import streamlit as st
from pages_code.attendance import attendance_page
from pages_code.committees import committees_page
from pages_code.interests import interests_page
from pages_code.legislation import legislation_page
from pages_code.legislation_poc import legislation_poc_page
from pages_code.legislation_si_poc import statutory_instruments_page
from pages_code.lobbying_2 import lobbying_page
from pages_code.lobbyist_poc import lobbyist_poc_page
from pages_code.member_overview import member_overview_page
from pages_code.payments import payments_page
from pages_code.votes import votes_page

st.set_page_config(
    page_title="Oireachtas Explorer",
    page_icon=":material/account_balance:",
    layout="wide",
)

# url_path is pinned explicitly so cross-page <a href> links don't break if a
# title is renamed. Slugs must match utility/ui/entity_links.PAGES.
pg = st.navigation(
    [
        st.Page(attendance_page,       title="Attendance",      icon=":material/calendar_today:", url_path="attendance",      default=True),
        st.Page(member_overview_page,  title="Member Overview", icon=":material/person:",         url_path="member-overview"),
        st.Page(votes_page,            title="Votes",           icon=":material/how_to_vote:",    url_path="votes"),
        st.Page(interests_page,        title="Interests",       icon=":material/interests:",      url_path="interests"),
        st.Page(payments_page,         title="Payments",        icon=":material/payments:",       url_path="payments"),
        st.Page(lobbying_page,         title="Lobbying",        icon=":material/groups:",         url_path="lobbying"),
        st.Page(lobbyist_poc_page,     title="Lobbyist (POC)",  icon=":material/integration_instructions:", url_path="lobbyist-poc"),
        st.Page(legislation_page,      title="Legislation",     icon=":material/gavel:",          url_path="legislation"),
        st.Page(legislation_poc_page,  title="Legislation (POC)", icon=":material/integration_instructions:", url_path="legislation-poc"),
        st.Page(statutory_instruments_page, title="SI (POC)",    icon=":material/article:",        url_path="si-poc"),
        st.Page(committees_page,       title="Committees",      icon=":material/account_balance:",url_path="committees"),
    ]
)
pg.run()
