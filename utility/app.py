import streamlit as st
from pages_code.attendance import attendance_page
from pages_code.committees import committees_page
from pages_code.glossary import glossary_page
from pages_code.interests import interests_page
from pages_code.legislation import legislation_page
from pages_code.lobbying_2 import lobbying_page
from pages_code.lobbying_3 import lobbying_poc_page
from pages_code.member_overview import member_overview_page
from pages_code.payments import payments_page
from pages_code.statutory_instruments import statutory_instruments_page
from pages_code.votes import votes_page

st.set_page_config(
    page_title="Oireachtas Explorer",
    page_icon=":material/account_balance:",
    layout="wide",
)

# url_path is pinned explicitly so cross-page <a href> links don't break if a
# title is renamed. Slugs must match utility/ui/entity_links.PAGES.
#
# /member-overview is the canonical TD page (TheyWorkForYou pattern). The
# dimension pages get a `rankings-` prefix — they are discovery / league
# tables that funnel into the canonical profile. Hyphens not slashes:
# st.Page rejects nested url_path values.
# Sidebar audit 2026-05-27 P0-1: position="hidden" — the per-page
# sidebar content used to sit ~440px below the page-nav fold on a
# default 1440x900 viewport, hiding member pickers / chips / year
# filters from any user who didn't scroll the sidebar. Custom
# horizontal nav strip now lives in shared_css.py's inject_css() and
# carries the cross-page navigation in the dark banner row. Streamlit
# still resolves the URL slug via st.Page; this just suppresses
# Streamlit's own nav widget so the sidebar is 100% per-page content.
pg = st.navigation(
    [
        st.Page(
            member_overview_page,
            title="Member Overview",
            icon=":material/person:",
            url_path="member-overview",
            default=True,
        ),
        st.Page(
            attendance_page,
            title="Attendance",
            icon=":material/calendar_today:",
            url_path="rankings-attendance",
        ),
        st.Page(votes_page, title="Votes", icon=":material/how_to_vote:", url_path="rankings-votes"),
        st.Page(interests_page, title="Interests", icon=":material/interests:", url_path="rankings-interests"),
        st.Page(payments_page, title="Payments", icon=":material/payments:", url_path="rankings-payments"),
        st.Page(lobbying_page, title="Lobbying", icon=":material/groups:", url_path="rankings-lobbying"),
        st.Page(
            lobbying_poc_page,
            title="Lobbying (PoC)",
            icon=":material/science:",
            url_path="rankings-lobbying-poc",
        ),
        st.Page(
            legislation_page,
            title="Legislation",
            icon=":material/gavel:",
            url_path="rankings-legislation",
        ),
        st.Page(
            statutory_instruments_page,
            title="Statutory Instruments",
            icon=":material/article:",
            url_path="rankings-statutory-instruments",
        ),
        st.Page(
            committees_page,
            title="Committees",
            icon=":material/account_balance:",
            url_path="rankings-committees",
        ),
        st.Page(glossary_page, title="Glossary", icon=":material/menu_book:", url_path="glossary"),
    ],
    position="hidden",
)
pg.run()
