import streamlit as st
from pages_code.attendance import attendance_page
from pages_code.committees import committees_page
from pages_code.glossary import glossary_page
from pages_code.interests import interests_page
from pages_code.legislation import legislation_page
from pages_code.lobbying_3 import lobbying_poc_page
from pages_code.member_overview import member_overview_page
from pages_code.payments import payments_page
from pages_code.public_appointments import public_appointments_page
from pages_code.statutory_instruments import statutory_instruments_page
from pages_code.votes import votes_page
from shared_css import inject_css

st.set_page_config(
    page_title="Oireachtas Explorer",
    page_icon=":material/account_balance:",
    layout="wide",
    # Sidebar→filter-bar migration: all filters live in main-panel bars now;
    # every page calls ui.components.hide_sidebar(). Collapsing by default
    # stops the rail flashing on first paint before that CSS applies.
    initial_sidebar_state="collapsed",
)

# Reset the once-per-run guard so inject_css() renders exactly once this run.
# app.py re-executes top-to-bottom on every rerun, so this clears the flag
# before the app-level inject_css() call below.
st.session_state["_dt_css_injected"] = False


def _home_page() -> None:
    """Default landing page for `/`. Renders the member-overview body
    so `/` and `/member-overview` both work as entry points.

    Without this wrapper, marking member_overview_page itself as the
    default would force its url_path to "" (Streamlit's StreamlitPage
    silently ignores url_path on the default page — see
    streamlit/navigation/page.py: `return "" if self._default else self._url_path`).
    Every internal `<a href="/member-overview">` link would then fire a
    spurious "Page not found" modal before falling back to the default.
    Keeping the two pages distinct gives Streamlit a real route at
    /member-overview while leaving `/` rooted on the same content.
    """
    member_overview_page()


# url_path is pinned explicitly so cross-page <a href> links don't break if a
# title is renamed. Slugs must match utility/ui/entity_links.PAGES.
#
# /member-overview is the canonical TD page (TheyWorkForYou pattern). The
# dimension pages get a `rankings-` prefix — they are discovery / league
# tables that funnel into the canonical profile. Hyphens not slashes:
# st.Page rejects nested url_path values.
#
# position="top" uses Streamlit's built-in top-nav widget. Links are routed
# via Streamlit's internal onClick → handlePageChange(pageScriptHash) — no
# full URL reload, no target="_blank" sanitizer fight, no custom JS active
# painter required. Replaces an earlier custom HTML strip rendered through
# st.html, which DOMPurify rewrote to target="_blank" (it strips arbitrary
# `target` attributes but specifically preserves "_blank" via a hook,
# silently dropping "_self"). The native widget bypasses all of that.
pg = st.navigation(
    [
        st.Page(
            _home_page,
            title="Home",
            url_path="home",
            default=True,
            visibility="hidden",
        ),
        st.Page(
            member_overview_page,
            title="Member Overview",
            icon=":material/person:",
            url_path="member-overview",
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
        st.Page(lobbying_poc_page, title="Lobbying", icon=":material/groups:", url_path="rankings-lobbying"),
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
            public_appointments_page,
            title="Appointments",
            icon=":material/assignment_ind:",
            url_path="rankings-appointments",
        ),
        st.Page(
            committees_page,
            title="Committees",
            icon=":material/account_balance:",
            url_path="rankings-committees",
        ),
        st.Page(glossary_page, title="Glossary", icon=":material/menu_book:", url_path="glossary"),
    ],
    position="top",
)
# Render the shared design system + banner once at app level, before the
# page body. Because these elements sit OUTSIDE pg.run()'s per-page subtree,
# Streamlit keeps them mounted across navigations instead of tearing them
# down with each page — eliminating the masthead/stylesheet flicker.
inject_css()
pg.run()
