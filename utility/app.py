import streamlit as st
from pages_code.accommodation_spend import accommodation_spend_page
from pages_code.attendance import attendance_page
from pages_code.committees import committees_page
from pages_code.company import company_page
from pages_code.constituency import constituency_page
from pages_code.corporate import corporate_page
from pages_code.council_spending import council_spending_page
from pages_code.election_2024 import election_2024_page
from pages_code.follow_the_money import follow_the_money_page
from pages_code.glossary import glossary_page
from pages_code.housing import housing_page
from pages_code.judiciary import judiciary_page
from pages_code.legislation import legislation_page
from pages_code.lobbying_3 import lobbying_poc_page
from pages_code.local_government import local_government_page
from pages_code.member_overview import member_overview_page
from pages_code.ministerial_diaries import ministerial_diaries_page
from pages_code.payments import payments_page
from pages_code.procurement import procurement_page
from pages_code.public_appointments import public_appointments_page
from pages_code.public_payments import public_payments_page
from pages_code.statutory_instruments import statutory_instruments_page
from pages_code.votes import votes_page
from pages_code.what_they_own import what_they_own_page
from shared_css import inject_css
from ui.page_analytics import log_page_view
from ui.spa_links import install_spa_links

st.set_page_config(
    page_title="Dáil Tracker",
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


def _interests_redirect_page() -> None:
    """Back-compat redirect: the old ``/rankings-interests`` league table was
    replaced by ``/what-they-own``. Bookmarks and external deep links to the
    old route land here (a hidden page that keeps the URL alive) and are
    forwarded on. ``st.markdown`` not ``st.html`` — an st.html iframe would
    redirect the iframe rather than the parent tab (same reason the interests
    member-jump used a meta-refresh)."""
    import streamlit as _st

    _st.markdown(
        '<meta http-equiv="refresh" content="0;url=/what-they-own">',
        unsafe_allow_html=True,
    )
    _st.stop()


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
# Grouped into four labelled sections + Glossary (Phase-0 IA decision,
# doc/archive/APP_REDESIGN_PHASE0.md §3). st.navigation accepts a {section: [pages]}
# dict; grouping is purely presentational, so every url_path below is
# UNCHANGED and utility/ui/entity_links.PAGES stays valid. The hidden default
# Home lives in the first group (visibility="hidden" keeps it off the bar).
pg = st.navigation(
    {
        # "What They Own" leads the entire bar (far-left, first slot): the
        # plain-language front door to the Register of Members' Interests —
        # what property/shares/companies the people who govern us own, across
        # the whole record (sitting + former members, historic backfill). It is
        # expected to be the highest-traffic feature, so it gets the prime slot
        # ahead of even "Your Area". The existing /interests page stays put as
        # the year-by-year league table under Members & Parliament.
        "What They Own": [
            st.Page(
                what_they_own_page,
                title="What They Own",
                icon=":material/real_estate_agent:",
                url_path="what-they-own",
            ),
        ],
        # "Your Area" leads the bar: the constituency-first dossier (who represents you,
        # what they do in the Dáil, housing + council money where you live) is the citizen
        # entry point, so it gets a prominent top-level slot rather than nesting under
        # Members & Parliament.
        "Your Area": [
            st.Page(
                constituency_page,
                title="Constituencies",
                icon=":material/map:",
                url_path="constituencies",
            ),
            st.Page(
                local_government_page,
                title="Who Runs Your County",
                icon=":material/account_balance:",
                url_path="local-government",
            ),
            # Council Spending sits alongside "Who Runs Your County": both are local-government
            # finance for the citizen's own county, so the spending dossier belongs in "Your Area"
            # rather than the national "The Money" group. url_path keeps its historic
            # "rankings-council-spending" so existing deep links / entity_links resolve.
            st.Page(
                council_spending_page,
                title="Council Spending",
                icon=":material/location_city:",
                url_path="rankings-council-spending",
            ),
            st.Page(
                housing_page,
                title="Housing",
                icon=":material/home:",
                url_path="housing",
            ),
        ],
        "Members & Parliament": [
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
            # The year-by-year Interests league table was REPLACED by the
            # left-most "What They Own" page (the all-time, citizen-first front
            # door to the Register of Members' Interests). entity_links.PAGES
            # keeps "interests" → "what-they-own" so internal links resolve, and
            # the hidden redirect page below keeps the old /rankings-interests
            # route alive for external bookmarks (forwards to /what-they-own).
            st.Page(
                _interests_redirect_page,
                title="Interests",
                url_path="rankings-interests",
                visibility="hidden",
            ),
            st.Page(
                committees_page,
                title="Committees",
                icon=":material/account_balance:",
                url_path="rankings-committees",
            ),
            # NOTE: the cross-member "In the News" feed page is parked in
            # pipeline_sandbox/news_mentions/page_news.py while the feature is
            # tested further — unregistered here so it doesn't ship to main.
        ],
        "The Money": [
            st.Page(payments_page, title="Payments", icon=":material/payments:", url_path="rankings-payments"),
            # url_path kept as "rankings-election-spending" so existing deep links and
            # entity_links.PAGES["election_spending"] keep resolving; the page is now the
            # unified GE2024 hub (donations + party spending + candidate spending).
            st.Page(
                election_2024_page,
                title="Election 2024",
                icon=":material/savings:",
                url_path="rankings-election-spending",
            ),
            st.Page(
                procurement_page,
                title="Procurement",
                icon=":material/request_quote:",
                url_path="rankings-procurement",
            ),
            # A guided trail through the published payment graph (body → companies it pays →
            # the individual records), with a bounded breadcrumb. Reuses Procurement's payment
            # renderers; the value here is the navigation. See pages_code/follow_the_money.py.
            st.Page(
                follow_the_money_page,
                title="Follow the Money",
                icon=":material/conversion_path:",
                url_path="follow-the-money",
            ),
            st.Page(
                accommodation_spend_page,
                title="Accommodation Spend",
                icon=":material/hotel:",
                url_path="accommodation-spend",
            ),
            st.Page(
                public_payments_page,
                title="Public Payments",
                icon=":material/account_balance_wallet:",
                url_path="rankings-public-payments",
            ),
            # Company dossier (entity-first flagship): the org-first front door.
            # Visible in the nav so users can browse/search every firm directly;
            # also reached from supplier cards on Procurement / Public Payments.
            st.Page(
                company_page,
                title="Companies",
                icon=":material/domain:",
                url_path="company",
            ),
        ],
        "Law & Records": [
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
                corporate_page,
                title="Corporate Notices",
                icon=":material/business_center:",
                url_path="rankings-corporate",
            ),
            st.Page(
                judiciary_page,
                title="Courts & Judiciary",
                icon=":material/balance:",
                url_path="rankings-judiciary",
            ),
        ],
        "Influence": [
            st.Page(lobbying_poc_page, title="Lobbying", icon=":material/groups:", url_path="rankings-lobbying"),
            st.Page(
                ministerial_diaries_page,
                title="Who Ministers Meet",
                icon=":material/event_note:",
                url_path="rankings-ministerial-diaries",
            ),
            st.Page(
                public_appointments_page,
                title="Appointments",
                icon=":material/assignment_ind:",
                url_path="rankings-appointments",
            ),
        ],
        "Glossary": [
            st.Page(glossary_page, title="Glossary", icon=":material/menu_book:", url_path="glossary"),
        ],
    },
    position="top",
)
# Render the shared design system + banner once at app level, before the
# page body. Because these elements sit OUTSIDE pg.run()'s per-page subtree,
# Streamlit keeps them mounted across navigations instead of tearing them
# down with each page — eliminating the masthead/stylesheet flicker.
inject_css()
# Intercept in-page <a href="?param"> tile/chip/card clicks so they soft-rerun
# over the live websocket instead of full-reloading the browser tab. App-level
# (outside pg.run()) so the listener survives page navigations; before pg.run()
# so the clicked query params are visible to the page body in the same run.
install_spa_links()
# Cookieless page-view count: records only {timestamp, url_path} to
# logs/page_views.jsonl — no session id, no IP, no per-person data. Best-effort
# (never raises). See ui/page_analytics.py.
log_page_view(pg.url_path)
pg.run()
