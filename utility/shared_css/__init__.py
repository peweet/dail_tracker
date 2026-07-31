"""Shared design system for all Dáil Tracker pages.

Split 2026-07-31 (C1, doc/REFACTORING_CANDIDATES.md) out of the former single
6,572-line utility/shared_css.py into this package: mechanical extraction only
-- the concatenated CSS is byte-identical to the old file's <style> string
(verified against `git show`; see doc/REFACTORING_CANDIDATES.md C1 for the
verification method). No selector was moved, reformatted, deduped, or
regrouped.

⚠️  CSS IS ORDER-DEPENDENT (equal specificity → last rule wins) and the
    families are FRAGMENTED across multiple modules below -- e.g. .dt-* and
    .lob-* each recur in several fragments. NEVER reorder IMPORT_ORDER, and
    when editing a family CHECK ALL ITS RUNS (grep the class across every
    fragment), not just the first hit.

IMPORT_ORDER below is the cascade order (was file order in the old module) --
it is also the read order: `describe`/grep a class name across
utility/shared_css/*.py to find every run, or open one fragment at a time
instead of a single 6,572-line file.

    chrome                            app chrome, masthead, top-nav, mobile
                                       menu, sidebar nav links, design tokens,
                                       .sr-only
    widgets_profile                   Streamlit widget theming, .section-*
                                       .stat-* strips, glossary, TD name/meta,
                                       profile header with avatar
    hero_nav                          editorial hero band, hero meta row,
                                       vote outcome labels, dataframe theming,
                                       nav button, info card
    cards_theme                       success/calm-blue theme, rank card,
                                       Member Overview card grid + search
    member_overview                   .mo-* Member Overview audit-fix bundle,
                                       full-card-clickable link, ministerial
                                       diaries
    interests_attendance              .int-* interests, attendance ranking
                                       cards, participation model, .pay-*
                                       payments start
    payments_lobbying                 payments continued, data provenance
                                       box, attendance extras, .lob-* lobbying,
                                       .leg-* legislation start
    questions_legislation             .q-* questions, legislation pipeline
                                       phase/SI card/debate list, cross-page
                                       entity links, mobile layout
    votes_interests_pager             interests member index cards, .vt-*
                                       votes division index cards, TD picker
                                       landing cards, pager
    committees_lobbying3              .cmt-* committee register, .lp3-*
                                       lobbying PoC v3
    elections_judiciary_procurement   .e24-* Election 2024, .jud-* judiciary,
                                       .pr-*/.mf-* procurement + money-lifecycle
    constituencies_support            tab strip, constituencies/Your Area,
                                       county performance cards, committee
                                       meeting-history timeline, .sup-* support
                                       page + app-level footer

⚠️  PAGE-LOCAL CSS also exists — a rule may live there instead:
    corporate.py:118 (_inject_corp_css) · judiciary.py:176 ·
    statutory_instruments.py:66 · public_appointments.py:72 · siting_check.py:123
"""

import streamlit as st

from . import (
    cards_theme,
    chrome,
    committees_lobbying3,
    constituencies_support,
    elections_judiciary_procurement,
    hero_nav,
    interests_attendance,
    member_overview,
    payments_lobbying,
    questions_legislation,
    votes_interests_pager,
    widgets_profile,
)

# The cascade order -- MUST match the original file's line order. Each entry's
# .CSS is a plain (non-raw) triple-quoted string; two fragments
# (member_overview, constituencies_support) carry a real Python string escape
# that a raw string would change the value of, so no fragment uses r"""...""".
IMPORT_ORDER = (
    chrome,
    widgets_profile,
    hero_nav,
    cards_theme,
    member_overview,
    interests_attendance,
    payments_lobbying,
    questions_legislation,
    votes_interests_pager,
    committees_lobbying3,
    elections_judiciary_procurement,
    constituencies_support,
)

_CSS = "".join(mod.CSS for mod in IMPORT_ORDER)


def inject_css() -> None:
    """Inject the design system once per script run.

    Rendered once per script run at app level (utility/app.py, before
    pg.run()) so the stylesheet + banner stay mounted across page
    navigations. Previously each page called this inside its own function,
    so the <style> and .site-banner lived under the page's element subtree
    and were torn down on every navigation — a frame with no design system
    (white/unstyled, collapsed content) that read as a flash/flicker,
    worst on the heavier pages. The per-run guard below makes the legacy
    per-page inject_css() calls harmless no-ops; the guard is reset at the
    top of each run in app.py."""
    if st.session_state.get("_dt_css_injected"):
        return
    st.session_state["_dt_css_injected"] = True
    st.markdown(
        _CSS,
        unsafe_allow_html=True,
    )
    # Banner only — no nav links. Streamlit's native top nav widget
    # (st.navigation(position="top") in utility/app.py) renders the
    # cross-page navigation below this band with internal routing and
    # built-in active-state painting; no custom HTML or JS needed.
    st.html(
        """
        <div class="site-banner">
          <div class="site-banner-inner">
            <a class="site-banner-title" href="./" aria-label="Dáil Tracker — back to home">Dáil Tracker</a>
            <span class="site-banner-sep"></span>
            <span class="site-banner-sub">Irish public data, made searchable</span>
          </div>
        </div>
        """
    )
