"""The page-lifecycle tier: ``dt_page`` + its supporting boot/boundary helpers.

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.

DEVIATION from the original section order: ``hide_sidebar`` lived much later
in the monolithic file (next to ``main_member_jump``), but it is moved here,
next to ``dt_page``, because ``dt_page`` calls it as a bare, module-global
name (``hide_sidebar()``) inside its boot sequence. Keeping both in one
module is what lets ``test/utility/test_dt_page.py``'s
``monkeypatch.setattr(components, "hide_sidebar", ...)`` continue to reach
the name ``dt_page`` actually resolves at call time — a name imported into
a *different* submodule's globals would not be affected by a patch applied
to the ``ui.components`` package object alone. ``dt_page`` is used by every
page, so this module's own imports stay deliberately acyclic: no import of
any sibling ``ui.components.*`` submodule.
"""

from __future__ import annotations

import functools
import logging
import traceback
from html import escape as _h

import streamlit as st

# fmt_civic_date's canonical home is now ui.format (the shared display-formatting
# module, 2026-07 consolidation); re-exported here because many pages import it
# from components.
from ui.format import fmt_civic_date  # noqa: F401

_log = logging.getLogger(__name__)


def period_year_pills(df, key: str) -> tuple[str | None, str | None]:
    """Year filter pills above a lobbying-style returns table.

    Reads unique years from ``df["period_start_date"]`` (datetime-like or
    string) and renders ``st.pills`` with "All years" + each year (pills, not
    a segmented control — year navigation is pills app-wide).
    Returns a SQL-ready ``(start_iso, end_iso)`` tuple, or ``(None, None)``
    when "All years" is selected or when no years can be derived. Selection is
    pushed back to SQL via the returned tuple — callers do no pandas row
    masking on the year here.

    Used to be byte-equivalent ``_year_pills`` / ``_year_selector`` in
    lobbying_2 and lobbying_3.
    """
    import pandas as pd

    if df.empty or "period_start_date" not in df.columns:
        return None, None
    try:
        years = sorted(
            pd.to_datetime(df["period_start_date"], errors="coerce").dropna().dt.year.unique().tolist(),
            reverse=True,
        )
    except Exception:
        return None, None
    if not years:
        return None, None
    options = ["All years"] + [str(y) for y in years]
    chosen = st.pills("Year", options, default=options[0], key=key, label_visibility="collapsed") or options[0]
    if chosen == "All years":
        return None, None
    return f"{chosen}-01-01", f"{chosen}-12-31"


def page_error_boundary(page_fn):
    """Decorator: catch any unhandled exception in a page entry point and
    show a calm civic-voice empty_state instead of Streamlit's red traceback.

    Logs full traceback for debugging; exposes a brief technical summary in
    a collapsed expander so journalists/devs can paste it into a GitHub issue.
    Only catches Exception (not BaseException), so st.stop() and Ctrl+C work.
    """

    @functools.wraps(page_fn)
    def wrapper(*args, **kwargs):
        try:
            return page_fn(*args, **kwargs)
        except Exception as exc:
            tb = traceback.format_exc()
            _log.exception("page entry crashed: %s", page_fn.__name__)
            try:
                from shared_css import inject_css

                inject_css()
            except Exception:
                pass
            st.html(
                '<div class="dt-callout">'
                "<strong>Something went wrong rendering this page.</strong><br>"
                '<span style="color:var(--text-meta)">'
                "Try refreshing. If it persists, the underlying view may be "
                "missing or the data file may be stale. "
                f"({_h(type(exc).__name__)})"
                "</span>"
                "</div>"
            )
            with st.expander("Technical details", expanded=False):
                st.code(tb, language="text")
            return None

    return wrapper


def dt_page(page_fn):
    """THE page bootstrap — one decorator for every ``pages_code`` entry point.

    Composes the boot sequence pages previously hand-rolled (inconsistently —
    12 of 26 pages had no error boundary and could surface raw red tracebacks):

      1. ``inject_css()``  — harmless no-op after the app-level injection
         (once-per-run guard in shared_css); belt-and-braces for a page
         rendered outside ``app.py``.
      2. ``hide_sidebar()`` — the app-wide convention (filters live in
         main-panel filter bars).
      3. ``page_error_boundary`` — outermost, so a failure inside the boot
         itself still renders the calm fallback.

    New pages should use this instead of re-typing the three lines.
    """

    @functools.wraps(page_fn)
    def _booted(*args, **kwargs):
        from shared_css import inject_css  # local: defer the 6k-line CSS module until render

        inject_css()
        hide_sidebar()
        return page_fn(*args, **kwargs)

    return page_error_boundary(_booted)


def hide_sidebar() -> None:
    """Hide the (empty) sidebar rail on a page whose filters have moved into a
    main-panel :func:`filter_bar`.

    Hides the rail and its collapse/expand controls, and reverts the dark
    brand band's 22rem sidebar-clearing gutter to a normal main gutter. Every
    content page now calls this, so the sidebar is effectively gone app-wide;
    ``app.py`` also sets ``initial_sidebar_state="collapsed"`` so it never
    flashes on first paint.

    Desktop-only (min-width 768px): below Streamlit's md breakpoint the
    top-nav widget is not rendered at all and st.navigation falls back to the
    sidebar — hiding the sidebar + expand button there removed ALL cross-page
    navigation on phones, trapping users on the landing page.
    """
    st.markdown(
        "<style>"
        "@media (min-width: 768px){"
        '[data-testid="stSidebar"],'
        '[data-testid="stSidebarCollapsedControl"],'
        '[data-testid="stExpandSidebarButton"]{display:none !important;}'
        "}"
        ".site-banner-inner{padding-left:2rem !important;}"
        "</style>",
        unsafe_allow_html=True,
    )
