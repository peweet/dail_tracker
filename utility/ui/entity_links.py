"""Cross-page navigation helpers for Dáil Tracker.

Every entity (TD, division, bill) has one canonical URL. Use these helpers
anywhere you render an entity name; never hand-roll the URL string. If a
page is renamed, update PAGES here and utility/app.py at the same time.

Why ``<a href>`` and not ``st.button`` + ``st.switch_page``:
- keyboard accessible by default (Tab + Enter)
- right-click → "Open in new tab" works
- shareable via the address bar
- screen readers announce them as links, not buttons
"""
from __future__ import annotations

from html import escape as _h
from urllib.parse import quote


# Canonical url_path slugs. MUST match utility/app.py st.Page(url_path=...).
PAGES: dict[str, str] = {
    "attendance":      "attendance",
    "member_overview": "member-overview",
    "votes":           "votes",
    "interests":       "interests",
    "payments":        "payments",
    "lobbying":        "lobbying",
    "legislation":     "legislation",
    "committees":      "committees",
}


def _q(value: object) -> str:
    return quote(str(value), safe="")


def member_profile_url(member_id: str) -> str:
    """Canonical TD profile URL: /member-overview?member=<unique_member_code>."""
    return f"/{PAGES['member_overview']}?member={_q(member_id)}"


def member_votes_url(member_id: str) -> str:
    """Voting-record URL on the votes page (Mode B)."""
    return f"/{PAGES['votes']}?member={_q(member_id)}"


def division_url(vote_id: str) -> str:
    """Division-evidence URL on the votes page (Mode C)."""
    return f"/{PAGES['votes']}?vote={_q(vote_id)}"


def member_link_html(
    member_id: str | None,
    name: str,
    *,
    css_class: str = "dt-member-link",
    aria_prefix: str = "View profile of",
) -> str:
    """Anchor tag linking a TD name to their member-overview profile.

    Returns the plain (escaped) name with no link when ``member_id`` is
    falsy — graceful degradation for rows that haven't been ID-enriched.
    """
    name_safe = _h(str(name) if name is not None else "")
    if not member_id:
        return name_safe
    return (
        f'<a class="{_h(css_class)}" href="{_h(member_profile_url(member_id))}" '
        f'target="_self" aria-label="{_h(aria_prefix)} {name_safe}">{name_safe}</a>'
    )


def entity_cta_html(
    href: str,
    label: str,
    *,
    css_class: str = "dt-entity-cta",
) -> str:
    """Bold pill-styled anchor for prominent profile-jump links.

    Pair with the helpers above:
        entity_cta_html(member_votes_url(jk), "Full voting history →")
    """
    return (
        f'<a class="{_h(css_class)}" href="{_h(href)}" target="_self">'
        f'{_h(label)}</a>'
    )
