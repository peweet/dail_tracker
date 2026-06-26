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

import re
import unicodedata
from html import escape as _h
from urllib.parse import quote

# ── Identity normalisation ─────────────────────────────────────────────────────
#
# The pipeline's `normalise_join_key.normalise_df_td_name` produces a sorted-
# character key from a member's full name. The same value is stored as
# `unique_member_code` on the silver/gold views, so it's the canonical
# cross-page member ID. When a view doesn't yet expose `unique_member_code`
# (see TODO_PIPELINE_VIEW_REQUIRED notes on interests / payments / attendance),
# this string-level helper bridges the gap. Once the column ships, prefer
# reading it directly instead of re-deriving from the name.


def name_join_key(name: str) -> str:
    """DEPRECATED — round-3 audit confirmed this DOES NOT match
    ``unique_member_code`` on the registered views.

    The registered codes use the Oireachtas-API format
    ``<Name>.<Chamber>.<DateElected>`` (e.g. ``Mary-Lou-McDonald.D.2011-03-09``).
    This function returns the sorted-letters internal pipeline join form
    (e.g. ``aacddllmmnooruy``) which only matches certain internal joins,
    NOT the public view's ``unique_member_code``.

    Existing callers were producing 404-equivalent URLs for every member.
    Use :func:`data_access.identity_resolver.resolve_member_code` instead,
    which queries ``v_member_registry`` for the real code.

    Kept for any caller that genuinely needs the sorted-letters key.
    """
    s = name.lower()
    s = unicodedata.normalize("NFD", s)
    s = re.sub(r"[̀-ͯ]", "", s)
    s = re.sub(r"[\x27‘’ʼʹ`´＇]", "", s)
    s = re.sub(r"[^a-z\s]", "", s)
    s = re.sub(r"^\s*(dr|prof|rev|fr|sr|mr|mrs|ms|miss|br)\s+", "", s)
    s = re.sub(r"\s+", "", s)
    return "".join(sorted(s))


# Canonical url_path slugs. MUST match utility/app.py st.Page(url_path=...).
#
# /member-overview is the canonical TD page (TheyWorkForYou pattern). The
# dimension pages get a `rankings-` prefix (rankings-attendance, etc.) — they
# are discovery / league tables that funnel into the canonical profile.
# Hyphens not slashes: st.Page rejects nested url_path values.
PAGES: dict[str, str] = {
    "what_they_own": "what-they-own",
    "member_overview": "member-overview",
    "attendance": "rankings-attendance",
    "votes": "rankings-votes",
    # "Interests" league table was replaced by the "What They Own" page; the
    # legacy key is kept as a back-compat alias pointing at the new slug so any
    # internal link built via PAGES["interests"] resolves instead of 404-ing.
    "interests": "what-they-own",
    "payments": "rankings-payments",
    "election_spending": "rankings-election-spending",
    "lobbying": "rankings-lobbying",
    "legislation": "rankings-legislation",
    "statutory_instruments": "rankings-statutory-instruments",
    "committees": "rankings-committees",
    "corporate": "rankings-corporate",
    "procurement": "rankings-procurement",
    # /company is the supplier dossier (entity-first flagship, now a visible
    # "Companies" tab) — the canonical URL for one firm's public-money footprint.
    "company": "company",
    # /local-government is the "Who runs your county" dossier — one council's
    # appointed Chief Executive + published accountability indicators, keyed by
    # ?la=<local_authority>. The constituency dossier links serving councils here.
    "local_government": "local-government",
    # /rankings-council-spending is the Council Spending page (council index ->
    # per-council dossier -> supplier line items), keyed by ?paid_publisher=. The
    # url_path keeps its historic "rankings-" prefix so existing deep links resolve
    # even though the page now lives under the "Your Area" nav group.
    "council_spending": "rankings-council-spending",
    # NOTE: the "news" → "in-the-news" mapping was removed while the cross-member
    # news feed is parked (the page is unregistered in app.py; see
    # pipeline_sandbox/news_mentions/). A slug here that no registered route serves
    # fails test_internal_link_slugs. Re-add it AND register the page together when
    # the feature ships.
}


def _q(value: object) -> str:
    return quote(str(value), safe="")


def member_profile_url(member_id: str, *, section: str | None = None) -> str:
    """Canonical TD profile URL: /member-overview?member=<unique_member_code>.

    Pass ``section`` to append a section-anchor fragment, e.g.
    ``member_profile_url(code, section="payments")`` →
    ``/member-overview?member=<code>#mo-section-payments``. The fragment must
    match the anchor divs emitted on the member-overview page
    (``id="mo-section-<sid>"``) AND the prefix the page's scroll-honouring
    script tests for (``hash.startsWith('#mo-section-')``); a bare ``#payments``
    matches neither and silently lands the user at the top of the page.
    """
    url = f"/{PAGES['member_overview']}?member={_q(member_id)}"
    if section:
        url = f"{url}#mo-section-{_q(section)}"
    return url


def member_votes_url(member_id: str) -> str:
    """Voting-record URL on the votes ranking page (Mode B)."""
    return f"/{PAGES['votes']}?member={_q(member_id)}"


def division_url(vote_id: str) -> str:
    """Division-evidence URL on the votes ranking page (Mode C)."""
    return f"/{PAGES['votes']}?vote={_q(vote_id)}"


def bill_detail_url(bill_id: str) -> str:
    """Canonical bill detail URL: /rankings-legislation?bill=<bill_id>."""
    return f"/{PAGES['legislation']}?bill={_q(bill_id)}"


def si_detail_url(si_id: str) -> str:
    """Canonical statutory-instrument detail URL:
    /rankings-statutory-instruments?si=<si_id>."""
    return f"/{PAGES['statutory_instruments']}?si={_q(si_id)}"


def company_profile_url(supplier_norm: str) -> str:
    """Canonical company dossier URL: /company?supplier=<supplier_norm>.

    ``supplier_norm`` is the normalised join key from the ``v_procurement_*``
    views (shared/name_norm form), NOT the display name.
    """
    return f"/{PAGES['company']}?supplier={_q(supplier_norm)}"


def authority_profile_url(authority: str) -> str:
    """Cross-page link to a contracting authority's procurement dossier:
    /rankings-procurement?authority=<contracting_authority>. The Procurement page
    resolves ``?authority=`` into the buyer's award record (see procurement_page).

    Use this from OTHER pages (e.g. the company dossier) to close the supplier↔buyer
    loop. Within the Procurement page itself, prefer the relative ``?authority=`` form
    so the click is a soft rerun (spa_links) rather than a full cross-page load.
    ``authority`` is the raw ``contracting_authority`` string the award views carry.
    """
    return f"/{PAGES['procurement']}?authority={_q(authority)}"


def council_accountability_url(local_authority: str) -> str:
    """Cross-page link to a council's "Who runs your county" dossier:
    /local-government?la=<local_authority>. The local-government page resolves
    ``?la=`` against ``v_la_chief_executives.local_authority`` (the 31-LA roster).

    ``local_authority`` must be the exact join key the council-grain views carry
    (e.g. ``Dun Laoghaire-Rathdown``, ``Limerick``, ``Waterford``) — the same value
    ``v_constituency_council_context`` / ``v_la_chief_executives`` share. Use this
    from the constituency dossier to carry a serving council into the CE page rather
    than dropping the user on the generic council index.
    """
    return f"/{PAGES['local_government']}?la={_q(local_authority)}"


def council_spending_url(council: str, tier: str = "COMMITTED") -> str:
    """Cross-page link into a council's spending dossier on the Council Spending page:
    /rankings-council-spending?paid_publisher=<council>&paid_tier=<tier>. The page resolves
    ``paid_publisher`` against ``v_procurement_council_summary.council`` (which is the same
    join key as ``v_la_chief_executives.local_authority`` for the ~23 publishing councils),
    landing the reader on that council's suppliers and, one click deeper, the published line
    items. ``tier`` is ``COMMITTED`` (purchase orders) or ``SPENT`` (actual payments) — pass
    the tier the council actually publishes so the dossier opens populated.

    Use this from OTHER pages (e.g. the "Who runs your county" dossier under Your Area) to
    carry a council straight into its spending breakdown rather than the generic index.
    """
    return f"/{PAGES['council_spending']}?paid_publisher={_q(council)}&paid_tier={_q(tier)}"


def lobbying_org_url(org_name: str) -> str:
    """Link to one organisation's lobbying record: /rankings-lobbying?lp3_org=<org_name>.
    The Lobbying page validates ``lp3_org`` against the register's exact org-name set and
    shows "Organisation not found" on a miss — so callers MUST only build this for a name
    known to resolve (the procurement↔lobbying overlap name matches the register only ~64%
    of the time; the company dossier checks membership before linking)."""
    return f"/{PAGES['lobbying']}?lp3_org={_q(org_name)}"


def corporate_notices_url(query: str | None = None) -> str:
    """Corporate Notices page URL: /rankings-corporate, optionally pre-filtered to a
    firm's notices via ``?q=`` (the page reads ``q`` into its search box). Pass a
    company name to land the user on that firm's notices in the dense notices browser
    — the single home of per-notice detail (the company dossier only summarises +
    links here, so notice rendering never diverges across two pages)."""
    base = f"/{PAGES['corporate']}"
    return f"{base}?q={_q(query)}" if query else base


# ── External profile builders ─────────────────────────────────────────────────
#
# `unique_member_code` is the same slug as the Oireachtas API's `memberCode`
# (e.g. "Ciarán-Ahern.D.2024-11-29") — and that slug is the path segment on
# the public-facing profile page at oireachtas.ie/en/members/member/<slug>/.
# Verified live: status 200, title "<Full Name> – Houses of the Oireachtas".

_OIREACHTAS_PUBLIC_BASE = "https://www.oireachtas.ie/en/members/member"


def oireachtas_profile_url(member_code: str | None) -> str | None:
    """Public-facing oireachtas.ie profile URL for a TD.

    Returns ``None`` when ``member_code`` is missing — callers should guard
    against that before passing to ``source_link_html`` (which already
    no-ops on falsy URLs, but ``None`` lets the caller skip rendering
    the surrounding "Official sources" label entirely).
    """
    code = (member_code or "").strip()
    if not code:
        return None
    return f"{_OIREACHTAS_PUBLIC_BASE}/{quote(code, safe='.-')}/"


# ── Social-icon chip builders ─────────────────────────────────────────────────
#
# Compact round chips for the member-overview hero, rendered alongside the
# TD/Minister/Revolving badges in a single .dt-hero-meta-row strip. We deliberately
# use single-character text glyphs instead of brand SVG marks:
#   1. no trademark/licensing burden (X, Meta, etc.),
#   2. trivial to keep contrast/colour consistent across the design system,
#   3. zero binary assets, zero CSP/SVG-sanitiser surprises.
#
# The glyph maps below carry just enough recognisability — pair them with a
# tooltip (title=) and an aria-label so a screen reader announces the full
# platform name. The mapping is exposed as a public constant so tests can
# assert it without re-parsing the helper.

# Mathematical Italic Capital X (U+1D54F) — visually unambiguous as "X / Twitter"
# without using the trademarked logo. Letter glyphs for Facebook (lowercase f),
# Bluesky (B), and Instagram ("IG") follow the same convention.
SOCIAL_GLYPHS: dict[str, tuple[str, str]] = {
    # platform_key: (glyph, accessible name)
    # Audit P3-2: was 𝕏 (Mathematical Italic Capital X) which renders
    # inconsistently across fonts — some showed it as lowercase "x". Plain
    # ASCII "X" with the dt-icon-chip[data-glyph="X"] CSS rule (bold,
    # filled black background) reads as a deliberate brand chip.
    "twitter": ("X", "Twitter / X"),
    "bluesky": ("B", "Bluesky"),
    "facebook": ("f", "Facebook"),
    "instagram": ("IG", "Instagram"),
    "website": ("🌐", "Website"),
}


def social_icon_chip_html(
    platform: str,
    href: str | None,
    *,
    person_name: str = "",
) -> str:
    """Round icon chip for a single social/external link.

    Returns an empty string when ``href`` is missing or not http(s) — same
    "no-op on missing" contract as ``source_link_html`` so callers can splice
    the result into a joined HTML string without conditionals.

    ``platform`` must be one of the keys in ``SOCIAL_GLYPHS``; unknown keys
    yield an empty string (no chip) rather than raising, because the data
    pipeline determines which keys appear and a typo there shouldn't crash
    the whole hero render.
    """
    spec = SOCIAL_GLYPHS.get(platform)
    if spec is None:
        return ""
    glyph, label = spec
    url = str(href or "").strip()
    if not url.startswith(("http://", "https://")):
        return ""
    who = f" of {person_name}" if person_name else ""
    aria = f"Open {label}{who} in a new tab"
    return (
        f'<a class="dt-icon-chip" data-glyph="{_h(glyph)}" '
        f'href="{_h(url)}" target="_blank" rel="noopener" '
        f'title="{_h(label)}" aria-label="{_h(aria)}">{_h(glyph)}</a>'
    )


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
    return f'<a class="{_h(css_class)}" href="{_h(href)}" target="_self">{_h(label)}</a>'


# ── Free-text URL normalisation ────────────────────────────────────────────────
#
# Some register-sourced "website" fields arrive scheme-less (``www.ibec.ie``,
# ``irishheart.ie``) or as outright junk (an org name typed into the website box,
# e.g. ``Chambers Ireland``, or ``n/a``). ``source_link_html`` deliberately
# no-ops on anything that is not already http(s), so passing these raw would
# silently drop the majority of valid websites. This presentation helper accepts
# a domain-shaped value, prepends ``https://`` when no scheme is present, and
# rejects values that don't look like a host — so the UI renders real links and
# hides junk. It is a display guard only (no business logic): the underlying
# register value is unchanged.

_DOMAIN_RE = re.compile(r"^(?:https?://)?[\w-]+(?:\.[\w-]+)+(?:[/?#].*)?$", re.IGNORECASE)


def normalise_external_url(raw: object) -> str:
    """Return an http(s) URL for a domain-shaped free-text value, else ``""``.

    ``www.ibec.ie`` → ``https://www.ibec.ie``; ``https://x.ie`` passes through;
    ``Chambers Ireland`` / ``n/a`` / empty → ``""`` (so callers can splice the
    result into ``source_link_html`` unconditionally — it no-ops on ``""``).
    """
    s = str(raw or "").strip()
    if not s or s.lower() == "n/a" or not _DOMAIN_RE.match(s):
        return ""
    if not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    return s


def source_link_html(
    url: str | None,
    label: str = "Oireachtas",
    *,
    aria_label: str | None = None,
) -> str:
    """Canonical anchor for an external/official-source link.

    Renders with the ``.dt-source-link`` class — accent colour, no underline
    by default, underline on hover, focus ring, and an automatic ``↗`` glyph
    appended via CSS. Pass a clean label string; do **not** include "↗" — it
    is added by the stylesheet so visual treatment stays consistent app-wide.

    Returns an empty string when the URL is missing or not http(s) — callers
    can splice the result into HTML unconditionally.

    Examples
    --------
    >>> source_link_html("https://www.oireachtas.ie/en/debates/vote/2025-06-25/3/")
    '<a class="dt-source-link" href="..." target="_blank" rel="noopener" ...>Oireachtas</a>'
    >>> source_link_html("https://www.lobbying.ie/return/12345", "lobbying.ie")
    """
    s = str(url or "").strip()
    if not s.startswith(("http://", "https://")):
        return ""
    aria = aria_label or f"Open {label} in a new tab"
    return (
        f'<a class="dt-source-link" href="{_h(s)}" target="_blank" '
        f'rel="noopener" aria-label="{_h(aria)}">{_h(label)}</a>'
    )


def api_json_link(path: str, label: str = "View as JSON") -> str:
    """Quiet developer affordance: link this record to the public JSON API.

    ``path`` is an API path beginning with '/', e.g.
    ``/v1/members/<code>/dossier``. Config-gated on the ``DAIL_API_BASE_URL`` env
    var (read directly — there are two ``config`` modules on the path depending on
    caller, so the env var is the unambiguous source): returns ``""`` when unset,
    so it renders nothing until the API is deployed — callers can splice it in
    unconditionally. The app never imports the API; this only builds a URL string.
    """
    import os

    base = os.getenv("DAIL_API_BASE_URL", "").rstrip("/")
    if not base or not path.startswith("/"):
        return ""
    href = base + path
    return (
        f'<a class="dt-api-link" href="{_h(href)}" target="_blank" rel="noopener" '
        f'aria-label="Open this record as JSON on the open-data API">'
        f'<span aria-hidden="true">{{ }}</span> {_h(label)}</a>'
    )
