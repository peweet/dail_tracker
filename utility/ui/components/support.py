"""Support page + app-level footer (.sup-* in shared_css.py).

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.

Rendered by pages_code/support.py, and the footer by app.py after pg.run().

NO THIRD-PARTY JAVASCRIPT. The coffee link is a plain anchor, not Buy Me a
Coffee's widget script: Streamlit's DOMPurify pass strips <script> outright
but specifically preserves target="_blank" (see the note in app.py), so the
anchor survives and the app stays free of third-party cookies and the consent
banner they would require.

TWO ENV GATES, both OFF by default, so a fresh checkout publishes neither a
payment link nor an email address:
  DT_COFFEE_URL     — the Buy Me a Coffee page. Unset → no ask panel, no
                      footer coffee link. The page is still useful: it says
                      what the site costs and how to report an error.
  DT_CONTACT_EMAIL  — a burnable hide-my-email alias forwarding to a mailbox
                      kept for this project. Unset → no private-contact box.
                      Env, not a constant, so the alias can be burned and
                      reminted without a commit or a redeploy — that
                      rotatability is the actual defence against scraping.
                      Obfuscating it in markup would be theatre: the
                      sanitiser strips <script>, so it cannot be assembled
                      client-side anyway.
"""

from __future__ import annotations

import os
from html import escape as _h
from urllib.parse import quote as _q

_SUPPORT_PATH = "/support"

# Public repo with issues enabled (GitHub API, 2026-07-27: private=false,
# has_issues=true), so an anonymous reader can actually file one.
_GITHUB_REPO = "https://github.com/peweet/dail_tracker"

_CORRECTION_BODY = """**Which page?**
(paste the URL)

**What does it say now?**

**What should it say?**

**Source**
A link to the register, report or publication that shows the correct figure.
Corrections that cite a source can be checked and fixed; ones that don't, can't.
"""

_ENHANCEMENT_BODY = """**What would you like to see?**

**What would you use it for?**
The question you're trying to answer matters more than the feature — it often
turns out the data is already here under a different name.
"""


def coffee_url() -> str:
    """The configured Buy Me a Coffee page, or "" when unset."""
    return os.getenv("DT_COFFEE_URL", "").strip()


def contact_email() -> str:
    """The configured contact alias, or "" when unset."""
    return os.getenv("DT_CONTACT_EMAIL", "").strip()


def _issue_url(*, labels: str, title: str, body: str) -> str:
    """A GitHub "new issue" URL with the form pre-filled.

    GitHub reads title/body/labels from the query string, so a reporter lands
    on a part-completed form rather than a blank box. The body asks for the
    SOURCE — the one field that makes a correction actionable.
    """
    return f"{_GITHUB_REPO}/issues/new?labels={_q(labels)}&title={_q(title)}&body={_q(body)}"


def support_hero_html() -> str:
    return (
        '<div class="sup-hero">'
        "<h1>Dáil Tracker is free.<br>It is <em>not</em> free to run.</h1>"
        "<p>Every figure on this site is pulled from a public register, cleaned, and "
        "published with its source attached. That work is a person and a server bill, "
        "not a grant. If the site has told you something you could not easily find "
        "elsewhere, you can put a few euro toward keeping it running.</p>"
        "</div>"
    )


def support_costs_html(stats) -> str:
    """Three scale figures. ``stats`` is a data_access.support_data.SupportStats,
    or None when a figure could not be derived — in which case dashes render
    rather than a guess.

    Each rounded or composite headline discloses its parts in the sub-label, so
    a reader can check the number instead of taking it on trust — which is the
    argument the whole page is making.
    """
    if stats is None:
        cells = [
            ("—", "public officials tracked", "figure unavailable"),
            ("—", "sources watched", "figure unavailable"),
            ("—", "records published", "figure unavailable"),
        ]
    else:
        cells = [
            (
                f"{stats.officials:,}",
                "public officials tracked",
                f"{stats.oireachtas_members} TDs & senators + {stats.judges} judges",
            ),
            (
                f"{stats.sources:,}",
                "sources watched",
                f"feeds from {stats.publishers} publishers",
            ),
            (
                stats.records_display,
                "records published",
                f"{stats.records:,} rows across {stats.record_datasets} registers",
            ),
        ]
    body = "".join(
        '<div class="sup-cost">'
        f'<span class="sup-cost-fig">{_h(fig)}</span>'
        f'<span class="sup-cost-lab">{_h(label)}</span>'
        f'<span class="sup-cost-sub">{_h(sub)}</span>'
        "</div>"
        for fig, label, sub in cells
    )
    return f'<div class="sup-costs">{body}</div>'


def coffee_button_html(url: str, *, label: str = "Buy me a coffee") -> str:
    return (
        f'<a class="sup-btn" href="{_h(url, quote=True)}" '
        'target="_blank" rel="noopener noreferrer">'
        '<span class="sup-icon">local_cafe</span>'
        f"<span>{_h(label)}</span></a>"
    )


def support_ask_html() -> str:
    """The coffee ask. Empty string when DT_COFFEE_URL is unset — the rest of
    the page stands on its own without it."""
    url = coffee_url()
    if not url:
        return ""
    return (
        '<div class="sup-ask">'
        "<h2>Buy me a coffee</h2>"
        "<p>One-off, any amount, no account needed. Payment is handled entirely by "
        "Buy&nbsp;Me&nbsp;a&nbsp;Coffee — this site never sees your card details, your "
        "name, or your email, and sets no tracking cookie on you for clicking.</p>"
        f'<div class="sup-btn-row">{coffee_button_html(url)}</div>'
        '<p class="sup-btn-note">There is no membership tier and no subscription. '
        "One cup, whenever you feel like it.</p>"
        "</div>"
    )


def support_help_html() -> str:
    """ "Spot something wrong?" — two public routes plus a private fallback.

    Public issues lead deliberately. A correction filed in the open is
    auditable by anyone, which is the standard this site holds its own figures
    to; email is the exception, not the front door.
    """
    correction = _issue_url(labels="data-correction", title="Data correction: ", body=_CORRECTION_BODY)
    enhancement = _issue_url(labels="enhancement", title="Suggestion: ", body=_ENHANCEMENT_BODY)
    email = contact_email()
    private = ""
    if email:
        private = (
            '<p class="sup-private"><strong>Something you would rather not post in '
            "public?</strong> If it concerns you personally — a record that names you, or "
            "anything with a legal dimension — a GitHub issue is the wrong place, because it "
            "is permanent and world-readable. Email "
            f'<a href="mailto:{_h(email, quote=True)}">{_h(email)}</a> instead. That address '
            "forwards to a mailbox kept for this project only.</p>"
        )
    return (
        '<div class="sup-help">'
        "<h2>Spot something wrong? Want something added?</h2>"
        '<p class="sup-help-intro">This site republishes other people\'s registers, so it '
        "inherits their mistakes as well as their facts. If a figure looks wrong, it "
        "probably is — and telling me is worth more than a coffee.</p>"
        '<div class="sup-routes">'
        '<div class="sup-route">'
        "<h3>A figure looks wrong</h3>"
        "<p>Opens a pre-filled report asking which page, what it shows, and the source "
        "that says otherwise.</p>"
        f'<a class="sup-btn-ghost" href="{_h(correction, quote=True)}" '
        'target="_blank" rel="noopener noreferrer">'
        '<span class="sup-icon">bug_report</span><span>Report a correction</span></a>'
        "</div>"
        '<div class="sup-route">'
        "<h3>Something is missing</h3>"
        "<p>A register that should be here, a view that would help, a page that is hard "
        "to use. Tell me what you were trying to find out.</p>"
        f'<a class="sup-btn-ghost" href="{_h(enhancement, quote=True)}" '
        'target="_blank" rel="noopener noreferrer">'
        '<span class="sup-icon">lightbulb</span><span>Suggest an improvement</span></a>'
        "</div>"
        "</div>"
        f"{private}"
        "</div>"
    )


def support_honesty_html() -> str:
    """What the money does not buy.

    Load-bearing, not decoration: a transparency site that accepts money has to
    answer "who funds this, and does it shape what you publish". This is that
    answer. Reword it if you like; do not remove it while the ask is live.
    """
    items = [
        (
            "It does not buy influence over what is published.",
            "No supporter can ask for a record to be added, changed, softened or removed. "
            "What is on the site is what the public registers say.",
        ),
        (
            "It does not unlock anything.",
            "There is no supporter tier, no paywall and no data held back. Every page is "
            "the same for everyone, whether you have paid or not.",
        ),
        (
            "It does not make me accountable to you for the service.",
            "This is a tip, not a subscription. It buys no uptime promise, no support "
            "queue and no refund claim if a source goes dark.",
        ),
        (
            "It does not go to a company.",
            "It covers running costs — hosting, storage, and the constant repair work "
            "the scrapers need when a council changes its website.",
        ),
    ]
    lis = "".join(f"<li><strong>{_h(head)}</strong> {_h(body)}</li>" for head, body in items)
    return f'<div class="sup-honest"><h2>What your money does not buy</h2><ul>{lis}</ul></div>'


def support_page_html(stats) -> str:
    """The whole page body as ONE html string.

    Assembled in one piece on purpose. Streamlit renders each st.markdown/st.html
    call into its own container and auto-closes unbalanced tags, so an opening
    ``<div class="sup-wrap">`` in one call does NOT wrap elements emitted by
    later calls — it closes immediately and the content renders at full
    ``layout="wide"`` width. A single string is the only reliable way to hold a
    max-width column.

    Help sits ABOVE the honesty panel: reporting an error is the contribution
    most readers can actually make, and it should not sit below the small print
    about money.
    """
    return (
        '<div class="sup-wrap">'
        f"{support_hero_html()}{support_costs_html(stats)}{support_ask_html()}"
        f"{support_help_html()}{support_honesty_html()}"
        "</div>"
    )


def site_footer_html(*, last_refresh: str | None = None) -> str:
    """The app-level footer strip.

    Rendered in app.py AFTER pg.run(), outside the per-page subtree, so it stays
    mounted across navigation instead of being torn down on every page switch —
    the same reason inject_css() and install_spa_links() sit at app level.

    The coffee link points at the internal /support page rather than straight
    out to Buy Me a Coffee, so the existing cookieless page-view log counts
    interest for free. An external link could not be measured without adding
    script: install_spa_links only intercepts ``href^="?"`` anchors.
    """
    refresh = f"<span>Data refreshed {_h(last_refresh)}</span>" if last_refresh else ""
    coffee = ""
    if coffee_url():
        coffee = (
            f'<a class="sup-footer-coffee" href="{_h(_SUPPORT_PATH, quote=True)}?from=footer">'
            '<span class="sup-icon" style="font-size:1.05rem">local_cafe</span>'
            "<span>Support this site</span></a>"
        )
    return (
        '<div class="sup-wrap"><div class="sup-footer">'
        '<span class="sup-footer-name">Dáil Tracker</span>'
        "<span>Built from public registers &middot; sources cited on every page</span>"
        f"{refresh}"
        '<span class="sup-footer-spacer"></span>'
        f"{coffee}"
        "</div></div>"
    )
