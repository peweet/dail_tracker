"""Support / "buy me a coffee" surface — HTML + CSS builders. SANDBOX ONLY.

NOT WIRED INTO THE APP. Nothing here is imported by utility/app.py,
utility/shared_css.py or utility/ui/components.py. Preview it with the
standalone demo:

    .venv/Scripts/python -m streamlit run pipeline_sandbox/support_coffee/demo_app.py

Why the classes are all ``sc-`` prefixed: this CSS is injected as its own
<style> block on top of the real design system, so a name collision with a
live component would silently restyle the production app the moment anyone
imported this module. The prefix makes that impossible.

Two surfaces, per the plan:
  * ``site_footer_html``  — an app-level footer strip (the app has NO footer
    today; nothing renders after pg.run()).
  * the support-page blocks — hero, cost strip, the coffee button, and the
    "what it does not buy" panel that keeps the provenance position intact.

NO THIRD-PARTY JS. The coffee link is a plain <a target="_blank" rel="noopener">
to buymeacoffee.com, not their widget script. Streamlit sanitises rendered HTML
through DOMPurify, which strips <script> outright but specifically preserves
target="_blank" (see utility/app.py:118-123) — so the anchor survives and no
consent banner, CSP work or iframe is needed.

FIGURES: all three are computed at run time by coffee_stats.py from the repo's
own metadata and registers — real, exact, and re-derived on every render. No
number is typed into this markup; a typed figure is true the day it is typed
and quietly wrong afterwards.
"""

from __future__ import annotations

import html

from coffee_stats import SupportStats, load_stats

# ── Tunables the real thing would read from config, not hard-code ────────────
COFFEE_URL = "https://buymeacoffee.com/dailtracker"  # slug NOT yet registered
SUPPORT_PATH = "/support"

# No placeholders left: all three figures are queried at render time. Shown only
# when every one of them resolves — a strip with two real numbers and one dash
# invites the reader to trust the dash as much as the rest.
_UNAVAILABLE_CELLS = [
    ("—", "public officials tracked", "figure unavailable"),
    ("—", "sources watched", "figure unavailable"),
    ("—", "records published", "figure unavailable"),
]


# ── CSS ──────────────────────────────────────────────────────────────────────
# Tokens (--accent, --ink-*, --surface, --border, Zilla Slab / Epilogue) come
# from the real design system in utility/shared_css.py; this block only adds
# sc-* rules on top and defines NO tokens of its own.
SUPPORT_CSS = """
<style>
/* ═══ Support page — SANDBOX (pipeline_sandbox/support_coffee) ═══════════ */

.sc-wrap { max-width: 62rem; margin: 0 auto; }

/* One optical column. The hero's 4px stripe + 1.4rem gutter pushes its text
   right; every block below is inset by the same amount so figures, panels and
   the footer rule all start on the same vertical line as the headline. */
.sc-costs, .sc-ask, .sc-honest, .sc-footer { margin-left: calc(1.4rem + 4px); }

/* ── Hero: editorial statement, side-stripe signature ─────────────────── */
.sc-hero {
    border-left: 4px solid var(--accent);
    padding: 0.15rem 0 0.15rem 1.4rem;
    margin: 0.4rem 0 2.2rem 0;
}
.sc-hero h1 {
    font-family: 'Zilla Slab', Georgia, serif;
    font-weight: 700;
    font-size: 2.45rem;
    line-height: 1.12;
    letter-spacing: -0.012em;
    color: var(--text-primary);
    margin: 0 0 0.55rem 0;
}
.sc-hero h1 em {
    font-style: normal;
    color: var(--accent);
}
.sc-hero p {
    font-family: 'Epilogue', sans-serif;
    font-size: 1.06rem;
    line-height: 1.6;
    color: var(--text-secondary);
    margin: 0;
    max-width: 46rem;
}

/* ── Cost strip: three figures, tabular, no chrome ─────────────────────── */
.sc-costs {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 0;
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    margin: 0 0 2.2rem 0;
}
.sc-cost { padding: 1.15rem 1.4rem 1.2rem 0; }
.sc-cost + .sc-cost {
    border-left: 1px solid var(--border);
    padding-left: 1.4rem;
}
.sc-cost-fig {
    font-family: 'Zilla Slab', Georgia, serif;
    font-variant-numeric: tabular-nums;
    font-weight: 700;
    font-size: 2.05rem;
    line-height: 1;
    color: var(--ink-strong);
    display: block;
}
.sc-cost-lab {
    font-family: 'Epilogue', sans-serif;
    font-size: 0.86rem;
    font-weight: 600;
    color: var(--text-primary);
    display: block;
    margin-top: 0.42rem;
}
.sc-cost-sub {
    font-family: 'Epilogue', sans-serif;
    font-size: 0.79rem;
    color: var(--ink-muted);
    display: block;
    margin-top: 0.15rem;
}

/* ── The ask: warm panel + the button ──────────────────────────────────── */
.sc-ask {
    background: var(--accent-subtle);
    border: 1px solid var(--accent-dim);
    border-radius: 10px;
    padding: 1.7rem 1.8rem 1.8rem;
    margin: 0 0 2.2rem 0;
}
.sc-ask h2 {
    font-family: 'Zilla Slab', Georgia, serif;
    font-weight: 600;
    font-size: 1.42rem;
    color: var(--text-primary);
    margin: 0 0 0.5rem 0;
}
.sc-ask p {
    font-family: 'Epilogue', sans-serif;
    font-size: 0.97rem;
    line-height: 1.62;
    color: var(--text-secondary);
    margin: 0 0 1.3rem 0;
    max-width: 42rem;
}
.sc-btn {
    display: inline-flex;
    align-items: center;
    gap: 0.6rem;
    background: var(--accent);
    color: #fff !important;
    font-family: 'Epilogue', sans-serif;
    font-weight: 600;
    font-size: 1.02rem;
    text-decoration: none !important;
    padding: 0.82rem 1.6rem;
    border-radius: 8px;
    border: 1px solid transparent;
    box-shadow: 0 1px 2px oklch(0% 0 0 / .10);
    transition: transform .12s ease, box-shadow .12s ease, background .12s ease;
}
.sc-btn:hover {
    background: var(--signal-bad-deep);
    box-shadow: 0 4px 14px oklch(0% 0 0 / .16);
    transform: translateY(-1px);
    color: #fff !important;
}
/* Material Symbols ligature icon. The font is already loaded by the app's own
   @import in shared_css.py, so this costs nothing extra — and it renders as a
   crisp mono-line glyph that inherits `color`, unlike the ☕ emoji, which the
   OS paints in its own colours and reads as a smudge at button size. */
.sc-icon {
    font-family: 'Material Symbols Outlined';
    font-feature-settings: 'liga';
    -webkit-font-feature-settings: 'liga';
    font-size: 1.25rem;
    line-height: 1;
    font-weight: 400;
}
/* The button sits in its own row. Streamlit's markdown container styles <p>
   margins with enough specificity to flatten a margin-top on the note, so the
   gap is owned by this wrapper instead of by the paragraph. */
.sc-btn-row { margin: 0 0 1.15rem 0; }
.sc-btn-note {
    font-family: 'Epilogue', sans-serif;
    font-size: 0.8rem;
    color: var(--text-meta);
    margin: 0 !important;
    display: block;
}

/* ── The honesty panel — what money does NOT buy ───────────────────────── */
.sc-honest {
    border: 1px solid var(--border);
    border-left: 3px solid var(--ink-muted);
    border-radius: 8px;
    background: #ffffff;
    padding: 1.5rem 1.7rem 1.55rem;
    margin: 0 0 2.4rem 0;
}
.sc-honest h2 {
    font-family: 'Zilla Slab', Georgia, serif;
    font-weight: 600;
    font-size: 1.24rem;
    color: var(--text-primary);
    margin: 0 0 1rem 0;
}
.sc-honest ul { margin: 0; padding: 0; list-style: none; }
.sc-honest li {
    font-family: 'Epilogue', sans-serif;
    font-size: 0.95rem;
    line-height: 1.55;
    color: var(--text-secondary);
    padding: 0 0 0 1.6rem;
    position: relative;
    margin-bottom: 0.72rem;
}
.sc-honest li:last-child { margin-bottom: 0; }
.sc-honest li::before {
    content: "—";
    position: absolute;
    left: 0;
    top: 0;
    color: var(--accent);
    font-weight: 700;
}
.sc-honest li strong { color: var(--text-primary); font-weight: 600; }

/* ── App-level footer strip ────────────────────────────────────────────── */
.sc-footer {
    border-top: 1px solid var(--border);
    margin: 3.2rem 0 0 0;
    padding: 1.15rem 0 1.6rem 0;
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.55rem 1.5rem;
    font-family: 'Epilogue', sans-serif;
    font-size: 0.83rem;
    color: var(--text-meta);
}
.sc-footer-name { font-weight: 600; color: var(--text-secondary); }
.sc-footer a { color: var(--text-secondary); text-decoration: none; border-bottom: 1px solid var(--border-strong); }
.sc-footer a:hover { color: var(--accent); border-bottom-color: var(--accent); }
.sc-footer-spacer { flex: 1 1 auto; }
.sc-footer-coffee {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    color: var(--accent) !important;
    font-weight: 600;
    border-bottom: 1px solid var(--accent-dim) !important;
    white-space: nowrap;
}
.sc-footer-coffee:hover { border-bottom-color: var(--accent) !important; }

/* ── Sandbox marker — never ships; makes the preview unmistakable ──────── */
.sc-sandbox {
    font-family: 'Epilogue', sans-serif;
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: .04em;
    text-transform: uppercase;
    color: var(--signal-bad-deep);
    background: var(--signal-warn-subtle);
    border: 1px dashed var(--signal-warn-border);
    border-radius: 6px;
    padding: 0.5rem 0.85rem;
    margin: 0 0 1.6rem 0;
    display: block;
}

/* ── Mobile ────────────────────────────────────────────────────────────── */
@media (max-width: 640px) {
    .sc-hero h1 { font-size: 1.85rem; }
    /* Reclaim the optical inset — 26px of gutter is worth more than alignment
       on a 390px screen. */
    .sc-costs, .sc-ask, .sc-honest, .sc-footer { margin-left: 0; }
    .sc-costs { grid-template-columns: 1fr; }
    .sc-cost + .sc-cost { border-left: none; border-top: 1px solid var(--border); padding-left: 0; }
    .sc-footer-spacer { display: none; }
}
</style>
"""


# ── Builders ─────────────────────────────────────────────────────────────────
def sandbox_banner_html(note: str) -> str:
    """The unmissable "this is a preview" strip. Preview-only by design."""
    return f'<div class="sc-sandbox">Sandbox preview — {html.escape(note)}</div>'


def hero_html() -> str:
    return (
        '<div class="sc-hero">'
        "<h1>Dáil Tracker is free.<br>It is <em>not</em> free to run.</h1>"
        "<p>Every figure on this site is pulled from a public register, cleaned, and "
        "published with its source attached. That work is a person and a server bill, "
        "not a grant. If the site has told you something you could not easily find "
        "elsewhere, you can put a few euro toward keeping it running.</p>"
        "</div>"
    )


def _stat_cells(stats: SupportStats | None) -> list[tuple[str, str, str]]:
    """The three scale figures, each carrying its own provenance.

    Every rounded or composite headline discloses its parts in the sub-label,
    so a reader can check the number instead of taking it on trust — which is
    the whole argument the page is making.
    """
    if stats is None:
        return _UNAVAILABLE_CELLS
    return [
        (
            f"{stats.officials:,}",
            "public officials tracked",
            f"{stats.oireachtas_members} TDs & senators + {stats.judges} judges",
        ),
        (
            f"{stats.sources}",
            "sources watched",
            f"feeds from {stats.publishers} publishers",
        ),
        (
            stats.records_display,
            "records published",
            f"{stats.records:,} rows across {stats.record_datasets} registers",
        ),
    ]


def costs_html(costs: list[tuple[str, str, str]] | None = None) -> str:
    """Three-figure strip. Real counts come from coffee_stats at render time."""
    cells = "".join(
        '<div class="sc-cost">'
        f'<span class="sc-cost-fig">{html.escape(fig)}</span>'
        f'<span class="sc-cost-lab">{html.escape(label)}</span>'
        f'<span class="sc-cost-sub">{html.escape(sub)}</span>'
        "</div>"
        for fig, label, sub in (costs if costs is not None else _stat_cells(load_stats()))
    )
    return f'<div class="sc-costs">{cells}</div>'


def coffee_button_html(url: str = COFFEE_URL, *, label: str = "Buy me a coffee") -> str:
    """The button itself — a plain anchor, no third-party script."""
    return (
        f'<a class="sc-btn" href="{html.escape(url, quote=True)}" '
        'target="_blank" rel="noopener noreferrer">'
        '<span class="sc-icon">local_cafe</span>'
        f"<span>{html.escape(label)}</span>"
        "</a>"
    )


def ask_html(url: str = COFFEE_URL) -> str:
    return (
        '<div class="sc-ask">'
        "<h2>Buy me a coffee</h2>"
        "<p>One-off, any amount, no account needed. Payment is handled entirely by "
        "Buy&nbsp;Me&nbsp;a&nbsp;Coffee — this site never sees your card details, your "
        "name, or your email, and sets no tracking cookie on you for clicking.</p>"
        f'<div class="sc-btn-row">{coffee_button_html(url)}</div>'
        '<p class="sc-btn-note">There is no membership tier and no subscription. '
        "One cup, whenever you feel like it.</p>"
        "</div>"
    )


def honesty_html() -> str:
    """The panel that protects the provenance position. Do not cut this."""
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
    lis = "".join(
        f"<li><strong>{html.escape(head)}</strong> {html.escape(body)}</li>" for head, body in items
    )
    return f'<div class="sc-honest"><h2>What your money does not buy</h2><ul>{lis}</ul></div>'


def site_footer_html(
    *,
    support_path: str = SUPPORT_PATH,
    last_refresh: str | None = None,
) -> str:
    """The app-level footer strip.

    In the real app this renders in utility/app.py AFTER pg.run(), outside the
    per-page subtree, so it stays mounted across navigation — the same reason
    inject_css() and install_spa_links() sit at app level.

    The coffee link points at the internal /support page (not straight out to
    Buy Me a Coffee) so the existing cookieless page-view log counts interest
    for free, with no new tracking and no outbound-click JS.
    """
    refresh = (
        f'<span>Data refreshed {html.escape(last_refresh)}</span>' if last_refresh else ""
    )
    return (
        '<div class="sc-footer">'
        '<span class="sc-footer-name">Dáil Tracker</span>'
        "<span>Built from public registers &middot; sources cited on every page</span>"
        f"{refresh}"
        '<span class="sc-footer-spacer"></span>'
        f'<a class="sc-footer-coffee" href="{html.escape(support_path, quote=True)}?from=footer">'
        '<span class="sc-icon" style="font-size:1.05rem">local_cafe</span>'
        "<span>Support this site</span></a>"
        "</div>"
    )


def support_page_html(*, sandbox_note: str | None = None) -> str:
    """The whole page body as ONE html string.

    Assembled in one piece on purpose. Streamlit renders each st.markdown call
    into its own container and auto-closes unbalanced tags, so an opening
    ``<div class="sc-wrap">`` in one call does NOT wrap the elements emitted by
    later calls — it closes immediately and the content renders at the full
    `layout="wide"` width instead. Building the body as a single string is the
    only reliable way to hold a max-width column.
    """
    banner = sandbox_banner_html(sandbox_note) if sandbox_note else ""
    return (
        '<div class="sc-wrap">'
        f"{banner}{hero_html()}{costs_html()}{ask_html()}{honesty_html()}"
        "</div>"
    )
