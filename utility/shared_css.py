import streamlit as st


def inject_css() -> None:
    """Shared design system for all Dáil Tracker pages.

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
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Zilla+Slab:wght@400;600;700&family=Epilogue:ital,wght@0,400;0,500;0,600;1,400&family=Material+Symbols+Outlined&display=swap');

        /* ── spa_links click interceptor (utility/ui/spa_links.py) ── */
        /* Zero-height app-level component iframe; drop its element
           container from flow so it adds no gap above the banner.
           display:none iframes still load and run their script. */
        .st-key-_dt_spa_links,
        div[data-testid="stElementContainer"]:has(iframe[title*="dt_spa_links"]) {
            display: none;
        }

        /* ── Site banner ─────────────────────────── */
        /* Sits at the very top of every page, above Streamlit's native
           top nav (st.navigation(position="top") in utility/app.py).
           Native nav handles routing — banner is pure presentation. */
        .site-banner {
            position: relative;
            left: 50%;
            margin-left: -50vw;
            width: 100vw;
            margin-top: -1.5rem;
            margin-bottom: 0.5rem;
            background: #111827;
            border-bottom: 3px solid oklch(51% 0.130 62);
        }
        .site-banner-inner {
            /* Round-3 audit P0-4 fix: previously max-width + margin auto
               left the title centred in viewport coordinates, which on a
               1440-wide screen with the open sidebar (~336px) hid the
               first word "Oireachtas" behind the sidebar rail. Left-anchor
               with padding-left wide enough to clear the sidebar so the
               title is always visible. The band itself still goes
               viewport-to-viewport via the parent's full-bleed trick. */
            max-width: 1340px;
            padding: 1.1rem 2rem 1.1rem 22rem;
            display: flex;
            align-items: baseline;
            gap: 1.25rem;
        }
        @media (max-width: 768px) {
            /* Mobile: sidebar collapses behind a toggle so the heavy
               padding becomes wasted space. Revert to a slim gutter. */
            .site-banner-inner {
                padding: 1.1rem 1rem;
            }
            /* The tagline wraps and clips against the fixed band height on a
               phone; it is pure decoration, so drop it (and its separator)
               below tablet width and let the brand stand alone. */
            .site-banner-sub,
            .site-banner-sep {
                display: none;
            }
        }
        .site-banner-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.35rem;
            font-weight: 700;
            color: #ffffff;
            letter-spacing: -0.02em;
            line-height: 1;
            white-space: nowrap;
        }
        /* The brand title doubles as the home link (standard masthead
           affordance: logo/wordmark → landing page). Same full-reload
           <a href> routing the rest of the app uses for cross-page links;
           `/` is rooted on the hidden Home page in utility/app.py.
           href is "./" not "/": on Streamlit Cloud the app document lives
           in an iframe under /~/+/<page>, so "/" escapes to the hosting
           shell's root (click appears dead) while "./" resolves to the
           app root in both environments (locally "/", on Cloud "/~/+/"). */
        a.site-banner-title,
        a.site-banner-title:visited {
            color: #ffffff;
            text-decoration: none;
        }
        a.site-banner-title:hover,
        a.site-banner-title:focus-visible {
            color: #ffffff;
            text-decoration: underline;
            text-decoration-color: oklch(70% 0.130 62);
            text-underline-offset: 0.35em;
            text-decoration-thickness: 2px;
        }
        .site-banner-sep {
            width: 1px;
            height: 1rem;
            background: rgba(255,255,255,0.2);
            flex-shrink: 0;
            align-self: center;
        }
        .site-banner-sub {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            font-weight: 400;
            color: rgba(255,255,255,0.45);
            letter-spacing: 0.01em;
            line-height: 1;
        }


        /* ── Masthead: brand band on top, native top-nav beneath ── */
        /* st.navigation(position="top") renders the cross-page nav inside
           the header toolbar, which Streamlit pins absolute at top:0. We
           paint it #111827 and push it down by the brand-band height so
           the "Oireachtas Explorer" .site-banner reads as the top row and
           the nav row sits directly under it — one dark masthead. Routing
           stays Streamlit's; this is pure presentation. */
        header[data-testid="stHeader"],
        [data-testid="stToolbar"] {
            background: #111827 !important;
            border: none !important;
            box-shadow: none !important;
        }
        /* Drop the nav row beneath the brand band (header is absolute, so
           shift its top). 56px butts it flush against the brand band's
           bottom (~57px) with a hair of overlap — the header is opaque, so
           no off-white body gap shows between the two bars. The amber
           masthead rule rides the nav's foot. */
        header[data-testid="stHeader"] {
            top: 56px !important;
            border-bottom: 3px solid var(--accent) !important;
        }
        /* Hide dev chrome but keep the nav + sidebar toggle. Deploy and
           the hamburger menu sit OUTSIDE stToolbarActions, so name them
           explicitly. */
        [data-testid="stToolbarActions"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        [data-testid="stAppDeployButton"],
        [data-testid="stMainMenu"] {
            display: none !important;
        }
        .main .block-container {
            padding-top: 0 !important;
        }
        /* Pin the brand band to the viewport top so the masthead stays put
           while the page scrolls. The native nav row is already pinned
           (absolute, top:56 — its offset parent doesn't scroll), but the
           brand band lives INSIDE the scrolling main container, so on its
           own it scrolled away and left page content showing above the
           still-pinned nav row. position:fixed pins it independently of the
           scroll container (sticky can't grip here — the banner's parent is
           no taller than the banner). Content padding-top below replaces the
           in-flow spacing this used to provide. */
        .site-banner {
            position: fixed !important;
            top: 0 !important;
            left: 0 !important;
            width: 100vw !important;
            margin: 0 !important;
            z-index: 1 !important;
            border-bottom: none !important;
        }
        /* Masthead is now fully out of flow (fixed brand band + absolute nav
           row). Pad the main content so its first element clears the ~120px
           masthead instead of hiding beneath it. */
        [data-testid="stMainBlockContainer"] {
            padding-top: 128px !important;
        }
        /* Sidebar collapse/expand chevron lives in the now-dark header —
           lighten it so it stays visible against #111827. */
        header[data-testid="stHeader"] [data-testid="stIconMaterial"] {
            color: rgba(255,255,255,0.85) !important;
        }
        /* Force Material Symbols ligature activation on every Streamlit
           icon span. Streamlit's own emotion stylesheet ships the font
           family but not `font-feature-settings: 'liga'`; without it, the
           browser renders the literal ligature text ("keyboard_arrow_right",
           "person", "calendar_today" …) instead of the icon glyph, which
           looks like leaked function names across every page. Pair `liga`
           with `clip` + `width:1em` so the literal text — visible for the
           one frame before the font loads — never overflows the icon box
           into adjacent layout. */
        [data-testid="stIconMaterial"] {
            font-feature-settings: 'liga' !important;
            -webkit-font-feature-settings: 'liga' !important;
            text-rendering: optimizeLegibility !important;
            overflow: hidden !important;
            width: 1em !important;
            white-space: nowrap !important;
        }

        /* ── Mobile menu button ─────────────────────────────────── */
        /* Below 768px Streamlit drops the top nav and the sidebar drawer
           becomes the only navigation, opened by stExpandSidebarButton —
           by default a bare 28px » chevron that nothing identifies as a
           menu. Dress it as a labelled pill ("☰ Menu") with a ~44px touch
           target so first-time phone users can actually find the nav.
           Desktop never sees it (hide_sidebar hides it >=768px). */
        @media (max-width: 767.98px) {
            [data-testid="stExpandSidebarButton"] {
                display: inline-flex !important;
                align-items: center !important;
                gap: 0.45rem !important;
                width: auto !important;
                min-height: 2.6rem !important;
                padding: 0 1rem 0 0.8rem !important;
                border: 1px solid rgba(255,255,255,0.35) !important;
                border-radius: 999px !important;
                background: rgba(255,255,255,0.07) !important;
            }
            [data-testid="stExpandSidebarButton"]::after {
                content: "Menu";
                font-family: 'Epilogue', sans-serif;
                font-size: 0.85rem;
                font-weight: 700;
                letter-spacing: 0.05em;
                color: rgba(255,255,255,0.92);
            }
            /* Swap the » glyph for the universal hamburger. The inner icon
               span keeps the width:1em clip rule above, so zero out the
               ligature text and draw the ☰ via ::before at its own size. */
            [data-testid="stExpandSidebarButton"] [data-testid="stIconMaterial"] {
                font-size: 0 !important;
                width: auto !important;
                overflow: visible !important;
            }
            [data-testid="stExpandSidebarButton"] [data-testid="stIconMaterial"]::before {
                content: "menu";
                font-family: 'Material Symbols Outlined';
                font-size: 1.3rem;
                line-height: 1;
                font-feature-settings: 'liga';
                -webkit-font-feature-settings: 'liga';
                color: rgba(255,255,255,0.92);
            }
        }

        /* ── Native top-nav links ───────────────────────────────── */
        [data-testid="stTopNavLink"] {
            background: transparent !important;
            border-radius: 2px !important;
            padding-left: 0.35rem !important;
            padding-right: 0.35rem !important;
        }
        /* Collapse the whole icon slot (the first <span>, not just the
           glyph) so each link is ~30px narrower. The label is the
           second <span>. */
        [data-testid="stTopNavLink"] > span:first-child {
            display: none !important;
        }
        /* The toolbar reserves a ~200px slot on the right for the
           Deploy/menu/status chrome, which squeezed the nav and forced
           an early "5 more" overflow with empty space beside it. That
           chrome is hidden, so reclaim the slot — all 11 sections then
           fit on one row. */
        [data-testid="stToolbar"] > div > div:last-child {
            display: none !important;
        }
        [data-testid="stTopNavLink"] [data-testid="stMarkdownContainer"] p,
        [data-testid="stTopNavSection"] [data-testid="stMarkdownContainer"] p {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.82rem !important;
            font-weight: 600 !important;
            color: rgba(255,255,255,0.72) !important;
            letter-spacing: 0.01em !important;
            margin: 0 !important;
        }
        [data-testid="stTopNavLink"]:hover {
            background: rgba(255,255,255,0.08) !important;
        }
        [data-testid="stTopNavLink"]:hover [data-testid="stMarkdownContainer"] p {
            color: #ffffff !important;
        }
        /* Active page → amber underline + lightened amber label. */
        [data-testid="stTopNavLink"][aria-current="page"] {
            background: transparent !important;
            border-radius: 0 !important;
            border-bottom: 2px solid var(--accent) !important;
        }
        [data-testid="stTopNavLink"][aria-current="page"] [data-testid="stMarkdownContainer"] p {
            color: oklch(72% 0.14 66) !important;
        }
        /* "5 more" overflow trigger picks up the same link styling. */
        [data-testid="stTopNavSection"] {
            background: transparent !important;
            border-radius: 2px !important;
        }
        [data-testid="stTopNavSection"]:hover {
            background: rgba(255,255,255,0.08) !important;
        }

        /* ── Sidebar nav links ───────────────────────────────────── */
        [data-testid="stSidebarNav"] a {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.82rem !important;
            font-weight: 600 !important;
            color: var(--text-secondary) !important;
            padding: 0.38rem 0.75rem !important;
            border-radius: 2px !important;
            /* flex (not block): keeps the Material icon on the same row as
               the label — block stacked them and doubled each item's height
               in the mobile nav drawer. */
            display: flex !important;
            align-items: center !important;
            gap: 0.5rem !important;
            letter-spacing: 0.01em !important;
            text-decoration: none !important;
            transition: background 80ms ease, color 80ms ease !important;
        }
        [data-testid="stSidebarNav"] a:hover {
            background: var(--surface-deep) !important;
            color: var(--text-primary) !important;
        }
        [data-testid="stSidebarNav"] a[aria-current="page"] {
            background: var(--accent-subtle) !important;
            color: var(--accent) !important;
            border-left: 2px solid var(--accent) !important;
            padding-left: calc(0.75rem - 2px) !important;
        }
        [data-testid="stSidebarNav"] {
            padding: 0.5rem 0 !important;
        }
        [data-testid="stSidebarNav"] a span[class*="material-symbols"],
        [data-testid="stSidebarNav"] a [data-testid="stIconMaterial"] {
            color: var(--accent) !important;
            font-size: 1.25rem !important;
            font-variation-settings: 'FILL' 1, 'wght' 500 !important;
        }
        [data-testid="stSidebarNav"] a[aria-current="page"] span[class*="material-symbols"],
        [data-testid="stSidebarNav"] a[aria-current="page"] [data-testid="stIconMaterial"] {
            color: var(--text-primary) !important;
        }

        :root {
            --bg:             oklch(97.5% 0.004 75);
            --surface:        oklch(94%   0.007 75);
            --surface-deep:   oklch(90%   0.010 75);
            --border:         oklch(85%   0.008 75);
            --border-strong:  oklch(72%   0.010 75);
            --text-primary:   oklch(18%   0.008 75);
            --text-secondary: oklch(44%   0.010 75);
            --text-meta:      oklch(52%   0.012 75);
            --accent:         oklch(51%   0.130 62);
            --accent-subtle:  oklch(95%   0.055 72);
            --accent-dim:     oklch(86%   0.040 72);
            --new-bg:         oklch(94%   0.045 145);
            --removed-bg:     oklch(94%   0.030  22);

            /* ── Signal tokens (good/bad semantic pairs) ──────────────────
               Replace ad-hoc Tailwind hexes (#1d4ed8/#3b82f6/#c2410c/...).
               Tinted slightly toward the warm neutral hue for cohesion.
               PRODUCT.md documents these as the canonical good/bad palette. */
            --signal-good:         oklch(45%   0.150 250);  /* deep blue   ≈ #1d4ed8 */
            --signal-good-mid:     oklch(60%   0.180 250);  /* mid blue    ≈ #3b82f6 */
            --signal-good-border:  oklch(78%   0.110 250);  /* light blue  ≈ #93c5fd */
            --signal-good-subtle:  oklch(96%   0.025 250);  /* tint        ≈ #eff6ff */
            --signal-good-deep:    oklch(35%   0.150 255);  /* navy        ≈ #1e40af */

            --signal-bad:          oklch(50%   0.160  40);  /* burnt orange ≈ #c2410c */
            --signal-bad-mid:      oklch(67%   0.180  45);  /* mid orange   ≈ #f97316 */
            --signal-bad-border:   oklch(80%   0.110  60);  /* light orange ≈ #fdba74 */
            --signal-bad-subtle:   oklch(96%   0.030  60);  /* tint         ≈ #fff7ed */
            --signal-bad-deep:     oklch(45%   0.160  35);  /* deep rust    ≈ #9a3412 */

            /* Round-3 audit P3 fix: amber "warn" tokens for EU-derived
               legislation badges + similar callouts that need a neutral
               warning shade distinct from the alarming signal-bad red. */
            --signal-warn-subtle:  oklch(94%   0.060  90);  /* amber tint   ≈ #fef3c7 */
            --signal-warn-border:  oklch(82%   0.130  85);  /* amber border ≈ #fcd34d */
            --signal-warn-deep:    oklch(40%   0.120  60);  /* amber deep   ≈ #92400e */

            /* ── Neutral ink ramp (warm-gray, hue 75) ─────────────────────
               Exact values that were previously inlined across components.
               Tokenised so the gray scale has one source of truth; each
               holds the identical literal it replaced (zero visual change). */
            --ink-strong:  oklch(25% 0.012 75);   /* dark heading / value on cards */
            --ink-700:     oklch(28% 0.012 75);   /* slightly lighter dark text   */
            --ink-muted:   oklch(62% 0.008 75);   /* muted label / secondary value */

            /* ── Vote-outcome colours (separate from the blue/orange signal
               family): carried = green, lost = red. Previously inlined in the
               vote tables and outcome labels. */
            --vote-carried: oklch(38% 0.130 145);
            --vote-lost:    oklch(45% 0.180 30);

            /* ── Literal sRGB blue/orange ramp ────────────────────────────
               The exact Tailwind hexes used by a number of components
               (interests pills, attendance heads, calm-blue cards, etc).
               Kept as literals — and NOT folded into the oklch --signal-*
               family — because that family is already used in 50+ places at
               its own (very slightly different) oklch values, so unifying
               would shift those. The oklch --signal-* tokens remain the
               preferred semantic set; migrate a component here to --signal-*
               only when an imperceptible 1-3% colour shift is acceptable.
               Note: the dataframe/GDG header keeps its own literal #eff6ff
               (it mirrors .streamlit/config.toml) and is intentionally not
               wired to --blue-050. */
            --blue-700:   #1d4ed8;
            --blue-500:   #3b82f6;
            --blue-300:   #93c5fd;
            --blue-800:   #1e40af;
            --blue-050:   #eff6ff;
            --orange-700: #c2410c;
            --orange-500: #f97316;
            --orange-300: #fdba74;
            --orange-900: #9a3412;
            --orange-050: #fff7ed;
        }

        /* Reusable EU-derived badge — use on any chip / signal that
           should read as "regulated by an EU instrument". Replaces the
           inline-style amber blocks scattered across SI-related code. */
        .signal-eu {
            display: inline-flex;
            align-items: center;
            background: var(--signal-warn-subtle);
            border: 1px solid var(--signal-warn-border);
            color: var(--signal-warn-deep);
            border-radius: 2px;
            padding: 0.1rem 0.45rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            margin-left: 0.25rem;
        }

        /* Visually-hidden text for screen-readers (captions, hidden col headers). */
        .sr-only {
            position: absolute !important;
            width: 1px !important; height: 1px !important;
            padding: 0 !important; margin: -1px !important;
            overflow: hidden !important; clip: rect(0, 0, 0, 0) !important;
            white-space: nowrap !important; border: 0 !important;
        }

        html, body, .stApp,
        p, li, label, input, select, textarea,
        button, div.stMarkdown {
            font-family: 'Epilogue', -apple-system, sans-serif !important;
        }
        .stApp { color: var(--text-primary); background-color: var(--bg) !important; }

        .main .block-container {
            padding-bottom: 4rem;
            max-width: 1300px;
        }

        /* ── Sidebar ─────────────────────────────── */
        [data-testid="stSidebar"] {
            background-color: var(--surface) !important;
            border-right: 1px solid var(--border) !important;
        }
        [data-testid="stSidebar"] > div:first-child { padding-top: 1.75rem; }
        [data-testid="stSidebarContent"] { padding: 0 1rem 1rem 1rem; }

        /* ── Headings ────────────────────────────── */
        h1, h2, h3, h4 {
            font-family: 'Zilla Slab', Georgia, serif !important;
            letter-spacing: -0.015em;
        }

        /* ── Text inputs ─────────────────────────── */
        .stTextInput input {
            background: var(--bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.92rem !important;
            color: var(--text-primary) !important;
            padding: 0.45rem 0.75rem !important;
        }
        .stTextInput input:focus {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 2px var(--accent-dim) !important;
        }
        .stTextInput input:focus-visible {
            outline: 2px solid var(--accent) !important;
            outline-offset: 2px !important;
        }

        /* ── Selectbox ───────────────────────────── */
        .stSelectbox > div > div {
            background: var(--bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.92rem !important;
        }

        /* ── Multiselect ─────────────────────────── */
        .stMultiSelect > div > div {
            background: var(--bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
        }

        /* ── Buttons ─────────────────────────────── */
        .stButton > button {
            background: var(--bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            color: var(--text-primary) !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.78rem !important;
            font-weight: 600 !important;
            padding: 0.28rem 0.55rem !important;
            transition: background 100ms ease, border-color 100ms ease !important;
        }
        .stButton > button:hover {
            background: var(--accent-subtle) !important;
            border-color: var(--accent) !important;
        }

        /* ── Back buttons (rendered via components.back_button) ────────
           Stands out against the beige page bg via dark-navy fill +
           pill radius. Scoped by the `dt_back_` key prefix that
           components.back_button enforces, so one rule covers every
           back-to-X button across the app. */
        [class*="st-key-dt_back_"] .stButton > button {
            background: var(--text-primary) !important;
            color: #ffffff !important;
            border: 1px solid var(--text-primary) !important;
            border-radius: 999px !important;
            padding: 0.4rem 1rem !important;
        }
        [class*="st-key-dt_back_"] .stButton > button:hover {
            background: var(--accent) !important;
            border-color: var(--accent) !important;
            color: #ffffff !important;
        }

        /* ── Breadcrumb (components.breadcrumb) ────────────────────────
           Each breadcrumb segment is a tight link-style button scoped
           by the `dt_crumb_` key prefix. Separators (›) and the trailing
           current-page label are inline HTML inside the same row. */
        [class*="st-key-dt_crumb_"] .stButton > button {
            background: transparent !important;
            border: none !important;
            color: var(--accent) !important;
            padding: 0.1rem 0 !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.85rem !important;
            font-weight: 600 !important;
            text-decoration: none !important;
            min-height: 1.6rem !important;
            line-height: 1.6rem !important;
        }
        [class*="st-key-dt_crumb_"] .stButton > button:hover {
            background: transparent !important;
            color: var(--text-primary) !important;
            text-decoration: underline !important;
        }
        .dt-crumb-current {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.85rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.6rem;
        }
        .dt-crumb-sep {
            font-size: 0.95rem;
            color: var(--text-meta);
            line-height: 1.6rem;
            user-select: none;
            display: inline-block;
            text-align: center;
        }
        /* Tighten the row that holds a breadcrumb so it reads as one line. */
        div[data-testid="stHorizontalBlock"]:has(> div .dt-crumb-row-marker) {
            margin-bottom: 0.6rem;
            align-items: center;
        }

        /* ── Promoted CTA button (st-key-dt_cta_*) ─────────────────────
           Used for primary actions like "Explore all revolving door
           cases →". Bolder than .stButton default so it reads as a
           call-to-action against the amber callout background. */
        [class*="st-key-dt_cta_"] .stButton > button {
            background: var(--text-primary) !important;
            color: #ffffff !important;
            border: 1px solid var(--text-primary) !important;
            border-radius: 999px !important;
            padding: 0.45rem 1.1rem !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.85rem !important;
            font-weight: 700 !important;
            letter-spacing: 0.02em !important;
        }
        [class*="st-key-dt_cta_"] .stButton > button:hover {
            background: var(--accent) !important;
            border-color: var(--accent) !important;
            color: #ffffff !important;
        }

        /* ── Download button ─────────────────────── */
        .stDownloadButton > button {
            background: var(--text-primary) !important;
            color: var(--bg) !important;
            border: none !important;
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.78rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.05em !important;
            text-transform: uppercase !important;
            padding: 0.4rem 1rem !important;
        }
        .stDownloadButton > button:hover { opacity: 0.82 !important; }

        /* ── Radio ───────────────────────────────── */
        div[data-testid="stRadio"] > label {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.07em !important;
            text-transform: uppercase !important;
            color: var(--text-meta) !important;
            margin-bottom: 0.35rem !important;
        }
        div[data-testid="stRadio"] > div {
            flex-direction: row !important;
            flex-wrap: wrap !important;
            gap: 0.35rem !important;
        }
        div[data-testid="stRadio"] > div > label {
            background: var(--surface) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            padding: 0.22rem 0.65rem !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.85rem !important;
            font-weight: 600 !important;
            color: var(--text-secondary) !important;
            cursor: pointer !important;
            transition: all 90ms ease !important;
        }
        div[data-testid="stRadio"] > div > label:has(input:checked) {
            background: var(--accent) !important;
            color: var(--bg) !important;
            border-color: var(--accent) !important;
        }

        /* ── Expander ────────────────────────────── */
        .stExpander {
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            background: var(--bg) !important;
            margin-bottom: 0.4rem !important;
        }
        .stExpander summary p {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.78rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.06em !important;
            text-transform: uppercase !important;
            color: var(--text-secondary) !important;
            margin: 0 !important;
        }
        .stExpander summary { padding: 0.6rem 0.9rem !important; align-items: center !important; }
        .stExpander summary:hover { background: var(--surface) !important; }
        details[open] > summary { background: var(--surface) !important; }
        details[open] > summary p { color: var(--text-primary) !important; }

        /* ── Checkbox ────────────────────────────── */
        .stCheckbox > label {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            color: var(--text-secondary) !important;
        }

        /* ── Divider ─────────────────────────────── */
        hr {
            border: none !important;
            border-top: 1px solid var(--border) !important;
            margin: 1.25rem 0 !important;
        }

        /* ── Alerts ──────────────────────────────── */
        .stAlert {
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.88rem !important;
        }

        /* ── Shared custom components ────────────── */
        /* (The sidebar-shell header/subtitle/provenance/divider classes were
           removed with their helpers after the sidebar→filter-bar migration;
           .sidebar-label stays — the member pickers still render it.) */
        .sidebar-label {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin: 0.85rem 0 0.35rem 0;
        }
        /* ── Main-panel filter bar (sidebar → filter-bar migration) ──
           Inline, hairline-rule treatment under the page hero. Replaces the
           per-page sidebar filter stack. .dt-field-label is the same token
           as .sidebar-label but reset for the main panel; the row is scoped
           via a height:0 marker dropped in the first column. */
        .dt-field-label {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin: 0 0 0.35rem 0;
        }
        .dt-filterbar-marker { height: 0; margin: 0; overflow: hidden; }
        /* Bottom-align controls of unequal height (date_input vs selectbox). */
        [data-testid="stHorizontalBlock"]:has(.dt-filterbar-marker) {
            align-items: flex-end;
            margin-top: 0.25rem;
        }
        .dt-filterbar-rule {
            border: none;
            border-top: 1px solid var(--border);
            margin: 0.9rem 0 1.15rem;
        }
        /* Mobile: Streamlit collapses columns to full width on its own, but
           pin it explicitly so long controls never force horizontal overflow
           (the legislation-audit mobile-strip clip). */
        @media (max-width: 640px) {
            [data-testid="stHorizontalBlock"]:has(.dt-filterbar-marker)
                [data-testid="stColumn"] {
                width: 100% !important;
                flex: 1 1 100% !important;
                min-width: 0 !important;
            }
        }
        .section-rule {
            border: none;
            border-top: 2px solid var(--text-primary);
            margin: 0 0 1.5rem 0;
        }
        .stat-strip {
            display: flex;
            flex-wrap: wrap;
            gap: 1.25rem 2.5rem;
            padding: 1rem 0;
            border-top: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
            margin: 1rem 0 1.75rem 0;
        }
        /* Mobile: 4-column stat strips were overflowing the 390px viewport
           and clipping the rightmost stat off-screen. Switch to a 2-up
           grid so all stats stay visible. */
        @media (max-width: 640px) {
            .stat-strip {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 1rem 1.5rem;
            }
            .stat-strip > * {
                min-width: 0;
            }
        }
        .stat-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.65rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1;
        }
        .stat-lbl {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin-top: 0.2rem;
        }
        /* Acronym glossary strip — small, secondary, sits under the hero so
           first-time citizens can read TD/DPO/TAA without Googling.
           Journalists ignore it; it's not loud enough to compete with data. */
        .dt-glossary-strip {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem 1.25rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            color: var(--text-secondary);
            padding: 0.5rem 0 0.85rem;
            border-bottom: 1px solid var(--border);
            margin: 0 0 1rem;
        }
        .dt-glossary-term b {
            color: var(--text-primary);
            font-weight: 700;
            letter-spacing: 0.02em;
            margin-right: 0.3rem;
        }

        /* Full glossary page — definition-list with two-column row layout.
           Term left, definition right. Lots of breathing room, journalistic. */
        .dt-glossary-list {
            margin: 0.5rem 0 2rem;
            padding: 0;
        }
        .dt-glossary-row {
            display: grid;
            grid-template-columns: minmax(150px, 220px) 1fr;
            gap: 1.5rem;
            padding: 0.8rem 0;
            border-top: 1px solid var(--border);
            align-items: baseline;
        }
        .dt-glossary-row:last-child {
            border-bottom: 1px solid var(--border);
        }
        .dt-glossary-row-term {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.1rem;
            font-weight: 700;
            color: var(--text-primary);
            margin: 0;
            line-height: 1.2;
        }
        .dt-glossary-row-def {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.92rem;
            line-height: 1.55;
            color: var(--text-secondary);
            margin: 0;
            max-width: 70ch;
        }
        @media (max-width: 640px) {
            .dt-glossary-row {
                grid-template-columns: 1fr;
                gap: 0.3rem;
            }
        }

        /* Long-form explainer blocks on the glossary page. Each block has a
           serif title and a constrained measure of Epilogue prose. Ordered
           lists keep the legislative-stages numbering aligned. */
        .dt-explainer {
            padding: 1.1rem 0 0.4rem;
            border-top: 1px solid var(--border);
        }
        .dt-explainer:first-of-type {
            border-top: none;
            padding-top: 0.4rem;
        }
        .dt-explainer-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.35rem;
            font-weight: 700;
            color: var(--text-primary);
            margin: 0 0 0.5rem 0;
            line-height: 1.2;
        }
        .dt-explainer-body {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.92rem;
            line-height: 1.6;
            color: var(--text-secondary);
            max-width: 70ch;
        }
        .dt-explainer-body p {
            margin: 0 0 0.7rem 0;
        }
        .dt-explainer-body p:last-child {
            margin-bottom: 0;
        }
        .dt-explainer-body strong {
            color: var(--text-primary);
            font-weight: 600;
        }
        .dt-explainer-body ol {
            margin: 0.3rem 0 0.9rem 0;
            padding-left: 1.4rem;
        }
        .dt-explainer-body ol li {
            margin-bottom: 0.45rem;
        }
        .dt-explainer-body ol li:last-child {
            margin-bottom: 0;
        }

        /* Optional secondary label below the metric, used for comparative
           context: "rank 87 of 174", "12 below median", etc. Tame size,
           same colour as meta but normal-case. */
        .stat-sub {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            font-weight: 500;
            color: var(--text-secondary);
            margin-top: 0.15rem;
        }
        .td-name {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 2.1rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.1;
            margin: 0 0 0.35rem 0;
        }
        .td-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.88rem;
            color: var(--text-meta);
            font-weight: 500;
            margin-bottom: 0.75rem;
        }
        /* ── Profile header with avatar ─────────────────────────────────── */
        .dt-profile-header {
            display: flex;
            align-items: flex-start;
            gap: 1.1rem;
            margin: 0.25rem 0 0.5rem 0;
        }
        .dt-profile-avatar-col {
            flex-shrink: 0;
            display: flex;
            flex-direction: column;
            align-items: center;
            width: 96px;
        }
        .dt-profile-avatar {
            width: 96px;
            height: 96px;
            border-radius: 50%;
            object-fit: cover;
            object-position: center top;
            border: 2px solid #ffffff;
            box-shadow: 0 1px 4px rgba(0,0,0,0.12), 0 0 0 1px rgba(0,0,0,0.06);
            background: #f3f4f6;
            display: block;
        }
        .dt-profile-initials {
            width: 96px;
            height: 96px;
            border-radius: 50%;
            background: #e5e7eb;
            color: #4b5563;
            font-family: 'Epilogue', sans-serif;
            font-size: 2.1rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            display: flex;
            align-items: center;
            justify-content: center;
            border: 2px solid #ffffff;
            box-shadow: 0 1px 4px rgba(0,0,0,0.12), 0 0 0 1px rgba(0,0,0,0.06);
        }
        .dt-profile-avatar-credit {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.66rem;
            color: var(--text-meta);
            text-align: center;
            margin: 0.4rem 0 0 0;
            line-height: 1.35;
            max-width: 110px;
            word-wrap: break-word;
        }
        .dt-profile-avatar-credit a {
            color: var(--text-meta);
            text-decoration: underline;
            text-decoration-thickness: 1px;
            text-underline-offset: 2px;
        }
        .dt-profile-avatar-credit a:hover { color: var(--accent); }
        .dt-profile-avatar-empty {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.66rem;
            color: var(--text-meta);
            text-align: center;
            margin: 0.4rem 0 0 0;
            font-style: italic;
        }
        .dt-profile-meta-col {
            flex: 1;
            min-width: 0;
            padding-top: 0.1rem;
        }
        .dt-profile-meta-col .td-name { margin-bottom: 0.25rem; }
        .dt-profile-meta-col .td-meta { margin-bottom: 0.55rem; }
        @media (max-width: 640px) {
            .dt-profile-header { gap: 0.85rem; }
            .dt-profile-avatar-col { width: 72px; }
            .dt-profile-avatar,
            .dt-profile-initials { width: 72px; height: 72px; }
            .dt-profile-initials { font-size: 1.5rem; }
            .dt-profile-avatar-credit,
            .dt-profile-avatar-empty { max-width: 90px; font-size: 0.6rem; }
        }
        .signal {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            border-radius: 2px;
            margin-right: 0.3rem;
        }
        .signal-accent { background: var(--accent-subtle); color: var(--accent); border: 1px solid var(--accent-dim); }
        .signal-neutral { background: var(--surface); color: var(--text-meta); border: 1px solid var(--border); }
        .signal-dark { background: var(--surface-deep); color: var(--text-secondary); border: 1px solid var(--border-strong); }
        /* .lob-section-heading is an alias — both render identically */
        .section-heading,
        .lob-section-heading {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin: 1.5rem 0 0.6rem 0;
            padding-bottom: 0.35rem;
            border-bottom: 2px solid var(--accent);
        }
        /* Sub-section heading (h3) nested under a .section-heading (h2).
           Visually quieter — lighter rule, smaller top margin — so the
           heading level reads as subordinate, not a sibling section. */
        .section-subheading {
            font-size: 0.66rem;
            margin: 1rem 0 0.45rem 0;
            border-bottom-width: 1px;
            border-bottom-color: var(--border, #d9d2c4);
            color: var(--text-meta);
        }

        /* ── Editorial hero band (main content area) ── */
        .dt-hero {
            background: var(--surface);
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 2px;
            padding: 1.25rem 1.5rem 1rem;
            margin-bottom: 1rem;
        }
        .dt-kicker {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.10em;
            text-transform: uppercase;
            color: var(--accent);
            margin: 0 0 0.3rem 0;
        }
        .dt-dek {
            color: var(--text-secondary);
            font-size: 0.90rem;
            line-height: 1.5;
            margin: 0.3rem 0 0;
        }
        /* ── Finding lede ───────────────────────────────────────────────
           The page's opening findings (components.finding_lede). Replaces
           stat strips app-wide: facts in prose, numbers earned in-sentence.
           Editorial standfirst treatment — larger than body, measure-capped,
           with the accent side-stripe signature. Tokens only. */
        .dt-finding-lede {
            border-left: 3px solid var(--accent);
            padding: 0.15rem 0 0.15rem 0.85rem;
            margin: 0.35rem 0 1.15rem;
            max-width: 52rem;
        }
        .dt-finding-lede p {
            font-size: 1.02rem;
            line-height: 1.6;
            color: var(--text-primary);
            margin: 0 0 0.45rem;
        }
        .dt-finding-lede p:last-of-type { margin-bottom: 0; }
        .dt-finding-lede strong {
            color: var(--ink-strong);
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }
        .dt-lede-source {
            display: block;
            margin-top: 0.4rem;
            font-size: 0.78rem;
            color: var(--text-secondary);
        }
        /* ── Card conduit row ───────────────────────────────────────────
           Quiet official-source links in a card footer
           (components.card_sources_html). Sits below card meta; the
           .dt-source-link children already carry the ↗ glyph + focus ring. */
        .dt-card-sources {
            display: flex;
            flex-wrap: wrap;
            gap: 0.25rem 0.9rem;
            margin-top: 0.45rem;
            padding-top: 0.4rem;
            border-top: 1px solid var(--border);
            font-size: 0.78rem;
        }
        /* ── Main-panel search kicker ───────────────────────────────────
           Uppercase mini-label sitting above components.main_member_jump
           (and member_overview's inline search row). Same typographic
           weight as .dt-kicker but with extra top margin so it visually
           separates from the hero block above it. */
        .dt-main-search-kicker {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.10em;
            text-transform: uppercase;
            color: var(--accent);
            margin: 1.1rem 0 0.45rem;
        }
        .dt-badge {
            display: inline-flex;
            align-items: center;
            background: var(--surface-deep);
            border: 1px solid var(--border);
            color: var(--text-secondary);
            border-radius: 2px;
            padding: 0.15rem 0.55rem;
            font-size: 0.78rem;
            font-weight: 600;
            font-family: 'Epilogue', sans-serif;
        }
        .dt-badge-landlord { border-color:#dc2626; color:#dc2626; }

        /* Role badges — Minister / TD / Revolving door. Use --signal-* tokens
           so theme tweaks propagate. Override .dt-badge defaults for background,
           border, and text colour while keeping shape/typography. */
        .dt-badge-minister {
            background: var(--signal-good-subtle);
            border-color: var(--signal-good-border);
            color: var(--signal-good-deep);
        }
        .dt-badge-td {
            background: oklch(96% 0.045 80);
            border-color: oklch(82% 0.110 80);
            color: oklch(38% 0.110 60);
        }
        .dt-badge-revolving {
            background: var(--signal-bad-subtle);
            border-color: var(--signal-bad-border);
            color: var(--signal-bad-deep);
            margin-left: 0.35rem;
        }
        .dt-badge-revolving::before {
            content: "⚠";
            margin-right: 0.25rem;
        }

        /* ── Hero meta row + external-link chips ────────────────────────
           Single flex row that carries TD/Minister/Revolving badges AND
           external-link chips (Official profile, Wikipedia, social icons).
           One line, flex-wraps on narrow viewports. The vertical separator
           segments "role/status" (left) from "find online" (right) without
           adding a heavier visual divider. */
        .dt-hero-meta-row {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.35rem 0.5rem;
        }
        .dt-hero-sep {
            display: inline-block;
            width: 1px;
            height: 1rem;
            background: var(--border);
            margin: 0 0.2rem;
        }

        /* Icon chip: round, accent-coloured pill carrying a single glyph
           (or a 2-char tag like "IG"). Sized to match .dt-badge height so
           the row reads as one consistent strip. Hover lifts by 1px and
           tints the background — same micro-interaction as .dt-source-link. */
        .dt-icon-chip {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 1.65rem;
            height: 1.65rem;
            border-radius: 50%;
            background: #ffffff;
            border: 1px solid var(--border);
            color: var(--accent);
            font-family: 'Epilogue', sans-serif;
            font-size: 0.85rem;
            font-weight: 700;
            line-height: 1;
            text-decoration: none;
            transition: background 0.12s ease, border-color 0.12s ease, transform 0.12s ease;
        }
        .dt-icon-chip[data-glyph="IG"] {
            /* Instagram needs a smaller two-char glyph to fit in the same
               circle as the single-letter chips. */
            font-size: 0.66rem;
            letter-spacing: 0.02em;
        }
        /* Audit P3-2: X (Twitter) renders as a deliberate filled-black brand
           chip rather than a bare letter that looks like a placeholder. */
        .dt-icon-chip[data-glyph="X"] {
            background: #14171a;
            border-color: #14171a;
            color: #ffffff;
            font-weight: 800;
        }
        .dt-icon-chip[data-glyph="X"]:hover {
            background: #000000;
            border-color: #000000;
        }
        .dt-icon-chip[data-glyph="B"] {
            /* Bluesky blue */
            background: #1083fe;
            border-color: #1083fe;
            color: #ffffff;
        }
        .dt-icon-chip[data-glyph="B"]:hover {
            background: #0d6dd1;
            border-color: #0d6dd1;
        }
        .dt-icon-chip[data-glyph="f"] {
            /* Facebook blue */
            background: #1877f2;
            border-color: #1877f2;
            color: #ffffff;
        }
        .dt-icon-chip[data-glyph="f"]:hover {
            background: #1465c8;
            border-color: #1465c8;
        }
        .dt-icon-chip:hover {
            background: var(--surface-deep);
            border-color: var(--accent);
            transform: translateY(-1px);
        }
        .dt-icon-chip:focus-visible {
            outline: 2px solid var(--accent);
            outline-offset: 2px;
        }

        /* ── Callout / empty state / TODO ─────────── */
        .dt-callout {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 2px;
            padding: 0.9rem 1rem;
            color: var(--text-secondary);
            font-size: 0.90rem;
            line-height: 1.5;
            margin: 0.5rem 0;
        }

        /* ── Vote outcome labels ──────────────────── */
        .dt-outcome-carried { color: var(--vote-carried); font-weight: 700; }
        .dt-outcome-lost    { color: var(--vote-lost);  font-weight: 700; }
        .dt-outcome-unknown { color: var(--text-meta);     font-weight: 600; }

        /* ── Vote-type table (TD history / division member list) ── */
        .dt-vt-table {
            width: 100%;
            border-collapse: collapse;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.88rem;
            margin: 0.5rem 0 1rem;
        }
        .dt-vt-table th {
            font-size: 0.70rem;
            font-weight: 700;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: var(--text-meta);
            padding: 0.4rem 0.75rem;
            text-align: left;
            border-bottom: 2px solid var(--border);
            white-space: nowrap;
        }
        .dt-vt-table td {
            padding: 0.45rem 0.75rem;
            border-bottom: 1px solid var(--border);
            vertical-align: middle;
        }
        .dt-vt-table tr:last-child td { border-bottom: none; }
        .dt-vt-table tr:hover td { background: var(--surface); }
        .dt-vt-yes  { color: var(--vote-carried); font-weight: 700; white-space: nowrap; }
        .dt-vt-no   { color: var(--vote-lost);  font-weight: 700; white-space: nowrap; }
        .dt-vt-abs  { color: var(--text-meta);      font-weight: 500; white-space: nowrap; }
        .dt-vt-date { color: var(--text-meta);      white-space: nowrap; font-size: 0.82rem; }
        .dt-vt-meta { color: var(--text-meta);      font-size: 0.84rem; }
        .dt-vt-outcome-carried { color: var(--vote-carried); font-size: 0.78rem; font-weight: 600; white-space: nowrap; }
        .dt-vt-outcome-lost    { color: var(--vote-lost);  font-size: 0.78rem; font-weight: 600; white-space: nowrap; }
        .dt-vt-outcome-other   { color: var(--text-meta);     font-size: 0.78rem; }
        /* ── Canonical external-source link ─────────────────────────────
           One rule for every "open the official record on oireachtas.ie /
           lobbying.ie / etc" anchor across the app. The ↗ glyph is appended
           by CSS so callers pass a clean label string.
           Pair with utility/ui/entity_links.source_link_html(). */
        .dt-source-link {
            color: var(--accent, #b04a1a);
            text-decoration: none;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.80rem;
            font-weight: 600;
            white-space: nowrap;
            transition: color 0.12s, text-decoration-color 0.12s;
        }
        /* Quiet developer affordance — "View as JSON" link to the public API.
           Deliberately muted (meta colour, monospace, smaller) so it reads as a
           dev footnote, not a citizen-facing action. */
        .dt-api-link {
            color: var(--text-meta, #6b6356);
            text-decoration: none;
            font-family: 'IBM Plex Mono', ui-monospace, monospace;
            font-size: 0.72rem;
            font-weight: 500;
            white-space: nowrap;
        }
        .dt-api-link:hover {
            color: var(--accent, #b04a1a);
            text-decoration: underline;
            text-underline-offset: 2px;
        }
        .dt-api-footer { margin-top: 1.5rem; text-align: right; }
        .dt-source-link::after {
            content: " ↗";
            display: inline-block;
            margin-left: 0.15rem;
            font-weight: 400;
            transition: transform 0.12s;
        }
        .dt-source-link:hover {
            text-decoration: underline;
            text-decoration-color: var(--accent, #b04a1a);
            text-underline-offset: 2px;
        }
        .dt-source-link:hover::after {
            transform: translate(1px, -1px);
        }
        .dt-source-link:focus-visible {
            outline: 2px solid var(--accent, #b04a1a);
            outline-offset: 2px;
            border-radius: 2px;
        }
        /* Legacy aliases — keep until callers migrate. The ::after rule above
           gives all three the same auto-arrow + hover treatment. */
        .dt-vt-link, .vt-source-link {
            color: var(--accent, #b04a1a);
            text-decoration: none;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.80rem;
            font-weight: 600;
            white-space: nowrap;
        }
        .dt-vt-link:hover, .vt-source-link:hover { text-decoration: underline; }

        /* ── Dataframe (app-wide) ────────────────────────────────────
           Streamlit 1.28+ uses Glide Data Grid, which draws cells on
           <canvas> using --gdg-* CSS custom properties. AG Grid classes
           no longer apply. Primary colours are also set in
           .streamlit/config.toml (dataframeHeaderBackgroundColor,
           dataframeBorderColor) — change them there first.

           CSS variables below let you override per-page if needed,
           and also style the outer wrapper which IS DOM-targetable.

           Colour tokens (match config.toml):
             header bg   → #eff6ff   (dataframeHeaderBackgroundColor)
             border      → #bfdbfe   (dataframeBorderColor)
             header text → #1e40af
             cell bg     → #ffffff   (secondaryBackgroundColor)
        ──────────────────────────────────────────────────────────── */

        /* Outer wrapper — DOM-targetable, always works */
        [data-testid="stDataFrame"] {
            border:        1px solid #bfdbfe !important;
            border-radius: 4px              !important;
            overflow:      hidden           !important;
            box-shadow:    0 1px 6px rgba(0, 0, 0, 0.07) !important;

            /* Override Glide Data Grid CSS variables at container scope.
               GDG reads these via getComputedStyle() for canvas drawing. */
            --gdg-bg-header:           #eff6ff !important;
            --gdg-bg-header-has-focus: #dbeafe !important;
            --gdg-text-header:         #1e40af !important;
            --gdg-border-color:        #bfdbfe !important;
            --gdg-bg-cell:             #ffffff !important;
            --gdg-bg-cell-medium:      #f8fbff !important;
            --gdg-accent-color:        #2563eb !important;
            --gdg-accent-light:        #eff6ff !important;
        }

        /* Header cell DOM wrapper (non-canvas part of GDG header) */
        [data-testid="stDataFrame"] .gdg-c1tqibwd {
            background-color: #eff6ff !important;
        }

        /* ── Navigation arrow button ─────────────────────────────────
           Single source of truth for every → button produced by
           rank_card_row() in ui/components.py.

           SHAPE — change border-radius on one line:
             round rectangle  →  10px   (default)
             pill             →  999px
             circle           →  50%    (also set equal width & height)
             sharp rectangle  →  2px

           PALETTE — four colour tokens below:
             bg              background at rest
             border          border at rest
             color           arrow glyph colour
             hover-*         same three on hover

           The .dt-nav-anchor div is injected by rank_card_row()
           immediately before the button so :has() can scope the rule
           to just those columns without touching any other button.
        ─────────────────────────────────────────────────────────── */

        .dt-nav-anchor { margin-top: 1.1rem; }

        /* Scoped to any stColumn that owns a .dt-nav-anchor */
        [data-testid="stColumn"]:has(.dt-nav-anchor) .stButton > button,
        [data-testid="stColumn"]:has(.dt-nav-anchor) button {
            /* ── Shape ───────────────────────── */
            width:         2.1rem    !important;
            height:        2.1rem    !important;
            padding:       0         !important;
            border-radius: 10px      !important;   /* ← change shape here */

            /* ── Palette ─────────────────────── */
            background:    var(--surface)       !important;
            border:        1.5px solid var(--border-strong) !important;
            color:         var(--text-secondary) !important;

            /* ── Layout ──────────────────────── */
            display:         flex            !important;
            align-items:     center          !important;
            justify-content: center          !important;
            font-size:       1rem            !important;
            font-weight:     500             !important;
            line-height:     1               !important;
            transition:      background 100ms ease, border-color 100ms ease,
                             color 100ms ease !important;
        }

        [data-testid="stColumn"]:has(.dt-nav-anchor) .stButton > button:hover,
        [data-testid="stColumn"]:has(.dt-nav-anchor) button:hover {
            background:   var(--accent-subtle) !important;
            border-color: var(--accent)        !important;
            color:        var(--accent)        !important;
        }

        /* ── Reusable nav button (ui/components.py: nav_button) ───────────
           Canonical square arrow button used beside list cards. Marker div
           ``.dt-nav-btn`` is height:0 — exists only so :has() can scope
           uniform sizing and true vertical centering. Use this instead of
           the legacy dt-nav-anchor for new code.
           ─────────────────────────────────────────────────────────────── */
        .dt-nav-btn { height: 0; margin: 0; }

        /* Force uniform square shape for every nav-button instance, with
           min/max locked so Streamlit's button rendering can't drift. */
        [data-testid="stColumn"]:has(> div .dt-nav-btn) [data-testid="stButton"] > button,
        [data-testid="stColumn"]:has(> div .dt-nav-btn) button {
            width:         2.1rem !important;
            height:        2.1rem !important;
            min-width:     2.1rem !important;
            max-width:     2.1rem !important;
            min-height:    2.1rem !important;
            max-height:    2.1rem !important;
            padding:       0      !important;
            border-radius: 10px   !important;
            background:    var(--surface)            !important;
            border:        1.5px solid var(--border-strong) !important;
            color:         var(--text-secondary)     !important;
            display:         inline-flex !important;
            align-items:     center      !important;
            justify-content: center      !important;
            font-size:       1rem        !important;
            font-weight:     500         !important;
            line-height:     1           !important;
            transition: background 100ms ease, border-color 100ms ease,
                        color 100ms ease !important;
        }
        [data-testid="stColumn"]:has(> div .dt-nav-btn) [data-testid="stButton"] > button:hover,
        [data-testid="stColumn"]:has(> div .dt-nav-btn) button:hover {
            background:   var(--accent-subtle) !important;
            border-color: var(--accent)        !important;
            color:        var(--accent)        !important;
        }
        /* Center the button vertically inside its column so it lines up
           against multi-line cards (legislation bills, committees, etc).
           This works because the column flexes to the height of the sibling
           card column inside the parent stHorizontalBlock. */
        [data-testid="stColumn"]:has(> div .dt-nav-btn) {
            display: flex !important;
            flex-direction: column !important;
            align-items: center !important;
            justify-content: center !important;
        }
        [data-testid="stHorizontalBlock"]:has(.dt-nav-btn) {
            align-items: stretch !important;
        }

        /* ── Info card (ui/components.py: info_card / card_row) ────────────
           Styling anchor only — all dimensions and colours are set as
           inline styles from Python so callers can override per-instance
           without touching CSS.  card_row() pairs one with a nav button.
           ─────────────────────────────────────────────────────────────── */

        .dt-info-card {
            display:    block;
            transition: border-left-color 0.12s, box-shadow 0.12s;
        }
        .dt-info-card:hover {
            box-shadow: 0 2px 8px rgba(0,0,0,0.09) !important;
        }

        /* When a dt-name-card is nested inside an info card, strip its own
           border/shadow so the outer card frame is the sole visual container. */
        .dt-info-card .dt-name-card {
            border:        none !important;
            box-shadow:    none !important;
            border-radius: 0    !important;
            background:    transparent !important;
            padding:       0    !important;
        }

        /* Card-row layout — card column fills available space, button column
           shrinks to content (the → button).  Mirrors the leg-bill-card rules. */
        [data-testid="stHorizontalBlock"]:has(.dt-info-card) {
            gap:            0.35rem !important;
            margin-bottom:  0.3rem  !important;
            align-items:    stretch !important;
        }
        [data-testid="stHorizontalBlock"]:has(.dt-info-card)
            [data-testid="stColumn"]:first-child {
            flex:      1 1 auto !important;
            min-width: 0        !important;
        }
        [data-testid="stHorizontalBlock"]:has(.dt-info-card)
            [data-testid="stColumn"]:last-child {
            flex:  0 0 auto !important;
            width: auto     !important;
        }

        /* ── Success / calm-blue theme ───────────────────────────────
           Use for positive, affirming data: attendance streaks,
           high counts, achievements. Calming rather than celebratory.
           ─────────────────────────────────────────────────────────── */

        /* Standalone metric badge (e.g. "29 days attended") */
        .dt-success-badge {
            display: inline-flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 4px 10px;
            border-radius: 12px;
            background: var(--blue-050);
            border: 1px solid #bfdbfe;
            text-align: center;
        }
        .dt-success-num {
            font-size: 1.25rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            line-height: 1;
            color: var(--blue-700);
        }
        .dt-success-lbl {
            font-size: 0.58rem;
            font-weight: 600;
            color: var(--blue-500);
            line-height: 1.4;
        }

        /* Card / panel container (e.g. Hall of Fame card) */
        .dt-success-card {
            background: var(--blue-050);
            border: 1px solid #bfdbfe;
            border-left: 5px solid #2563eb;
            border-radius: 8px;
        }

        /* Inline pill / tag (e.g. inside a rank card) */
        .dt-success-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            background: var(--blue-050);
            border: 1px solid #bfdbfe;
            border-radius: 999px;
            padding: 0.1rem 0.55rem;
            font-size: 0.76rem;
            font-weight: 600;
            color: var(--blue-700);
        }

        /* Stat number inside a stat-strip or summary block */
        .dt-success-stat-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.65rem;
            font-weight: 700;
            color: var(--signal-good);
            line-height: 1;
        }

        /* ── Rank card component (ui/components.py: rank_card_row) ──── */
        .int-rank-card {
            display: inline-flex;
            align-items: flex-start;
            gap: 0.9rem;
            padding: 0.35rem 0.75rem;
            border: 1px solid var(--border);
            border-radius: 12px;
            background: #ffffff;
            margin-bottom: 0.35rem;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
            transition: border-color 0.15s;
            width: fit-content;
            max-width: 100%;
        }

        /* Collapse the entire row so the → button sits right next to the card */
        [data-testid="stHorizontalBlock"]:has(.int-rank-card) {
            width: fit-content !important;
            max-width: 100% !important;
            gap: 0.4rem !important;
        }
        [data-testid="stHorizontalBlock"]:has(.int-rank-card) [data-testid="stColumn"] {
            width: auto !important;
            flex: 0 0 auto !important;
            min-width: 0 !important;
        }
        .int-rank-card:hover { border-color: var(--accent); }
        .int-rank-num {
            font-size: 1.5rem;
            font-weight: 800;
            color: var(--border-strong);
            min-width: 2.4rem;
            text-align: right;
            line-height: 1.2;
            padding-top: 0.1rem;
            letter-spacing: -0.03em;
        }
        .int-rank-num-top { color: var(--accent); }
        .int-rank-body { flex: 1; min-width: 0; }
        .int-rank-name {
            margin: 0 0 0.15rem;
            font-size: 1.02rem;
            font-weight: 700;
            font-family: 'Zilla Slab', Georgia, serif;
            color: var(--text-primary);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .int-rank-meta {
            margin: 0 0 0.2rem;
            font-size: 0.8rem;
            color: var(--text-meta);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .int-rank-stats {
            margin: 0 0 0.35rem;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.35rem;
        }
        .int-stat-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            background: var(--surface-deep);
            border: 1px solid var(--border);
            border-radius: 999px;
            padding: 0.1rem 0.5rem;
            font-size: 0.76rem;
            font-weight: 600;
            color: var(--text-meta);
        }
        .int-stat-pill-accent { border-color: var(--accent); color: var(--accent); background: var(--accent-subtle); }
        /* Cross-page profile link rendered alongside int-stat-pill items. */
        .int-stat-pill-link {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            border: 1px solid var(--border);
            border-radius: 999px;
            padding: 0.1rem 0.5rem;
            font-size: 0.76rem;
            font-weight: 600;
            text-decoration: none;
            margin-left: 0.25rem;
        }
        .int-stat-pill-link:hover {
            border-color: var(--accent, #b04a1a);
            color: var(--accent, #b04a1a) !important;
        }
        .int-pill-decl    { background:var(--blue-050); border-color:var(--blue-300); color:#1e3a8a; }
        .int-pill-company { background:#f0fdfa; border-color:#5eead4; color:#0e6655; }
        .int-pill-prop    { background:#fffbeb; border-color:#fbbf24; color:#78350f; }
        .int-pill-shares  { background:#f5f3ff; border-color:#c4b5fd; color:#4c1d95; }
        .int-pill-owner   { background:#ecfdf5; border-color:#6ee7b7; color:#065f46; }
        .int-highlight-quote {
            margin: 0.1rem 0 0;
            font-size: 0.8rem;
            color: var(--text-meta);
            font-style: italic;
            line-height: 1.5;
            border-left: 2px solid var(--border);
            padding-left: 0.5rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* ══ Canonical member name card ══════════════════════════════
           Single reusable pattern for every ranked-list page.
           Avatar slot is ALWAYS rendered at fixed width (2.25 rem) so
           wiring in Wikidata photos later is a one-line change — no
           layout rework across pages.
           ══════════════════════════════════════════════════════════ */
        .dt-name-card {
            display: flex;
            align-items: center;
            gap: 0.65rem;
            padding: 0.38rem 0.9rem 0.38rem 0.6rem;
            background: #ffffff;
            border: 1px solid rgba(0,0,0,0.08);
            border-left: 3px solid rgba(0,0,0,0.14);
            border-radius: 12px;
            width: 100%;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            transition: border-left-color 0.12s, border-color 0.12s;
        }
        .dt-name-card:hover {
            border-left-color: var(--accent);
            border-color: var(--accent-dim);
        }
        /* Left slot: avatar OR rank number — always reserves the space.
           ``position: relative`` lets the rank overlay (.dt-name-card-rank-overlay)
           anchor to the avatar's bottom-right when both are present. */
        .dt-name-card-left {
            flex-shrink: 0;
            width: 2.75rem;
            display: flex;
            align-items: center;
            justify-content: center;
            position: relative;
        }
        .dt-name-card-avatar {
            width: 2.75rem;
            height: 2.75rem;
            border-radius: 50%;
            object-fit: cover;
            object-position: center top;
            border: 1px solid rgba(0,0,0,0.08);
            background: #f3f4f6;
            box-shadow: 0 1px 2px rgba(0,0,0,0.06);
        }
        /* Rank chip overlaid on the avatar bottom-right corner. Used when a
           card has BOTH a photo AND a rank — previously the rank was hidden
           by the avatar (interests audit P1-2). Crisp ring around the chip
           keeps it readable against the avatar edge. */
        .dt-name-card-rank-overlay {
            position: absolute;
            bottom: -2px;
            right: -2px;
            min-width: 1.4rem;
            height: 1.2rem;
            padding: 0 0.3rem;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: var(--text-primary, #111827);
            color: #ffffff;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.66rem;
            font-weight: 800;
            border-radius: 999px;
            border: 2px solid #ffffff;
            box-shadow: 0 1px 2px rgba(0,0,0,0.15);
            line-height: 1;
            letter-spacing: -0.02em;
        }
        .dt-name-card-rank-overlay-top {
            background: var(--accent, #b8860b);
            color: #ffffff;
        }
        /* Initials chip used when no photo is available */
        .dt-name-card-initials {
            width: 2.75rem;
            height: 2.75rem;
            border-radius: 50%;
            background: #e5e7eb;
            color: #4b5563;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.85rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border: 1px solid rgba(0,0,0,0.08);
        }
        .dt-name-card-rank {
            font-size: 0.78rem;
            font-weight: 800;
            color: #78350f;
            line-height: 1;
            text-align: right;
            width: 100%;
        }
        .dt-name-card-rank-top { color: var(--accent); }
        /* Body */
        .dt-name-card-body { flex: 1; min-width: 0; }
        .dt-name-card-name {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.97rem;
            font-weight: 700;
            color: var(--text-primary);
            margin: 0 0 0.08rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .dt-name-card-meta {
            font-size: 0.76rem;
            color: var(--text-meta);
            margin: 0 0 0.12rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .dt-name-card-pills {
            display: flex;
            flex-wrap: wrap;
            gap: 0.25rem;
            margin-top: 0.08rem;
        }
        /* Right badge — optional metric (days, amount, count) */
        .dt-name-card-badge {
            flex-shrink: 0;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            text-align: center;
            padding: 0.22rem 0.55rem;
            border-radius: 10px;
            min-width: 2.5rem;
        }
        .dt-name-card-badge-metric {
            background: var(--blue-050);
            border: 1px solid #bfdbfe;
        }
        .dt-name-card-badge-num {
            font-size: 1.1rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            color: var(--blue-700);
            line-height: 1;
            display: block;
        }
        .dt-name-card-badge-lbl {
            font-size: 0.56rem;
            font-weight: 600;
            color: var(--blue-500);
            display: block;
        }
        /* Streamlit column override — one rule for every page */
        [data-testid="stHorizontalBlock"]:has(.dt-name-card) {
            gap: 0.35rem !important;
            margin-bottom: 0.25rem !important;
            align-items: stretch !important;
            justify-content: flex-start !important;
        }
        [data-testid="stHorizontalBlock"]:has(.dt-name-card)
            [data-testid="stColumn"]:first-child {
            flex: 1 1 auto !important;
            max-width: 520px !important;
            min-width: 0 !important;
        }
        [data-testid="stHorizontalBlock"]:has(.dt-name-card)
            [data-testid="stColumn"]:last-child {
            flex: 0 0 auto !important;
            width: auto !important;
            display: flex !important;
            flex-direction: column !important;
            align-items: center !important;
            justify-content: center !important;
        }
        /* Inside dt-name-card rows the column itself centers vertically,
           so the legacy margin-top shim becomes a layout offset — null it.
           (Element stays in DOM so :has(.dt-nav-anchor) still matches.) */
        [data-testid="stHorizontalBlock"]:has(.dt-name-card) .dt-nav-anchor {
            margin-top: 0 !important;
            height: 0 !important;
        }

        /* ── Member Overview: card grid + prominent search ──────── */
        .mo-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 0.55rem;
            margin-top: 0.5rem;
        }
        /* Breathing room between the card grid and the pager rendered below it. */
        .mo-browse-pager-spacer {
            height: 1.5rem;
        }

        /* ── Member Overview profile: section nav chip row
           (sits below the hero/stat strip, links to #mo-section-<sid>
           anchors emitted alongside each section heading below).
           Mirrors the inline-flex chip pattern used elsewhere but stays
           visually quieter so the hero remains the focal point. */
        .mo-section-nav {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin: 0.25rem 0 0.6rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid var(--border);
        }
        .mo-section-chip {
            display: inline-flex;
            align-items: center;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            font-weight: 600;
            letter-spacing: 0.02em;
            color: var(--text-secondary);
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 999px;
            padding: 0.28rem 0.7rem;
            text-decoration: none !important;
            transition: background 80ms ease, color 80ms ease, border-color 80ms ease;
        }
        .mo-section-chip:hover,
        .mo-section-chip:focus-visible {
            color: var(--accent);
            border-color: var(--accent);
            outline: none;
        }
        .mo-section-chip:focus-visible {
            box-shadow: 0 0 0 2px var(--accent-soft, rgba(0, 102, 153, 0.25));
        }
        /* Invisible offset target so #mo-section-* anchors don't scroll the
           expander header underneath the page's sticky bits. */
        .mo-section-anchor {
            position: relative;
            top: -1rem;
            height: 0;
            visibility: hidden;
        }

        /* ── Member Overview: audit-fix bundle (2026-05-27) ──────────────────
           Replaces all inline `style=""` leaks in member_overview.py and
           adds the missing class rules referenced by markup (P2-1, P2-6).
           Also: P1-2 not-found callout, P1-3 mobile profile-nav row,
           P2-4 photo-credit clamp, P3-3 Open-all button weight. */

        /* P2-1: typography for the browse-stage hero <h1> (was inline). */
        .mo-browse-h1 {
            margin: 0.1rem 0 0.25rem;
            font-size: 1.85rem;
            font-weight: 700;
            font-family: 'Zilla Slab', Georgia, serif;
        }
        /* P2-1: profile-stage hero <h1> + meta (were inline). */
        .mo-profile-h1   { margin: 0.15rem 0 0.2rem; }
        .mo-profile-meta { margin: 0 0 0.55rem; }

        /* P2-1: per-card tight bottom-margin + link-row top-margin used by
           _section_legislation / _section_statutory_instruments / _section_debates. */
        .mo-bill-card           { margin-bottom: 0.3rem; }
        .mo-bill-card-link-row  { margin-top: 0.2rem; }
        .mo-debate-card-meta {
            margin-top: 0.2rem;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }

        /* Debates section — floor-speech "transcript" cards (2026-06-08).
           A faithful upgrade of the old debate-section card: same evidence
           stripe, plus a spoken-word excerpt and an As-Gaeilge accent badge. */
        /* Full-width like the Questions cards below them — the inherited
           600px .leg-bill-card width left these transcript cards floating
           half-width beside full-width controls. */
        .mo-speech-card { padding-bottom: 0.55rem; display: block; width: 100%; }
        .mo-speech-card details summary { cursor: pointer; list-style: none; }
        .mo-speech-card details summary::-webkit-details-marker { display: none; }
        .mo-speech-card details[open] .mo-speech-truncated { display: none; }
        .mo-speech-read-more {
            font-size: 0.78rem;
            font-weight: 600;
            color: var(--accent);
        }
        .mo-speech-card details[open] .mo-speech-read-more { display: none; }
        .mo-speech-badges { display: inline-flex; gap: 0.3rem; flex-wrap: wrap; }
        .mo-speech-crumb {
            font-size: 0.7rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: var(--ink-muted);
            margin: 0.15rem 0 0.05rem;
        }
        .mo-speech-excerpt {
            margin: 0.35rem 0 0.4rem;
            font-size: 0.92rem;
            line-height: 1.5;
            color: var(--text-secondary);
        }
        /* As-Gaeilge badge — green (distinct from the accent chamber badge), a
           sparing civic signal that a contribution was delivered in Irish. */
        .signal-gaeilge {
            background: var(--signal-good-subtle);
            color: var(--signal-good-deep);
            border: 1px solid var(--signal-good-border);
        }

        /* P2-1: cabinet-member callout spacing + secondary text colour
           (was two inline styles in the fallback render). */
        .mo-cabinet-callout       { margin: 1rem 0 1.75rem; }
        .mo-cabinet-callout-body  { color: var(--text-secondary); }

        /* Constituency civic-context strip (2026-05-31).
           Renders under the hero stat strip, before section nav. Provenance
           is a first-class element here — the source attribution line below
           the card is not optional and not hidden in an expander. */
        .mo-cc-row {
            display: flex;
            flex-wrap: wrap;
            align-items: baseline;
            gap: 0.45rem;
            line-height: 1.4;
        }
        .mo-cc-row-secondary {
            margin-top: 0.2rem;
            color: var(--text-secondary);
            font-size: 0.92rem;
        }
        .mo-cc-kicker {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-meta);
        }
        .mo-cc-headline {
            font-size: 1.35rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .mo-cc-headline-label {
            color: var(--text-secondary);
        }
        .mo-cc-sep {
            color: var(--text-meta);
        }
        .mo-cc-pos { color: var(--signal-good); font-weight: 600; }
        .mo-cc-neg { color: var(--signal-bad);  font-weight: 600; }
        .mo-cc-caveat {
            margin: 0.35rem 0 0;
            color: var(--text-secondary);
            font-size: 0.92rem;
            line-height: 1.5;
        }
        .mo-cc-caveat strong { color: var(--text-primary); font-weight: 600; }
        .mo-cc-source {
            margin: 0.35rem 0 1rem 0.15rem;
            font-size: 0.78rem;
            color: var(--text-meta);
            line-height: 1.4;
        }
        .mo-cc-source-label { font-weight: 600; }
        .mo-cc-source-body  { }
        .mo-cc-source-link a {
            color: var(--accent);
            text-decoration: none;
            border-bottom: 1px dotted var(--accent);
        }
        .mo-cc-source-link a:hover,
        .mo-cc-source-link a:focus-visible {
            border-bottom-style: solid;
        }

        /* P1-2: civic-voice not-found callout (replaces the dt-callout
           with raw inline `color:var(--text-meta)` body + inline CTA). */
        .mo-not-found-callout {
            background: var(--surface);
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 2px;
            padding: 1.1rem 1.35rem;
            margin: 1rem 0 1.5rem;
        }
        .mo-not-found-body { color: var(--text-meta); }
        .mo-not-found-cta {
            display: inline-block;
            margin-top: 0.6rem;
            font-family: 'Epilogue', sans-serif;
            font-weight: 600;
            color: var(--accent);
            text-decoration: none;
        }
        .mo-not-found-cta:hover { text-decoration: underline; }

        /* P2-1: lobbying revolving-door inner body (was inline). */
        .lob-revolving-body {
            margin: 0;
            font-size: 0.88rem;
            color: var(--text-secondary);
        }

        /* P2-6: dedicated revolving-door badge styling. The class name was
           referenced in member_overview markup but no rule existed, so the
           chip inherited the plain .dt-badge background. Distinct warning
           palette so the flag actually reads as a flag, not a routine label. */
        .dt-badge-revolving {
            background: var(--signal-warn-subtle, #fff7e6);
            border-color: var(--signal-warn-border, #f0d99b);
            color: var(--signal-warn-deep, #7a5a00);
            font-weight: 600;
        }
        .dt-badge-revolving::before {
            content: "\26A0";   /* warning sign */
            margin-right: 0.35rem;
            font-size: 0.85em;
        }

        /* P1-3: profile-nav row stays horizontal on mobile. Streamlit's
           st.columns collapse one-per-row at narrow widths; this :has()
           rule grabs the stHorizontalBlock following the marker div and
           forces it to flex horizontally so the 3 buttons share a row. */
        [data-testid="stHorizontalBlock"]:has(> div .mo-prof-nav-marker),
        [data-testid="stHorizontalBlock"]:has(.mo-prof-nav-marker) ~ [data-testid="stHorizontalBlock"]:first-of-type {
            /* fallback selector path — Streamlit nests the marker inside the
               first column, so the parent stHorizontalBlock is the target. */
        }
        /* Direct rule: when the marker div exists ANYWHERE in the next
           stHorizontalBlock, keep flex-row on mobile. */
        [data-testid="stHorizontalBlock"]:has(.mo-prof-nav-marker) {
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            gap: 0.4rem !important;
            margin-bottom: 0.4rem !important;
        }
        [data-testid="stHorizontalBlock"]:has(.mo-prof-nav-marker)
            > [data-testid="stColumn"] {
            flex: 0 0 auto !important;
            min-width: 0 !important;
        }
        @media (max-width: 640px) {
            /* Truncate long TD names in the prev/next buttons so they fit. */
            [data-testid="stHorizontalBlock"]:has(.mo-prof-nav-marker)
                button {
                font-size: 0.78rem !important;
                padding: 0.3rem 0.55rem !important;
                white-space: nowrap !important;
                overflow: hidden !important;
                text-overflow: ellipsis !important;
                max-width: 110px !important;
            }
            /* Drop the trailing spacer column on mobile — it was eating
               ~50% of the row width on a 390px viewport. The first three
               columns (back / prev / next) now share full available width
               (auto-shrunk to button content), leaving no wasted space. */
            [data-testid="stHorizontalBlock"]:has(.mo-prof-nav-marker)
                > [data-testid="stColumn"]:nth-child(4) {
                display: none !important;
            }
            /* And let the visible columns share natural-width rather than
               their ratio-implied widths, so the buttons hug their labels. */
            [data-testid="stHorizontalBlock"]:has(.mo-prof-nav-marker)
                > [data-testid="stColumn"] {
                flex: 0 1 auto !important;
                width: auto !important;
            }
        }

        /* P2-4: tighter photo-credit so long CC attributions don't wrap
           into 4 lines under the avatar. Caps at 2-line clamp. */
        .dt-profile-avatar-credit {
            max-width: 96px;
            font-size: 0.6rem;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* P3-3: "Open all sections" button needs visual weight to match
           the brand chrome. The button-of-interest sits inside the wrap
           below the section-nav chip row. Adds an accent border + tinted
           background so it reads as a deliberate CTA, not a stray default. */
        .st-key-mo_open_all_btn button {
            background: var(--accent-soft, rgba(0, 102, 153, 0.08)) !important;
            border: 1px solid var(--accent) !important;
            color: var(--accent) !important;
            font-weight: 600 !important;
        }
        .st-key-mo_open_all_btn button:hover {
            background: var(--accent) !important;
            color: #ffffff !important;
        }

        /* P2-5: legacy "Member profiles have moved" callout — replaces the
           inline-styled callout in components.member_moved_callout. The CTA
           is now a real-looking button (filled accent) so the redirect path
           is obvious; the body text inherits the muted meta colour. */
        .dt-moved-callout { margin: 0.5rem 0 1rem; }
        .dt-moved-body    { color: var(--text-meta); }
        .dt-moved-cta {
            display: inline-block;
            margin-top: 0.75rem;
            padding: 0.42rem 0.95rem;
            background: var(--accent);
            color: #ffffff !important;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.85rem;
            font-weight: 600;
            border-radius: 4px;
            text-decoration: none !important;
            transition: background 100ms ease, transform 80ms ease;
        }
        .dt-moved-cta:hover {
            background: var(--accent-deep, var(--accent));
            transform: translateY(-1px);
        }
        .dt-moved-cta:focus-visible {
            outline: 2px solid var(--accent);
            outline-offset: 2px;
        }
        .dt-moved-fallback {
            color: var(--text-meta);
            font-style: italic;
        }

        /* P2-3: party-colour swatch as a small dot in front of the party
           text. Lives in front of the hero meta line and inside each
           browse-card meta. Uses inline background-color (party_colour()
           lookup) — the only inline style permitted here because the value
           is data-driven, not theme-driven. */
        .mo-party-swatch {
            display: inline-block;
            width: 0.55rem;
            height: 0.55rem;
            border-radius: 50%;
            margin-right: 0.42rem;
            vertical-align: 0.04em;
            box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.12) inset;
        }

        /* ── Reusable: full-card-clickable link (ui/components.py:
           clickable_card_link). Stretched-link pattern: an absolute <a>
           covers the wrapper, so the whole card is the click target while
           inner interactive elements (Oireachtas ↗ etc.) remain clickable
           via z-index layering. Hover lifts + recolours the accent and
           slides the arrow. Works with any inner card class. */
        .dt-card-link-wrap {
            position: relative;
            display: block;
            transition: transform 80ms ease;
        }
        .dt-card-link-wrap:hover {
            transform: translateY(-1px);
        }
        /* The stretched <a> overlay — covers the wrap, no visible content. */
        .dt-card-link {
            position: absolute;
            inset: 0;
            z-index: 1;
            text-decoration: none !important;
            color: transparent !important;
            background: transparent;
        }
        /* Inner interactive elements (any <a> that isn't the stretched
           link, plus buttons) sit above the overlay so they remain
           independently clickable. */
        .dt-card-link-wrap a:not(.dt-card-link),
        .dt-card-link-wrap button {
            position: relative;
            z-index: 2;
        }
        /* Reserve space for the arrow on whatever card sits inside.
           Overrides per-card padding-right so generic + page-specific
           cards (dt-name-card, leg-bill-card, cmt-row, etc.) all work.
           Scoped via :has() so wrappers rendered with show_arrow=False
           don't leave dead space on the right. */
        .dt-card-link-wrap:has(> .dt-card-arrow) > *:not(.dt-card-link):not(.dt-card-arrow) {
            padding-right: 2.25rem !important;
        }
        /* Generic accent on the inner card when hovered. Card classes
           that opt in by exposing border-left-color / border-color /
           background pick up the visual lift automatically. */
        .dt-card-link-wrap:hover > *:not(.dt-card-link):not(.dt-card-arrow) {
            border-left-color: var(--accent) !important;
            border-color: var(--accent) !important;
            background: var(--accent-subtle) !important;
        }
        .dt-card-arrow {
            position: absolute;
            right: 0.85rem;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-meta);
            font-size: 1rem;
            font-weight: 700;
            pointer-events: none;
            transition: transform 120ms ease, color 120ms ease;
        }
        .dt-card-link-wrap:hover .dt-card-arrow {
            color: var(--accent);
            transform: translateY(-50%) translateX(3px);
        }
        /* Reusable: all main-area filter inputs + selectboxes get the
           prominent white treatment. Sidebar widgets are excluded by the
           [data-testid="stMain"] root, so they keep the muted style. */
        [data-testid="stMain"] [data-testid="stTextInput"] input {
            font-size: 0.95rem !important;
            padding: 0.6rem 0.9rem !important;
            background: #ffffff !important;
            border: 1.5px solid var(--border-strong) !important;
            border-radius: 8px !important;
        }
        [data-testid="stMain"] [data-testid="stTextInput"] input:focus {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 3px var(--accent-subtle) !important;
        }
        [data-testid="stMain"] [data-testid="stSelectbox"] [data-baseweb="select"] > div {
            background: #ffffff !important;
            border: 1.5px solid var(--border-strong) !important;
            border-radius: 8px !important;
            min-height: 2.6rem !important;
        }
        [data-testid="stMain"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:focus-within {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 3px var(--accent-subtle) !important;
        }

        /* Embedded Interests body (inside the Interests expander on
           member-overview) — year-aware Landlord / Property / Shareholder
           pills shown as a compact strip in lieu of the full member header. */
        .int-embedded-badge-strip {
            display: flex;
            flex-wrap: wrap;
            gap: 0.35rem;
            margin: 0.1rem 0 0.6rem;
        }

        /* ── Interests: category headings & diff badges ──────────── */
        .int-category-section {
            font-size: 0.68rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: var(--text-meta);
            border-bottom: 2px solid var(--accent);
            padding-bottom: 0.25rem;
            margin: 1.5rem 0 0.5rem;
        }
        .int-diff-badge-new {
            display: inline-block;
            font-size: 0.6rem;
            font-weight: 800;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: #15803d;
            background: #dcfce7;
            border-radius: 3px;
            padding: 0.05rem 0.3rem;
            margin-right: 0.55rem;
            vertical-align: middle;
        }
        .int-diff-badge-removed {
            display: inline-block;
            font-size: 0.6rem;
            font-weight: 800;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: #b91c1c;
            background: #fee2e2;
            border-radius: 3px;
            padding: 0.05rem 0.3rem;
            margin-right: 0.55rem;
            vertical-align: middle;
        }
        .int-empty-cats {
            color: var(--text-meta);
            font-style: italic;
            font-size: 0.85rem;
            margin: 0.25rem 0;
            line-height: 1.9;
        }

        /* ── Checkbox — rounded rectangle ───────────────────────────────
           Single source of truth for checkbox shape and colour.
           SHAPE: change border-radius on one line.
             4px  = rounded rectangle (default)
             50%  = circle
             0    = sharp square
           COLOUR: uses --accent (navy) for checked state.            */

        [data-testid="stCheckbox"] input[type="checkbox"],
        .stCheckbox input[type="checkbox"] {
            -webkit-appearance: none;
            appearance:         none;
            width:              1.1rem;
            height:             1.1rem;
            min-width:          1.1rem;
            border:             1.5px solid var(--border-strong);
            border-radius:      4px;          /* ← shape: 4px = rounded rect */
            background:         #ffffff;
            cursor:             pointer;
            flex-shrink:        0;
            vertical-align:     middle;
            position:           relative;
            transition:         border-color 100ms ease, background 100ms ease;
        }
        [data-testid="stCheckbox"] input[type="checkbox"]:checked,
        .stCheckbox input[type="checkbox"]:checked {
            background:   var(--accent);
            border-color: var(--accent);
        }
        [data-testid="stCheckbox"] input[type="checkbox"]:checked::after,
        .stCheckbox input[type="checkbox"]:checked::after {
            content:      '';
            position:     absolute;
            left:         0.21rem;
            top:          0.05rem;
            width:        0.5rem;
            height:       0.3rem;
            border:       2px solid #ffffff;
            border-top:   none;
            border-right: none;
            transform:    rotate(-45deg);
        }
        [data-testid="stCheckbox"] input[type="checkbox"]:focus-visible,
        .stCheckbox input[type="checkbox"]:focus-visible {
            outline:        2px solid var(--accent);
            outline-offset: 2px;
        }

        /* ── Attendance Hall cards ───────────────────────────────────── */
        /* Anchor both good/bad columns to the top so first cards align */
        [data-testid="stHorizontalBlock"]:has(.att-hall-heading-good) {
            align-items: flex-start !important;
        }
        .att-hall-heading-good {
            font-size: 1.3rem; font-weight: 800; letter-spacing: -0.02em;
            color: var(--signal-good); border-bottom: 3px solid var(--signal-good-mid);
            padding-bottom: 0.5rem; margin: 0 0 0.9rem;
        }
        .att-hall-heading-bad {
            font-size: 1.3rem; font-weight: 800; letter-spacing: -0.02em;
            color: var(--signal-bad); border-bottom: 3px solid var(--signal-bad-mid);
            padding-bottom: 0.5rem; margin: 0 0 0.9rem;
        }
        .att-hall-card-good,
        .att-hall-card-bad {
            display: flex; align-items: center; gap: 0.6rem;
            padding: 0.38rem 0.75rem; border-radius: 12px;
            margin-bottom: 0.3rem; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
            width: 100%;
        }
        .att-hall-card-good {
            background: var(--signal-good-subtle);
            border: 1px solid var(--signal-good-border);
            border-left: 5px solid var(--signal-good-mid);
        }
        .att-hall-card-bad {
            background: var(--signal-bad-subtle);
            border: 1px solid var(--signal-bad-border);
            border-left: 5px solid var(--signal-bad-mid);
        }
        .att-hall-rank {
            font-size: 0.7rem; font-weight: 800; letter-spacing: 0.04em;
            color: var(--text-meta); width: 1.6rem; text-align: center; flex-shrink: 0;
        }
        .att-hall-body { flex: 1; min-width: 0; }
        .att-hall-name {
            margin: 0 0 0.05rem; font-size: 0.95rem; font-weight: 700;
            color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }
        .att-hall-meta {
            margin: 0; font-size: 0.73rem; color: var(--text-meta);
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }
        .att-hall-badge-good,
        .att-hall-badge-bad {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            flex-shrink: 0; min-width: 3.4rem; padding: 0.3rem 0.6rem;
            border-radius: 12px; text-align: center; line-height: 1.1;
        }
        .att-hall-badge-good { background: var(--signal-good-subtle); border: 1px solid var(--signal-good-border); }
        .att-hall-badge-bad  { background: var(--signal-bad-subtle);  border: 1px solid var(--signal-bad-border); }
        .att-hall-badge-num {
            font-size: 1.25rem; font-weight: 800; letter-spacing: -0.03em;
            color: var(--text-primary); display: block;
        }
        .att-hall-badge-good .att-hall-badge-num { color: var(--signal-good-deep); }
        .att-hall-badge-bad  .att-hall-badge-num { color: var(--signal-bad-deep); }
        .att-hall-badge-label { font-size: 0.62rem; font-weight: 600; color: var(--text-meta); display: block; }

        /* ── Hall cards as full-card-clickable links (clickable_card_link) ─── */
        /* No arrow shown, so don't reserve right-padding for one. */
        .dt-card-link-wrap > .att-hall-card-good,
        .dt-card-link-wrap > .att-hall-card-bad {
            padding-right: 0.75rem !important;
        }
        /* Stack wraps with the same vertical rhythm the bare cards used.
           max-width 80% trims both columns so the cards aren't full-bleed. */
        .dt-card-link-wrap:has(> .att-hall-card-good),
        .dt-card-link-wrap:has(> .att-hall-card-bad) {
            margin-bottom: 0.3rem;
            max-width: 80%;
        }
        /* Preserve blue (good) and orange (bad) identity on hover —
           override the generic accent recolour. Lift + tinted shadow only. */
        .dt-card-link-wrap:hover > .att-hall-card-good {
            border-left-color: var(--blue-500) !important;
            border-color: var(--blue-300) !important;
            background: var(--blue-050) !important;
            box-shadow: 0 3px 10px rgba(59,130,246,0.22) !important;
        }
        .dt-card-link-wrap:hover > .att-hall-card-bad {
            border-left-color: var(--orange-500) !important;
            border-color: var(--orange-300) !important;
            background: var(--orange-050) !important;
            box-shadow: 0 3px 10px rgba(249,115,22,0.22) !important;
        }

        /* Ranked list row (partial-year view) */
        .att-list-row { display: flex; align-items: center; gap: 8px; padding: 2px 0; }
        [data-testid="stHorizontalBlock"]:has(.att-list-row) {
            align-items: stretch !important;
            gap: 0.3rem !important;
            margin-bottom: 0.15rem !important;
        }
        .att-list-rank {
            font-size: 0.78rem; font-weight: 800; color: var(--text-meta);
            width: 1.4rem; text-align: right; flex-shrink: 0;
        }
        .att-list-pill { background: #ffffff; border: 1px solid var(--border); border-radius: 12px; padding: 5px 12px; min-width: 0; flex: 1; }
        .att-list-pill-name { font-size: 0.95rem; font-weight: 700; color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .att-list-pill-meta { font-size: 0.70rem; color: var(--text-meta); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

        /* Missing-members rows (TDs absent from attendance parquet) */
        .att-miss-row {
            display: flex; flex-wrap: wrap; align-items: baseline; gap: 0.55rem;
            background: #ffffff; border: 1px solid var(--border); border-radius: 2px;
            padding: 0.45rem 0.7rem; margin: 0.25rem 0; line-height: 1.35;
        }
        .att-miss-name {
            font-size: 0.95rem; font-weight: 700; color: var(--text-primary);
            letter-spacing: -0.005em;
        }
        .att-miss-meta {
            font-size: 0.78rem; color: var(--text-secondary); font-weight: 500;
        }
        .att-miss-office {
            font-size: 0.72rem; color: var(--text-meta);
            font-style: italic; margin-left: auto;
            text-align: right; max-width: 60%;
        }

        /* ── Payments page ───────────────────────────────────────────── */
        .pay-amount-badge {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            min-width: 62px; padding: 5px 10px; border-radius: 12px;
            background: var(--blue-050); border: 1px solid var(--blue-300); text-align: center; flex-shrink: 0;
        }
        .pay-amount-badge-num  { font-size: 1.05rem; font-weight: 800; letter-spacing: -0.03em; color: var(--blue-800); line-height: 1; display: block; }
        .pay-amount-badge-label { font-size: 0.58rem; font-weight: 600; color: var(--blue-500); line-height: 1.4; display: block; }
        .pay-taa-pill {
            display: inline-flex; align-items: center; gap: 0.25rem; background: var(--blue-050); border: 1px solid var(--blue-300);
            border-radius: 999px; padding: 2px 8px; font-size: 0.68rem; font-weight: 600; color: var(--blue-800);
        }
        /* P1-6: unmapped TAA bands — quieter neutral tint so the caveat
           reads as "uncertainty", not "warning". Band string is still shown
           so the reader has the registry value to compare against. */
        .pay-taa-pill-unmapped {
            background: #f5f5f4;
            border-color: #d6d3d1;
            color: #57534e;
            font-style: italic;
        }
        .pay-taa-caveat {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 0.9rem;
            height: 0.9rem;
            background: #78716c;
            color: #ffffff;
            border-radius: 50%;
            font-size: 0.6rem;
            font-weight: 700;
            font-style: normal;
            cursor: help;
        }
        .pay-name-row { display: inline-flex; align-items: center; gap: 8px; padding: 2px 0; height: 100%; width: fit-content; max-width: 100%; }

        /* Collapse row so → button sits right next to the card */
        [data-testid="stHorizontalBlock"]:has(.pay-name-row) {
            width: fit-content !important;
            max-width: 100% !important;
            gap: 0.4rem !important;
        }
        [data-testid="stHorizontalBlock"]:has(.pay-name-row) [data-testid="stColumn"] {
            width: auto !important;
            flex: 0 0 auto !important;
            min-width: 0 !important;
        }
        .pay-name-rank { font-size: 0.75rem; font-weight: 800; color: var(--text-meta); width: 1.8rem; text-align: right; flex-shrink: 0; }
        .pay-name-body { background: #ffffff; border: 1px solid var(--border); border-radius: 12px; padding: 5px 12px; flex: 1; min-width: 0; }
        .pay-name-body-name { font-size: 0.95rem; font-weight: 700; color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .pay-name-body-pos  { font-size: 0.72rem; color: var(--text-meta); margin-bottom: 3px; }
        .pay-count-pill {
            display: inline-flex; align-items: center; background: #ffffff; border: 1px solid var(--border);
            border-radius: 999px; padding: 2px 7px; font-size: 0.68rem; font-weight: 600; color: var(--text-meta); margin-left: 4px;
        }
        .pay-identity-card { background: #ffffff; border: 1px solid var(--border); border-radius: 12px; padding: 10px 14px; margin-bottom: 0.75rem; }
        .pay-identity-card-name { font-size: 1.3rem; font-weight: 800; color: var(--text-primary); }
        .pay-identity-card-meta { font-size: 0.8rem; color: var(--text-meta); margin-top: 3px; }

        /* Embedded Payments body (inside the Payments expander on
           member-overview). All-years summary as a compact dl-style list,
           per-payment audit-trail rendered as cards instead of st.dataframe
           (member_overview never uses dataframes — see
           feedback_member_overview_no_dataframes). */
        .pay-year-list {
            display: grid;
            gap: 0.3rem;
            margin: 0.35rem 0 0.9rem;
        }
        .pay-year-row {
            display: grid;
            grid-template-columns: 4rem 1fr auto auto;
            gap: 0.6rem 1rem;
            align-items: baseline;
            padding: 0.4rem 0.7rem;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 2px;
            font-family: 'Epilogue', sans-serif;
        }
        .pay-year-yr {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.0rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .pay-year-amount {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.0rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .pay-year-payments {
            font-size: 0.78rem;
            color: var(--text-meta);
        }
        .pay-year-rank {
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--text-secondary);
            min-width: 2.5rem;
            text-align: right;
        }
        .pay-year-rank-missing { color: var(--text-meta); font-weight: 500; }
        @media (max-width: 540px) {
            .pay-year-row {
                grid-template-columns: 3.5rem 1fr;
                row-gap: 0.15rem;
            }
            .pay-year-payments, .pay-year-rank {
                grid-column: 2 / -1;
                text-align: left;
            }
        }

        .pay-record-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 2px;
            padding: 0.55rem 0.8rem;
            margin-bottom: 0.35rem;
        }
        .pay-record-card-header {
            display: flex;
            align-items: center;
            gap: 0.65rem;
            margin-bottom: 0.2rem;
        }
        .pay-record-card-date {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 600;
            color: var(--text-meta);
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .pay-record-card-amount {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.95rem;
            font-weight: 700;
            color: var(--text-primary);
            margin-left: auto;
        }
        .pay-record-card-desc {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.87rem;
            line-height: 1.45;
            color: var(--text-secondary);
        }

        /* Embedded Attendance body (inside the Attendance expander on
           member-overview). Year breakdown replaces st.dataframe's
           ProgressColumn with a CSS-width bar — same information density,
           but stays card-based per feedback_member_overview_no_dataframes. */
        .att-year-list {
            display: grid;
            gap: 0.3rem;
            margin: 0.35rem 0 0.9rem;
        }
        .att-year-row {
            display: grid;
            grid-template-columns: 3.5rem 1fr auto auto;
            gap: 0.6rem 1rem;
            align-items: center;
            padding: 0.45rem 0.7rem;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 2px;
            font-family: 'Epilogue', sans-serif;
        }
        .att-year-yr {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.0rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .att-year-bar-track {
            height: 0.45rem;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 2px;
            overflow: hidden;
            min-width: 8rem;
        }
        .att-year-bar-fill {
            height: 100%;
            background: var(--accent);
        }
        .att-year-days {
            font-size: 0.82rem;
            color: var(--text-secondary);
            font-variant-numeric: tabular-nums;
        }
        .att-year-pct {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.95rem;
            font-weight: 700;
            color: var(--text-primary);
            min-width: 2.8rem;
            text-align: right;
            font-variant-numeric: tabular-nums;
        }
        @media (max-width: 540px) {
            .att-year-row {
                grid-template-columns: 3.5rem 1fr auto;
                row-gap: 0.2rem;
            }
            .att-year-bar-track {
                grid-column: 1 / -1;
                order: 99;
            }
        }

        /* Sitting calendar — pure-CSS month grid replacing the Altair tick
           strip (2026-06-11): each dot is one day recorded present, so recess
           months read as empty cells. No chart iframe, house typography. */
        .att-cal-strip {
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 0.4rem;
            margin: 0.4rem 0 0.6rem;
        }
        .att-cal-month {
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 2px;
            padding: 0.35rem 0.3rem 0.4rem;
            text-align: center;
        }
        .att-cal-month-label {
            font-size: 0.66rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin-bottom: 0.3rem;
        }
        .att-cal-dots {
            display: flex;
            flex-wrap: wrap;
            gap: 2px;
            justify-content: center;
            align-content: flex-start;
            min-height: 1.7rem;
        }
        .att-cal-dot {
            width: 7px;
            height: 7px;
            border-radius: 50%;
            background: #2d7a52;
        }
        .att-cal-month-n {
            font-family: 'Zilla Slab', Georgia, serif;
            font-weight: 700;
            font-size: 0.9rem;
            color: var(--text-primary);
            margin-top: 0.25rem;
        }
        .att-cal-month-zero .att-cal-month-n {
            color: var(--text-meta);
            font-weight: 400;
        }
        @media (max-width: 760px) {
            .att-cal-strip { grid-template-columns: repeat(6, 1fr); }
        }

        /* Votes-by-year rows — same chassis as .att-year-row; the track holds
           a yes/no/abstained split instead of a single fill (replaces the
           embedded Plotly stacked chart, 2026-06-11). */
        .vote-year-track { display: flex; }
        .vote-year-seg { height: 100%; }
        .vote-year-seg-yes  { background: #2d7a52; }
        .vote-year-seg-no   { background: #bf4a1e; }
        .vote-year-seg-abst { background: #8c8c80; }
        .vote-year-counts strong { color: var(--text-primary); }
        .vote-year-legend {
            display: flex;
            align-items: center;
            gap: 0.35rem;
            font-size: 0.75rem;
            color: var(--text-secondary);
            margin-bottom: 0.15rem;
        }
        .vote-year-key {
            display: inline-block;
            width: 0.7rem;
            height: 0.7rem;
            border-radius: 2px;
        }
        .vote-year-key-yes  { background: #2d7a52; }
        .vote-year-key-no   { background: #bf4a1e; }
        .vote-year-key-abst { background: #8c8c80; }

        /* Total amount badge on payments ranked-list cards. Softer green
           replaces the prior bright-blue dt-name-card-badge-metric, with
           extra horizontal padding so €X,XXX figures don't feel pinched. */
        .pay-total-badge {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            text-align: center;
            padding: 0.32rem 0.85rem;
            border-radius: 10px;
            min-width: 3.1rem;
            background: #f0fdf4;
            border: 1px solid #bbf7d0;
        }
        .pay-total-badge-num {
            font-size: 1.05rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            color: #15803d;
            line-height: 1;
            display: block;
        }
        .pay-total-badge-lbl {
            font-size: 0.56rem;
            font-weight: 600;
            letter-spacing: 0.04em;
            color: #16a34a;
            display: block;
            margin-top: 0.18rem;
        }

        /* Coloured payments-count pill — slate-blue, distinct from the
           green total badge so the two metrics don't blur together. */
        .pay-count-pill-accent {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            background: #f1f5f9;
            border: 1px solid #cbd5e1;
            border-radius: 999px;
            padding: 0.1rem 0.5rem;
            font-size: 0.76rem;
            font-weight: 600;
            color: #334155;
        }

        /* Tight Total / Avg-per-TD strip on the payments year view —
           replaces two big st.metric blocks with one compact band. */
        .pay-totals-strip {
            display: inline-flex;
            align-items: center;
            gap: 1.4rem;
            padding: 0.5rem 1rem;
            margin: 0.4rem 0 0.6rem;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 10px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        }
        .pay-totals-item {
            display: inline-flex;
            flex-direction: column;
            align-items: flex-start;
        }
        .pay-totals-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.3rem;
            font-weight: 800;
            color: var(--text-primary);
            letter-spacing: -0.02em;
            line-height: 1;
        }
        .pay-totals-lbl {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.66rem;
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin-top: 0.25rem;
        }
        .pay-totals-divider {
            width: 1px;
            height: 1.7rem;
            background: var(--border);
        }

        /* Generic totals strip — used by the `totals_strip()` component on
           every Stage 2 view that previously emitted bare st.metric blocks
           (payments Rankings, lobbying org / topic / DPO Stage 2). Same
           visual treatment as .pay-totals-* but unprefixed for cross-page
           reuse. */
        .dt-totals-strip {
            display: inline-flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 1.4rem;
            padding: 0.5rem 1rem;
            margin: 0.4rem 0 0.6rem;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 10px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
            max-width: 100%;
        }
        .dt-totals-item {
            display: inline-flex;
            flex-direction: column;
            align-items: flex-start;
        }
        .dt-totals-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.3rem;
            font-weight: 800;
            color: var(--text-primary);
            letter-spacing: -0.02em;
            line-height: 1;
        }
        .dt-totals-lbl {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.66rem;
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin-top: 0.25rem;
        }
        .dt-totals-divider {
            width: 1px;
            height: 1.7rem;
            background: var(--border);
        }
        @media (max-width: 640px) {
            .dt-totals-strip { gap: 0.9rem; padding: 0.45rem 0.7rem; }
            .dt-totals-num { font-size: 1.1rem; }
            .dt-totals-divider { display: none; }
        }

        /* ── Data provenance box ────────────────────────────────────────
           Used when a callout needs a left accent border (source notes,
           per-year PDF links). Not the same as .dt-callout.           */
        .dt-provenance-box {
            background: var(--surface);
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 2px;
            padding: 0.9rem 1rem;
        }

        /* ── Attendance: extra heading variants ──────────────────────── */
        .att-hall-subheading { font-size: 0.75rem; color: #6b7280; margin: 0 0 0.75rem; }
        .att-cop-head-good { font-size: 0.68rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase; color: var(--blue-700); border-bottom: 3px solid var(--blue-500); padding-bottom: 0.3rem; margin: 0 0 0.6rem; }
        .att-cop-head-bad  { font-size: 0.68rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase; color: var(--orange-700); border-bottom: 3px solid var(--orange-500); padding-bottom: 0.3rem; margin: 0 0 0.6rem; }

        /* ── Attendance overview: year summary strip ─────────────────── */
        .att-ov-year-strip {
            display: flex; flex-wrap: nowrap; overflow-x: auto;
            gap: 0.5rem; padding: 0.6rem 0 0.9rem;
        }
        .att-ov-year-card {
            flex-shrink: 0;
            display: flex; flex-direction: column; align-items: center;
            padding: 0.5rem 0.9rem; border-radius: 10px;
            background: #ffffff; border: 1px solid var(--border);
            min-width: 6.5rem; text-align: center;
            transition: border-color 0.12s;
        }
        .att-ov-year-card-active {
            background: #f0fdf4; border: 1.5px solid #16a34a;
        }
        .att-ov-year-num {
            font-size: 1.05rem; font-weight: 800; color: var(--text-primary);
            letter-spacing: -0.02em; line-height: 1.2;
        }
        .att-ov-year-card-active .att-ov-year-num { color: #15803d; }
        .att-ov-year-members {
            font-size: 0.68rem; font-weight: 600; color: var(--text-meta);
            margin-top: 0.15rem; white-space: nowrap;
        }
        .att-ov-year-days {
            font-size: 0.63rem; color: var(--text-meta);
            margin-top: 0.05rem; white-space: nowrap;
        }

        /* ── Material Symbols Outlined (used by lobbying path cards) ────── */
        .material-symbols-outlined {
            font-family: 'Material Symbols Outlined';
            font-weight: normal;
            font-style: normal;
            display: inline-block;
            font-variation-settings: 'FILL' 0, 'wght' 300, 'GRAD' 0, 'opsz' 24;
            user-select: none;
        }

        /* ── Lobbying page ───────────────────────────────────────────────
           Navy (#0f3d5e) is deliberate — the lobbying page has a navy/rust
           palette distinct from the amber accent used on other pages.   */
        .lob-section-heading { border-bottom-color: #0f3d5e; }

        /* Attached references — lobbyist-supplied external PDFs (chambers.ie,
           amcham.ie, etc.). Rust accent + EXTERNAL tag signals these are not
           Oireachtas-issued and may rot. */
        .lob-attach-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 0.6rem; margin-top: 0.45rem;
        }
        .lob-attach-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 3px solid var(--orange-900);   /* rust to distinguish from navy/amber */
            border-radius: 8px;
            padding: 0.65rem 0.85rem 0.7rem;
        }
        .lob-attach-head {
            display: flex; align-items: center; gap: 0.4rem; margin-bottom: 0.3rem;
        }
        .lob-attach-host {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.84rem; font-weight: 700; color: var(--text);
            letter-spacing: -0.005em;
        }
        .lob-attach-tag {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.62rem; font-weight: 700; letter-spacing: 0.08em;
            color: var(--orange-900); background: var(--orange-050);
            border: 1px solid #fed7aa; border-radius: 3px;
            padding: 0.05rem 0.35rem;
        }
        .lob-attach-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem; color: var(--text-meta);
            line-height: 1.4; margin-bottom: 0.45rem;
        }
        .lob-attach-actions {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.82rem; font-weight: 600;
            display: flex; align-items: center; gap: 0.35rem;
        }
        .lob-attach-sep { color: var(--text-meta); }

        .lob-path-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-top: 4px solid #0f3d5e;
            border-radius: 12px;
            padding: 0.75rem 1rem 0.75rem;
            box-shadow: 0 1px 2px rgba(17,24,39,0.06), 0 8px 24px rgba(17,24,39,0.04);
            min-height: 175px;
            transition: border-top-color 0.15s, box-shadow 0.15s;
        }
        .lob-path-card:hover { border-top-color: var(--orange-900); box-shadow: 0 4px 16px rgba(17,24,39,0.1); }
        .lob-path-icon { font-size: 1.6rem; line-height: 1; margin-bottom: 0.55rem; }
        .lob-path-heading { margin: 0 0 0.3rem; font-size: 1.05rem; font-weight: 700; color: var(--text-primary); letter-spacing: -0.01em; }
        .lob-path-body { margin: 0 0 0.65rem; font-size: 0.82rem; color: var(--text-meta); line-height: 1.5; }
        .lob-path-stat { display: flex; align-items: baseline; gap: 0.3rem; }
        .lob-path-stat-num { font-size: 1.3rem; font-weight: 800; color: #0f3d5e; letter-spacing: -0.03em; }
        .lob-path-stat-lbl { font-size: 0.73rem; font-weight: 600; color: var(--text-meta); text-transform: uppercase; letter-spacing: 0.04em; }

        /* Topics rail — visually distinct from path cards (rust accent + dashed
           border) to signal "this is a free-text scan, not a register taxonomy". */
        .lob-topic-caveat {
            font-size: 0.83rem;
            color: var(--text-meta);
            line-height: 1.55;
            margin: 0 0 0.65rem;
            padding: 0.55rem 0.75rem;
            background: var(--orange-050);
            border-left: 3px solid var(--orange-700);
            border-radius: 0 8px 8px 0;
        }
        .lob-topic-caveat em { color: #0f3d5e; font-style: normal; font-weight: 600; }
        .lob-topic-card {
            background: #ffffff;
            border: 1px dashed var(--orange-700);
            border-top: 4px solid var(--orange-700);
            border-radius: 12px;
            padding: 0.75rem 1rem 0.75rem;
            box-shadow: 0 1px 2px rgba(17,24,39,0.06);
            min-height: 145px;
        }
        .lob-topic-icon { font-size: 1.6rem; line-height: 1; margin-bottom: 0.55rem; color: var(--orange-900); }
        .lob-topic-heading { margin: 0 0 0.3rem; font-size: 1.05rem; font-weight: 700; color: var(--text-primary); letter-spacing: -0.01em; }
        .lob-topic-body { margin: 0; font-size: 0.82rem; color: var(--text-meta); line-height: 1.5; }

        .lob-topic-banner {
            background: var(--orange-050);
            border: 1px solid #fed7aa;
            border-left: 5px solid var(--orange-700);
            border-radius: 12px;
            padding: 0.85rem 1.1rem;
            margin: 0.85rem 0;
        }
        .lob-topic-banner-heading {
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--orange-900);
            margin: 0 0 0.35rem;
        }
        .lob-topic-banner-body {
            font-size: 0.85rem;
            color: #7c2d12;
            line-height: 1.55;
            margin: 0;
        }
        .lob-topic-banner-body em { color: #0f3d5e; font-style: normal; font-weight: 600; }
        .lob-topic-keyword-row {
            display: flex; flex-wrap: wrap; gap: 0.35rem;
            margin: 0.2rem 0 0;
        }
        .lob-topic-keyword-pill {
            background: #ffffff;
            border: 1px solid #fed7aa;
            color: #7c2d12;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.15rem 0.55rem;
            border-radius: 999px;
            font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
        }

        .lob-topic-filter-banner {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 0.6rem;
            background: var(--orange-050);
            border: 1px solid #fed7aa;
            border-left: 5px solid var(--orange-700);
            border-radius: 10px;
            padding: 0.65rem 0.95rem;
            margin: 0.85rem 0 0.55rem;
            color: #7c2d12;
            font-size: 0.88rem;
        }
        .lob-topic-filter-label {
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--orange-900);
        }
        .lob-topic-filter-clear {
            margin-left: auto;
            background: #ffffff;
            border: 1px solid var(--orange-700);
            color: var(--orange-900) !important;
            font-size: 0.78rem;
            font-weight: 700;
            text-decoration: none;
            padding: 0.3rem 0.75rem;
            border-radius: 999px;
        }
        .lob-topic-filter-clear:hover {
            background: var(--orange-700);
            color: #ffffff !important;
        }

        .lob-revolving-callout {
            background: #fffbeb;
            border: 1px solid #fcd34d;
            border-left: 5px solid #d97706;
            border-radius: 12px;
            padding: 0.85rem 1.1rem;
            margin: 0.85rem 0;
        }
        .lob-revolving-heading { font-size: 0.72rem; font-weight: 800; letter-spacing: 0.08em; text-transform: uppercase; color: #92400e; margin-bottom: 0.3rem; }
        .lob-revolving-headline {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.15rem;
            font-weight: 700;
            color: #78350f;
            line-height: 1.35;
            margin: 0.1rem 0 0.35rem;
        }
        .lob-revolving-explain {
            font-size: 0.83rem;
            color: #78350f;
            line-height: 1.5;
            margin: 0 0 0.65rem;
        }
        .lob-revolving-list { margin: 0.55rem 0 0.45rem; border-top: 1px solid rgba(217,119,6,0.25); }
        .lob-revolving-row {
            display: flex; align-items: baseline; gap: 0.6rem;
            padding: 0.4rem 0;
            border-bottom: 1px solid rgba(217,119,6,0.18);
            font-size: 0.86rem;
        }
        .lob-revolving-row-rank {
            font-size: 0.7rem; font-weight: 800; color: #92400e;
            letter-spacing: 0.05em; min-width: 1.5rem;
        }
        .lob-revolving-row-name { flex: 1; font-weight: 700; color: #1f2937; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .lob-revolving-row-meta { color: #78350f; font-size: 0.78rem; white-space: nowrap; }

        /* Stage 2a prominent-cases sub-callout — sits inside the RD index
           hero zone to flag the highest-impact individuals. */
        .lob-rd-prominent {
            background: var(--orange-050);
            border: 1px solid var(--orange-300);
            border-left: 5px solid var(--orange-700);
            border-radius: 10px;
            padding: 0.7rem 1rem;
            margin: 0.5rem 0 1.1rem;
        }
        .lob-rd-prominent-heading { font-size: 0.7rem; font-weight: 800; letter-spacing: 0.09em; text-transform: uppercase; color: var(--orange-900); margin-bottom: 0.4rem; }
        .lob-rd-prominent-grid { display: flex; flex-wrap: wrap; gap: 0.55rem; }
        .lob-rd-prominent-pill {
            background: #ffffff; border: 1px solid var(--orange-300); border-radius: 999px;
            padding: 0.3rem 0.75rem; font-size: 0.82rem; color: #1f2937;
            display: inline-flex; align-items: baseline; gap: 0.4rem;
        }
        .lob-rd-prominent-pill strong { color: var(--orange-900); font-weight: 700; }

        .lob-activity-row { display: flex; align-items: flex-start; gap: 0.75rem; padding: 0.65rem 0; border-bottom: 1px solid var(--border); }
        .lob-activity-period { font-size: 0.73rem; font-weight: 700; color: #0f3d5e; white-space: nowrap; min-width: 5rem; padding-top: 0.1rem; }
        .lob-activity-body { flex: 1; min-width: 0; }
        .lob-activity-org { font-size: 0.88rem; font-weight: 700; color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .lob-activity-area { font-size: 0.75rem; color: var(--text-meta); margin-top: 0.1rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

        .lob-sidebar-label { font-size: 0.7rem; font-weight: 700; letter-spacing: 0.07em; text-transform: uppercase; color: var(--text-meta); margin: 0 0 0.4rem; }

        .lob-policy-pill {
            display: inline-flex; align-items: center; gap: 0.3rem;
            background: #ffffff; border: 1px solid var(--border);
            border-radius: 999px; padding: 0.2rem 0.7rem;
            font-size: 0.78rem; font-weight: 600; color: var(--text-meta);
            cursor: pointer; transition: border-color 0.12s, color 0.12s;
        }
        .lob-policy-pill:hover { border-color: #0f3d5e; color: #0f3d5e; }

        /* ── Legislation page ────────────────────────────────────────── */
        /* Status badge variants — extend the .signal base class */
        .leg-status-enacted  { background:#dcfce7; color:#15803d; border:1px solid #86efac; }
        .leg-status-active   { background:var(--accent-subtle); color:var(--accent); border:1px solid var(--accent-dim); }
        .leg-status-lapsed   { background:var(--surface); color:var(--text-meta); border:1px solid var(--border); }
        .leg-status-withdrawn { background:#fff1f2; color:#9f1239; border:1px solid #fda4af; }

        /* Stage timeline list */
        .leg-stage-list { display:flex; flex-direction:column; }
        .leg-stage-row {
            display: flex; gap: 1rem; padding: 0.55rem 0;
            border-bottom: 1px solid var(--border); align-items: baseline;
        }
        .leg-stage-row:last-child { border-bottom: none; }
        .leg-stage-num {
            font-size: 0.68rem; font-weight: 800; color: var(--text-meta);
            min-width: 1.6rem; text-align: right; flex-shrink: 0;
        }
        .leg-stage-label {
            font-size: 0.88rem; font-weight: 600; color: var(--text-primary); flex: 1;
        }
        .leg-stage-date { font-size: 0.78rem; color: var(--text-meta); white-space: nowrap; }
        .leg-stage-current .leg-stage-label { color: var(--accent); }
        .leg-stage-current .leg-stage-num   { color: var(--accent); }

        /* Bill identity strip in drilldown view */
        .leg-bill-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.65rem; font-weight: 700; color: var(--text-primary);
            line-height: 1.2; margin: 0.5rem 0 0.2rem;
        }
        .leg-bill-ref {
            font-size: 0.8rem; color: var(--text-meta); margin-bottom: 0.5rem;
        }
        .leg-bill-identity {
            padding: 0.75rem 0 0.5rem 0;
        }
        .leg-bill-badges {
            display: flex; gap: 0.4rem; align-items: center;
            flex-wrap: wrap; margin-bottom: 0.5rem;
        }
        .leg-hero-h2 {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.85rem; font-weight: 700; margin: 0.2rem 0 0.4rem;
            letter-spacing: -0.02em;
        }
        .leg-stage-chamber {
            font-weight: 400; color: var(--text-meta); font-size: 0.78rem;
        }
        .leg-long-title {
            font-size: 0.88rem; line-height: 1.6; color: var(--text-secondary);
        }
        .leg-stage-group {
            font-size: 0.65rem; font-weight: 800; letter-spacing: 0.09em;
            text-transform: uppercase; color: var(--accent);
            padding: 0.7rem 0 0.25rem; margin-top: 0.2rem;
            border-top: 1px solid var(--border);
        }
        .leg-stage-group:first-child { border-top: none; padding-top: 0.1rem; }

        /* Amendment-activity badge under the stage timeline (contestation proxy) */
        .leg-amend-badge {
            display: inline-flex; align-items: baseline; gap: 0.45rem;
            margin-top: 0.9rem; padding: 0.35rem 0.7rem;
            background: var(--accent-subtle); border-radius: 6px;
        }
        .leg-amend-count {
            font-size: 1.15rem; font-weight: 800; color: var(--accent);
            line-height: 1;
        }
        .leg-amend-label {
            font-size: 0.82rem; font-weight: 600; color: var(--text-primary);
        }
        .leg-amend-breakdown {
            font-size: 0.78rem; color: var(--text-meta); margin-top: 0.35rem;
        }

        /* Oireachtas link in bill identity strip */
        .leg-bill-oireachtas-link {
            display: inline-block;
            margin-top: 0.55rem;
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--accent);
            text-decoration: none;
        }
        .leg-bill-oireachtas-link:hover { text-decoration: underline; }

        /* Source link card */
        .leg-source-card {
            background: #ffffff; border: 1px solid var(--border);
            border-left: 4px solid var(--accent); border-radius: 2px;
            padding: 0.8rem 1rem; margin-bottom: 0.6rem;
        }
        .leg-source-label {
            font-size: 0.82rem; font-weight: 600;
            color: var(--text-primary); margin-bottom: 0.25rem;
        }
        .leg-source-link {
            font-size: 0.85rem; font-weight: 600;
            color: var(--accent); text-decoration: none;
        }
        .leg-source-link:hover { text-decoration: underline; }
        .leg-source-meta {
            font-size: 0.68rem; font-weight: 600; letter-spacing: 0.04em;
            color: var(--text-meta); text-transform: none;
        }

        /* ── Legislation: documents (versions / memos / amendments) ───── */
        .leg-doc-section { display: flex; flex-direction: column; gap: 0.35rem; }
        .leg-doc-group-label {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.95rem; font-weight: 700;
            color: var(--text); margin: 0.6rem 0 0.25rem;
        }
        .leg-doc-group-label:first-child { margin-top: 0; }
        .leg-doc-group-count {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem; font-weight: 600;
            color: var(--text-meta); margin-left: 0.2rem;
        }

        /* ── Legislation: bill card list ─────────────────────────────── */
        .leg-bill-card {
            display: inline-flex;
            flex-direction: column;
            padding: 0.45rem 0.9rem;
            border: 1px solid var(--border);
            border-left: 3px solid var(--border-strong);
            border-radius: 12px;
            background: #ffffff;
            /* Uniform card width — sized to roughly match the phase
               segmented control row (All / Dáil / Seanad / Enacted),
               extending only modestly past it. */
            width: 600px;
            max-width: 100%;
            transition: border-left-color 0.12s, border-color 0.12s;
        }
        .leg-bill-card:hover {
            border-left-color: var(--accent);
            border-color: var(--accent-dim);
        }
        .leg-bill-card-header {
            display: flex;
            align-items: center;
            gap: 0.55rem;
            margin-bottom: 0.28rem;
        }
        .leg-bill-card-date {
            font-size: 0.73rem;
            color: var(--text-meta);
            white-space: nowrap;
        }
        .leg-bill-card-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.97rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.35;
            margin-bottom: 0.25rem;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .leg-bill-card-footer {
            display: flex;
            align-items: baseline;
            gap: 1.2rem;
        }
        .leg-bill-card-meta {
            font-size: 0.75rem;
            color: var(--text-meta);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .leg-bill-card-link {
            font-size: 0.73rem;
            font-weight: 600;
            color: var(--accent);
            text-decoration: none;
            white-space: nowrap;
            flex-shrink: 0;
        }
        .leg-bill-card-link:hover { text-decoration: underline; }

        /* Card row — card shrinks to fit its title, button sits immediately
           after. Vertical centering of the nav_button is handled by the
           reusable .dt-nav-btn rules above (no per-card override needed). */
        [data-testid="stHorizontalBlock"]:has(.leg-bill-card) {
            width: fit-content !important;
            max-width: 100%;
            gap: 0.4rem !important;
            margin-bottom: 0.3rem !important;
            justify-content: flex-start !important;
        }
        [data-testid="stHorizontalBlock"]:has(.leg-bill-card)
            > [data-testid="stColumn"] {
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: 0 !important;
        }

        /* ── Questions section (member-overview) ─────────────────────
           Three bands inside the Questions expander:
             .q-header-strip  compact aggregate header
             (filters)        Streamlit-native widgets, no custom CSS
             .q-card          one card per question in the paginated feed
           Card pattern matches leg-bill-card: side-stripe + #ffffff bg.
        */
        .q-header-strip {
            display: grid;
            grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr) minmax(0, 1.4fr);
            gap: 1.25rem;
            align-items: start;
            padding: 0.9rem 1rem 0.85rem;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 8px;
            margin-bottom: 1rem;
        }
        @media (max-width: 720px) {
            .q-header-strip {
                grid-template-columns: 1fr;
                gap: 0.85rem;
            }
        }
        .q-strip-cell-label {
            font-size: 0.7rem;
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin-bottom: 0.3rem;
        }
        .q-strip-cell-hint {
            text-transform: none;
            font-weight: 400;
            letter-spacing: 0;
            color: var(--text-meta);
            opacity: 0.85;
        }
        .q-conc-pct {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 2rem;
            font-weight: 700;
            line-height: 1;
            color: var(--text-primary);
            letter-spacing: -0.02em;
        }
        .q-conc-ministry {
            font-size: 0.92rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-top: 0.25rem;
        }
        .q-conc-detail {
            font-size: 0.78rem;
            color: var(--text-meta);
            margin-top: 0.15rem;
        }
        .q-conc-sparse {
            font-size: 0.95rem;
            font-weight: 600;
            color: var(--text-primary);
            font-style: italic;
        }
        .q-total-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.55rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.1;
        }
        .q-total-sub {
            font-size: 0.78rem;
            color: var(--text-meta);
            margin-top: 0.2rem;
        }
        .q-topic-list {
            display: flex;
            flex-wrap: wrap;
            gap: 0.35rem;
            margin-top: 0.1rem;
        }
        .q-topic-chip {
            display: inline-flex;
            align-items: center;
            gap: 0.3rem;
            padding: 0.25rem 0.55rem;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 999px;
            font-size: 0.78rem;
            color: var(--text-primary);
            text-decoration: none;
            line-height: 1;
            transition: border-color 0.12s, color 0.12s;
        }
        .q-topic-chip:hover {
            border-color: var(--accent-dim);
            color: var(--accent);
            text-decoration: none;
        }
        .q-topic-chip-count {
            font-size: 0.72rem;
            color: var(--text-meta);
            font-variant-numeric: tabular-nums;
            margin-left: 0.05rem;
        }
        .q-topic-chip-action {
            font-size: 0.85rem;
            line-height: 1;
            color: var(--text-meta);
            opacity: 0.55;
            transition: opacity 0.12s, color 0.12s;
            margin-left: 0.1rem;
        }
        .q-topic-chip:hover .q-topic-chip-action {
            color: var(--accent);
            opacity: 1;
        }
        /* Active-filter chip — same shape as si-active-chip but with
           project tokens. Used when ?mo_q_topic= is set in the URL. */
        .q-active-filter-bar {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin: 0 0 0.7rem 0;
            font-size: 0.85rem;
            flex-wrap: wrap;
        }
        .q-active-filter-label {
            color: var(--text-meta);
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
        }
        .q-active-chip {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            padding: 0.25rem 0.6rem 0.25rem 0.7rem;
            background: var(--accent-subtle);
            border: 1px solid var(--accent-dim);
            border-radius: 999px;
            color: var(--accent);
            text-decoration: none;
            font-size: 0.82rem;
            line-height: 1;
            transition: background 0.12s, border-color 0.12s, color 0.12s;
        }
        .q-active-chip:hover {
            background: var(--accent);
            color: #ffffff;
            border-color: var(--accent);
            text-decoration: none;
        }
        .q-active-chip-x {
            font-size: 1.05rem;
            line-height: 1;
            margin-top: -1px;
        }
        .q-active-chip:focus-visible {
            outline: 2px solid var(--accent);
            outline-offset: 2px;
        }
        .q-shift-subtitle {
            grid-column: 1 / -1;
            padding-top: 0.55rem;
            margin-top: 0.5rem;
            border-top: 1px dashed var(--border);
            font-size: 0.82rem;
            font-style: italic;
            color: var(--text-secondary);
            line-height: 1.45;
        }
        .q-shift-subtitle strong {
            font-style: normal;
            font-weight: 700;
            color: var(--text-primary);
        }

        /* Question card. Side-stripe + #ffffff per PRODUCT.md intentional
           overrides. Wider than leg-bill-card because question text needs
           reading-length room.
        */
        .q-card {
            display: block;
            padding: 0.7rem 1rem 0.75rem;
            border: 1px solid var(--border);
            border-left: 3px solid var(--border-strong);
            border-radius: 6px;
            background: #ffffff;
            margin-bottom: 0.5rem;
            transition: border-left-color 0.12s, border-color 0.12s;
        }
        .q-card:hover {
            border-left-color: var(--accent);
            border-color: var(--accent-dim);
        }
        .q-card-head {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 0.55rem;
            font-size: 0.74rem;
            color: var(--text-meta);
            margin-bottom: 0.4rem;
        }
        .q-card-date {
            font-weight: 600;
            color: var(--text-secondary);
            white-space: nowrap;
            font-variant-numeric: tabular-nums;
        }
        .q-card-sep {
            color: var(--text-meta);
            opacity: 0.7;
        }
        .q-card-kicker {
            font-weight: 600;
            color: var(--text-primary);
            letter-spacing: 0.01em;
        }
        .q-card-type {
            display: inline-flex;
            align-items: center;
            padding: 0.15rem 0.5rem;
            font-size: 0.68rem;
            font-weight: 600;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            border-radius: 999px;
            margin-left: auto;
        }
        .q-card-type-written {
            background: #ffffff;
            color: var(--text-secondary);
            border: 1px solid var(--border);
        }
        .q-card-type-oral {
            background: var(--accent-subtle);
            color: var(--accent);
            border: 1px solid var(--accent-dim);
        }
        .q-card-body {
            font-size: 0.92rem;
            line-height: 1.5;
            color: var(--text-primary);
            margin: 0.1rem 0 0.55rem;
        }
        .q-card-body details summary {
            cursor: pointer;
            list-style: none;
        }
        .q-card-body details summary::-webkit-details-marker { display: none; }
        .q-card-body details summary::after {
            content: " Read full text ▾";
            font-size: 0.78rem;
            font-weight: 600;
            color: var(--accent);
            margin-left: 0.25rem;
        }
        .q-card-body details[open] summary::after {
            content: " Show less ▴";
        }
        .q-card-body details[open] .q-card-truncated { display: none; }
        .q-card-fulltext { margin-top: 0.4rem; }
        .q-card-foot {
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            gap: 0.85rem;
        }
        .q-card-ref {
            font-family: 'JetBrains Mono', 'Consolas', monospace;
            font-size: 0.72rem;
            color: var(--text-meta);
            letter-spacing: 0.01em;
            white-space: nowrap;
        }

        /* ── Legislation: pipeline phase strip ──────────────────────── */
        .leg-pipeline-strip {
            display: flex;
            align-items: stretch;
            margin: 1.25rem 0 1rem;
            border: 1px solid var(--border);
            border-radius: 2px;
            overflow: hidden;
        }
        .leg-pipeline-card {
            flex: 1;
            padding: 1.1rem 1.4rem;
            background: #ffffff;
        }
        .leg-pipeline-sep {
            display: flex;
            align-items: center;
            padding: 0 0.85rem;
            background: var(--surface);
            color: var(--border-strong);
            font-size: 1.1rem;
            border-left: 1px solid var(--border);
            border-right: 1px solid var(--border);
            flex-shrink: 0;
        }
        .leg-pipeline-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 2.4rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1;
            letter-spacing: -0.03em;
        }
        .leg-pipeline-label {
            font-size: 0.85rem;
            font-weight: 700;
            color: var(--text-primary);
            margin: 0.3rem 0 0.1rem;
        }
        .leg-pipeline-sub {
            font-size: 0.71rem;
            color: var(--text-meta);
            letter-spacing: 0.01em;
        }

        /* Mobile: stack pipeline cards vertically (was clipping the third
           "Enacted" card off-screen on 390px wide). Rotate the → separator
           to point down between stacked cards. */
        @media (max-width: 640px) {
            .leg-pipeline-strip {
                flex-direction: column;
            }
            .leg-pipeline-sep {
                padding: 0.35rem 0;
                border-left: none;
                border-right: none;
                border-top: 1px solid var(--border);
                border-bottom: 1px solid var(--border);
                justify-content: center;
                transform: rotate(90deg);
            }
        }

        /* ── Legislation: SI card + pre-2014 act long-title ─────────── */
        /* Inline-style extraction for legislation.py SI cards (P2-1 fix). */
        .leg-si-card {
            margin-bottom: 0.3rem;
        }
        .leg-si-meta {
            margin-top: 0.2rem;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }
        .leg-pre2014-long-title {
            margin: 0.45rem 0 0.35rem;
        }
        .leg-long-title-tight {
            margin: 0.45rem 0 0.35rem;
        }

        /* ── Legislation: debate list in detail view ────────────────── */
        .leg-debate-list { display: flex; flex-direction: column; }
        .leg-debate-row {
            display: flex; gap: 0.75rem; padding: 0.5rem 0;
            border-bottom: 1px solid var(--border); align-items: baseline;
        }
        .leg-debate-row:last-child { border-bottom: none; }
        .leg-debate-date {
            font-size: 0.75rem; color: var(--text-meta); white-space: nowrap;
            min-width: 5.5rem; flex-shrink: 0;
        }
        .leg-debate-title {
            font-size: 0.83rem; font-weight: 600; color: var(--accent);
            text-decoration: none; flex: 1; line-height: 1.4;
        }
        .leg-debate-title:hover { text-decoration: underline; }
        .leg-debate-title-plain {
            font-size: 0.83rem; font-weight: 600; color: var(--text-primary); flex: 1;
        }
        .leg-debate-chamber {
            font-size: 0.70rem; color: var(--text-meta); white-space: nowrap; flex-shrink: 0;
        }

        /* (Legislation pipeline-TODO callout removed 2026-05-26 — the
           unscoped fetcher now lands Government Bills in silver, so the
           "Government Bills not yet indexed" notice was inaccurate. If a
           page needs a citizen-facing "Coming soon" notice in future, use
           the shared `todo_callout()` helper in ui/components.py.) */

        /* ── Cross-page entity links ─────────────────────────────────── */
        /* Inline anchor used wherever a TD name links to their profile.
           See utility/ui/entity_links.py — never hand-roll these styles. */
        .dt-member-link {
            color: var(--text-primary, #111827);
            text-decoration: underline;
            text-decoration-color: rgba(0,0,0,0.22);
            text-underline-offset: 2px;
            text-decoration-thickness: 1px;
            font-weight: inherit;
            transition: color 0.12s, text-decoration-color 0.12s;
        }
        .dt-member-link:hover {
            color: var(--accent, #b04a1a);
            text-decoration-color: var(--accent, #b04a1a);
        }
        .dt-member-link:focus-visible {
            outline: 2px solid var(--accent, #b04a1a);
            outline-offset: 2px;
            border-radius: 2px;
        }

        /* Bold pill anchor for prominent profile-jump links. */
        .dt-entity-cta {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            margin-top: 0.5rem;
            padding: 0.5rem 1.1rem;
            background: var(--text-primary, #111827);
            color: #ffffff;
            border-radius: 2px;
            text-decoration: none;
            font-weight: 700;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.82rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            transition: background 0.12s;
        }
        .dt-entity-cta:hover {
            background: var(--accent, #b04a1a);
            color: #ffffff;
        }
        .dt-entity-cta:focus-visible {
            outline: 2px solid var(--accent, #b04a1a);
            outline-offset: 2px;
        }

        /* ── Mobile layout ───────────────────────────────────────────── */
        @media (max-width: 640px) {
            /* Stack st.columns vertically */
            [data-testid="stHorizontalBlock"] {
                flex-direction: column !important;
                gap: 0.25rem !important;
            }
            [data-testid="stColumn"] {
                width: 100% !important;
                min-width: 100% !important;
                flex: 1 1 100% !important;
            }

            /* Tighten main content padding */
            .main .block-container {
                padding-left: 0.75rem !important;
                padding-right: 0.75rem !important;
                max-width: 100% !important;
            }

            /* Member name: smaller on mobile */
            .td-name {
                font-size: 1.45rem !important;
            }

            /* Pills wrap on narrow screens */
            [data-testid="stPills"] > div {
                flex-wrap: wrap !important;
            }

            /* Metric values: reduce size */
            [data-testid="stMetric"] label {
                font-size: 0.68rem !important;
            }
            [data-testid="stMetricValue"] {
                font-size: 1.3rem !important;
            }

            /* Download button: full width */
            .stDownloadButton > button {
                width: 100% !important;
            }

            /* Hero / kicker / large headings scale down so they don't blow
               out the viewport on narrow phones. Targets the 1.5rem+ tier. */
            .dt-hero { padding: 0.9rem 1rem 0.8rem !important; }
            .dt-hero h1 { font-size: 1.35rem !important; }
            .dt-dek    { font-size: 0.85rem !important; }

            /* Cards: tighter padding so 100vw cards still breathe. */
            .dt-info-card,
            .int-member-card,
            .vt-card,
            .att-list-pill,
            .att-hall-card-good,
            .att-hall-card-bad {
                padding: 0.45rem 0.7rem !important;
            }

            /* Section dividers/sticky headings: smaller on mobile. */
            .section-heading,
            .lob-section-heading { font-size: 0.65rem !important; }

            /* Custom vote tables: allow horizontal scroll instead of
               crushing 5 columns into 360px. */
            .dt-vt-table {
                display: block !important;
                overflow-x: auto !important;
                white-space: nowrap !important;
                -webkit-overflow-scrolling: touch;
            }

            /* The right-hand "→" button column in card_row pairs: stretched
               to full-width feels wrong; make it a visible secondary action. */
            [data-testid="stColumn"] .stButton > button[kind="secondary"],
            [data-testid="stColumn"] .stButton > button {
                width: 100% !important;
            }

            /* Sidebar hidden on mobile by default (Streamlit behaviour);
               notable members are accessible via the sidebar toggle. */
        }

        /* ── Interests: member index cards ──────────────────────────────── */
        .int-member-card {
            padding: 0.4rem 0.9rem;
            border: 1px solid rgba(0,0,0,0.08);
            border-left: 3px solid rgba(0,0,0,0.14);
            border-radius: 12px;
            background: #ffffff;
            width: 100%;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            transition: border-left-color 0.12s, border-color 0.12s, box-shadow 0.12s;
        }
        .int-member-card:hover {
            border-left-color: var(--accent);
            border-color: var(--accent-dim);
            box-shadow: 0 2px 8px rgba(0,0,0,0.09);
        }
        [data-testid="stHorizontalBlock"]:has(.int-member-card) {
            gap: 0.35rem !important;
            margin-bottom: 0.3rem !important;
            align-items: stretch !important;
        }
        [data-testid="stHorizontalBlock"]:has(.int-member-card)
            [data-testid="stColumn"]:first-child {
            flex: 1 1 auto !important;
            min-width: 0 !important;
        }
        [data-testid="stHorizontalBlock"]:has(.int-member-card)
            [data-testid="stColumn"]:last-child {
            flex: 0 0 auto !important;
            width: auto !important;
        }

        /* ── Votes: division index cards (Mode A) ─────────────────────── */
        .vt-card {
            padding: 0.4rem 0.9rem;
            border: 1px solid rgba(0,0,0,0.08);
            border-left: 3px solid rgba(0,0,0,0.14);
            border-radius: 12px;
            background: #ffffff;
            width: 100%;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            transition: border-left-color 0.12s, border-color 0.12s, box-shadow 0.12s;
        }
        /* Cap the clickable wrap (and therefore the card + arrow strip)
           to a comfortable reading width. The card keeps width:100% and
           adapts to the wrap, so click area + arrow + visible card edge
           all stay aligned. Look, colours and behaviour are unchanged —
           the right-hand whitespace is just reclaimed. */
        .dt-card-link-wrap:has(.vt-card) {
            max-width: 760px;
        }
        .vt-card:hover {
            border-left-color: var(--accent);
            border-color: var(--accent-dim);
            box-shadow: 0 2px 8px rgba(0,0,0,0.09);
        }
        .vt-card-header {
            display: flex;
            align-items: center;
            gap: 0.55rem;
            margin-bottom: 0.28rem;
        }
        .vt-card-date {
            font-size: 0.73rem;
            color: var(--text-meta);
            white-space: nowrap;
        }
        .vt-card-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.97rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.35;
            margin-bottom: 0.25rem;
        }
        .vt-card-footer {
            display: flex;
            align-items: center;
            gap: 0.4rem;
            flex-wrap: wrap;
            margin-top: 0.15rem;
        }
        /* Yes/No use the deuteranopia-safe blue/orange signal tokens, not
           red/green; the "Yes"/"No" text label is the non-colour channel. */
        .vt-count-yes {
            background: var(--signal-good-subtle);
            color: var(--signal-good-deep);
            font-size: 0.75rem;
            font-weight: 700;
            padding: 0.12rem 0.55rem;
            border-radius: 999px;
            white-space: nowrap;
        }
        .vt-count-no {
            background: var(--signal-bad-subtle);
            color: var(--signal-bad-deep);
            font-size: 0.75rem;
            font-weight: 700;
            padding: 0.12rem 0.55rem;
            border-radius: 999px;
            white-space: nowrap;
        }
        .vt-count-abs {
            background: #f4f4f4;
            color: var(--text-meta);
            font-size: 0.75rem;
            font-weight: 500;
            padding: 0.12rem 0.55rem;
            border-radius: 999px;
            white-space: nowrap;
        }
        .vt-outcome-carried {
            background: var(--signal-good-subtle);
            color: var(--signal-good-deep);
            font-size: 0.72rem;
            font-weight: 700;
            padding: 0.12rem 0.55rem;
            border-radius: 2px;
            white-space: nowrap;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .vt-outcome-lost {
            background: var(--signal-bad-subtle);
            color: var(--signal-bad-deep);
            font-size: 0.72rem;
            font-weight: 700;
            padding: 0.12rem 0.55rem;
            border-radius: 2px;
            white-space: nowrap;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .vt-margin-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.32rem;
            background: linear-gradient(180deg, #ffffff 0%, #f5f5f1 100%);
            border: 1px solid rgba(0,0,0,0.10);
            box-shadow: inset 0 -1px 0 rgba(0,0,0,0.04), 0 1px 1px rgba(0,0,0,0.03);
            color: var(--text-meta);
            font-size: 0.75rem;
            padding: 0.12rem 0.55rem 0.12rem 0.45rem;
            border-radius: 999px;
            white-space: nowrap;
            margin-left: auto;
            font-variant-numeric: tabular-nums;
        }
        .vt-margin-label {
            color: var(--text-meta);
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            opacity: 0.7;
        }
        .vt-margin-value {
            color: var(--text-primary, #111827);
            font-weight: 700;
        }
        /* P1-1 stage pill: legislative stage extracted from debate_title
           after the first colon ("Committee and Remaining Stages",
           "Second Stage (Resumed)", "Motion (Resumed)", etc). Lives in
           the card header next to the date + outcome so cards for the
           same bill at different stages are visually distinct without
           cluttering the title line. */
        .vt-card-stage {
            background: #fafaf7;
            color: var(--text-meta);
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 600;
            letter-spacing: 0.03em;
            padding: 0.1rem 0.55rem;
            border-radius: 2px;
            border: 1px solid rgba(0,0,0,0.06);
            white-space: nowrap;
            text-transform: none;
            margin-left: auto;
            max-width: 18ch;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        /* P2-8 Private Members pill — same chip family as the stage pill
           but a slightly warmer surface so the two read as siblings
           (procedural facts about the vote) rather than competing. Sits
           in the same header strip; when both are present the stage
           pushes right via margin-left:auto and the private pill follows.
           Tooltip carries the citizen-facing definition. */
        .vt-card-private {
            background: var(--orange-050);
            color: #7a4500;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 600;
            letter-spacing: 0.03em;
            padding: 0.1rem 0.55rem;
            border-radius: 2px;
            border: 1px solid #fde0b8;
            white-space: nowrap;
            text-transform: none;
        }
        .vt-card-stage + .vt-card-private { margin-left: 0.35rem; }
        .vt-card-header > .vt-card-private:not(.vt-card-stage + *) {
            margin-left: auto;
        }
        /* Pillrow for cards that want a dedicated row below the title.
           Not currently used (header carries both), kept for forward
           compatibility if pill count grows. */
        .vt-card-pillrow {
            display: flex;
            gap: 0.35rem;
            flex-wrap: wrap;
            margin: 0.15rem 0 0.35rem;
        }
        .vt-card-pillrow:empty { display: none; }

        /* P1-4 + P2-2: Oireachtas link demoted from card header (accent-
           coloured, one per card) to card footer (quiet grey). The
           internal navigation arrow added by clickable_card_link is now
           the visually-primary affordance. */
        .vt-card-footer .dt-source-link {
            color: var(--text-meta);
            font-weight: 400;
            font-size: 0.75rem;
            margin-left: auto;
        }
        .vt-card-footer .dt-source-link::after {
            color: var(--text-meta);
            opacity: 0.8;
        }
        .vt-card-footer .dt-source-link:hover {
            color: var(--text-primary);
        }
        /* When both a margin pill AND a source link land in the footer,
           the margin pill keeps its right-aligned position and the link
           sits next to it (not pushed further right by its own auto). */
        .vt-card-footer .vt-margin-pill + .dt-source-link { margin-left: 0.4rem; }

        /* Legacy position rules for cards that still emit the source
           link in the header (other consumers of vt_division_card_html
           pattern). */
        .vt-card-header .dt-source-link,
        .vt-card-header .vt-source-link { margin-left: auto; }
        .vt-card-footer .dt-source-link + .vt-margin-pill,
        .vt-card-footer .vt-source-link + .vt-margin-pill { margin-left: 0; }

        /* ── Reusable member-vote card (vt_explorer.member_vote_card_html) ─
           One TD's vote on a single division. Used on Member Overview's
           "Voting record by issue" and anywhere a TD's per-division vote
           needs to be shown with green ✓ / red ✗. */
        .vt-rec-card {
            padding: 0.55rem 0.9rem;
            margin-bottom: 0.35rem;
            border: 1px solid rgba(0,0,0,0.08);
            border-left: 3px solid rgba(0,0,0,0.14);
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }
        .vt-rec-card-yes  { border-left-color: oklch(50% 0.140 145); }
        .vt-rec-card-no   { border-left-color: oklch(55% 0.180 30);  }
        .vt-rec-card-abs  { border-left-color: rgba(0,0,0,0.18); }
        .vt-rec-header {
            display: flex;
            align-items: center;
            gap: 0.55rem;
            margin-bottom: 0.25rem;
            flex-wrap: wrap;
        }
        .vt-rec-vote {
            display: inline-flex;
            align-items: center;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.03em;
            text-transform: uppercase;
            padding: 0.18rem 0.55rem;
            border-radius: 999px;
            white-space: nowrap;
        }
        .vt-rec-vote-yes {
            background: var(--signal-good-subtle);
            color: var(--signal-good-deep);
            border: 1px solid var(--signal-good-border);
        }
        .vt-rec-vote-no {
            background: var(--signal-bad-subtle);
            color: var(--signal-bad-deep);
            border: 1px solid var(--signal-bad-border);
        }
        .vt-rec-vote-abs {
            background: #f4f4f5;
            color: #52525b;
            border: 1px solid #e4e4e7;
        }
        .vt-rec-header .dt-source-link,
        .vt-rec-header .vt-source-link { margin-left: auto; }
        .vt-index-caption {
            font-size: 0.80rem;
            color: var(--text-meta);
            margin: 0.25rem 0 0.6rem;
        }
        .vt-division-header {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 0.3rem;
        }
        /* Oireachtas source link rides the right edge of the division header
           so the official record is one click away from the panel itself,
           not only the sources footer. */
        .vt-division-header .dt-source-link {
            margin-left: auto;
            white-space: nowrap;
        }
        .vt-division-title {
            font-size: 0.95rem;
            font-weight: 600;
            line-height: 1.45;
            margin: 0 0 0.5rem;
            color: var(--text-primary);
        }
        [data-testid="stHorizontalBlock"]:has(.vt-card) {
            gap: 0.35rem !important;
            margin-bottom: 0.3rem !important;
            align-items: stretch !important;
            justify-content: flex-start !important;
        }
        [data-testid="stHorizontalBlock"]:has(.vt-card)
            [data-testid="stColumn"]:first-child {
            flex: 1 1 auto !important;
            max-width: 860px !important;
            min-width: 0 !important;
        }
        [data-testid="stHorizontalBlock"]:has(.vt-card)
            [data-testid="stColumn"]:last-child {
            flex: 0 0 auto !important;
            width: auto !important;
        }

        /* ── TD picker landing cards ──────────────────────────────────── */
        .td-pick-dek {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.95rem;
            color: var(--text-meta, #5a5a5a);
            line-height: 1.55;
            margin: 0.1rem 0 1.25rem;
            max-width: 56ch;
        }
        .td-pick-card {
            border: 1px solid rgba(0,0,0,0.08);
            border-left: 4px solid rgba(0,0,0,0.14);
            border-radius: 12px;
            background: #ffffff;
            padding: 1.05rem 1.15rem 0.9rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            min-height: 13rem;
            display: flex;
            flex-direction: column;
            gap: 0.32rem;
            flex: 1 1 auto;
            transition: box-shadow 0.15s, border-left-color 0.15s;
        }
        .td-pick-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.09);
        }
        /* Card is now a single flowing statement: <Name> voted YES on <Bill>.
           Vote, name, title classes all render inline inside .td-pick-statement
           rather than as stacked blocks with a separate badge. */
        .td-pick-statement {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.02rem;
            line-height: 1.5;
            color: var(--text-primary, #111827);
            margin: 0 0 0.5rem;
        }
        .td-pick-vote {
            display: inline-block;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            padding: 0.08rem 0.5rem;
            border-radius: 999px;
            white-space: nowrap;
            vertical-align: 0.05em;
            margin: 0 0.15rem;
        }
        .td-pick-vote-yes {
            background: var(--signal-good-subtle);
            color: var(--signal-good-deep);
            border: 1px solid var(--signal-good-border);
        }
        .td-pick-vote-no {
            background: var(--signal-bad-subtle);
            color: var(--signal-bad-deep);
            border: 1px solid var(--signal-bad-border);
        }
        .td-pick-vote-abs {
            background: #f4f4f5;
            color: #52525b;
            border: 1px solid #e4e4e7;
        }
        .td-pick-statement .td-pick-name {
            font-family: 'Epilogue', sans-serif;
            font-weight: 700;
            color: var(--text-primary, #111827);
        }
        .td-pick-statement .td-pick-title {
            font-weight: 600;
        }
        .td-pick-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            color: var(--text-meta, #5a5a5a);
            margin: 0;
        }
        /* Stretch picker cards in a row to equal height — no separate
           action-button styling here any more (the card is the click target
           via clickable_card_link). */
        [data-testid="stHorizontalBlock"]:has(.td-pick-card) {
            align-items: stretch !important;
        }
        [data-testid="stHorizontalBlock"]:has(.td-pick-card)
            [data-testid="stColumn"] {
            display: flex;
            flex-direction: column;
        }
        .td-pick-foot {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            color: var(--text-meta, #5a5a5a);
            margin-top: 1.25rem;
            font-style: italic;
        }

        /* ── Pager (reusable page numbers + page-size selector) ────────── */
        .dt-pager {
            display: block;
            margin: 0.2rem 0 0;
        }
        .dt-pager-current {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 2.1rem;
            height: 2.1rem;
            padding: 0 0.55rem;
            border-radius: 6px;
            background: var(--text-primary, #111827);
            color: #ffffff;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.82rem;
            font-weight: 700;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        }
        .dt-pager-ellipsis {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            /* Match the number chips' min-width (.dt-pager-current and the
               .stButton chips below) so swapping a number for "…" between
               pages doesn't change the row's total width — chips stay put. */
            min-width: 2.1rem;
            height: 2.1rem;
            color: var(--text-meta);
            font-size: 0.95rem;
            font-weight: 600;
            user-select: none;
        }
        .dt-pager-caption {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            color: var(--text-meta);
            margin: 0.5rem 0 0;
            letter-spacing: 0.01em;
        }
        .dt-pager-caption strong {
            color: var(--text-primary, #111827);
            font-weight: 700;
        }
        .dt-pager-size-label {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin: 0 0 0.25rem;
            text-align: right;
        }
        /* Tight chip row: collapse the equal-width column layout that
           Streamlit applies to st.columns() so chips sit next to each other. */
        [data-testid="stColumn"]:has(> div .dt-pager) [data-testid="stHorizontalBlock"] {
            gap: 0.18rem !important;
            justify-content: flex-start !important;
            flex-wrap: wrap;
            align-items: center !important;
        }
        [data-testid="stColumn"]:has(> div .dt-pager) [data-testid="stHorizontalBlock"]
            > [data-testid="stColumn"] {
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: 0 !important;
            max-width: none !important;
        }
        /* Style Streamlit buttons inside the pager column to look like page chips */
        [data-testid="stColumn"]:has(> div .dt-pager) [data-testid="stButton"] > button {
            min-width: 2.1rem;
            height: 2.1rem;
            padding: 0 0.55rem;
            border-radius: 6px;
            border: 1px solid rgba(0,0,0,0.12);
            background: #ffffff;
            color: var(--text-primary);
            font-family: 'Epilogue', sans-serif;
            font-size: 0.82rem;
            font-weight: 600;
            line-height: 1;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }
        [data-testid="stColumn"]:has(> div .dt-pager) [data-testid="stButton"] > button:hover {
            border-color: var(--accent, #b04a1a);
            color: var(--accent, #b04a1a);
        }
        [data-testid="stColumn"]:has(> div .dt-pager) [data-testid="stButton"] > button:disabled {
            opacity: 0.35;
            cursor: not-allowed;
        }

        /* ── Committee Register (cmt-*) ──────────────────────────────────── */
        .cmt-row {
            display: inline-flex;
            align-items: stretch;
            gap: 0;
            width: fit-content;
            max-width: 100%;
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 3px solid var(--accent-dim);
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            padding: 0;
            overflow: hidden;
        }
        /* P2-2 audit fix: the rank-chip column carried a peach tint that
           read as a "selected first card" affordance even though no
           selection state existed. Neutralised to the same warm-surface
           token used elsewhere so it visually anchors but doesn't shout
           that the first row is special. */
        .cmt-row-rank {
            display: flex;
            align-items: center;
            justify-content: center;
            min-width: 2.6rem;
            padding: 0.55rem 0.4rem;
            background: var(--surface-deep, #f5f1ea);
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            font-weight: 800;
            color: var(--text-meta);
            border-right: 1px solid var(--border);
        }
        .cmt-row-body {
            flex: 1;
            min-width: 0;
            padding: 0.6rem 0.95rem;
            display: flex;
            flex-direction: column;
            gap: 0.32rem;
        }
        .cmt-row-head {
            display: flex;
            align-items: baseline;
            gap: 0.6rem;
            flex-wrap: wrap;
        }
        .cmt-row-name {
            font-family: 'Epilogue', sans-serif;
            font-size: 1rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.25;
        }
        .cmt-row-status {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.62rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            padding: 0.12rem 0.5rem;
            border-radius: 999px;
            border: 1px solid;
        }
        .cmt-row-status-active { color: var(--vote-carried); background: oklch(96% 0.045 145); border-color: oklch(82% 0.080 145); }
        .cmt-row-status-ended  { color: var(--text-meta);     background: var(--surface);     border-color: var(--border-strong); }
        .cmt-row-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            color: var(--text-meta);
            line-height: 1.4;
        }
        .cmt-row-meta strong { color: var(--text-secondary); font-weight: 700; }
        .cmt-row-pills {
            display: flex;
            flex-wrap: wrap;
            gap: 0.3rem;
            margin-top: 0.1rem;
        }
        .cmt-row-link {
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            color: var(--accent);
            text-decoration: none;
            border: 1px solid var(--accent-dim);
            border-radius: 999px;
            padding: 0.12rem 0.55rem;
            background: var(--accent-subtle);
        }
        .cmt-row-link:hover { text-decoration: underline; }

        /* Inline party stripe for the primary register card */
        .cmt-stripe {
            display: flex;
            width: 100%;
            height: 7px;
            border-radius: 4px;
            overflow: hidden;
            background: oklch(96% 0.005 75);
            margin-top: 0.15rem;
        }
        .cmt-stripe-seg { height: 100%; }
        .cmt-stripe-legend {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem 0.7rem;
            margin-top: 0.25rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            color: var(--text-meta);
        }
        .cmt-stripe-legend-dot {
            display: inline-block;
            width: 0.55rem;
            height: 0.55rem;
            border-radius: 2px;
            margin-right: 0.3rem;
            vertical-align: middle;
        }
        .cmt-stripe-legend strong { color: var(--text-secondary); font-weight: 700; }

        /* Collapse the Streamlit columns row that holds <card> + <→> so the
           button sits adjacent to the fit-content card, not at the far right. */
        [data-testid="stHorizontalBlock"]:has(.cmt-row) {
            width: fit-content !important;
            max-width: 100%;
            gap: 0.4rem !important;
            align-items: center;
        }
        [data-testid="stHorizontalBlock"]:has(.cmt-row) > [data-testid="stColumn"] {
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: 0 !important;
        }

        /* Stage-2 committee identity strip */
        .cmt-identity {
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 10px;
            padding: 0.85rem 1.1rem;
            margin: 0.3rem 0 0.9rem;
        }
        /* P2-3: identity-head wraps name + status chip so the chip sits
           beside the committee name (same chip styling as register cards
           via .cmt-row-status-*). Without this, the status was inline
           text in the meta line; register and detail diverged. */
        .cmt-identity-head {
            display: flex;
            align-items: baseline;
            gap: 0.6rem;
            flex-wrap: wrap;
        }
        .cmt-identity-name {
            font-family: 'Epilogue', sans-serif;
            font-size: 1.45rem;
            font-weight: 800;
            color: var(--text-primary);
            line-height: 1.2;
            margin: 0;
        }
        .cmt-identity-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.86rem;
            color: var(--text-meta);
            margin: 0.25rem 0 0.5rem;
        }
        .cmt-identity-links {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin-top: 0.4rem;
        }

        /* Mobile flow: stack stripe legend, drop arrow below card */
        @media (max-width: 720px) {
            .cmt-row { width: 100%; flex-direction: column; }
            .cmt-row-rank { min-width: 100%; border-right: none; border-bottom: 1px solid var(--border); }
            [data-testid="stHorizontalBlock"]:has(.cmt-row) { width: 100% !important; }
            .cmt-identity-name { font-size: 1.15rem; }
        }

        /* ── Lobbying PoC (lobbying_3.py) ─────────────────────────────────
           lp3-* prefix prevents collision with lobby_2's lob-* classes.
           All rules use existing tokens (--text-primary, --text-meta, --border,
           --accent, --surface) — no raw hex. Goal is calm: TWFY-style prose
           heroes, Datasette-tone tables, ranked cards only where they earn
           their place. */

        .lp3-hero {
            margin: 0 0 2rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border);
        }
        .lp3-h1 {
            margin: 0 0 0.4rem;
            font-family: var(--font-heading, "Source Serif 4", serif);
            font-size: 2.1rem;
            font-weight: 700;
            color: var(--text-primary);
            letter-spacing: -0.025em;
            line-height: 1.15;
        }
        .lp3-dek {
            margin: 0;
            max-width: 65ch;
            font-size: 1rem;
            line-height: 1.55;
            color: var(--text-meta);
        }
        /* Numbers inside the dek prose are bolded AND tinted navy — the eye
           skims them out of the grey body text without breaking the prose
           rhythm. Same treatment in any .lp3-prose paragraph. */
        .lp3-dek strong,
        .lp3-prose strong {
            color: var(--signal-good-deep);
            font-weight: 700;
        }
        .lp3-prose {
            margin: 0 0 1rem;
            max-width: 70ch;
            font-size: 0.95rem;
            line-height: 1.6;
            color: var(--text-primary);
        }

        .lp3-section-head {
            margin: 1.75rem 0 0.75rem;
            padding-bottom: 0.45rem;
            border-bottom: 1px solid var(--border);
        }
        .lp3-h2 {
            margin: 0;
            font-family: var(--font-heading, "Source Serif 4", serif);
            font-size: 1.15rem;
            font-weight: 700;
            color: var(--text-primary);
            letter-spacing: -0.01em;
        }
        .lp3-section-dek {
            margin: 0.2rem 0 0;
            max-width: 65ch;
            font-size: 0.85rem;
            color: var(--text-meta);
            line-height: 1.5;
        }

        /* Gateway tile — navy border-top (lobby_2's signature accent) gives
           the trio a quiet brand colour without re-introducing icons or
           large stat numbers. Hover shifts the stripe to the warmer accent. */
        .lp3-tile {
            background: #ffffff;
            border: 1px solid var(--border);
            border-top: 3px solid var(--signal-good-deep);
            border-radius: 8px;
            padding: 1rem 1.1rem 0.9rem;
            min-height: 110px;
            transition: border-color 0.15s, box-shadow 0.15s;
        }
        .lp3-tile:hover {
            border-color: var(--text-meta);
            border-top-color: var(--accent);
            box-shadow: 0 1px 3px rgba(17,24,39,0.05);
        }
        .lp3-tile-heading {
            margin: 0 0 0.4rem;
            font-family: var(--font-heading, "Source Serif 4", serif);
            font-size: 1rem;
            font-weight: 700;
            color: var(--text-primary);
            letter-spacing: -0.005em;
        }
        .lp3-tile-body {
            margin: 0;
            font-size: 0.85rem;
            line-height: 1.55;
            color: var(--text-meta);
        }

        /* Topic tile — same shape as the gateway tile but the brand's warm
           accent (rust) carries the left stripe to signal "free-text scan,
           not a register category". Stronger than the dim stripe it had
           before; matches lobby_2's rust topic treatment without dashed borders. */
        .lp3-topic-tile {
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 8px;
            padding: 1rem 1.1rem 0.9rem;
            min-height: 110px;
            transition: border-color 0.15s, box-shadow 0.15s;
        }
        .lp3-topic-tile:hover {
            border-color: var(--accent);
            box-shadow: 0 1px 3px rgba(17,24,39,0.05);
        }
        .lp3-topic-tile .lp3-tile-heading {
            color: var(--signal-bad-deep);
        }

        /* Switcher selectboxes (Switch organisation / Switch policy area)
           on the Lobbying-PoC Stage 2 pages need a pure white background;
           the default var(--surface) is warm beige and looked off. Replaces
           the inline <style> blocks previously injected by the page (audit
           P2-2). */
        .st-key-lp3_org_switcher .stSelectbox > div > div,
        .st-key-lp3_org_switcher [data-baseweb="select"] > div,
        .st-key-lp3_area_switcher .stSelectbox > div > div,
        .st-key-lp3_area_switcher [data-baseweb="select"] > div {
            background: #ffffff !important;
        }

        /* Latest-returns prose list — replaces lobby_2's custom row HTML
           with a clean <ul> of dated entries. Reads as a record, not a UI. */
        .lp3-recent-list {
            list-style: none;
            margin: 0;
            padding: 0;
        }
        .lp3-recent-item {
            display: flex;
            gap: 0.85rem;
            padding: 0.55rem 0;
            border-bottom: 1px solid var(--border);
            font-size: 0.9rem;
            line-height: 1.5;
        }
        .lp3-recent-item:last-child { border-bottom: none; }
        .lp3-recent-period {
            flex-shrink: 0;
            min-width: 5rem;
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--signal-good-deep);
            letter-spacing: 0.02em;
            text-transform: uppercase;
        }
        .lp3-recent-body { color: var(--text-primary); }
        .lp3-recent-body strong { font-weight: 700; }
        .lp3-recent-body em {
            font-style: italic;
            color: var(--text-meta);
        }
        .lp3-recent-link {
            color: var(--text-primary);
            text-decoration: none;
        }
        .lp3-recent-link strong { font-weight: 700; }
        .lp3-recent-link em {
            font-style: italic;
            color: var(--text-meta);
        }
        .lp3-recent-link:hover,
        .lp3-recent-link:focus-visible {
            color: var(--accent);
            text-decoration: underline;
            text-underline-offset: 2px;
        }
        .lp3-recent-link:hover em,
        .lp3-recent-link:focus-visible em { color: var(--accent); }
        .lp3-recent-link:focus-visible {
            outline: 2px solid var(--accent);
            outline-offset: 2px;
            border-radius: 2px;
        }

        /* Topic Stage 2 return card — narrative entry; the lobbying.ie
           source-link rides the header row. Per-return, not row-in-table.
           max-width keeps the card a readable column on wide screens —
           full-bleed cards pushed the right-aligned header link out of the
           reader's scanning path and it was routinely missed. */
        .lp3-return-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 0.85rem 1rem 0.75rem;
            margin: 0.5rem 0;
            max-width: 760px;
            transition: border-color 0.15s;
        }
        .lp3-return-card:hover { border-color: var(--text-meta); }
        .lp3-return-head {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 0.35rem;
        }
        /* Period chip — navy like lobby_2's .lob-activity-period. Lets the
           date read as the temporal anchor of the card at a glance. */
        .lp3-return-period {
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--signal-good-deep);
            letter-spacing: 0.02em;
            text-transform: uppercase;
        }
        /* Area pill — subtle rust tint signals "policy area / topic" without
           competing with the period chip. Matches the .lp3-topic-tile family. */
        .lp3-return-area {
            font-size: 0.74rem;
            font-weight: 600;
            color: var(--signal-bad-deep);
            background: var(--signal-bad-subtle);
            border: 1px solid var(--signal-bad-border);
            padding: 0.1rem 0.5rem;
            border-radius: 999px;
        }
        /* Return-# + source link share the right edge of the header row.
           Whichever of the two renders first takes the auto margin; when
           both are present the link sits flush beside the id. */
        .lp3-return-id {
            font-size: 0.74rem;
            color: var(--text-meta);
            margin-left: auto;
        }
        .lp3-return-head .dt-source-link {
            white-space: nowrap;
            margin-left: auto;
        }
        .lp3-return-id + .dt-source-link {
            margin-left: 0;
        }
        .lp3-return-org {
            margin: 0 0 0.2rem;
            font-family: var(--font-heading, "Source Serif 4", serif);
            font-size: 1rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .lp3-return-sub {
            margin: 0 0 0.4rem;
            font-size: 0.85rem;
            font-weight: 400;
            color: var(--text-meta);
        }
        /* "Filed by …" — quiet meta line carrying the lobbyist-side
           person_primarily_responsible field from the lobbying.ie return.
           Reads as a byline, not as a competing title. */
        .lp3-return-filed-by {
            margin: 0 0 0.35rem;
            font-size: 0.78rem;
            color: var(--text-meta);
        }
        .lp3-return-filed-by strong {
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 600;
            font-size: 0.7rem;
            margin-right: 0.35rem;
            color: var(--text-meta);
        }
        .lp3-return-snippet {
            margin: 0 0 0.55rem;
            font-size: 0.88rem;
            line-height: 1.55;
            color: var(--text-meta);
        }

        .lp3-sidebar-label {
            font-size: 0.72rem;
            font-weight: 700;
            color: var(--text-meta);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin: 1rem 0 0.35rem;
        }

        /* Mobile: tighter section spacing, single-col gateway. */
        @media (max-width: 720px) {
            .lp3-h1 { font-size: 1.55rem; }
            .lp3-section-head { margin: 1.25rem 0 0.55rem; }
            .lp3-recent-item { flex-direction: column; gap: 0.2rem; }
            .lp3-recent-period { min-width: 0; }
        }

        /* ── Party / candidate finance cards (Election 2024 page) ────────────
           Shared .don-* card + receipt styling used by the donations, party-
           spending and candidate tabs on the unified Election 2024 hub.
           Ink-on-paper ledger: money is one ink, party identity is the 3px
           editorial side-stripe (project signature), figures are tabular. */
        .don-grid { display:grid; grid-template-columns:1fr 1fr; gap:0.9rem; margin:0.4rem 0 0.2rem; }
        @media (max-width: 760px){ .don-grid { grid-template-columns:1fr; } }
        .don-card { display:block; background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75));
            border-left:3px solid var(--don-stripe, var(--ink-muted)); border-radius:7px;
            padding:0.95rem 1.05rem 0.9rem; text-decoration:none; color:inherit; position:relative;
            transition:box-shadow .18s cubic-bezier(.22,1,.36,1), border-color .18s; }
        .don-card:hover { box-shadow:0 1px 10px oklch(0% 0 0 / .07); border-color:var(--don-stripe); }
        .don-card .don-dir { position:absolute; top:0.9rem; right:1.05rem; font-size:0.72rem;
            letter-spacing:.04em; color:var(--ink-muted); }
        .don-ptitle { display:flex; align-items:center; gap:0.5rem; margin:0 0 0.1rem; }
        .don-swatch { width:9px; height:9px; border-radius:2px; background:var(--don-stripe); flex:none; }
        .don-ptitle h3 { font-size:0.98rem; font-weight:600; margin:0; color:var(--ink-strong); }
        .don-amount { font-size:1.7rem; font-weight:650; letter-spacing:-.015em; line-height:1.1;
            font-variant-numeric:tabular-nums; color:var(--ink-strong); margin:0.35rem 0 0.1rem; }
        .don-sub { font-size:0.8rem; color:var(--ink-muted); }
        .don-cardfoot { display:flex; justify-content:space-between; align-items:baseline; margin-top:0.7rem; }
        .don-cardfoot .go { color:var(--accent); font-weight:600; font-size:0.82rem; }
        /* receipts (donor list) */
        .don-receipts { background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75));
            border-left:3px solid var(--don-stripe, var(--ink-muted)); border-radius:7px; padding:0.2rem 1.1rem; }
        .don-rrow { display:grid; grid-template-columns:1fr auto auto auto; align-items:baseline;
            gap:0.4rem 1.1rem; padding:0.65rem 0; border-bottom:1px solid oklch(92% 0.005 75); }
        .don-rrow:last-child { border-bottom:none; }
        .don-rrow .dn { font-weight:500; color:var(--ink-strong); }
        .don-rrow .dt { color:var(--ink-muted); font-size:0.83rem; font-variant-numeric:tabular-nums; }
        .don-rrow .mt { font-size:0.66rem; letter-spacing:.06em; text-transform:uppercase; color:oklch(45% 0.01 75);
            border:1px solid oklch(90% 0.006 75); border-radius:4px; padding:0.1rem 0.4rem; align-self:center; }
        .don-rrow .da { font-variant-numeric:tabular-nums; font-weight:600; min-width:5.3rem; text-align:right;
            color:var(--ink-strong); }
        .don-vmark { font-size:0.64rem; font-weight:600; letter-spacing:.07em; text-transform:uppercase;
            color:var(--ink-muted); white-space:nowrap; }
        .don-rrow .don-vmark { grid-column:1 / -1; padding-top:0.1rem; }

        /* ── Election 2024 hub (unified GE2024 political finance) ─────────────
           Three SIPO returns under one roof: a tab strip, a "money map" flow of
           three independent totals (never summed), and a per-party "full picture"
           card showing all three streams as aligned proportional bars. */
        .e24-tabs { display:flex; flex-wrap:wrap; gap:0.3rem; margin:0.2rem 0 1.1rem;
            border-bottom:1px solid var(--border, oklch(88% 0.006 75)); padding-bottom:0.55rem; }
        .e24-tab { font-size:0.86rem; font-weight:600; color:var(--ink-muted); text-decoration:none;
            padding:0.4rem 0.85rem; border-radius:999px; border:1px solid transparent; white-space:nowrap;
            transition:background .15s, color .15s, border-color .15s; }
        .e24-tab:hover { color:var(--ink-strong); background:var(--surface-deep, oklch(96% 0.006 75)); }
        .e24-tab.active { color:#ffffff; background:var(--accent); border-color:var(--accent); }

        /* money map — three big totals connected by a directional flow. The
           framing copy makes clear these are separate records, not a balance. */
        .e24-map { display:grid; grid-template-columns:1fr auto 1fr auto 1fr; align-items:stretch;
            gap:0.4rem; margin:0.3rem 0 0.5rem; }
        @media (max-width: 820px){ .e24-map { grid-template-columns:1fr; }
            .e24-arrow { display:none; } }
        .e24-tier { background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75)); border-radius:9px;
            border-top:3px solid var(--e24-stripe, var(--ink-muted)); padding:0.85rem 1rem 0.9rem; display:flex;
            flex-direction:column; gap:0.1rem; }
        .e24-tier .lbl { font-size:0.7rem; font-weight:700; letter-spacing:.06em; text-transform:uppercase;
            color:var(--ink-muted); }
        .e24-tier .amt { font-size:1.85rem; font-weight:680; letter-spacing:-.02em; line-height:1.08;
            font-variant-numeric:tabular-nums; color:var(--ink-strong); margin:0.15rem 0 0.05rem; }
        .e24-tier .meta { font-size:0.78rem; color:var(--ink-muted); }
        .e24-tier .grain { font-size:0.72rem; color:oklch(48% 0.01 75); margin-top:0.3rem; font-style:italic; }
        .e24-arrow { display:flex; align-items:center; justify-content:center; color:var(--ink-muted);
            font-size:1.3rem; font-weight:400; }
        .e24-nosum { font-size:0.8rem; color:oklch(40% 0.03 60); background:oklch(96% 0.02 75);
            border:1px solid oklch(88% 0.03 70); border-left:3px solid oklch(70% 0.08 60); border-radius:6px;
            padding:0.55rem 0.8rem; margin:0.2rem 0 0.6rem; line-height:1.45; }

        /* per-party "full picture" card — three aligned stream bars */
        .e24-pcard { background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75));
            border-left:3px solid var(--e24-stripe, var(--ink-muted)); border-radius:8px;
            padding:0.85rem 1.05rem 0.9rem; }
        .e24-pcard .phead { display:flex; align-items:center; gap:0.5rem; margin-bottom:0.55rem; }
        .e24-pcard .phead .sw { width:9px; height:9px; border-radius:2px; background:var(--e24-stripe); flex:none; }
        .e24-pcard .phead h3 { font-size:1rem; font-weight:650; margin:0; color:var(--ink-strong); }
        .e24-streams { display:flex; flex-direction:column; gap:0.5rem; }
        .e24-stream { display:grid; grid-template-columns:9.5rem 1fr 6.5rem; align-items:center; gap:0.6rem; }
        @media (max-width: 620px){ .e24-stream { grid-template-columns:1fr; gap:0.15rem; } }
        .e24-stream-lbl { display:block; text-decoration:none; color:inherit; }
        .e24-stream .sl { font-size:0.78rem; color:var(--ink-muted); }
        .e24-stream-lbl:hover .sl { color:var(--accent); text-decoration:underline; }
        .e24-track { background:oklch(94% 0.005 75); border-radius:4px; height:0.7rem; overflow:hidden; }
        .e24-bar { display:block; height:100%; }
        .e24-bar.in  { background:#2e7d6b; }
        .e24-bar.agent { background:#3a6ea5; }
        .e24-bar.cand { background:#8a5a9e; }
        .e24-stream .sv { text-align:right; font-variant-numeric:tabular-nums; font-weight:600;
            color:var(--ink-strong); font-size:0.9rem; }
        .e24-stream .sv.none { color:var(--ink-muted); font-weight:400; }
        .e24-legend { display:flex; flex-wrap:wrap; gap:0.9rem; margin:0.1rem 0 0.7rem; font-size:0.76rem;
            color:var(--ink-muted); }
        .e24-legend .lk { display:inline-flex; align-items:center; gap:0.35rem; }
        .e24-legend .dot { width:9px; height:9px; border-radius:2px; flex:none; }

        /* ───────────────────────── Judiciary: The Bench & Courts ──────────
           Bench roster cards, career-arc timeline, appointing-authority chips
           and vacancy-lifecycle cards. Ink-on-paper restraint: white cards,
           full borders (no ad-hoc side-stripes — that signature is reserved
           for info_card/card_row), one accent on hover via .dt-card-link-wrap.
           Authority colours are blue/amber (deuteranopia-safe) AND text-labelled. */
        .jud-grid {
            display:grid; grid-template-columns:repeat(auto-fill, minmax(16.5rem, 1fr));
            gap:0.7rem; margin-top:0.3rem;
        }
        .jud-card {
            background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.7rem 0.85rem; display:flex; flex-direction:column; gap:0.18rem; height:100%;
        }
        .jud-card.vacant { background:var(--surface); border-style:dashed; }
        .jud-jn { font-weight:650; color:var(--ink-strong); font-size:0.95rem; line-height:1.2; }
        .jud-jc { font-size:0.76rem; color:var(--text-meta); }
        .jud-appt { font-size:0.8rem; color:oklch(38% 0.012 75); margin-top:0.1rem; }
        .jud-chiprow { display:flex; flex-wrap:wrap; gap:0.3rem; margin-top:0.4rem; align-items:center; }
        .jud-chip {
            font-size:0.66rem; font-weight:650; letter-spacing:0.02em; border-radius:999px;
            padding:0.08rem 0.5rem; white-space:nowrap; border:1px solid transparent;
        }
        .jud-chip.elev { background:var(--signal-good-subtle); color:var(--signal-good-deep);
            border-color:var(--signal-good-border); }
        .jud-chip.assign { background:var(--accent-subtle); color:var(--accent); border-color:var(--accent-dim); }
        .jud-chip.review { background:var(--signal-bad-subtle); color:var(--signal-bad-deep);
            border-color:var(--signal-bad-border); }
        .jud-chip.gap { background:var(--surface-deep); color:var(--text-meta); border-color:var(--border); }
        /* neutral (not gov/pres-coloured) — flags Chief Justice / President / ex-officio premium */
        .jud-chip.office { background:var(--surface-deep); color:var(--ink-700); border-color:var(--border-strong); }
        /* appointing-authority chips (blue=Government, amber=President, neutral=other) */
        .jud-auth { font-size:0.66rem; font-weight:650; letter-spacing:0.02em; border-radius:999px;
            padding:0.08rem 0.5rem; white-space:nowrap; border:1px solid; }
        .jud-auth.gov { background:var(--signal-good-subtle); color:var(--signal-good-deep);
            border-color:var(--signal-good-border); }
        .jud-auth.pres { background:var(--signal-bad-subtle); color:var(--signal-bad-deep);
            border-color:var(--signal-bad-border); }
        .jud-auth.other { background:var(--surface-deep); color:var(--text-meta); border-color:var(--border); }

        /* career-arc timeline — the one bespoke flourish; horizontal nodes + connector */
        .jud-arc { display:flex; flex-wrap:wrap; align-items:stretch; gap:0; margin:0.6rem 0 0.4rem; }
        /* fixed-width nodes pack left — a two-step career reads as two adjacent steps,
           not two halves of the page with a long empty connector between them. */
        .jud-node { position:relative; flex:0 0 11rem; padding:0 1.1rem 0.2rem 0; }
        .jud-node:not(:last-child)::after {
            content:""; position:absolute; top:0.42rem; right:0.45rem; left:1.0rem; height:2px;
            background:var(--border-strong);
        }
        .jud-node-dot { width:0.85rem; height:0.85rem; border-radius:999px; background:#ffffff;
            border:2.5px solid var(--accent); position:relative; z-index:1; }
        .jud-node.now .jud-node-dot { background:var(--accent); }
        .jud-node-court { font-weight:650; color:var(--ink-strong); font-size:0.88rem; margin-top:0.35rem; }
        .jud-node-date { font-size:0.74rem; color:var(--text-meta); }
        .jud-node-auth { font-size:0.72rem; color:oklch(40% 0.012 75); margin-top:0.1rem; }
        .jud-node-link { display:inline-block; font-size:0.7rem; color:var(--accent);
            text-decoration:none; margin-top:0.15rem; }
        .jud-node-link:hover { text-decoration:underline; }

        /* profile header + provenance + vacancy-lifecycle */
        .jud-prof-head { margin:0.2rem 0 0.5rem; }
        .jud-prof-name { font-size:1.5rem; font-weight:700; color:oklch(22% 0.012 75); line-height:1.15; margin:0; padding:0; }
        .jud-prof-sub { font-size:0.9rem; color:var(--text-meta); margin-top:0.15rem; }
        .jud-vac { background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.65rem 0.85rem; margin-bottom:0.5rem; }
        .jud-vac-cause { font-weight:600; color:var(--ink-700); font-size:0.86rem; }
        .jud-vac-pred { font-size:0.78rem; color:var(--text-meta); margin-top:0.15rem; }
        .jud-vac-nom { font-size:0.82rem; color:oklch(35% 0.012 75); margin-top:0.25rem; }
        /* appointing-authority stats — compact chips that size to content, packed left,
           not full-width cards stranding a number in empty space. */
        .jud-statwrap { display:flex; flex-wrap:wrap; gap:0.5rem; margin:0.3rem 0 0.2rem; }
        .jud-stat { background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.5rem 0.85rem; display:flex; align-items:center; gap:0.6rem; }
        .jud-stat-n { font-weight:700; font-size:1.5rem; color:var(--ink-strong);
            font-variant-numeric:tabular-nums; line-height:1; }
        .jud-ladder { display:flex; flex-wrap:wrap; gap:0.5rem; margin:0.3rem 0 0.2rem; }
        .jud-rung { background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.55rem 0.8rem; display:flex; align-items:baseline; gap:0.6rem; }
        .jud-rung-path { font-weight:600; color:var(--ink-700); font-size:0.84rem; }
        .jud-rung-n { font-weight:700; color:var(--accent); font-size:1.05rem; font-variant-numeric:tabular-nums; }
        .jud-foot { font-size:0.76rem; color:var(--text-meta); line-height:1.5; margin-top:1.4rem;
            border-top:1px solid var(--border); padding-top:0.8rem; max-width:64rem; }
        .jud-foot a { color:var(--accent); }

        /* ───────────────────────── Public Procurement ──────────────────
           Supplier / authority / category register cards + supplier profile.
           Ink-on-paper: white cards, full borders, blue/neutral chips
           (deuteranopia-safe, text-labelled — never red/green). Supplier and
           lobbying-overlap cards drill down via .dt-card-link-wrap (hover lift). */
        .pr-caveat {
            background: var(--signal-bad-subtle); border: 1px solid var(--signal-bad-border);
            border-left: 3px solid var(--signal-bad-mid); border-radius: 8px;
            padding: 0.7rem 0.95rem; margin: 0.4rem 0 0.9rem;
            font-size: 0.86rem; color: var(--ink-700); line-height: 1.55; max-width: 64rem;
        }
        .pr-caveat strong { color: var(--signal-bad-deep); }
        /* scale anchor / trust strip under the caveat: real corpus magnitude +
           what's in / out. Numbers are tabular; labels are quiet meta. */
        /* "The €570bn that isn't" — naive total shown struck-through only to demolish it.
           Neutral/ink palette (no red/green): the mirage is muted grey, the real figure
           is accent ink. The multiplier badge is the one bold accent. */
        .pr-contrast {
            background: #ffffff; border: 1px solid var(--border); border-radius: 10px;
            padding: 0.85rem 1.1rem; margin: 0 0 0.9rem; max-width: 64rem;
        }
        .pr-contrast-row {
            display: flex; align-items: center; flex-wrap: wrap; gap: 0.6rem 1.2rem;
        }
        .pr-contrast-cell { display: flex; flex-direction: column; line-height: 1.15; }
        .pr-contrast-num { font-size: 1.7rem; font-weight: 800; font-variant-numeric: tabular-nums; }
        .pr-contrast-naive .pr-contrast-num { color: var(--text-meta); }
        .pr-strike { text-decoration: line-through; text-decoration-thickness: 2px; }
        .pr-contrast-safe .pr-contrast-num { color: var(--accent); }
        .pr-contrast-lbl { font-size: 0.74rem; color: var(--text-meta); margin-top: 0.1rem; }
        .pr-contrast-mult {
            font-size: 0.82rem; font-weight: 700; color: #ffffff; background: var(--accent);
            padding: 0.22rem 0.6rem; border-radius: 999px; white-space: nowrap; flex: none;
        }
        .pr-contrast-note {
            font-size: 0.82rem; color: var(--ink-700); line-height: 1.5;
            margin-top: 0.6rem; padding-top: 0.55rem; border-top: 1px solid var(--border);
        }
        .pr-contrast-note strong { color: var(--ink-strong); }
        @media (max-width: 560px) {
            .pr-contrast-num { font-size: 1.35rem; }
        }

        .pr-stats {
            display: flex; flex-wrap: wrap; gap: 0.45rem 1.4rem;
            padding: 0.7rem 0.95rem; margin: 0 0 0.9rem; max-width: 64rem;
            background: #ffffff; border: 1px solid var(--border); border-radius: 10px;
        }
        .pr-stat { display: flex; flex-direction: column; line-height: 1.2; }
        .pr-stat-num {
            font-weight: 750; color: var(--ink-strong); font-size: 1.02rem;
            font-variant-numeric: tabular-nums;
        }
        .pr-stat-lbl { font-size: 0.72rem; color: var(--text-meta); }
        .pr-grid {
            display: grid; grid-template-columns: repeat(auto-fill, minmax(18rem, 1fr));
            gap: 0.7rem; margin-top: 0.5rem;
        }
        .pr-card {
            background: #ffffff; border: 1px solid var(--border); border-radius: 10px;
            padding: 0.7rem 0.85rem; display: flex; flex-direction: column; gap: 0.35rem;
            height: 100%;
        }
        .pr-card-head { display: flex; align-items: baseline; gap: 0.5rem; }
        .pr-rank {
            font-weight: 700; color: var(--accent); font-size: 0.82rem;
            font-variant-numeric: tabular-nums; flex: none;
        }
        .pr-name { font-weight: 650; color: var(--ink-strong); font-size: 0.93rem; line-height: 1.3; }
        /* secondary line inside a card name — the published tender/contract title under the buyer */
        .pr-sub { display: block; font-weight: 500; color: var(--text-meta); font-size: 0.8rem;
            line-height: 1.3; margin-top: 0.12rem; }
        .pr-meta { font-size: 0.78rem; color: var(--text-meta); }
        /* labelled divider between two procurement registers (national eTenders vs EU-journal TED),
           rendered as a hairline with a centred caption so neither register's values read as one list */
        .pr-register-rule { display: flex; align-items: center; gap: 0.6rem; margin: 1.6rem 0 0.9rem;
            color: var(--text-meta); font-size: 0.7rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; }
        .pr-register-rule::before, .pr-register-rule::after {
            content: ""; flex: 1; height: 1px; background: var(--border); }
        .pr-pills { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-top: auto; padding-top: 0.15rem; }
        .pr-pill {
            font-size: 0.72rem; font-weight: 600; padding: 0.08rem 0.5rem; border-radius: 999px;
            background: var(--surface-deep); color: var(--ink-700); border: 1px solid var(--border);
            white-space: nowrap;
        }
        .pr-pill-val { background: var(--accent-subtle); color: var(--accent); border-color: var(--accent-dim); }
        .pr-pill-cro { background: var(--signal-good-subtle); color: var(--signal-good-deep);
            border-color: var(--signal-good-border); }
        /* lobbying co-occurrence is informational, NOT an alarm — neutral chip,
           never red, so the colour never implies wrongdoing (honesty rail). */
        .pr-pill-lob { background: var(--surface-deep); color: var(--ink-700); border-color: var(--border-strong); }

        /* supplier / buyer profile (?supplier= / ?paid_publisher=) */
        .pr-prof-head { margin: 0.2rem 0 0.5rem; }
        .pr-prof-kicker { font-size: 0.7rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; color: var(--text-meta); margin-bottom: 0.15rem; }
        .pr-prof-name { font-size: 1.5rem; font-weight: 700; color: var(--ink-strong);
            line-height: 1.15; margin: 0; padding: 0; }
        .pr-prof-sub { font-size: 0.9rem; color: var(--text-meta); margin-top: 0.15rem; }
        .pr-award {
            background: #ffffff; border: 1px solid var(--border); border-radius: 8px;
            padding: 0.55rem 0.8rem; margin-bottom: 0.4rem;
            display: flex; align-items: baseline; gap: 0.7rem;
        }
        .pr-award-body { flex: 1; min-width: 0; }
        .pr-award-auth { font-weight: 600; color: var(--ink-700); font-size: 0.88rem; }
        /* published contract title — the descriptive line between entity and meta */
        .pr-award-title { font-size: 0.8rem; color: var(--ink-strong); line-height: 1.35; margin-top: 0.1rem; }
        .pr-award-meta { font-size: 0.76rem; color: var(--text-meta); margin-top: 0.1rem; }
        .pr-award-meta a { color: var(--accent); text-decoration: none; white-space: nowrap; }
        .pr-award-meta a:hover { text-decoration: underline; }
        .pr-award-val { font-weight: 700; color: var(--accent); font-size: 0.92rem;
            font-variant-numeric: tabular-nums; white-space: nowrap; text-align: right; }
        .pr-award-val small { display: block; font-weight: 500; color: var(--text-meta); font-size: 0.64rem; }
        .pr-award-val.ceiling { color: var(--signal-bad-deep); }
        .pr-foot {
            font-size: 0.8rem; color: var(--text-meta); line-height: 1.55;
            margin-top: 1.4rem; padding-top: 0.7rem; border-top: 1px solid var(--border); max-width: 64rem;
        }
        .pr-foot a { color: var(--accent); }
        .pr-cap { font-size: 0.86rem; color: var(--ink-700); line-height: 1.5; margin: 0.2rem 0 0.6rem; max-width: 60rem; }
        .pr-cap em { color: var(--text-meta); font-style: italic; }
        /* stale-snapshot warning — rust (signal-bad), NEVER true red: this is a freshness
           caution, not an alarm, and red would imply wrongdoing (honesty rail). */
        .pr-cap-stale { color: var(--signal-bad-deep); font-weight: 700; }
        /* Authoritative-source conduit: the list of TED notices that open the real record.
           Each row is a doorway out to the Official Journal, with a quiet value-kind tag. */
        .pr-notice-list { list-style: none; margin: 0.2rem 0 0; padding: 0; }
        .pr-notice { padding: 0.3rem 0; border-top: 1px solid var(--border); font-size: 0.88rem; }
        .pr-notice:first-child { border-top: none; }
        .pr-notice a { color: var(--accent); font-weight: 600; text-decoration: none; }
        .pr-notice a:hover { text-decoration: underline; }
        .pr-notice-tag { color: var(--text-meta); font-size: 0.78rem; margin-left: 0.4rem; }
        /* TED cross-reference block on a supplier profile — a quiet, clearly-separate
           "other register" callout. Neutral surface, left rule in accent (informational,
           never alarm); the copy says "not added" so it can't read as a bigger total. */
        .pr-ted-xref {
            background: var(--surface-deep); border: 1px solid var(--border);
            border-left: 3px solid var(--accent); border-radius: 8px;
            padding: 0.6rem 0.85rem; margin: 1rem 0 0.4rem; max-width: 60rem;
        }
        .pr-ted-xref-h { font-weight: 700; color: var(--ink-strong); font-size: 0.84rem; }
        .pr-ted-xref-b { font-size: 0.82rem; color: var(--ink-700); line-height: 1.5; margin-top: 0.2rem; }
        .pr-ted-xref-b em { color: var(--text-meta); }
        /* AFS (audited-accounts) context on a local-authority dossier — a SIBLING budget fact,
           visually fenced off from the purchase-order section above so the two grains never read
           as one total. Teal accents (vs the PO section's brown) reinforce "different measure". */
        .pr-afs { margin: 1.4rem 0 0.5rem; padding-top: 1rem; border-top: 1px solid var(--border); }
        .pr-afs-head { font-weight: 800; color: var(--ink-strong); font-size: 1.02rem; letter-spacing: -0.01em; }
        .pr-afs-trace {
            background: #ffffff; border: 1px solid var(--border);
            border-left: 3px solid #3a6b7e; border-radius: 8px;
            padding: 0.6rem 0.85rem; margin: 0.5rem 0 0.7rem; max-width: 60rem;
        }
        .pr-afs-trace-fig { font-size: 0.95rem; color: var(--ink-strong); line-height: 1.5; }
        .pr-afs-trace-cap { font-size: 0.8rem; color: var(--ink-700); line-height: 1.45; margin-top: 0.3rem; }
        /* Dossier LANES — the three honest grains of council money (Running / Building / Paying),
           each opened by its own band. The tag is the small-caps stratum; the <h2> is the section
           heading; the dek carries the never-sum framing. A coloured left rule keys each lane to its
           chart/bar colour (teal=revenue, green=capital, brown=PO) so the grains stay visually distinct. */
        .pr-lane { margin: 1.8rem 0 0.7rem; padding: 0.75rem 0 0.2rem; border-top: 2px solid var(--border-strong); }
        .pr-lane:first-of-type { margin-top: 1rem; }
        .pr-lane-tag { font-size: 0.72rem; font-weight: 700; letter-spacing: 0.09em; text-transform: uppercase;
            color: var(--text-meta); margin-bottom: 0.15rem; }
        .pr-lane-head { font-size: 1.32rem; font-weight: 800; color: var(--ink-strong);
            letter-spacing: -0.02em; line-height: 1.15; margin: 0 0 0.3rem; }
        .pr-lane-dek { font-size: 0.86rem; color: var(--ink-700); line-height: 1.5; max-width: 60rem; margin: 0; }
        /* Horizontal labelled bars for the audited lanes (net cost by service / capital by service).
           Bar width is a display scaling against the lane's own max — the figure is the truth, the bar
           is a glance. tabular-nums keeps the right-aligned euros in a clean column. */
        .pr-afsbars { margin: 0.3rem 0 0.6rem; max-width: 60rem; }
        .pr-afsbar { margin: 0.55rem 0; }
        .pr-afsbar-top { display: flex; align-items: baseline; justify-content: space-between; gap: 0.75rem; }
        .pr-afsbar-label { font-size: 0.9rem; font-weight: 600; color: var(--ink-strong); }
        .pr-afsbar-fig { font-size: 0.92rem; color: var(--ink-strong); white-space: nowrap;
            font-variant-numeric: tabular-nums; }
        .pr-afsbar-zero { font-size: 0.82rem; font-weight: 500; color: var(--text-meta); font-style: italic; }
        .pr-afsbar-track { height: 9px; background: var(--surface-2, #eee7dc); border-radius: 5px;
            overflow: hidden; margin: 0.22rem 0 0.1rem; }
        .pr-afsbar-fill { height: 100%; border-radius: 5px; min-width: 2px; }
        .pr-afsbar-note { font-size: 0.76rem; color: var(--text-meta); line-height: 1.4; }
        /* "Your council" index — province bands + lifecycle-tier pills.
           Province band header is a SEMANTIC <h3> (heading-navigable for screen
           readers); geography is encoded by the fixed N->S band order, never colour. */
        .pr-region-head {
            display: flex; align-items: baseline; justify-content: space-between;
            gap: 0.75rem; margin: 1.6rem 0 0.5rem; padding-bottom: 0.3rem;
            border-bottom: 1px solid var(--border-strong);
        }
        .pr-region-head:first-of-type { margin-top: 0.4rem; }
        .pr-region-name {
            font-size: 0.82rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; color: var(--ink-strong); line-height: 1.2;
        }
        .pr-region-count { font-size: 0.78rem; font-weight: 500; color: var(--text-meta);
            font-variant-numeric: tabular-nums; white-space: nowrap; }
        /* Two STAGES of public money, NEVER a sum. solid pill = realised ('paid'),
           dashed pill = committed ('ordered', provisional). The dashed/solid contrast plus
           the verb baked into each chip make summing them obviously wrong; no glyph ever
           joins them. The verb is also the non-colour accessibility carrier.
           Pills sit in _card's standard .pr-pills wrapper (bottom-aligned, wrapping). */
        .pr-pill-paid {     /* realised — the firmest fact, strongest ink */
            background: var(--accent-subtle); color: var(--accent); border: 1px solid var(--accent-dim); }
        .pr-pill-ordered {  /* committed — provisional, hollow, dashed */
            background: #ffffff; color: var(--ink-700); border: 1px dashed var(--border-strong); }

        /* Section heading inside a tab (semantic <h2> under the page <h1> hero, so the
           page is heading-navigable). Visual size set here, not by the UA default. */
        .pr-section-h {
            margin: 1.1rem 0 0.4rem; font-weight: 700; color: var(--ink-strong);
            font-size: 0.92rem; line-height: 1.3;
        }
        /* Spacing utilities (replace ad-hoc inline height divs). */
        .pr-sp-md { height: 1rem; }
        .pr-sp-sm { height: 0.6rem; }
        .pr-cap-flush { margin-top: 0; }

        @media (max-width: 640px) {
            .pr-grid { grid-template-columns: 1fr; }
        }

        /* ── Tab strip (st.tabs) ──────────────────────────────────────────────
           Streamlit's default tabs are a faint label + a thin sliding underline,
           which wash out against the warm-neutral page background — users were
           missing them entirely. Render the strip as a row of clear pill-buttons:
           inactive = light surface with readable secondary ink; active = filled
           brand accent so the current section is unmistakable. Applies app-wide
           (the cross-page top nav is st.navigation, not .stTabs, so it is
           unaffected). */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.3rem;
            border-bottom: 2px solid var(--border-strong) !important;
        }
        .stTabs [data-baseweb="tab"] {
            height: auto;
            padding: 0.45rem 1rem !important;
            border-radius: 8px 8px 0 0;
            background: var(--surface-deep);
            color: var(--text-secondary) !important;
            border: 1px solid var(--border-strong) !important;
            border-bottom: none !important;
            margin-bottom: -2px;            /* overlap the list's bottom border */
            transition: background 0.12s ease, color 0.12s ease;
        }
        .stTabs [data-baseweb="tab"] p {
            font-weight: 600 !important;
            font-size: 0.92rem !important;
        }
        .stTabs [data-baseweb="tab"]:hover {
            background: var(--accent-subtle);
            color: var(--accent) !important;
        }
        .stTabs [data-baseweb="tab"]:hover p { color: var(--accent) !important; }
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background: var(--accent);
            border-color: var(--accent) !important;
        }
        .stTabs [data-baseweb="tab"][aria-selected="true"] p {
            color: #ffffff !important;
            font-weight: 700 !important;
        }
        /* The filled active pill now carries selection state; neutralise the
           default sliding highlight bar (it mis-aligns under the padded pills)
           and keep the base border subtle. */
        .stTabs [data-baseweb="tab-highlight"] { background: transparent !important; }
        .stTabs [data-baseweb="tab-border"]    { background: transparent !important; }

        </style>
        """,
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
            <a class="site-banner-title" href="./" aria-label="Oireachtas Explorer — back to home">Oireachtas Explorer</a>
            <span class="site-banner-sep"></span>
            <span class="site-banner-sub">Irish parliamentary data, made searchable</span>
          </div>
        </div>
        """
    )
