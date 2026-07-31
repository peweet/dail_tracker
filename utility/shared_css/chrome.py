"""CSS fragment: App chrome, masthead, top-nav, mobile menu, sidebar nav links, design tokens, sr-only.

Mechanically split from the original utility/shared_css.py (lines 75-580 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """
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
           the "Dáil Tracker" .site-banner reads as the top row and
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
            background: #ffffff !important;
            border: 1px solid var(--border-strong) !important;
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
            background: #ffffff !important;
            border: 1px solid var(--border-strong) !important;
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

"""
