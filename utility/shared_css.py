import streamlit as st


def inject_css() -> None:
    """Shared design system for all Dáil Tracker pages."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Zilla+Slab:wght@400;600;700&family=Epilogue:ital,wght@0,400;0,500;0,600;1,400&family=Material+Symbols+Outlined&display=swap');

        /* ── Site banner ─────────────────────────── */
        .site-banner {
            position: relative;
            left: 50%;
            margin-left: -50vw;
            width: 100vw;
            margin-top: -1.5rem;
            margin-bottom: 1.75rem;
            background: #111827;
            border-bottom: 3px solid oklch(51% 0.130 62);
        }
        .site-banner-inner {
            max-width: 1340px;
            margin: 0 auto;
            padding: 1.1rem 2rem;
            display: flex;
            align-items: baseline;
            gap: 1.25rem;
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

        /* ── Streamlit toolbar: hide chrome, keep sidebar toggle ── */
        /* Strategy: leave the header's height intact so the sidebar
           collapse/expand button keeps working. Hide only the visual
           contents (deploy btn, status widget, decoration). Then pull
           the banner up with a negative margin-top so it covers the
           now-transparent header area.                               */
        header[data-testid="stHeader"] {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
        }
        [data-testid="stToolbarActions"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"] {
            visibility: hidden !important;
        }
        .main .block-container {
            padding-top: 0 !important;
        }
        /* Pull banner up into the transparent header zone */
        .site-banner {
            margin-top: -4rem !important;
            position: relative !important;
            z-index: 1000 !important;
        }

        /* ── Sidebar nav links ───────────────────────────────────── */
        [data-testid="stSidebarNav"] a {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.82rem !important;
            font-weight: 600 !important;
            color: var(--text-secondary) !important;
            padding: 0.38rem 0.75rem !important;
            border-radius: 2px !important;
            display: block !important;
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
            --text-meta:      oklch(58%   0.010 75);
            --accent:         oklch(51%   0.130 62);
            --accent-subtle:  oklch(95%   0.055 72);
            --accent-dim:     oklch(86%   0.040 72);
            --new-bg:         oklch(94%   0.045 145);
            --removed-bg:     oklch(94%   0.030  22);
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
            outline: none !important;
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
        .page-kicker {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: var(--accent);
            margin-bottom: 0.3rem;
        }
        .page-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.55rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.15;
            margin-bottom: 0.2rem;
        }
        .page-subtitle {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.8rem;
            color: var(--text-meta);
            line-height: 1.5;
            margin-bottom: 1.2rem;
        }
        .sidebar-label {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin: 0.85rem 0 0.35rem 0;
        }
        .section-rule {
            border: none;
            border-top: 2px solid var(--text-primary);
            margin: 0 0 1.5rem 0;
        }
        .stat-strip {
            display: flex;
            gap: 2.5rem;
            padding: 1rem 0;
            border-top: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
            margin: 1rem 0 1.75rem 0;
        }
        .stat-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.75rem;
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
        .dt-outcome-carried { color: oklch(38% 0.130 145); font-weight: 700; }
        .dt-outcome-lost    { color: oklch(45% 0.180 30);  font-weight: 700; }
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
        .dt-vt-yes  { color: oklch(38% 0.130 145); font-weight: 700; white-space: nowrap; }
        .dt-vt-no   { color: oklch(45% 0.180 30);  font-weight: 700; white-space: nowrap; }
        .dt-vt-abs  { color: var(--text-meta);      font-weight: 500; white-space: nowrap; }
        .dt-vt-date { color: var(--text-meta);      white-space: nowrap; font-size: 0.82rem; }
        .dt-vt-meta { color: var(--text-meta);      font-size: 0.84rem; }
        .dt-vt-outcome-carried { color: oklch(38% 0.130 145); font-size: 0.78rem; font-weight: 600; white-space: nowrap; }
        .dt-vt-outcome-lost    { color: oklch(45% 0.180 30);  font-size: 0.78rem; font-weight: 600; white-space: nowrap; }
        .dt-vt-outcome-other   { color: var(--text-meta);     font-size: 0.78rem; }
        .dt-vt-link {
            color: var(--accent);
            text-decoration: none;
            font-size: 0.80rem;
        }
        .dt-vt-link:hover { text-decoration: underline; }

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
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            text-align: center;
        }
        .dt-success-num {
            font-size: 1.25rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            line-height: 1;
            color: #1d4ed8;
        }
        .dt-success-lbl {
            font-size: 0.58rem;
            font-weight: 600;
            color: #3b82f6;
            line-height: 1.4;
        }

        /* Card / panel container (e.g. Hall of Fame card) */
        .dt-success-card {
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            border-left: 5px solid #2563eb;
            border-radius: 8px;
        }

        /* Inline pill / tag (e.g. inside a rank card) */
        .dt-success-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            border-radius: 999px;
            padding: 0.1rem 0.55rem;
            font-size: 0.76rem;
            font-weight: 600;
            color: #1d4ed8;
        }

        /* Stat number inside a stat-strip or summary block */
        .dt-success-stat-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.75rem;
            font-weight: 700;
            color: #1d4ed8;
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
        .int-pill-decl    { background:#eff6ff; border-color:#93c5fd; color:#1e3a8a; }
        .int-pill-company { background:#f0fdfa; border-color:#5eead4; color:#0e6655; }
        .int-pill-prop    { background:#fffbeb; border-color:#fbbf24; color:#78350f; }
        .int-pill-shares  { background:#f5f3ff; border-color:#c4b5fd; color:#4c1d95; }
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
        /* Left slot: avatar OR rank number — always reserves the space */
        .dt-name-card-left {
            flex-shrink: 0;
            width: 2.25rem;
            display: flex;
            align-items: center;
            justify-content: flex-end;
        }
        .dt-name-card-avatar {
            width: 2.25rem;
            height: 2.25rem;
            border-radius: 50%;
            object-fit: cover;
            border: 1px solid rgba(0,0,0,0.1);
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
            background: #eff6ff;
            border: 1px solid #bfdbfe;
        }
        .dt-name-card-badge-num {
            font-size: 1.1rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            color: #1d4ed8;
            line-height: 1;
            display: block;
        }
        .dt-name-card-badge-lbl {
            font-size: 0.56rem;
            font-weight: 600;
            color: #3b82f6;
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
        .mo-grid-link {
            text-decoration: none !important;
            color: inherit !important;
            position: relative;
            display: block;
            transition: transform 80ms ease;
        }
        .mo-grid-link:hover {
            transform: translateY(-1px);
        }
        .mo-grid-link .dt-name-card {
            padding-right: 2.25rem;
        }
        .mo-grid-link:hover .dt-name-card {
            border-left-color: var(--accent) !important;
            border-color: var(--accent) !important;
            background: var(--accent-subtle) !important;
        }
        .mo-grid-arrow {
            position: absolute;
            right: 0.85rem;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-meta);
            font-size: 1rem;
            font-weight: 700;
            transition: transform 120ms ease, color 120ms ease;
        }
        .mo-grid-link:hover .mo-grid-arrow {
            color: var(--accent);
            transform: translateY(-50%) translateX(3px);
        }
        [data-testid="stMain"]
            [data-testid="stTextInput"]:has(input[aria-label="Search TDs"]) input {
            font-size: 1rem !important;
            padding: 0.7rem 0.95rem !important;
            background: #ffffff !important;
            border: 1.5px solid var(--border-strong) !important;
            border-radius: 8px !important;
        }
        [data-testid="stMain"]
            [data-testid="stTextInput"]:has(input[aria-label="Search TDs"]) input:focus {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 3px var(--accent-subtle) !important;
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
            color: #1d4ed8; border-bottom: 3px solid #3b82f6;
            padding-bottom: 0.5rem; margin: 0 0 0.9rem;
        }
        .att-hall-heading-bad {
            font-size: 1.3rem; font-weight: 800; letter-spacing: -0.02em;
            color: #c2410c; border-bottom: 3px solid #f97316;
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
            background: #eff6ff; border: 1px solid #bfdbfe; border-left: 5px solid #3b82f6;
        }
        .att-hall-card-bad {
            background: #fff7ed; border: 1px solid #fed7aa; border-left: 5px solid #f97316;
        }
        /* Inner card rows: card fills column, button shrinks to fit */
        [data-testid="stHorizontalBlock"]:has(.att-hall-card-good),
        [data-testid="stHorizontalBlock"]:has(.att-hall-card-bad) {
            gap: 0.35rem !important;
            margin-bottom: 0.25rem !important;
            align-items: stretch !important;
        }
        [data-testid="stHorizontalBlock"]:has(.att-hall-card-good) [data-testid="stColumn"]:first-child,
        [data-testid="stHorizontalBlock"]:has(.att-hall-card-bad) [data-testid="stColumn"]:first-child {
            flex: 1 1 auto !important;
            min-width: 0 !important;
        }
        [data-testid="stHorizontalBlock"]:has(.att-hall-card-good) [data-testid="stColumn"]:last-child,
        [data-testid="stHorizontalBlock"]:has(.att-hall-card-bad) [data-testid="stColumn"]:last-child {
            flex: 0 0 auto !important;
            width: auto !important;
        }
        .att-hall-rank {
            font-size: 0.7rem; font-weight: 800; letter-spacing: 0.04em;
            color: var(--text-meta); width: 1.6rem; text-align: center; flex-shrink: 0;
        }
        .att-hall-medal {
            font-size: 1.2rem; line-height: 1; flex-shrink: 0; width: 1.4rem; text-align: center;
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
        .att-hall-badge-good { background: #dbeafe; border: 1px solid #93c5fd; }
        .att-hall-badge-bad  { background: #ffedd5; border: 1px solid #fdba74; }
        .att-hall-badge-num {
            font-size: 1.25rem; font-weight: 800; letter-spacing: -0.03em;
            color: var(--text-primary); display: block;
        }
        .att-hall-badge-good .att-hall-badge-num { color: #1d4ed8; }
        .att-hall-badge-bad  .att-hall-badge-num { color: #c2410c; }
        .att-hall-badge-label { font-size: 0.62rem; font-weight: 600; color: var(--text-meta); display: block; }

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

        /* ── Payments page ───────────────────────────────────────────── */
        .pay-amount-badge {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            min-width: 62px; padding: 5px 10px; border-radius: 12px;
            background: #eff6ff; border: 1px solid #93c5fd; text-align: center; flex-shrink: 0;
        }
        .pay-amount-badge-num  { font-size: 1.05rem; font-weight: 800; letter-spacing: -0.03em; color: #1e40af; line-height: 1; display: block; }
        .pay-amount-badge-label { font-size: 0.58rem; font-weight: 600; color: #3b82f6; line-height: 1.4; display: block; }
        .pay-taa-pill {
            display: inline-flex; align-items: center; background: #eff6ff; border: 1px solid #93c5fd;
            border-radius: 999px; padding: 2px 8px; font-size: 0.68rem; font-weight: 600; color: #1e40af;
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
        .att-cop-head-good { font-size: 0.68rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase; color: #1d4ed8; border-bottom: 3px solid #3b82f6; padding-bottom: 0.3rem; margin: 0 0 0.6rem; }
        .att-cop-head-bad  { font-size: 0.68rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase; color: #c2410c; border-bottom: 3px solid #f97316; padding-bottom: 0.3rem; margin: 0 0 0.6rem; }

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
        .lob-path-card:hover { border-top-color: #9a3412; box-shadow: 0 4px 16px rgba(17,24,39,0.1); }
        .lob-path-icon { font-size: 1.6rem; line-height: 1; margin-bottom: 0.55rem; }
        .lob-path-heading { margin: 0 0 0.3rem; font-size: 1.05rem; font-weight: 700; color: var(--text-primary); letter-spacing: -0.01em; }
        .lob-path-body { margin: 0 0 0.65rem; font-size: 0.82rem; color: var(--text-meta); line-height: 1.5; }
        .lob-path-stat { display: flex; align-items: baseline; gap: 0.3rem; }
        .lob-path-stat-num { font-size: 1.3rem; font-weight: 800; color: #0f3d5e; letter-spacing: -0.03em; }
        .lob-path-stat-lbl { font-size: 0.73rem; font-weight: 600; color: var(--text-meta); text-transform: uppercase; letter-spacing: 0.04em; }

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
            background: #fff7ed;
            border: 1px solid #fdba74;
            border-left: 5px solid #c2410c;
            border-radius: 10px;
            padding: 0.7rem 1rem;
            margin: 0.5rem 0 1.1rem;
        }
        .lob-rd-prominent-heading { font-size: 0.7rem; font-weight: 800; letter-spacing: 0.09em; text-transform: uppercase; color: #9a3412; margin-bottom: 0.4rem; }
        .lob-rd-prominent-grid { display: flex; flex-wrap: wrap; gap: 0.55rem; }
        .lob-rd-prominent-pill {
            background: #ffffff; border: 1px solid #fdba74; border-radius: 999px;
            padding: 0.3rem 0.75rem; font-size: 0.82rem; color: #1f2937;
            display: inline-flex; align-items: baseline; gap: 0.4rem;
        }
        .lob-rd-prominent-pill strong { color: #9a3412; font-weight: 700; }

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
            font-size: 1.7rem; font-weight: 700; color: var(--text-primary);
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
            font-size: 0.68rem; font-weight: 700; letter-spacing: 0.07em;
            text-transform: uppercase; color: var(--text-meta); margin-bottom: 0.25rem;
        }
        .leg-source-link {
            font-size: 0.85rem; font-weight: 600;
            color: var(--accent); text-decoration: none;
        }
        .leg-source-link:hover { text-decoration: underline; }

        /* ── Legislation: bill card list ─────────────────────────────── */
        .leg-bill-card {
            padding: 0.45rem 0.9rem;
            border: 1px solid var(--border);
            border-left: 3px solid var(--border-strong);
            border-radius: 12px;
            background: #ffffff;
            width: 100%;
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

        /* Card row — card capped at readable width, button sits immediately after */
        [data-testid="stHorizontalBlock"]:has(.leg-bill-card) {
            gap: 0.35rem !important;
            margin-bottom: 0.3rem !important;
            align-items: stretch !important;
            justify-content: flex-start !important;
        }
        [data-testid="stHorizontalBlock"]:has(.leg-bill-card)
            [data-testid="stColumn"]:first-child {
            flex: 1 1 auto !important;
            max-width: 860px !important;
            min-width: 0 !important;
        }
        [data-testid="stHorizontalBlock"]:has(.leg-bill-card)
            [data-testid="stColumn"]:last-child {
            flex: 0 0 auto !important;
            width: auto !important;
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

        /* ── Legislation: pipeline TODO callout ─────────────────────── */
        .leg-todo-callout {
            background: #fffbeb;
            border: 1px solid #fcd34d;
            border-left: 4px solid #d97706;
            border-radius: 2px;
            padding: 0.55rem 0.85rem;
            font-size: 0.80rem;
            color: #78350f;
            line-height: 1.5;
            margin: 0.6rem 0;
        }
        .leg-todo-callout code {
            background: #fef3c7;
            border: 1px solid #fcd34d;
            border-radius: 2px;
            padding: 0.05rem 0.3rem;
            font-size: 0.75rem;
            color: #92400e;
        }
        .leg-todo-label {
            font-size: 0.65rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #b45309;
            margin-right: 0.35rem;
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
        .vt-count-yes {
            background: #f0fdf4;
            color: #166534;
            font-size: 0.75rem;
            font-weight: 700;
            padding: 0.12rem 0.55rem;
            border-radius: 999px;
            white-space: nowrap;
        }
        .vt-count-no {
            background: #fef2f2;
            color: #991b1b;
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
            background: #f0fdf4;
            color: #166534;
            font-size: 0.72rem;
            font-weight: 700;
            padding: 0.12rem 0.55rem;
            border-radius: 2px;
            white-space: nowrap;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .vt-outcome-lost {
            background: #fef2f2;
            color: #991b1b;
            font-size: 0.72rem;
            font-weight: 700;
            padding: 0.12rem 0.55rem;
            border-radius: 2px;
            white-space: nowrap;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .vt-margin-pill {
            background: #f4f4f4;
            color: var(--text-meta);
            font-size: 0.75rem;
            padding: 0.12rem 0.55rem;
            border-radius: 999px;
            white-space: nowrap;
            margin-left: auto;
        }
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
        .td-pick-vote {
            display: inline-flex;
            align-items: center;
            align-self: flex-start;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            padding: 0.3rem 0.7rem;
            border-radius: 999px;
            white-space: nowrap;
            margin-bottom: 0.2rem;
        }
        .td-pick-vote-yes {
            background: #ecfdf5;
            color: #065f46;
            border: 1px solid #a7f3d0;
        }
        .td-pick-vote-no {
            background: #fef2f2;
            color: #991b1b;
            border: 1px solid #fecaca;
        }
        .td-pick-vote-abs {
            background: #f4f4f5;
            color: #52525b;
            border: 1px solid #e4e4e7;
        }
        .td-pick-prompt {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            color: var(--text-meta, #5a5a5a);
            text-transform: lowercase;
            letter-spacing: 0.02em;
            margin: 0;
        }
        .td-pick-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.05rem;
            font-weight: 700;
            color: var(--text-primary, #111827);
            line-height: 1.3;
            margin: 0;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
            min-height: calc(1.05rem * 1.3 * 2);
        }
        .td-pick-name {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.98rem;
            font-weight: 700;
            color: var(--text-primary, #111827);
            margin-top: auto;
            padding-top: 0.45rem;
        }
        .td-pick-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            color: var(--text-meta, #5a5a5a);
            margin: 0;
        }
        /* Tighten the gap between the picker card and its action button,
           and stretch all columns in the row to equal height. */
        [data-testid="stHorizontalBlock"]:has(.td-pick-card) {
            align-items: stretch !important;
        }
        [data-testid="stHorizontalBlock"]:has(.td-pick-card)
            [data-testid="stColumn"] {
            display: flex;
            flex-direction: column;
        }
        [data-testid="stHorizontalBlock"]:has(.td-pick-card)
            [data-testid="stButton"] > button {
            margin-top: 0.4rem;
            border: 1px solid rgba(0,0,0,0.12);
            background: #ffffff;
            color: var(--text-primary);
            font-family: 'Epilogue', sans-serif;
            font-weight: 600;
            font-size: 0.85rem;
        }
        [data-testid="stHorizontalBlock"]:has(.td-pick-card)
            [data-testid="stButton"] > button:hover {
            border-color: var(--accent, #b04a1a);
            color: var(--accent, #b04a1a);
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
            min-width: 1.2rem;
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
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="site-banner">
          <div class="site-banner-inner">
            <span class="site-banner-title">Oireachtas Explorer</span>
            <span class="site-banner-sep"></span>
            <span class="site-banner-sub">Irish parliamentary data, made searchable</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
