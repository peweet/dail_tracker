"""CSS fragment: Editorial hero band, finding lede, search kicker, hero meta row, callout, vote outcome labels, dataframe theming, navigation arrow + reusable nav button, info card.

Mechanically split from the original utility/shared_css.py (lines 1185-1695 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Editorial hero band (main content area) ── */
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

"""
