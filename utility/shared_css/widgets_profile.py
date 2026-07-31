"""CSS fragment: Streamlit widget theming (inputs/buttons/expander/checkbox/...), section/stat strips, glossary, TD name/meta, profile header with avatar.

Mechanically split from the original utility/shared_css.py (lines 581-1184 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Default buttons (e.g. "Notable members" chips) ───────────
           Streamlit 1.58's config-driven theme paints st.button surfaces the
           same warm tone as the page, so the chips read as invisible. The
           plain `.stButton > button` rule above (specificity 0,1,1) is too
           weak to beat the theme — it must be stacked under stMain like the
           working selectbox rule further down. We override colour only and
           leave the theme's pill radius untouched. This sits BEFORE the
           scoped back/crumb/cta/nav rules below, so those still win for their
           own buttons; this covers every other main-area button. */
        [data-testid="stMain"] [data-testid="stButton"] button {
            background: #ffffff !important;
            border: 1px solid var(--border-strong) !important;
            color: var(--text-primary) !important;
        }
        [data-testid="stMain"] [data-testid="stButton"] button:hover {
            background: var(--accent-subtle) !important;
            border-color: var(--accent) !important;
        }

        /* ── Segmented control & pills ─────────────────────────────────
           (Dáil/Seanad, category toggles like Everyone/Landlords, party
           filters.) Same specificity problem: a single data-testid (0,1,0)
           loses to the theme, so we stack stMain + stButtonGroup + the button
           test ID. Every option becomes a white chip with a defined border;
           the SELECTED option is filled with the accent so the active choice
           is unmistakable. Test IDs verified against Streamlit 1.58:
           stBaseButton-segmented_control / …Active, stBaseButton-pills / …Active. */
        [data-testid="stMain"] [data-testid="stButtonGroup"] [data-testid="stBaseButton-segmented_control"],
        [data-testid="stMain"] [data-testid="stButtonGroup"] [data-testid="stBaseButton-pills"] {
            background: #ffffff !important;
            border: 1px solid var(--border-strong) !important;
            color: var(--text-secondary) !important;
            font-weight: 600 !important;
        }
        [data-testid="stMain"] [data-testid="stButtonGroup"] [data-testid="stBaseButton-segmented_control"]:hover,
        [data-testid="stMain"] [data-testid="stButtonGroup"] [data-testid="stBaseButton-pills"]:hover {
            background: var(--accent-subtle) !important;
            border-color: var(--accent) !important;
            color: var(--text-primary) !important;
        }
        [data-testid="stMain"] [data-testid="stButtonGroup"] [data-testid="stBaseButton-segmented_controlActive"],
        [data-testid="stMain"] [data-testid="stButtonGroup"] [data-testid="stBaseButton-pillsActive"] {
            background: var(--accent) !important;
            border: 1px solid var(--accent) !important;
            color: #ffffff !important;
            font-weight: 600 !important;
        }
        /* Committee-register status filter: keep the 3 chips ("All statuses /
           Active / Ended") on a single line. In its narrow filter-bar column
           the group otherwise wraps "Ended" below the others, breaking the
           row baseline. Scoped to the reg_status key so other segmented
           controls keep their default wrapping. */
        [class*="st-key-reg_status"] [data-testid="stButtonGroup"] {
            flex-wrap: nowrap !important;
        }
        /* Votes View toggle (2026-07-20): "Divisions"/"Members" wrapped into
           two stacked pills that read as separate buttons, not one segmented
           control. Root cause (found via computed layout): the INNER flex div
           inside stButtonGroup keeps flex-wrap:wrap, and the two options total
           ~192px in the ~189px column, so "Members" wrapped below. Force nowrap
           on that inner div (flex-shrink absorbs the 3px). Scoped to the
           v_view_widget key so other segmented controls keep default wrapping. */
        [class*="st-key-v_view_widget"] [data-testid="stButtonGroup"] > div {
            flex-wrap: nowrap !important;
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
        /* Tighten the row that holds a breadcrumb so it reads as one line.
           flex-direction:row !important overrides the blanket mobile breakpoint
           (≤640px flips every stHorizontalBlock to column) which otherwise
           stacks each crumb segment + › separator vertically on phones. */
        div[data-testid="stHorizontalBlock"]:has(> div .dt-crumb-row-marker) {
            flex-direction: row !important;
            flex-wrap: wrap;
            margin-bottom: 0.6rem;
            align-items: center;
        }
        div[data-testid="stHorizontalBlock"]:has(> div .dt-crumb-row-marker)
            > [data-testid="stColumn"] {
            width: auto !important;
            min-width: 0 !important;
            flex: 0 0 auto !important;
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

        /* ── Download button ─────────────────────────────────────────────
           QUIET BY DESIGN (2026-07-20 clutter pass). This was solid black +
           uppercase, which made "DOWNLOAD 51,217 NOTICES (CSV)" the loudest
           element on several pages — louder than the records it exports.
           Exporting is never the primary action on a civic page, so it now
           reads as a secondary control: outlined, sentence-case, muted ink.
           It darkens on hover so it's still obviously clickable. */
        .stDownloadButton > button {
            background: transparent !important;
            color: var(--text-meta) !important;
            border: 1px solid rgba(0,0,0,0.18) !important;
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.76rem !important;
            font-weight: 500 !important;
            letter-spacing: 0.01em !important;
            text-transform: none !important;
            padding: 0.3rem 0.8rem !important;
        }
        .stDownloadButton > button:hover {
            color: var(--text-primary) !important;
            border-color: rgba(0,0,0,0.38) !important;
            background: rgba(0,0,0,0.02) !important;
        }

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

"""
