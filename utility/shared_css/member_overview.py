"""CSS fragment: Member Overview (.mo-*) audit-fix bundle, full-card-clickable link, Ministerial diaries.

Mechanically split from the original utility/shared_css.py (lines 2228-2746 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Member Overview: audit-fix bundle (2026-05-27) ──────────────────
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

        /* Official contact-details card (address / phone / email scraped from
           the member's oireachtas.ie profile). Sits directly under the hero. */
        .mo-contact-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 2px;
            padding: 0.85rem 1.1rem;
            margin: 0 0 1.1rem;
        }
        .mo-contact-title {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin-bottom: 0.5rem;
        }
        .mo-contact-row {
            display: flex;
            align-items: baseline;
            gap: 0.55rem;
            font-size: 0.95rem;
            line-height: 1.5;
            color: var(--text-primary);
            margin: 0.18rem 0;
        }
        .mo-contact-ico {
            flex: 0 0 auto;
            width: 1.1rem;
            text-align: center;
            font-size: 0.9rem;
        }
        .mo-contact-val { min-width: 0; word-break: break-word; }
        .mo-contact-link {
            color: var(--accent);
            text-decoration: none;
            border-bottom: 1px dotted var(--accent);
        }
        .mo-contact-link:hover,
        .mo-contact-link:focus-visible { border-bottom-style: solid; }
        .mo-contact-empty {
            color: var(--text-secondary);
            font-size: 0.92rem;
        }
        .mo-contact-source {
            margin-top: 0.55rem;
            font-size: 0.78rem;
            color: var(--text-meta);
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

        /* ── Ministerial diaries ("Who Ministers Meet") ──────────────────────
           Ranked org row + drill-down. Inner card for clickable_card_link, so it
           exposes border-color/background for the wrapper's hover lift. ──────── */
        .dt-diary-card {
            display: flex; align-items: center; gap: 1rem;
            background: #ffffff;
            border: 1.5px solid var(--border-strong);
            border-left: 4px solid var(--border-strong);
            border-radius: 10px;
            padding: 0.7rem 0.95rem; margin-bottom: 0.4rem;
            transition: border-color 0.12s, background 0.12s, box-shadow 0.12s;
        }
        .dt-diary-card.is-corr { border-left-color: var(--accent); }
        .dt-diary-main { flex: 1 1 auto; min-width: 0; }
        .dt-diary-title { font-weight: 650; font-size: 1.02rem; color: var(--text-primary); }
        .dt-diary-sub { font-size: 0.8rem; color: var(--text-meta); text-transform: capitalize; margin-top: 0.1rem; }
        .dt-diary-metrics { display: flex; align-items: center; gap: 1.1rem; flex-shrink: 0; }
        .dt-diary-metric { font-size: 0.82rem; color: var(--text-secondary); white-space: nowrap; }
        .dt-diary-metric b { font-size: 1.05rem; color: var(--text-primary); }
        .dt-diary-corr {
            font-size: 0.78rem; font-weight: 600; color: var(--accent);
            background: var(--accent-subtle); border: 1px solid var(--accent);
            border-radius: 999px; padding: 0.15rem 0.6rem; white-space: nowrap;
        }
        .dt-diary-corr.is-muted { color: var(--text-meta); background: transparent; border-color: var(--border-strong); }
        /* Department chips on a minister/department card + the "most-met" strip — both are
           descriptive context, deliberately neutral (no accent) so they never read as a
           ranking or influence flag. */
        .dt-diary-badges { display: flex; flex-wrap: wrap; gap: 0.35rem; margin-top: 0.28rem; }
        .dt-diary-badge {
            font-size: 0.72rem; font-weight: 600; color: var(--text-secondary);
            background: #ffffff; border: 1px solid var(--border-strong);
            border-radius: 999px; padding: 0.12rem 0.55rem; white-space: nowrap;
        }
        .dt-diary-most { font-size: 0.78rem; color: var(--text-meta); margin-top: 0.3rem; }
        .dt-diary-most b { color: var(--text-secondary); font-weight: 600; }
        .dt-diary-hero { margin: 0.4rem 0 0.9rem; }
        .dt-diary-hero h2 { margin: 0 0 0.2rem; font-size: 1.5rem; }
        .dt-diary-hero p { margin: 0; color: var(--text-secondary); font-size: 0.92rem; }
        .dt-diary-lobby {
            display: inline-flex; align-items: center; gap: 0.4rem; margin-top: 0.6rem;
            font-size: 0.9rem; font-weight: 600; color: var(--accent) !important;
            background: var(--accent-subtle); border: 1.5px solid var(--accent);
            border-radius: 8px; padding: 0.45rem 0.8rem; text-decoration: none !important;
        }
        .dt-diary-lobby:hover { background: var(--accent); color: #ffffff !important; }
        .dt-diary-back { display:inline-block; margin-bottom:0.6rem; font-size:0.88rem;
            color: var(--text-secondary) !important; text-decoration:none !important; }
        .dt-diary-back:hover { color: var(--accent) !important; }
        .dt-diary-eng {
            display:flex; align-items:center; gap:1rem; background:#ffffff;
            border:1px solid var(--border); border-radius:8px; padding:0.55rem 0.85rem; margin-bottom:0.3rem;
        }
        .dt-diary-eng-main { flex:1 1 auto; min-width:0; }
        .dt-diary-eng-subj { font-weight:550; color: var(--text-primary); font-size:0.92rem; }
        .dt-diary-eng-meta { font-size:0.78rem; color: var(--text-meta); margin-top:0.1rem; }
        .dt-diary-src { font-size:0.8rem; color: var(--accent) !important; text-decoration:none !important; white-space:nowrap; }
        .dt-diary-prov {
            margin-top:1.4rem; padding-top:0.9rem; border-top:1px solid var(--border);
            font-size:0.8rem; color: var(--text-meta); line-height:1.5;
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
        [data-testid="stMain"] [data-testid="stMultiSelect"] [data-baseweb="select"] > div {
            background: #ffffff !important;
            border: 1.5px solid var(--border-strong) !important;
            border-radius: 8px !important;
        }
        [data-testid="stMain"] [data-testid="stMultiSelect"] [data-baseweb="select"] > div:focus-within {
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

"""
