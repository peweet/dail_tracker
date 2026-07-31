"""CSS fragment: Questions section (.q-*), Legislation pipeline phase strip / SI card / debate list, cross-page entity links, mobile layout.

Mechanically split from the original utility/shared_css.py (lines 3934-4454 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Questions section (member-overview) ─────────────────────
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
            .att-hall-card {
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

"""
