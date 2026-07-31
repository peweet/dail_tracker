"""CSS fragment: Success/calm-blue theme, rank card component, Member Overview card grid + search.

Mechanically split from the original utility/shared_css.py (lines 1696-2227 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Success / calm-blue theme ───────────────────────────────
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
        /* Active section tab — the switcher chip for the section currently
           rendered. Filled accent so the reader always knows where they are.
           Placed AFTER the :hover rule so the active state wins on hover. */
        .mo-section-chip[aria-current="true"],
        .mo-section-chip[aria-current="true"]:hover,
        .mo-section-chip[aria-current="true"]:focus-visible {
            background: var(--accent);
            color: #ffffff;
            border-color: var(--accent);
        }

        /* ── Member Overview · Overview summary grid ─────────────────────────
           Default landing for a profile: one card per domain, each a link into
           that section. Replaces the old all-sections-rendered flat scroll. */
        .mo-overview-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 0.7rem;
            margin-top: 0.6rem;
        }
        .mo-overview-card {
            display: flex;
            flex-direction: column;
            gap: 0.2rem;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 0.85rem 1rem 0.95rem;
            text-decoration: none !important;
            transition: border-color 100ms ease, box-shadow 100ms ease;
        }
        .mo-overview-card:hover,
        .mo-overview-card:focus-visible {
            border-color: var(--accent);
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.06);
            outline: none;
        }
        /* Votes leads the Overview — full-width, heavier, accent spine. */
        .mo-ov-lead {
            grid-column: 1 / -1;
            border-left: 4px solid var(--accent);
        }
        .mo-ov-label {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: var(--text-meta);
        }
        .mo-ov-figure {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.7rem;
            font-weight: 700;
            line-height: 1.1;
            color: var(--text-primary);
        }
        .mo-ov-lead .mo-ov-figure {
            font-size: 2.15rem;
        }
        .mo-ov-signal {
            font-size: 0.85rem;
            line-height: 1.4;
            color: var(--text-secondary);
        }
        .mo-ov-cta {
            margin-top: 0.35rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem;
            font-weight: 600;
            color: var(--accent);
        }

        /* ── Member Overview · payments summary tiles ────────────────────────
           Two compact tiles (statutory salary | reimbursed expenses) that
           replace the salary card + divider + lead paragraph stack. States the
           salary-vs-expenses distinction once, cleanly, above the year list. */
        .mo-pay-tiles {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.7rem;
            margin: 0.3rem 0 1.1rem;
        }
        @media (max-width: 640px) {
            .mo-pay-tiles {
                grid-template-columns: 1fr;
            }
        }
        .mo-pay-tile {
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 0.8rem 1rem 0.9rem;
        }
        .mo-pay-tile-expenses {
            border-left: 4px solid var(--accent);
        }
        .mo-pay-tile-eyebrow {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-meta);
        }
        .mo-pay-tile-figure {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.7rem;
            font-weight: 700;
            line-height: 1.15;
            color: var(--text-primary);
            margin: 0.1rem 0 0.25rem;
        }
        .mo-pay-tile-per {
            font-size: 0.9rem;
            font-weight: 600;
            color: var(--text-meta);
        }
        .mo-pay-tile-note {
            font-size: 0.8rem;
            line-height: 1.45;
            color: var(--text-secondary);
        }

"""
