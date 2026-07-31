"""CSS fragment: Committee Register (.cmt-*), Lobbying PoC v3 (.lp3-*).

Mechanically split from the original utility/shared_css.py (lines 4999-5510 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Committee Register (cmt-*) ──────────────────────────────────── */
        /* Full container width (2026-07-20 clutter pass). This was
           inline-flex + fit-content, so every card sized to its own title —
           25 cards produced 25 different widths and a torn right edge down a
           half-empty page. The sole producer (committee_row_html) now renders
           into one st.html list with no adjacent button column, so nothing
           needs the shrink-to-fit box any more. */
        .cmt-row {
            display: flex;
            align-items: stretch;
            gap: 0;
            width: 100%;
            max-width: 100%;
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 3px solid var(--accent-dim);
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            padding: 0;
            overflow: hidden;
            /* Own the vertical rhythm: as inline-flex boxes the cards were
               separated by the source newlines between them, which no longer
               applies now they are block-level. */
            margin-bottom: 0.45rem;
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

        /* One shared party-colour key above the register grid (2026-07-21):
           replaces the per-card dot-and-count legend that repeated on all 25
           cards. Colour + name only — counts live on each card's stacked bar. */
        .cmt-legend {
            display: flex;
            flex-wrap: wrap;
            gap: 0.35rem 0.9rem;
            margin: 0.1rem 0 0.7rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            color: var(--text-meta);
        }
        .cmt-legend-item { display: inline-flex; align-items: center; }
        .cmt-legend-dot {
            display: inline-block;
            width: 0.6rem;
            height: 0.6rem;
            border-radius: 2px;
            margin-right: 0.32rem;
        }

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

"""
