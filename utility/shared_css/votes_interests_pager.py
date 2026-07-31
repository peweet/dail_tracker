"""CSS fragment: Interests member index cards, Votes (.vt-*) division index cards, reusable member-vote card, TD picker landing cards, pager.

Mechanically split from the original utility/shared_css.py (lines 4455-4998 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Interests: member index cards ──────────────────────────────── */
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
            /* 2026-07-17 audit: 18ch truncated ~20 of the first 25 cards
               ("Committee an…", "Second Stage (Re…") and the tooltip rescue
               doesn't exist on touch. 34ch fits the common worst case
               ("Restoration to Order Paper (Resumed)", 37ch) at this font
               size; phones keep the tighter cap below. */
            max-width: 38ch;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        @media (max-width: 640px) {
            .vt-card-stage { max-width: 18ch; }
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
            /* flex-direction:row !important overrides Streamlit's mobile
               breakpoint, which otherwise flips the chip row to column and
               stacks pages 1,2,3…N vertically on phones. */
            flex-direction: row !important;
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

"""
