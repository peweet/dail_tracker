"""CSS fragment: Interests (.int-*) category headings, rounded checkbox, attendance ranking cards, participation model, clickable ranking cards, Payments page (.pay-*) start.

Mechanically split from the original utility/shared_css.py (lines 2747-3433 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Interests: category headings & diff badges ──────────── */
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

        /* ── Attendance ranking cards (neutral — no good/bad framing) ──── */
        /* Both columns share one neutral style; only the heading text differs,
           so the page reports the record without colour-coding low attendance
           as a failing (attendance can be low for illness, leave, ministerial
           or constituency duties, or mid-year membership). */
        [data-testid="stHorizontalBlock"]:has(.att-hall-heading) {
            align-items: flex-start !important;
        }
        .att-hall-heading {
            font-size: 1.3rem; font-weight: 800; letter-spacing: -0.02em;
            color: var(--text-primary); border-bottom: 3px solid var(--border-strong);
            padding-bottom: 0.5rem; margin: 0 0 0.9rem;
        }
        /* ── Participation & absence model (Showing up) ─────────────────── */
        .part-name { font-size: 1rem; font-weight: 700; color: var(--text-primary); margin: 0; line-height: 1.25; }
        .part-meta { font-size: 0.8rem; color: var(--text-secondary); margin: 0.1rem 0 0; }
        .part-role-chip {
            display: inline-block; margin: 0.3rem 0.3rem 0 0; padding: 0.08rem 0.5rem;
            font-size: 0.7rem; font-weight: 600; border-radius: 999px;
            background: var(--surface-2, #eef0f2); color: var(--text-secondary);
            border: 1px solid var(--border-strong);
        }
        .part-news-chip {
            display: inline-block; margin: 0.3rem 0 0; padding: 0.08rem 0.55rem;
            font-size: 0.72rem; font-weight: 600; border-radius: 999px; text-decoration: none;
            background: rgba(37,99,235,0.08); color: var(--accent);
            border: 1px solid rgba(37,99,235,0.25);
        }
        .part-news-chip:hover { background: rgba(37,99,235,0.16); }
        .part-noexpl { display: inline-block; margin-top: 0.3rem; font-size: 0.72rem; color: var(--text-secondary); font-style: italic; }

        .part-turnout-card {
            display: grid; grid-template-columns: minmax(0,1.6fr) 2fr auto; align-items: center; gap: 0.9rem;
            padding: 0.5rem 0.8rem; border-radius: 12px; margin-bottom: 0.35rem;
            background: var(--surface, #fff); box-shadow: 0 1px 4px rgba(0,0,0,0.07);
        }
        .part-turnout-bar-track { height: 10px; border-radius: 6px; background: var(--border-soft, #e6e8ea); overflow: hidden; }
        .part-turnout-bar-fill { height: 100%; border-radius: 6px; background: var(--accent); }
        .part-turnout-bar-fill.part-bar-muted { background: var(--text-secondary); opacity: 0.55; }
        .part-turnout-num { text-align: right; min-width: 84px; }
        .part-turnout-pct { display: block; font-size: 1.15rem; font-weight: 800; color: var(--text-primary); letter-spacing: -0.02em; }
        .part-turnout-sub { display: block; font-size: 0.72rem; color: var(--text-secondary); }

        .part-absence-row {
            display: grid; grid-template-columns: minmax(0,1.4fr) 2fr; align-items: center; gap: 0.9rem;
            padding: 0.55rem 0.8rem; border-radius: 12px; margin-bottom: 0.35rem;
            background: var(--surface, #fff); box-shadow: 0 1px 4px rgba(0,0,0,0.07);
        }
        .part-absence-figure { display: flex; flex-direction: column; align-items: flex-start; }
        .part-absence-run { font-size: 1.05rem; font-weight: 800; color: var(--text-primary); letter-spacing: -0.02em; }
        .part-absence-span { font-size: 0.78rem; color: var(--text-secondary); }

        .part-taa-row {
            display: grid; grid-template-columns: minmax(0,1.8fr) auto auto; align-items: center; gap: 0.9rem;
            padding: 0.5rem 0.8rem; border-radius: 12px; margin-bottom: 0.3rem;
            background: var(--surface, #fff); box-shadow: 0 1px 4px rgba(0,0,0,0.07);
        }
        .part-taa-days { font-size: 0.9rem; font-weight: 700; color: var(--text-primary); white-space: nowrap; }
        .part-taa-ded { font-size: 0.9rem; font-weight: 800; color: #b45309; white-space: nowrap; }

        .att-hall-card {
            display: flex; align-items: center; gap: 0.6rem;
            padding: 0.38rem 0.75rem; border-radius: 12px;
            margin-bottom: 0.3rem; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
            width: 100%;
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 5px solid var(--border-strong);
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
        .att-hall-badge {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            flex-shrink: 0; min-width: 3.4rem; padding: 0.3rem 0.6rem;
            border-radius: 12px; text-align: center; line-height: 1.1;
            background: var(--surface); border: 1px solid var(--border);
        }
        .att-hall-badge-num {
            font-size: 1.25rem; font-weight: 800; letter-spacing: -0.03em;
            color: var(--text-primary); display: block;
        }
        .att-hall-badge-label { font-size: 0.62rem; font-weight: 600; color: var(--text-meta); display: block; }

        /* ── Ranking cards as full-card-clickable links (clickable_card_link) ─── */
        /* No arrow shown, so don't reserve right-padding for one. */
        .dt-card-link-wrap > .att-hall-card {
            padding-right: 0.75rem !important;
        }
        /* Stack wraps with the same vertical rhythm the bare cards used.
           max-width 80% trims both columns so the cards aren't full-bleed. */
        .dt-card-link-wrap:has(> .att-hall-card) {
            margin-bottom: 0.3rem;
            max-width: 80%;
        }
        /* Neutral hover: lift + neutral shadow, no good/bad colour identity. */
        .dt-card-link-wrap:hover > .att-hall-card {
            border-color: var(--border-strong) !important;
            background: var(--surface) !important;
            box-shadow: 0 3px 10px rgba(0,0,0,0.12) !important;
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
            text-decoration: none;
            color: inherit;
            cursor: pointer;
            transition: border-color 0.12s ease, box-shadow 0.12s ease, background 0.12s ease;
        }
        a.pay-year-row:hover {
            border-color: var(--text-secondary);
            box-shadow: 0 1px 4px rgba(0,0,0,0.08);
        }
        a.pay-year-row:focus-visible {
            outline: 2px solid var(--text-primary);
            outline-offset: 1px;
        }
        .pay-year-row-active {
            border-color: var(--text-primary);
            border-left: 3px solid var(--text-primary);
            padding-left: calc(0.7rem - 2px);
            background: var(--surface);
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

"""
