"""CSS fragment: Election 2024 (.e24-*) finance cards + hub, Judiciary (.jud-*), Procurement (.pr-*) + money-lifecycle strip, Follow the Money (.mf-*).

Mechanically split from the original utility/shared_css.py (lines 5511-6080 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Party / candidate finance cards (Election 2024 page) ────────────
           Shared .don-* card + receipt styling used by the donations, party-
           spending and candidate tabs on the unified Election 2024 hub.
           Ink-on-paper ledger: money is one ink, party identity is the 3px
           editorial side-stripe (project signature), figures are tabular. */
        .don-grid { display:grid; grid-template-columns:1fr 1fr; gap:0.9rem; margin:0.4rem 0 0.2rem; }
        @media (max-width: 760px){ .don-grid { grid-template-columns:1fr; } }
        .don-card { display:block; background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75));
            border-left:3px solid var(--don-stripe, var(--ink-muted)); border-radius:7px;
            padding:0.95rem 1.05rem 0.9rem; text-decoration:none; color:inherit; position:relative;
            transition:box-shadow .18s cubic-bezier(.22,1,.36,1), border-color .18s; }
        .don-card:hover { box-shadow:0 1px 10px oklch(0% 0 0 / .07); border-color:var(--don-stripe); }
        .don-card .don-dir { position:absolute; top:0.9rem; right:1.05rem; font-size:0.72rem;
            letter-spacing:.04em; color:var(--ink-muted); }
        .don-ptitle { display:flex; align-items:center; gap:0.5rem; margin:0 0 0.1rem; }
        .don-swatch { width:9px; height:9px; border-radius:2px; background:var(--don-stripe); flex:none; }
        .don-ptitle h3 { font-size:0.98rem; font-weight:600; margin:0; color:var(--ink-strong); }
        .don-amount { font-size:1.7rem; font-weight:650; letter-spacing:-.015em; line-height:1.1;
            font-variant-numeric:tabular-nums; color:var(--ink-strong); margin:0.35rem 0 0.1rem; }
        .don-sub { font-size:0.8rem; color:var(--ink-muted); }
        .don-cardfoot { display:flex; justify-content:space-between; align-items:baseline; margin-top:0.7rem; }
        .don-cardfoot .go { color:var(--accent); font-weight:600; font-size:0.82rem; }
        /* receipts (donor list) */
        .don-receipts { background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75));
            border-left:3px solid var(--don-stripe, var(--ink-muted)); border-radius:7px; padding:0.2rem 1.1rem; }
        .don-rrow { display:grid; grid-template-columns:1fr auto auto auto; align-items:baseline;
            gap:0.4rem 1.1rem; padding:0.65rem 0; border-bottom:1px solid oklch(92% 0.005 75); }
        .don-rrow:last-child { border-bottom:none; }
        .don-rrow .dn { font-weight:500; color:var(--ink-strong); }
        .don-rrow .dt { color:var(--ink-muted); font-size:0.83rem; font-variant-numeric:tabular-nums; }
        .don-rrow .mt { font-size:0.66rem; letter-spacing:.06em; text-transform:uppercase; color:oklch(45% 0.01 75);
            border:1px solid oklch(90% 0.006 75); border-radius:4px; padding:0.1rem 0.4rem; align-self:center; }
        .don-rrow .da { font-variant-numeric:tabular-nums; font-weight:600; min-width:5.3rem; text-align:right;
            color:var(--ink-strong); }
        .don-vmark { font-size:0.64rem; font-weight:600; letter-spacing:.07em; text-transform:uppercase;
            color:var(--ink-muted); white-space:nowrap; }
        .don-rrow .don-vmark { grid-column:1 / -1; padding-top:0.1rem; }
        /* Mobile: the 4-track donor/party-spend row (name · date · method ·
           amount) has no room on a ~390px phone — the name is crushed and the
           amount can overflow its 5.3rem min-width. Reflow to: name on its own
           row (full width), then date + amount on a second row, method chip on
           a third. CSS grids do not auto-stack the way st.columns does, so this
           explicit fallback is required. */
        @media (max-width: 620px) {
            .don-rrow { grid-template-columns: 1fr auto; gap: 0.1rem 0.6rem; }
            .don-rrow .dn { grid-column: 1 / -1; }
            .don-rrow .dt { grid-column: 1; grid-row: 2; }
            .don-rrow .da { grid-column: 2; grid-row: 2; min-width: 0; }
            .don-rrow .mt { grid-column: 1 / -1; grid-row: 3; justify-self: start; }
        }

        /* ── Election 2024 hub (unified GE2024 political finance) ─────────────
           Three SIPO returns under one roof: a tab strip, a "money map" flow of
           three independent totals (never summed), and a per-party "full picture"
           card showing all three streams as aligned proportional bars. */
        .e24-tabs { display:flex; flex-wrap:wrap; gap:0.3rem; margin:0.2rem 0 1.1rem;
            border-bottom:1px solid var(--border, oklch(88% 0.006 75)); padding-bottom:0.55rem; }
        .e24-tab { font-size:0.86rem; font-weight:600; color:var(--ink-muted); text-decoration:none;
            padding:0.4rem 0.85rem; border-radius:999px; border:1px solid transparent; white-space:nowrap;
            transition:background .15s, color .15s, border-color .15s; }
        .e24-tab:hover { color:var(--ink-strong); background:var(--surface-deep, oklch(96% 0.006 75)); }
        .e24-tab.active { color:#ffffff; background:var(--accent); border-color:var(--accent); }

        /* money map — three big totals connected by a directional flow. The
           framing copy makes clear these are separate records, not a balance. */
        .e24-map { display:grid; grid-template-columns:1fr auto 1fr auto 1fr; align-items:stretch;
            gap:0.4rem; margin:0.3rem 0 0.5rem; }
        @media (max-width: 820px){ .e24-map { grid-template-columns:1fr; }
            .e24-arrow { display:none; } }
        .e24-tier { background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75)); border-radius:9px;
            border-top:3px solid var(--e24-stripe, var(--ink-muted)); padding:0.85rem 1rem 0.9rem; display:flex;
            flex-direction:column; gap:0.1rem; }
        .e24-tier .lbl { font-size:0.7rem; font-weight:700; letter-spacing:.06em; text-transform:uppercase;
            color:var(--ink-muted); }
        .e24-tier .amt { font-size:1.85rem; font-weight:680; letter-spacing:-.02em; line-height:1.08;
            font-variant-numeric:tabular-nums; color:var(--ink-strong); margin:0.15rem 0 0.05rem; }
        .e24-tier .meta { font-size:0.78rem; color:var(--ink-muted); }
        .e24-tier .grain { font-size:0.72rem; color:oklch(48% 0.01 75); margin-top:0.3rem; font-style:italic; }
        .e24-arrow { display:flex; align-items:center; justify-content:center; color:var(--ink-muted);
            font-size:1.3rem; font-weight:400; }
        .e24-nosum { font-size:0.8rem; color:oklch(40% 0.03 60); background:oklch(96% 0.02 75);
            border:1px solid oklch(88% 0.03 70); border-left:3px solid oklch(70% 0.08 60); border-radius:6px;
            padding:0.55rem 0.8rem; margin:0.2rem 0 0.6rem; line-height:1.45; }

        /* per-party "full picture" card — three aligned stream bars */
        .e24-pcard { background:#ffffff; border:1px solid var(--border, oklch(88% 0.006 75));
            border-left:3px solid var(--e24-stripe, var(--ink-muted)); border-radius:8px;
            padding:0.85rem 1.05rem 0.9rem; }
        .e24-pcard .phead { display:flex; align-items:center; gap:0.5rem; margin-bottom:0.55rem; }
        .e24-pcard .phead .sw { width:9px; height:9px; border-radius:2px; background:var(--e24-stripe); flex:none; }
        .e24-pcard .phead h3 { font-size:1rem; font-weight:650; margin:0; color:var(--ink-strong); }
        .e24-streams { display:flex; flex-direction:column; gap:0.5rem; }
        .e24-stream { display:grid; grid-template-columns:9.5rem 1fr 6.5rem; align-items:center; gap:0.6rem; }
        @media (max-width: 620px){ .e24-stream { grid-template-columns:1fr; gap:0.15rem; } }
        .e24-stream-lbl { display:block; text-decoration:none; color:inherit; }
        .e24-stream .sl { font-size:0.78rem; color:var(--ink-muted); }
        .e24-stream-lbl:hover .sl { color:var(--accent); text-decoration:underline; }
        .e24-track { background:oklch(94% 0.005 75); border-radius:4px; height:0.7rem; overflow:hidden; }
        .e24-bar { display:block; height:100%; }
        .e24-bar.in  { background:#2e7d6b; }
        .e24-bar.agent { background:#3a6ea5; }
        .e24-bar.cand { background:#8a5a9e; }
        .e24-stream .sv { text-align:right; font-variant-numeric:tabular-nums; font-weight:600;
            color:var(--ink-strong); font-size:0.9rem; }
        .e24-stream .sv.none { color:var(--ink-muted); font-weight:400; }
        .e24-legend { display:flex; flex-wrap:wrap; gap:0.9rem; margin:0.1rem 0 0.7rem; font-size:0.76rem;
            color:var(--ink-muted); }
        .e24-legend .lk { display:inline-flex; align-items:center; gap:0.35rem; }
        .e24-legend .dot { width:9px; height:9px; border-radius:2px; flex:none; }

        /* ───────────────────────── Judiciary: The Bench & Courts ──────────
           Bench roster cards, career-arc timeline, appointing-authority chips
           and vacancy-lifecycle cards. Ink-on-paper restraint: white cards,
           full borders (no ad-hoc side-stripes — that signature is reserved
           for info_card/card_row), one accent on hover via .dt-card-link-wrap.
           Authority colours are blue/amber (deuteranopia-safe) AND text-labelled. */
        .jud-grid {
            display:grid; grid-template-columns:repeat(auto-fill, minmax(16.5rem, 1fr));
            gap:0.7rem; margin-top:0.3rem;
        }
        .jud-card {
            background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.7rem 0.85rem; display:flex; flex-direction:column; gap:0.18rem; height:100%;
        }
        .jud-card.vacant { background:var(--surface); border-style:dashed; }
        .jud-jn { font-weight:650; color:var(--ink-strong); font-size:0.95rem; line-height:1.2; }
        .jud-jc { font-size:0.76rem; color:var(--text-meta); }
        .jud-appt { font-size:0.8rem; color:oklch(38% 0.012 75); margin-top:0.1rem; }
        .jud-chiprow { display:flex; flex-wrap:wrap; gap:0.3rem; margin-top:0.4rem; align-items:center; }
        .jud-chip {
            font-size:0.66rem; font-weight:650; letter-spacing:0.02em; border-radius:999px;
            padding:0.08rem 0.5rem; white-space:nowrap; border:1px solid transparent;
        }
        .jud-chip.elev { background:var(--signal-good-subtle); color:var(--signal-good-deep);
            border-color:var(--signal-good-border); }
        .jud-chip.assign { background:var(--accent-subtle); color:var(--accent); border-color:var(--accent-dim); }
        .jud-chip.review { background:var(--signal-bad-subtle); color:var(--signal-bad-deep);
            border-color:var(--signal-bad-border); }
        .jud-chip.gap { background:var(--surface-deep); color:var(--text-meta); border-color:var(--border); }
        /* neutral (not gov/pres-coloured) — flags Chief Justice / President / ex-officio premium */
        .jud-chip.office { background:var(--surface-deep); color:var(--ink-700); border-color:var(--border-strong); }
        /* appointing-authority chips (blue=Government, amber=President, neutral=other) */
        .jud-auth { font-size:0.66rem; font-weight:650; letter-spacing:0.02em; border-radius:999px;
            padding:0.08rem 0.5rem; white-space:nowrap; border:1px solid; }
        .jud-auth.gov { background:var(--signal-good-subtle); color:var(--signal-good-deep);
            border-color:var(--signal-good-border); }
        .jud-auth.pres { background:var(--signal-bad-subtle); color:var(--signal-bad-deep);
            border-color:var(--signal-bad-border); }
        .jud-auth.other { background:var(--surface-deep); color:var(--text-meta); border-color:var(--border); }

        /* career-arc timeline — the one bespoke flourish; horizontal nodes + connector */
        .jud-arc { display:flex; flex-wrap:wrap; align-items:stretch; gap:0; margin:0.6rem 0 0.4rem; }
        /* fixed-width nodes pack left — a two-step career reads as two adjacent steps,
           not two halves of the page with a long empty connector between them. */
        .jud-node { position:relative; flex:0 0 11rem; padding:0 1.1rem 0.2rem 0; }
        .jud-node:not(:last-child)::after {
            content:""; position:absolute; top:0.42rem; right:0.45rem; left:1.0rem; height:2px;
            background:var(--border-strong);
        }
        .jud-node-dot { width:0.85rem; height:0.85rem; border-radius:999px; background:#ffffff;
            border:2.5px solid var(--accent); position:relative; z-index:1; }
        .jud-node.now .jud-node-dot { background:var(--accent); }
        .jud-node-court { font-weight:650; color:var(--ink-strong); font-size:0.88rem; margin-top:0.35rem; }
        .jud-node-date { font-size:0.74rem; color:var(--text-meta); }
        .jud-node-auth { font-size:0.72rem; color:oklch(40% 0.012 75); margin-top:0.1rem; }
        .jud-node-link { display:inline-block; font-size:0.7rem; color:var(--accent);
            text-decoration:none; margin-top:0.15rem; }
        .jud-node-link:hover { text-decoration:underline; }

        /* profile header + provenance + vacancy-lifecycle */
        .jud-prof-head { margin:0.2rem 0 0.5rem; }
        .jud-prof-name { font-size:1.5rem; font-weight:700; color:oklch(22% 0.012 75); line-height:1.15; margin:0; padding:0; }
        .jud-prof-sub { font-size:0.9rem; color:var(--text-meta); margin-top:0.15rem; }
        .jud-vac { background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.65rem 0.85rem; margin-bottom:0.5rem; }
        .jud-vac-cause { font-weight:600; color:var(--ink-700); font-size:0.86rem; }
        .jud-vac-pred { font-size:0.78rem; color:var(--text-meta); margin-top:0.15rem; }
        .jud-vac-nom { font-size:0.82rem; color:oklch(35% 0.012 75); margin-top:0.25rem; }
        /* appointing-authority stats — compact chips that size to content, packed left,
           not full-width cards stranding a number in empty space. */
        .jud-statwrap { display:flex; flex-wrap:wrap; gap:0.5rem; margin:0.3rem 0 0.2rem; }
        .jud-stat { background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.5rem 0.85rem; display:flex; align-items:center; gap:0.6rem; }
        .jud-stat-n { font-weight:700; font-size:1.5rem; color:var(--ink-strong);
            font-variant-numeric:tabular-nums; line-height:1; }
        .jud-ladder { display:flex; flex-wrap:wrap; gap:0.5rem; margin:0.3rem 0 0.2rem; }
        .jud-rung { background:#ffffff; border:1px solid var(--border); border-radius:10px;
            padding:0.55rem 0.8rem; display:flex; align-items:baseline; gap:0.6rem; }
        .jud-rung-path { font-weight:600; color:var(--ink-700); font-size:0.84rem; }
        .jud-rung-n { font-weight:700; color:var(--accent); font-size:1.05rem; font-variant-numeric:tabular-nums; }
        .jud-foot { font-size:0.76rem; color:var(--text-meta); line-height:1.5; margin-top:1.4rem;
            border-top:1px solid var(--border); padding-top:0.8rem; max-width:64rem; }
        .jud-foot a { color:var(--accent); }

        /* ───────────────────────── Public Procurement ──────────────────
           Supplier / authority / category register cards + supplier profile.
           Ink-on-paper: white cards, full borders, blue/neutral chips
           (deuteranopia-safe, text-labelled — never red/green). Supplier and
           lobbying-overlap cards drill down via .dt-card-link-wrap (hover lift). */
        .pr-caveat {
            background: var(--signal-bad-subtle); border: 1px solid var(--signal-bad-border);
            border-left: 3px solid var(--signal-bad-mid); border-radius: 8px;
            padding: 0.7rem 0.95rem; margin: 0.4rem 0 0.9rem;
            font-size: 0.86rem; color: var(--ink-700); line-height: 1.55; max-width: 64rem;
        }
        .pr-caveat strong { color: var(--signal-bad-deep); }
        /* ── "How public money moves" lifecycle strip ──────────────────────────
           The page's sections (open / wins / paid) ARE the four realisation tiers of one
           contract's life. This strip names that sequence ONCE, above the section bar, so a
           first-time reader sees four stages of one thing rather than four unrelated lists.
           Honesty rail: stages sit side by side and are NEVER summed; AFS is a sibling
           measure (different grain) deliberately set OFF the line. Per-stage accent is set
           inline via --lc-accent. */
        .pr-lc { max-width: 64rem; margin: 0.2rem 0 0.9rem; }
        .pr-lc-head { font-size: 0.86rem; color: var(--ink-700); line-height: 1.5; margin-bottom: 0.55rem; }
        .pr-lc-head strong { color: var(--ink-strong); }
        .pr-lc-track { display: flex; flex-wrap: wrap; align-items: stretch; gap: 0.35rem; }
        .pr-lc-stage {
            flex: 1 1 11rem; min-width: 9.5rem; text-decoration: none;
            background: #ffffff; border: 1px solid var(--border);
            border-top: 3px solid var(--lc-accent, var(--accent)); border-radius: 10px;
            padding: 0.6rem 0.75rem; display: flex; flex-direction: column; gap: 0.22rem;
            transition: box-shadow 0.12s ease, transform 0.12s ease;
        }
        .pr-lc-stage:hover { box-shadow: 0 2px 10px rgba(0,0,0,0.08); transform: translateY(-1px); }
        /* Static modifier — the strip is now a NON-clickable explainer (the section bar is the
           page's one navigation), so a cell reads as a diagram tile, not a control: no pointer
           cursor, no hover-lift. */
        .pr-lc-stage--static { cursor: default; }
        .pr-lc-stage--static:hover { box-shadow: none; transform: none; }
        .pr-lc-tier {
            font-size: 0.66rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
            color: var(--lc-accent, var(--accent));
        }
        .pr-lc-q { font-weight: 700; color: var(--ink-strong); font-size: 0.91rem; line-height: 1.25; }
        .pr-lc-note { font-size: 0.755rem; color: var(--text-meta); line-height: 1.4; }
        .pr-lc-go { font-size: 0.73rem; font-weight: 650; color: var(--accent); margin-top: auto; padding-top: 0.2rem; }
        .pr-lc-arrow { align-self: center; color: var(--border-strong); font-size: 1rem; flex: none; }
        @media (max-width: 720px) { .pr-lc-arrow { display: none; } }
        /* AFS sibling — visually OFF the lifecycle line (different grain, never summed);
           teal left-rule matches the council dossier's "Running the services" lane. */
        .pr-lc-sibling {
            margin-top: 0.55rem; font-size: 0.78rem; color: var(--ink-700); line-height: 1.5;
            border-left: 3px solid #3a6b7e; background: var(--surface-deep); border-radius: 0 8px 8px 0;
            padding: 0.5rem 0.8rem; max-width: 64rem;
        }
        .pr-lc-sibling strong { color: #2c5260; }
        /* scale anchor / trust strip under the caveat: real corpus magnitude +
           what's in / out. Numbers are tabular; labels are quiet meta. */
        /* "The €570bn that isn't" — naive total shown struck-through only to demolish it.
           Neutral/ink palette (no red/green): the mirage is muted grey, the real figure
           is accent ink. The multiplier badge is the one bold accent. */
        .pr-contrast {
            background: #ffffff; border: 1px solid var(--border); border-radius: 10px;
            padding: 0.85rem 1.1rem; margin: 0 0 0.9rem; max-width: 64rem;
        }
        .pr-contrast-row {
            display: flex; align-items: center; flex-wrap: wrap; gap: 0.6rem 1.2rem;
        }
        .pr-contrast-cell { display: flex; flex-direction: column; line-height: 1.15; }
        .pr-contrast-num { font-size: 1.7rem; font-weight: 800; font-variant-numeric: tabular-nums; }
        .pr-contrast-naive .pr-contrast-num { color: var(--text-meta); }
        .pr-strike { text-decoration: line-through; text-decoration-thickness: 2px; }
        .pr-contrast-safe .pr-contrast-num { color: var(--accent); }
        .pr-contrast-lbl { font-size: 0.74rem; color: var(--text-meta); margin-top: 0.1rem; }
        .pr-contrast-mult {
            font-size: 0.82rem; font-weight: 700; color: #ffffff; background: var(--accent);
            padding: 0.22rem 0.6rem; border-radius: 999px; white-space: nowrap; flex: none;
        }
        .pr-contrast-note {
            font-size: 0.82rem; color: var(--ink-700); line-height: 1.5;
            margin-top: 0.6rem; padding-top: 0.55rem; border-top: 1px solid var(--border);
        }
        .pr-contrast-note strong { color: var(--ink-strong); }
        @media (max-width: 560px) {
            .pr-contrast-num { font-size: 1.35rem; }
        }

        .pr-stats {
            display: flex; flex-wrap: wrap; gap: 0.45rem 1.4rem;
            padding: 0.7rem 0.95rem; margin: 0 0 0.9rem; max-width: 64rem;
            background: #ffffff; border: 1px solid var(--border); border-radius: 10px;
        }
        .pr-stat { display: flex; flex-direction: column; line-height: 1.2; }
        .pr-stat-num {
            font-weight: 750; color: var(--ink-strong); font-size: 1.02rem;
            font-variant-numeric: tabular-nums;
        }
        .pr-stat-lbl { font-size: 0.72rem; color: var(--text-meta); }
        .pr-grid {
            display: grid; grid-template-columns: repeat(auto-fill, minmax(18rem, 1fr));
            gap: 0.7rem; margin-top: 0.5rem;
        }
        .pr-card {
            background: #ffffff; border: 1px solid var(--border); border-radius: 10px;
            padding: 0.7rem 0.85rem; display: flex; flex-direction: column; gap: 0.35rem;
            height: 100%;
        }
        .pr-card-head { display: flex; align-items: baseline; gap: 0.5rem; }
        /* National-scale finance anchor (render_national_finance_context). */
        .dt-natfin { display: flex; flex-direction: column; gap: 0.1rem; margin: 0.6rem 0 0.9rem;
            padding: 0.6rem 0.85rem; border-left: 3px solid #3a6b7e; background: var(--surface-1, #faf7f0);
            border-radius: 0 8px 8px 0; }
        .dt-natfin-k { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            font-weight: 700; color: #3a6b7e; }
        .dt-natfin-v { font-size: 1rem; color: var(--ink-strong); font-variant-numeric: tabular-nums; }
        .dt-natfin-c { font-size: 0.78rem; color: var(--text-meta); line-height: 1.4; }
        .pr-card-src { margin-top: 0.4rem; font-size: 0.8rem; }
        .pr-card-src a { color: var(--link, #2b6b7e); text-decoration: none; }
        .pr-card-src a:hover { text-decoration: underline; }
        .pr-rank {
            font-weight: 700; color: var(--accent); font-size: 0.82rem;
            font-variant-numeric: tabular-nums; flex: none;
        }
        .pr-name { font-weight: 650; color: var(--ink-strong); font-size: 0.93rem; line-height: 1.3; }
        /* secondary line inside a card name — the published tender/contract title under the buyer */
        .pr-sub { display: block; font-weight: 500; color: var(--text-meta); font-size: 0.8rem;
            line-height: 1.3; margin-top: 0.12rem; }
        .pr-meta { font-size: 0.78rem; color: var(--text-meta); }
        /* labelled divider between two procurement registers (national eTenders vs EU-journal TED),
           rendered as a hairline with a centred caption so neither register's values read as one list */
        .pr-register-rule { display: flex; align-items: center; gap: 0.6rem; margin: 1.6rem 0 0.9rem;
            color: var(--text-meta); font-size: 0.7rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; }
        .pr-register-rule::before, .pr-register-rule::after {
            content: ""; flex: 1; height: 1px; background: var(--border); }
        .pr-pills { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-top: auto; padding-top: 0.15rem; }
        .pr-pill {
            font-size: 0.72rem; font-weight: 600; padding: 0.08rem 0.5rem; border-radius: 999px;
            background: var(--surface-deep); color: var(--ink-700); border: 1px solid var(--border);
            white-space: nowrap;
        }
        .pr-pill-val { background: var(--accent-subtle); color: var(--accent); border-color: var(--accent-dim); }
        .pr-pill-cro { background: var(--signal-good-subtle); color: var(--signal-good-deep);
            border-color: var(--signal-good-border); }
        /* EPA environmental-licence holder — factual register membership (earthy green,
           distinct from the CRO good-standing green); NOT an enforcement/alarm signal. */
        .pr-pill-epa { background: #eef3ec; color: #2c4a23; border-color: #cfe0c8; }
        /* lobbying co-occurrence is informational, NOT an alarm — neutral chip,
           never red, so the colour never implies wrongdoing (honesty rail). */
        .pr-pill-lob { background: var(--surface-deep); color: var(--ink-700); border-color: var(--border-strong); }
        /* a pill that is itself a link (e.g. the dossier's lobbying chip → that org's
           lobbying record). Only used where the pill is NOT inside a clickable card. */
        a.pr-pill { text-decoration: none; cursor: pointer; }
        a.pr-pill:hover { border-color: var(--ink-strong); color: var(--ink-strong); }
        a.pr-pill:focus-visible { outline: 2px solid var(--ink-strong); outline-offset: 1px; }

        /* supplier / buyer profile (?supplier= / ?paid_publisher=) */
        .pr-prof-head { margin: 0.2rem 0 0.5rem; }
        .pr-prof-kicker { font-size: 0.7rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; color: var(--text-meta); margin-bottom: 0.15rem; }
        .pr-prof-name { font-size: 1.5rem; font-weight: 700; color: var(--ink-strong);
            line-height: 1.15; margin: 0; padding: 0; }
        .pr-prof-sub { font-size: 0.9rem; color: var(--text-meta); margin-top: 0.15rem; }
        .pr-award {
            background: #ffffff; border: 1px solid var(--border); border-radius: 8px;
            padding: 0.55rem 0.8rem; margin-bottom: 0.4rem;
            display: flex; align-items: baseline; gap: 0.7rem;
        }
        .pr-award-body { flex: 1; min-width: 0; }
        .pr-award-auth { font-weight: 600; color: var(--ink-700); font-size: 0.88rem; }
        /* buyer-dossier link on a supplier's award/relationship rows — keeps the
           authority-name weight, signals clickability on hover/focus (supplier↔buyer loop) */
        .pr-auth-link { color: inherit; text-decoration: none; }
        .pr-auth-link:hover { color: var(--accent); text-decoration: underline; }
        .pr-auth-link:focus-visible { outline: 2px solid var(--ink-strong); outline-offset: 1px; border-radius: 2px; }
        /* published contract title — the descriptive line between entity and meta */
        .pr-award-title { font-size: 0.8rem; color: var(--ink-strong); line-height: 1.35; margin-top: 0.1rem; }
        .pr-award-meta { font-size: 0.76rem; color: var(--text-meta); margin-top: 0.1rem; }
        .pr-award-meta a { color: var(--accent); text-decoration: none; white-space: nowrap; }
        .pr-award-meta a:hover { text-decoration: underline; }
        .pr-award-val { font-weight: 700; color: var(--accent); font-size: 0.92rem;
            font-variant-numeric: tabular-nums; white-space: nowrap; text-align: right; }
        .pr-award-val small { display: block; font-weight: 500; color: var(--text-meta); font-size: 0.64rem; }
        .pr-award-val.ceiling { color: var(--signal-bad-deep); }
        .pr-foot {
            font-size: 0.8rem; color: var(--text-meta); line-height: 1.55;
            margin-top: 1.4rem; padding-top: 0.7rem; border-top: 1px solid var(--border); max-width: 64rem;
        }
        .pr-foot a { color: var(--accent); }
        /* ── Follow the Money (mf-*) — the trail breadcrumb + landing entries ───────────
           Reuses the pr-* card/award grammar for node bodies; mf-* covers only the rail and
           the landing's featured trail / data-wall note. */
        .mf-rail {
            background: #ffffff; border: 1px solid var(--border); border-left: 4px solid var(--accent);
            border-radius: 8px; padding: 0.5rem 0.85rem; margin: 0 0 0.9rem;
            display: flex; flex-wrap: wrap; align-items: center; gap: 0.35rem 0.7rem;
        }
        .mf-rail-lede { font-size: 0.7rem; font-weight: 700; letter-spacing: 0.06em;
            text-transform: uppercase; color: var(--text-meta); white-space: nowrap; }
        .mf-rail-path { display: flex; flex-wrap: wrap; align-items: center; gap: 0.3rem 0.45rem;
            font-size: 0.9rem; min-width: 0; }
        .mf-rail-step { color: var(--accent); text-decoration: none; font-weight: 600; }
        .mf-rail-step:hover { text-decoration: underline; }
        .mf-rail-step:focus-visible { outline: 2px solid var(--ink-strong); outline-offset: 1px; border-radius: 2px; }
        .mf-rail-here { color: var(--ink-strong); font-weight: 700; }
        .mf-rail-sep { color: var(--text-meta); }
        .mf-rail-gap { color: var(--text-meta); font-weight: 700; }
        .mf-rail-reset { margin-left: 0.5rem; font-size: 0.78rem; color: var(--text-meta);
            text-decoration: none; border-bottom: 1px dashed var(--border); }
        .mf-rail-reset:hover { color: var(--accent); }
        /* featured ready-made trail on the landing — a bold whole-card link */
        .mf-featured {
            display: block; background: #ffffff; border: 1px solid var(--border);
            border-left: 4px solid var(--accent); border-radius: 10px; padding: 0.9rem 1.1rem;
            margin: 0.2rem 0 0.8rem; text-decoration: none; transition: border-color 0.12s, box-shadow 0.12s;
        }
        .mf-featured:hover { border-color: var(--accent); box-shadow: 0 2px 10px rgba(0,0,0,0.06); }
        .mf-featured:focus-visible { outline: 2px solid var(--ink-strong); outline-offset: 2px; }
        .mf-featured-kick { font-size: 0.68rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; color: var(--accent); }
        .mf-featured-name { font-size: 1.15rem; font-weight: 700; color: var(--ink-strong); margin: 0.1rem 0; }
        .mf-featured-blurb { font-size: 0.86rem; color: var(--ink-700); line-height: 1.45; }
        /* Public Payments hub "go deeper" entry cards (Money nav declutter Phase 1):
           the two .mf-featured whole-card links to the hidden-but-routable
           /follow-the-money and /accommodation-spend routes. Reuses the featured-card
           look above; this wrapper only supplies the compact 2-up row (same
           two-column + mobile-collapse pattern as .don-grid). */
        .pp-deeper { display: grid; grid-template-columns: 1fr 1fr; gap: 0.7rem; margin: 0.7rem 0 0.5rem; }
        .pp-deeper > .mf-featured { margin: 0; }
        @media (max-width: 760px) { .pp-deeper { grid-template-columns: 1fr; } }
        /* the data-wall note — where the trail stops (direct contractor only) */
        .mf-wall { font-size: 0.84rem; color: var(--ink-700); line-height: 1.5;
            background: var(--surface); border: 1px dashed var(--border); border-radius: 8px;
            padding: 0.6rem 0.85rem; margin: 0 0 1.1rem; max-width: 60rem; }
        .mf-wall strong { color: var(--ink-strong); }
        /* State-as-investor (ISIF) lane on the Follow-the-money landing. */
        .mf-isif { max-width: 60rem; margin: 0 0 1.2rem; padding: 0.85rem 1rem;
            border: 1px solid var(--border); border-left: 3px solid #2f7d5b; border-radius: 0 8px 8px 0;
            background: var(--surface-1, #faf7f0); }
        .mf-isif-kick { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            font-weight: 700; color: #2f7d5b; margin-bottom: 0.25rem; }
        .mf-isif-sub { font-size: 0.84rem; color: var(--ink-700); line-height: 1.5; margin-bottom: 0.7rem; }
        .mf-isif-row { padding: 0.45rem 0; border-top: 1px solid var(--border); }
        .mf-isif-head { display: flex; align-items: baseline; gap: 0.55rem; flex-wrap: wrap; }
        .mf-isif-name { font-weight: 700; color: var(--ink-strong); }
        .mf-isif-amt { font-weight: 700; color: #2f7d5b; font-variant-numeric: tabular-nums; }
        .mf-isif-amt-none { font-weight: 400; font-style: italic; color: var(--text-meta); }
        .mf-isif-yr { font-size: 0.78rem; color: var(--text-meta); margin-left: auto; }
        .mf-isif-desc { font-size: 0.8rem; color: var(--ink-700); line-height: 1.4; margin-top: 0.15rem; }
        .pr-cap { font-size: 0.86rem; color: var(--ink-700); line-height: 1.5; margin: 0.2rem 0 0.6rem; max-width: 60rem; }
        .pr-cap em { color: var(--text-meta); font-style: italic; }
        /* stale-snapshot warning — rust (signal-bad), NEVER true red: this is a freshness
           caution, not an alarm, and red would imply wrongdoing (honesty rail). */
        .pr-cap-stale { color: var(--signal-bad-deep); font-weight: 700; }
        /* Authoritative-source conduit: the list of TED notices that open the real record.
           Each row is a doorway out to the Official Journal, with a quiet value-kind tag. */
        .pr-notice-list { list-style: none; margin: 0.2rem 0 0; padding: 0; }
        .pr-notice { padding: 0.3rem 0; border-top: 1px solid var(--border); font-size: 0.88rem; }
        .pr-notice:first-child { border-top: none; }
        .pr-notice a { color: var(--accent); font-weight: 600; text-decoration: none; }
        .pr-notice a:hover { text-decoration: underline; }
        .pr-notice-tag { color: var(--text-meta); font-size: 0.78rem; margin-left: 0.4rem; }
        /* Per-line payment-status pill (Paid / Part paid / Not paid) — shown only where the body
           published a status. Quiet, factual: Paid in the calm signal-good tint, Not paid in the
           burnt-orange signal-bad tint (never alarm-red), Part paid neutral. Not a verdict. */
        .pr-paid-tag {
            display: inline-block; font-size: 0.68rem; font-weight: 600; letter-spacing: 0.01em;
            padding: 0.05rem 0.4rem; border-radius: 999px; border: 1px solid transparent;
            vertical-align: middle; white-space: nowrap; margin-left: 0.4rem;
        }
        .pr-paid-tag.is-paid { background: var(--signal-good-subtle); color: var(--signal-good-deep);
            border-color: var(--signal-good-border); }
        .pr-paid-tag.is-notpaid { background: var(--signal-bad-subtle); color: var(--signal-bad-deep);
            border-color: var(--signal-bad-border); }
        .pr-paid-tag.is-partpaid { background: var(--surface-deep); color: var(--text-meta);
            border-color: var(--border); }
        /* Recurring annual / PPP charge — a literacy caution (this amount repeats yearly, do not
           total it as one-off spend), not a verdict. Calm amber-neutral, distinct from paid tints. */
        .pr-paid-tag.is-recurring { background: var(--surface-deep); color: #9c5b2e;
            border-color: var(--border); white-space: normal; }
        /* TED cross-reference block on a supplier profile — a quiet, clearly-separate
           "other register" callout. Neutral surface, left rule in accent (informational,
           never alarm); the copy says "not added" so it can't read as a bigger total. */
        .pr-ted-xref {
            background: var(--surface-deep); border: 1px solid var(--border);
            border-left: 3px solid var(--accent); border-radius: 8px;
            padding: 0.6rem 0.85rem; margin: 1rem 0 0.4rem; max-width: 60rem;
        }
        .pr-ted-xref-h { font-weight: 700; color: var(--ink-strong); font-size: 0.84rem; }
        .pr-ted-xref-b { font-size: 0.82rem; color: var(--ink-700); line-height: 1.5; margin-top: 0.2rem; }
        .pr-ted-xref-b em { color: var(--text-meta); }
        /* AFS (audited-accounts) context on a local-authority dossier — a SIBLING budget fact,
           visually fenced off from the purchase-order section above so the two grains never read
           as one total. Teal accents (vs the PO section's brown) reinforce "different measure". */
        .pr-afs { margin: 1.4rem 0 0.5rem; padding-top: 1rem; border-top: 1px solid var(--border); }
        .pr-afs-head { font-weight: 800; color: var(--ink-strong); font-size: 1.02rem; letter-spacing: -0.01em; }
        .pr-afs-trace {
            background: #ffffff; border: 1px solid var(--border);
            border-left: 3px solid #3a6b7e; border-radius: 8px;
            padding: 0.6rem 0.85rem; margin: 0.5rem 0 0.7rem; max-width: 60rem;
        }
        .pr-afs-trace-fig { font-size: 0.95rem; color: var(--ink-strong); line-height: 1.5; }
        .pr-afs-trace-cap { font-size: 0.8rem; color: var(--ink-700); line-height: 1.45; margin-top: 0.3rem; }
        /* Dossier LANES — the three honest grains of council money (Running / Building / Paying),
           each opened by its own band. The tag is the small-caps stratum; the <h2> is the section
           heading; the dek carries the never-sum framing. A coloured left rule keys each lane to its
           chart/bar colour (teal=revenue, green=capital, brown=PO) so the grains stay visually distinct. */
        .pr-lane { margin: 1.8rem 0 0.7rem; padding: 0.75rem 0 0.2rem; border-top: 2px solid var(--border-strong); }
        .pr-lane:first-of-type { margin-top: 1rem; }
        .pr-lane-tag { font-size: 0.72rem; font-weight: 700; letter-spacing: 0.09em; text-transform: uppercase;
            color: var(--text-meta); margin-bottom: 0.15rem; }
        .pr-lane-head { font-size: 1.32rem; font-weight: 800; color: var(--ink-strong);
            letter-spacing: -0.02em; line-height: 1.15; margin: 0 0 0.3rem; }
        .pr-lane-dek { font-size: 0.86rem; color: var(--ink-700); line-height: 1.5; max-width: 60rem; margin: 0; }
        /* Horizontal labelled bars for the audited lanes (net cost by service / capital by service).
           Bar width is a display scaling against the lane's own max — the figure is the truth, the bar
           is a glance. tabular-nums keeps the right-aligned euros in a clean column. */
        .pr-afsbars { margin: 0.3rem 0 0.6rem; max-width: 60rem; }
        .pr-afsbar { margin: 0.55rem 0; }
        .pr-afsbar-top { display: flex; align-items: baseline; justify-content: space-between; gap: 0.75rem; }
        .pr-afsbar-label { font-size: 0.9rem; font-weight: 600; color: var(--ink-strong); }
        .pr-afsbar-fig { font-size: 0.92rem; color: var(--ink-strong); white-space: nowrap;
            font-variant-numeric: tabular-nums; }
        .pr-afsbar-zero { font-size: 0.82rem; font-weight: 500; color: var(--text-meta); font-style: italic; }
        .pr-afsbar-track { height: 9px; background: var(--surface-2, #eee7dc); border-radius: 5px;
            overflow: hidden; margin: 0.22rem 0 0.1rem; }
        .pr-afsbar-fill { height: 100%; border-radius: 5px; min-width: 2px; }
        .pr-afsbar-note { font-size: 0.76rem; color: var(--text-meta); line-height: 1.4; }
        /* "What the money buys" category lens — clickable ranked bars + publisher chips. */
        .pp-cat-bars a { display: block; text-decoration: none; color: inherit;
            padding: 0.5rem 0.7rem; margin: 0.3rem 0; border: 1px solid transparent;
            border-radius: 8px; transition: background 0.12s, border-color 0.12s; }
        .pp-cat-bars a:hover { background: var(--surface-1, #faf7f0); border-color: var(--border-strong); }
        .pp-cat-bars .pr-afsbar { margin: 0; }
        .pp-cat-chips { display: flex; flex-wrap: wrap; gap: 0.4rem; margin: 0.1rem 0 0.9rem; }
        .pp-cat-chip { font-size: 0.84rem; padding: 0.22rem 0.6rem; border-radius: 999px;
            background: var(--surface-2, #eee7dc); color: var(--ink-strong); text-decoration: none;
            white-space: nowrap; }
        .pp-cat-chip:hover { background: var(--border-strong); }
        /* "Your council" index — province bands + lifecycle-tier pills.
           Province band header is a SEMANTIC <h3> (heading-navigable for screen
           readers); geography is encoded by the fixed N->S band order, never colour. */
        .pr-region-head {
            display: flex; align-items: baseline; justify-content: space-between;
            gap: 0.75rem; margin: 1.6rem 0 0.5rem; padding-bottom: 0.3rem;
            border-bottom: 1px solid var(--border-strong);
        }
        .pr-region-head:first-of-type { margin-top: 0.4rem; }
        .pr-region-name {
            font-size: 0.82rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; color: var(--ink-strong); line-height: 1.2;
        }
        .pr-region-count { font-size: 0.78rem; font-weight: 500; color: var(--text-meta);
            font-variant-numeric: tabular-nums; white-space: nowrap; }
        /* Two STAGES of public money, NEVER a sum. solid pill = realised ('paid'),
           dashed pill = committed ('ordered', provisional). The dashed/solid contrast plus
           the verb baked into each chip make summing them obviously wrong; no glyph ever
           joins them. The verb is also the non-colour accessibility carrier.
           Pills sit in _card's standard .pr-pills wrapper (bottom-aligned, wrapping). */
        .pr-pill-paid {     /* realised — the firmest fact, strongest ink */
            background: var(--accent-subtle); color: var(--accent); border: 1px solid var(--accent-dim); }
        .pr-pill-ordered {  /* committed — provisional, hollow, dashed */
            background: #ffffff; color: var(--ink-700); border: 1px dashed var(--border-strong); }

        /* Section heading inside a tab (semantic <h2> under the page <h1> hero, so the
           page is heading-navigable). Visual size set here, not by the UA default. */
        .pr-section-h {
            margin: 1.1rem 0 0.4rem; font-weight: 700; color: var(--ink-strong);
            font-size: 0.92rem; line-height: 1.3;
        }
        /* Spacing utilities (replace ad-hoc inline height divs). */
        .pr-sp-md { height: 1rem; }
        .pr-sp-sm { height: 0.6rem; }
        .pr-cap-flush { margin-top: 0; }

        @media (max-width: 640px) {
            .pr-grid { grid-template-columns: 1fr; }
        }

"""
