"""CSS fragment: Tab strip, Constituencies/Your Area, county performance cards, committee meeting-history timeline, Support page + app-level footer (.sup-*).

Mechanically split from the original utility/shared_css.py (lines 6081-6554 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Tab strip (st.tabs) ──────────────────────────────────────────────
           Streamlit's default tabs are a faint label + a thin sliding underline,
           which wash out against the warm-neutral page background — users were
           missing them entirely. Render the strip as a row of clear pill-buttons:
           inactive = light surface with readable secondary ink; active = filled
           brand accent so the current section is unmistakable. Applies app-wide
           (the cross-page top nav is st.navigation, not .stTabs, so it is
           unaffected). */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.3rem;
            border-bottom: 2px solid var(--border-strong) !important;
        }
        .stTabs [data-baseweb="tab"] {
            height: auto;
            padding: 0.45rem 1rem !important;
            border-radius: 8px 8px 0 0;
            background: var(--surface-deep);
            color: var(--text-secondary) !important;
            border: 1px solid var(--border-strong) !important;
            border-bottom: none !important;
            margin-bottom: -2px;            /* overlap the list's bottom border */
            transition: background 0.12s ease, color 0.12s ease;
        }
        .stTabs [data-baseweb="tab"] p {
            font-weight: 600 !important;
            font-size: 0.92rem !important;
        }
        .stTabs [data-baseweb="tab"]:hover {
            background: var(--accent-subtle);
            color: var(--accent) !important;
        }
        .stTabs [data-baseweb="tab"]:hover p { color: var(--accent) !important; }
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background: var(--accent);
            border-color: var(--accent) !important;
        }
        .stTabs [data-baseweb="tab"][aria-selected="true"] p {
            color: #ffffff !important;
            font-weight: 700 !important;
        }
        /* The filled active pill now carries selection state; neutralise the
           default sliding highlight bar (it mis-aligns under the padded pills)
           and keep the base border subtle. */
        .stTabs [data-baseweb="tab-highlight"] { background: transparent !important; }
        .stTabs [data-baseweb="tab-border"]    { background: transparent !important; }

        /* ── Constituencies page (Your Area) ───────────────────────────────── */
        .con-card-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
            gap: 0.6rem;
            margin: 0.4rem 0 0.6rem;
        }
        .con-card-inner { padding: 0.65rem 0.85rem; }
        .con-card-name {
            font-weight: 700; font-size: 1.02rem; color: var(--text-primary);
            line-height: 1.25;
        }
        .con-card-meta {
            margin-top: 0.25rem; font-size: 0.84rem; color: var(--text-secondary);
            font-variant-numeric: tabular-nums;
        }
        .con-card-sub {
            margin-top: 0.15rem; font-size: 0.76rem; color: var(--text-meta);
            font-variant-numeric: tabular-nums;
        }
        .con-hero {
            margin: 0.2rem 0 0.5rem; display: flex; gap: 1rem;
            justify-content: space-between; align-items: flex-start;
        }
        .con-hero-text { flex: 1 1 auto; min-width: 0; }
        .con-hero-map { flex: 0 0 auto; }
        .con-locator { width: 92px; height: auto; max-height: 150px; display: block; }
        .con-hero-title { margin: 0.1rem 0 0.2rem; font-size: 1.7rem; font-weight: 700; }
        .con-hero-meta {
            margin: 0; color: var(--text-secondary); font-size: 0.95rem;
            font-variant-numeric: tabular-nums;
        }
        .con-party-bar { margin: 0.2rem 0 1.1rem; }
        /* national choropleth (index "Compare every constituency") */
        .con-choro {
            display: flex; flex-direction: column; align-items: center;
            gap: 0.5rem; margin: 0.3rem 0 0.2rem;
        }
        /* Fixed size: image-map <area> coords are in image pixels and do NOT
           rescale with CSS, so the <img> must render at its natural size. */
        .con-choropleth { display: block; height: auto; cursor: pointer; }
        .con-choro-legend {
            display: flex; align-items: center; gap: 0.18rem;
            font-size: 0.74rem; color: var(--text-meta);
        }
        .con-choro-sw {
            width: 1.5rem; height: 0.6rem; display: inline-block; border-radius: 1px;
        }
        .con-choro-end { padding: 0 0.25rem; }
        .con-section-note {
            font-size: 0.86rem; color: var(--text-secondary); line-height: 1.55;
            margin: 0.1rem 0 0.7rem; max-width: 60rem;
        }
        .con-council-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 0.6rem; margin: 0.2rem 0 0.6rem;
        }
        .con-council-card {
            background: #ffffff; border: 1px solid rgba(0,0,0,0.08);
            border-left: 3px solid #8d6e63; border-radius: 6px;
            padding: 0.6rem 0.85rem; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        .con-council-card-partial { border-left-color: rgba(0,0,0,0.14); opacity: 0.9; }
        .con-council-name {
            font-weight: 700; font-size: 0.98rem; color: var(--text-primary);
            margin-bottom: 0.35rem;
        }
        .con-council-partial {
            font-size: 0.66rem; font-weight: 700; text-transform: uppercase;
            letter-spacing: 0.04em; color: var(--text-meta);
            background: rgba(0,0,0,0.05); border-radius: 3px; padding: 0.05rem 0.3rem;
        }
        .con-grain-row { display: flex; flex-wrap: wrap; gap: 0.35rem; }
        .con-grain {
            font-size: 0.78rem; border-radius: 4px; padding: 0.12rem 0.45rem;
            font-variant-numeric: tabular-nums; white-space: nowrap;
            /* Default chip surface so plain (unmodified) pills don't vanish into
               the warm near-white page background. Coloured variants below set
               their own background; the border stays subtle on those tints. */
            background: #ffffff; color: var(--text-secondary);
            border: 1px solid var(--border-strong);
        }
        .con-grain em { font-style: normal; color: var(--text-meta); }
        .con-grain-rev { background: #eef4f3; color: #2f5d57; }
        .con-grain-cap { background: #f0f7f0; color: #356b3a; }
        .con-grain-po  { background: #f5efe9; color: #7a5b43; }
        .con-grain-vac { background: #fdf0ed; color: #9a4a35; }
        .con-grain-price { background: #eef1f7; color: #3a4d73; }
        .con-grain-comp { background: #edf5ee; color: #356b3a; }
        .con-grain-ssha { background: #f0f3f8; color: #344b73; }
        .con-grain-wait { background: #f7f1ec; color: #7a4f2f; }
        .con-grain-perf { background: #f1f2f4; color: #3d4654; }
        /* rendered as <h3> for screen-reader heading nav; reset the default h3 box so it
           reads as a compact card label, not a section heading. */
        h3.hou-dim-title { font-weight: 600; font-size: 0.9rem; line-height: 1.3;
            margin: 0.2rem 0 0.4rem; padding: 0; color: var(--text-body); }
        h3.hou-chart-label { margin-top: 0.8rem; }
        .hou-lead { font-size: 1.02rem; line-height: 1.5; margin: 0.9rem 0 0.2rem;
            color: var(--text-body); max-width: 62ch; }
        .hou-crumb { font-size: 0.85rem; color: var(--text-meta); margin: 0 0 0.5rem; }
        .hou-crumb a { color: var(--text-meta); text-decoration: underline; }
        .con-council-note {
            margin-top: 0.4rem; font-size: 0.74rem; color: var(--text-meta);
            font-style: italic;
        }
        .con-council-empty { font-size: 0.82rem; color: var(--text-meta); }
        /* ── "Who runs your county" performance cards (clean stat rows, not chips) ── */
        .lg-perf-grid {
            display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 0.7rem; margin: 0.3rem 0 0.6rem;
        }
        .lg-card {
            background: #ffffff; border: 1px solid rgba(0,0,0,0.08);
            border-left: 3px solid #8d6e63; border-radius: 8px;
            padding: 0.8rem 1rem 0.6rem; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        .lg-card-title {
            font-weight: 700; font-size: 0.96rem; color: var(--text-primary); margin: 0 0 0.5rem;
        }
        .lg-metric {
            display: grid; grid-template-columns: 1fr auto; align-items: baseline;
            gap: 0.4rem 0.9rem; padding: 0.45rem 0; border-top: 1px solid rgba(0,0,0,0.06);
        }
        .lg-metric:first-of-type { border-top: none; }
        .lg-metric-main { min-width: 0; }
        .lg-metric-value {
            font-size: 1.3rem; font-weight: 700; font-variant-numeric: tabular-nums;
            color: var(--text-primary);
        }
        .lg-metric-label {
            display: block; font-size: 0.8rem; color: var(--text-secondary); line-height: 1.3;
            margin-top: 0.05rem;
        }
        .lg-metric-bench {
            font-size: 0.78rem; color: var(--text-meta); white-space: nowrap;
            text-align: right; font-variant-numeric: tabular-nums;
        }
        /* neutral position-vs-benchmark marker: ▲ above / ▼ below, no good/bad colour
           (firewall: no judgement implied; and the glyph already carries above/below,
           so good/bad must not live in colour alone — inaccessible under deuteranopia).
           NB: the old green/red .lg-arrow-up / .lg-arrow-down classes were removed — nothing
           emitted them after _bench went neutral, and they were latent good/bad styling. */
        .lg-arrow-neutral { color: var(--text-secondary); font-weight: 700; }
        /* per-metric source deep-link (-> exact NOAC report page) */
        .lg-metric-doc {
            font-size: 0.7rem; font-weight: 600; color: #8d4f24; text-decoration: none;
            white-space: nowrap; margin-left: 0.3rem;
        }
        .lg-metric-doc:hover { text-decoration: underline; }
        .lg-metric-doc:focus-visible { outline: 2px solid #8d4f24; outline-offset: 2px; border-radius: 2px; }
        /* "n/a — service provided regionally" note in the benchmark cell */
        .lg-na-note { font-size: 0.72rem; color: var(--text-meta); font-style: italic; }
        /* tiny 2022-2024 trend sparkline under the benchmark line — neutral (inherits a
           muted colour), no good/bad encoding; just the shape of the change. */
        .lg-spark { display: block; margin: 0.2rem 0 0 auto; color: var(--text-meta); opacity: 0.85; }
        .lg-badge {
            display: inline-block; font-size: 0.72rem; font-weight: 700; letter-spacing: 0.02em;
            color: #7a4f2f; background: #f7f1ec; border: 1px solid rgba(122,79,47,0.25);
            border-radius: 4px; padding: 0.1rem 0.4rem; margin-top: 0.3rem;
        }
        .lg-card-src { font-size: 0.74rem; color: var(--text-meta); margin-top: 0.55rem; }
        /* verbatim auditor opinion — set as a quiet pull-quote so it reads as the auditor's
           own words, not the site's editorial voice */
        .lg-audit-quote {
            font-size: 0.82rem; line-height: 1.45; color: var(--text-body);
            font-style: italic; margin-top: 0.55rem; padding-left: 0.7rem;
            border-left: 2px solid rgba(122,79,47,0.3);
        }
        /* council-money card -> Council Spending dossier drill-down (internal nav, not external) */
        .lg-card-cta {
            display: inline-block; margin-top: 0.6rem; font-size: 0.84rem; font-weight: 700;
            color: #8d4f24; text-decoration: none; border-bottom: 1px solid transparent;
        }
        .lg-card-cta:hover { border-bottom-color: #8d4f24; }
        .lg-card-cta:focus-visible { outline: 2px solid #8d4f24; outline-offset: 2px; border-radius: 2px; }
        /* council card -> by-division drill-down */
        .con-div-wrap { display: flex; flex-direction: column; gap: 1.1rem; margin: 0.3rem 0 0.4rem; }
        .con-div-head {
            font-weight: 700; font-size: 0.9rem; color: var(--text-secondary);
            margin-bottom: 0.45rem;
        }
        .con-div-row {
            display: grid; grid-template-columns: 13rem 1fr 6.5rem; align-items: center;
            gap: 0.65rem; padding: 0.16rem 0; font-size: 0.85rem;
        }
        .con-div-name {
            color: var(--text-primary); white-space: nowrap; overflow: hidden;
            text-overflow: ellipsis;
        }
        .con-div-track { background: #efeae0; border-radius: 3px; height: 0.72rem; overflow: hidden; }
        .con-div-bar { height: 100%; background: #8d6e63; border-radius: 3px; }
        .con-div-block:nth-child(2) .con-div-bar { background: #5a8f5e; }  /* capital lane = green */
        .con-div-val {
            text-align: right; font-variant-numeric: tabular-nums; color: var(--text-secondary);
        }
        @media (max-width: 640px) {
            .con-div-row { grid-template-columns: 8rem 1fr 5rem; font-size: 0.78rem; }
        }

        /* ── Committee meeting-history timeline (committees page) ───────── */
        .cmt-meeting-list { display: flex; flex-direction: column; gap: 0.6rem; margin: 0.2rem 0 0.4rem; }
        .cmt-meeting-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 3px solid var(--accent);
            border-radius: 2px;
            padding: 0.7rem 0.9rem;
        }
        .cmt-meeting-head {
            display: flex; align-items: baseline; justify-content: space-between;
            gap: 0.75rem; flex-wrap: wrap; margin-bottom: 0.35rem;
        }
        .cmt-meeting-date {
            font-weight: 700; color: var(--ink-strong);
            font-variant-numeric: tabular-nums; font-size: 0.95rem;
        }
        .cmt-meeting-transcript {
            font-size: 0.8rem; font-weight: 600; color: var(--accent);
            text-decoration: none; white-space: nowrap;
        }
        .cmt-meeting-transcript:hover { text-decoration: underline; }
        .cmt-topic-list { list-style: none; margin: 0 0 0.45rem; padding: 0; }
        .cmt-topic-list li {
            position: relative; padding-left: 0.85rem; margin: 0.12rem 0;
            font-size: 0.9rem; color: var(--ink-strong); line-height: 1.35;
        }
        .cmt-topic-list li::before {
            content: ""; position: absolute; left: 0; top: 0.52em;
            width: 4px; height: 4px; border-radius: 50%; background: var(--accent);
        }
        .cmt-meeting-witnesses { font-size: 0.82rem; color: var(--text-meta); line-height: 1.45; }
        .cmt-meeting-witnesses .cmt-w-label { font-weight: 600; color: var(--text-secondary); }
        .cmt-meeting-witnesses .cmt-w-none { font-style: italic; }

        /* ── .sup-*  SUPPORT page + app-level footer (utility/pages_code/support.py,
              builders in ui/components.py). Appended LAST on purpose: .sup-footer
              renders after pg.run() on every page, so it must win any equal-specificity
              collision with a page family above it. Defines no tokens of its own. */
        .sup-wrap { max-width: 62rem; margin: 0 auto; }
        /* One optical column: the hero's 4px stripe + 1.4rem gutter pushes its text
           right, so every block below is inset by the same amount. */
        .sup-costs, .sup-ask, .sup-help, .sup-honest { margin-left: calc(1.4rem + 4px); }

        .sup-hero { border-left: 4px solid var(--accent); padding: 0.15rem 0 0.15rem 1.4rem;
            margin: 0.4rem 0 2.2rem 0; }
        .sup-hero h1 {
            font-family: 'Zilla Slab', Georgia, serif; font-weight: 700; font-size: 2.45rem;
            line-height: 1.12; letter-spacing: -0.012em; color: var(--text-primary);
            margin: 0 0 0.55rem 0;
        }
        .sup-hero h1 em { font-style: normal; color: var(--accent); }
        .sup-hero p {
            font-family: 'Epilogue', sans-serif; font-size: 1.06rem; line-height: 1.6;
            color: var(--text-secondary); margin: 0; max-width: 46rem;
        }

        .sup-costs {
            display: grid; grid-template-columns: repeat(3, 1fr); gap: 0;
            border-top: 1px solid var(--border); border-bottom: 1px solid var(--border);
            margin-bottom: 2.2rem;
        }
        .sup-cost { padding: 1.15rem 1.4rem 1.2rem 0; }
        .sup-cost + .sup-cost { border-left: 1px solid var(--border); padding-left: 1.4rem; }
        .sup-cost-fig {
            font-family: 'Zilla Slab', Georgia, serif; font-variant-numeric: tabular-nums;
            font-weight: 700; font-size: 2.05rem; line-height: 1; color: var(--ink-strong);
            display: block;
        }
        .sup-cost-lab {
            font-family: 'Epilogue', sans-serif; font-size: 0.86rem; font-weight: 600;
            color: var(--text-primary); display: block; margin-top: 0.42rem;
        }
        .sup-cost-sub {
            font-family: 'Epilogue', sans-serif; font-size: 0.79rem; color: var(--ink-muted);
            display: block; margin-top: 0.15rem;
        }

        .sup-ask {
            background: var(--accent-subtle); border: 1px solid var(--accent-dim);
            border-radius: 10px; padding: 1.7rem 1.8rem 1.8rem; margin-bottom: 2.2rem;
        }
        .sup-ask h2 {
            font-family: 'Zilla Slab', Georgia, serif; font-weight: 600; font-size: 1.42rem;
            color: var(--text-primary); margin: 0 0 0.5rem 0;
        }
        .sup-ask p {
            font-family: 'Epilogue', sans-serif; font-size: 0.97rem; line-height: 1.62;
            color: var(--text-secondary); margin: 0 0 1.3rem 0; max-width: 42rem;
        }
        /* Material Symbols ligature; the font is already loaded by the @import above.
           Inherits `color`, unlike the ☕ emoji, which the OS paints in its own colours
           and which reads as a smudge at button size. */
        .sup-icon {
            font-family: 'Material Symbols Outlined'; font-feature-settings: 'liga';
            -webkit-font-feature-settings: 'liga'; font-size: 1.25rem; line-height: 1;
            font-weight: 400;
        }
        .sup-btn {
            display: inline-flex; align-items: center; gap: 0.6rem; background: var(--accent);
            color: #fff !important; font-family: 'Epilogue', sans-serif; font-weight: 600;
            font-size: 1.02rem; text-decoration: none !important; padding: 0.82rem 1.6rem;
            border-radius: 8px; border: 1px solid transparent;
            box-shadow: 0 1px 2px oklch(0% 0 0 / .10);
            transition: transform .12s ease, box-shadow .12s ease, background .12s ease;
        }
        .sup-btn:hover {
            background: var(--signal-bad-deep); box-shadow: 0 4px 14px oklch(0% 0 0 / .16);
            transform: translateY(-1px); color: #fff !important;
        }
        /* The gap belongs to this row: Streamlit's markdown container styles <p>
           margins with enough specificity to flatten a margin-top on the note. */
        .sup-btn-row { margin: 0 0 1.15rem 0; }
        .sup-btn-note {
            font-family: 'Epilogue', sans-serif; font-size: 0.8rem; color: var(--text-meta);
            margin: 0 !important; display: block;
        }

        .sup-help { margin-bottom: 2.4rem; }
        .sup-help > h2 {
            font-family: 'Zilla Slab', Georgia, serif; font-weight: 700; font-size: 1.62rem;
            color: var(--text-primary); margin: 0 0 0.45rem 0;
        }
        .sup-help > p.sup-help-intro {
            font-family: 'Epilogue', sans-serif; font-size: 0.97rem; line-height: 1.6;
            color: var(--text-secondary); margin: 0 0 1.35rem 0; max-width: 44rem;
        }
        .sup-routes { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
        .sup-route {
            border: 1px solid var(--border); border-left: 3px solid var(--accent);
            border-radius: 8px; background: #ffffff; padding: 1.25rem 1.35rem 1.35rem;
            display: flex; flex-direction: column;
        }
        .sup-route h3 {
            font-family: 'Zilla Slab', Georgia, serif; font-weight: 600; font-size: 1.1rem;
            color: var(--text-primary); margin: 0 0 0.4rem 0;
        }
        .sup-route p {
            font-family: 'Epilogue', sans-serif; font-size: 0.89rem; line-height: 1.55;
            color: var(--text-secondary); margin: 0 0 1.1rem 0; flex: 1 1 auto;
        }
        /* Ghost button: secondary to .sup-btn, which owns the filled accent.
           Two filled buttons on one page would compete. */
        .sup-btn-ghost {
            display: inline-flex; align-items: center; gap: 0.5rem; align-self: flex-start;
            font-family: 'Epilogue', sans-serif; font-weight: 600; font-size: 0.9rem;
            color: var(--accent) !important; text-decoration: none !important;
            padding: 0.55rem 1.05rem; border: 1px solid var(--accent-dim); border-radius: 7px;
            background: var(--accent-subtle);
            transition: border-color .12s ease, background .12s ease;
        }
        .sup-btn-ghost:hover {
            border-color: var(--accent); background: var(--accent-dim);
            color: var(--signal-bad-deep) !important;
        }
        /* Deliberately quieter than the two public routes — a public issue is better
           for the project and better for the reader than a private email. */
        .sup-private {
            margin: 1rem 0 0 0; padding: 0.95rem 1.15rem;
            border: 1px dashed var(--border-strong); border-radius: 8px;
            font-family: 'Epilogue', sans-serif; font-size: 0.87rem; line-height: 1.58;
            color: var(--text-secondary);
        }
        .sup-private strong { color: var(--text-primary); font-weight: 600; }
        .sup-private a {
            color: var(--accent); font-weight: 600; text-decoration: none;
            border-bottom: 1px solid var(--accent-dim);
        }
        .sup-private a:hover { border-bottom-color: var(--accent); }

        .sup-honest {
            border: 1px solid var(--border); border-left: 3px solid var(--ink-muted);
            border-radius: 8px; background: #ffffff; padding: 1.5rem 1.7rem 1.55rem;
            margin-bottom: 2.4rem;
        }
        .sup-honest h2 {
            font-family: 'Zilla Slab', Georgia, serif; font-weight: 600; font-size: 1.24rem;
            color: var(--text-primary); margin: 0 0 1rem 0;
        }
        .sup-honest ul { margin: 0; padding: 0; list-style: none; }
        .sup-honest li {
            font-family: 'Epilogue', sans-serif; font-size: 0.95rem; line-height: 1.55;
            color: var(--text-secondary); padding: 0 0 0 1.6rem; position: relative;
            margin-bottom: 0.72rem;
        }
        .sup-honest li:last-child { margin-bottom: 0; }
        .sup-honest li::before {
            content: "\\2014"; position: absolute; left: 0; top: 0;
            color: var(--accent); font-weight: 700;
        }
        .sup-honest li strong { color: var(--text-primary); font-weight: 600; }

        /* App-level footer — rendered after pg.run() in utility/app.py, so it appears
           on EVERY page. .sup-wrap keeps it on the same column as page content. */
        .sup-footer {
            border-top: 1px solid var(--border); margin: 3.2rem 0 0 0;
            padding: 1.15rem 0 1.6rem 0; display: flex; flex-wrap: wrap;
            align-items: baseline; gap: 0.55rem 1.5rem;
            font-family: 'Epilogue', sans-serif; font-size: 0.83rem; color: var(--text-meta);
        }
        .sup-footer-name { font-weight: 600; color: var(--text-secondary); }
        .sup-footer a {
            color: var(--text-secondary); text-decoration: none;
            border-bottom: 1px solid var(--border-strong);
        }
        .sup-footer a:hover { color: var(--accent); border-bottom-color: var(--accent); }
        .sup-footer-spacer { flex: 1 1 auto; }
        .sup-footer-coffee {
            display: inline-flex; align-items: center; gap: 0.4rem;
            color: var(--accent) !important; font-weight: 600;
            border-bottom: 1px solid var(--accent-dim) !important; white-space: nowrap;
        }
        .sup-footer-coffee:hover { border-bottom-color: var(--accent) !important; }

        @media (max-width: 640px) {
            .sup-hero h1 { font-size: 1.85rem; }
            /* Reclaim the optical inset — 26px of gutter is worth more than
               alignment on a 390px screen. */
            .sup-costs, .sup-ask, .sup-help, .sup-honest { margin-left: 0; }
            .sup-costs { grid-template-columns: 1fr; }
            .sup-cost + .sup-cost {
                border-left: none; border-top: 1px solid var(--border); padding-left: 0;
            }
            .sup-routes { grid-template-columns: 1fr; }
            .sup-footer-spacer { display: none; }
        }

        </style>
        """
