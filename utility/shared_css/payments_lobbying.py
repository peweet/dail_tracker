"""CSS fragment: Payments continued, data provenance box, attendance extras/year strip, Material Symbols, Lobbying page (.lob-*), Legislation page (.leg-*) start.

Mechanically split from the original utility/shared_css.py (lines 3434-3933 of that file, plus the shared boundary line the fragment inherits from the split). Do not reorder relative to the other shared_css/ fragments -- the cascade is order-dependent (equal specificity, last rule wins); see shared_css/__init__.py IMPORT_ORDER.

Plain (non-raw) triple-quoted string, matching the original -- two fragments (member_overview.py, constituencies_support.py) carry a real Python string escape that a raw string would change the value of.
"""

CSS = """        /* ── Data provenance box ────────────────────────────────────────
           Used when a callout needs a left accent border (source notes,
           per-year PDF links). Not the same as .dt-callout.           */
        .dt-provenance-box {
            background: var(--surface);
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 2px;
            padding: 0.9rem 1rem;
        }

        /* ── Attendance: extra heading variants ──────────────────────── */
        .att-hall-subheading { font-size: 0.75rem; color: #6b7280; margin: 0 0 0.75rem; }
        .att-cop-head-good { font-size: 0.68rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase; color: var(--blue-700); border-bottom: 3px solid var(--blue-500); padding-bottom: 0.3rem; margin: 0 0 0.6rem; }
        .att-cop-head-bad  { font-size: 0.68rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase; color: var(--orange-700); border-bottom: 3px solid var(--orange-500); padding-bottom: 0.3rem; margin: 0 0 0.6rem; }

        /* ── Attendance overview: year summary strip ─────────────────── */
        .att-ov-year-strip {
            display: flex; flex-wrap: nowrap; overflow-x: auto;
            gap: 0.5rem; padding: 0.6rem 0 0.9rem;
        }
        .att-ov-year-card {
            flex-shrink: 0;
            display: flex; flex-direction: column; align-items: center;
            padding: 0.5rem 0.9rem; border-radius: 10px;
            background: #ffffff; border: 1px solid var(--border);
            min-width: 6.5rem; text-align: center;
            transition: border-color 0.12s;
        }
        .att-ov-year-card-active {
            background: #f0fdf4; border: 1.5px solid #16a34a;
        }
        .att-ov-year-num {
            font-size: 1.05rem; font-weight: 800; color: var(--text-primary);
            letter-spacing: -0.02em; line-height: 1.2;
        }
        .att-ov-year-card-active .att-ov-year-num { color: #15803d; }
        .att-ov-year-members {
            font-size: 0.68rem; font-weight: 600; color: var(--text-meta);
            margin-top: 0.15rem; white-space: nowrap;
        }
        .att-ov-year-days {
            font-size: 0.63rem; color: var(--text-meta);
            margin-top: 0.05rem; white-space: nowrap;
        }

        /* ── Material Symbols Outlined (used by lobbying path cards) ────── */
        .material-symbols-outlined {
            font-family: 'Material Symbols Outlined';
            font-weight: normal;
            font-style: normal;
            display: inline-block;
            font-variation-settings: 'FILL' 0, 'wght' 300, 'GRAD' 0, 'opsz' 24;
            user-select: none;
        }

        /* ── Lobbying page ───────────────────────────────────────────────
           Navy (#0f3d5e) is deliberate — the lobbying page has a navy/rust
           palette distinct from the amber accent used on other pages.   */
        .lob-section-heading { border-bottom-color: #0f3d5e; }

        /* Attached references — lobbyist-supplied external PDFs (chambers.ie,
           amcham.ie, etc.). Rust accent + EXTERNAL tag signals these are not
           Oireachtas-issued and may rot. */
        .lob-attach-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 0.6rem; margin-top: 0.45rem;
        }
        .lob-attach-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-left: 3px solid var(--orange-900);   /* rust to distinguish from navy/amber */
            border-radius: 8px;
            padding: 0.65rem 0.85rem 0.7rem;
        }
        .lob-attach-head {
            display: flex; align-items: center; gap: 0.4rem; margin-bottom: 0.3rem;
        }
        .lob-attach-host {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.84rem; font-weight: 700; color: var(--text);
            letter-spacing: -0.005em;
        }
        .lob-attach-tag {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.62rem; font-weight: 700; letter-spacing: 0.08em;
            color: var(--orange-900); background: var(--orange-050);
            border: 1px solid #fed7aa; border-radius: 3px;
            padding: 0.05rem 0.35rem;
        }
        .lob-attach-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem; color: var(--text-meta);
            line-height: 1.4; margin-bottom: 0.45rem;
        }
        .lob-attach-actions {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.82rem; font-weight: 600;
            display: flex; align-items: center; gap: 0.35rem;
        }
        .lob-attach-sep { color: var(--text-meta); }

        .lob-path-card {
            background: #ffffff;
            border: 1px solid var(--border);
            border-top: 4px solid #0f3d5e;
            border-radius: 12px;
            padding: 0.75rem 1rem 0.75rem;
            box-shadow: 0 1px 2px rgba(17,24,39,0.06), 0 8px 24px rgba(17,24,39,0.04);
            min-height: 175px;
            transition: border-top-color 0.15s, box-shadow 0.15s;
        }
        .lob-path-card:hover { border-top-color: var(--orange-900); box-shadow: 0 4px 16px rgba(17,24,39,0.1); }
        .lob-path-icon { font-size: 1.6rem; line-height: 1; margin-bottom: 0.55rem; }
        .lob-path-heading { margin: 0 0 0.3rem; font-size: 1.05rem; font-weight: 700; color: var(--text-primary); letter-spacing: -0.01em; }
        .lob-path-body { margin: 0 0 0.65rem; font-size: 0.82rem; color: var(--text-meta); line-height: 1.5; }
        .lob-path-stat { display: flex; align-items: baseline; gap: 0.3rem; }
        .lob-path-stat-num { font-size: 1.3rem; font-weight: 800; color: #0f3d5e; letter-spacing: -0.03em; }
        .lob-path-stat-lbl { font-size: 0.73rem; font-weight: 600; color: var(--text-meta); text-transform: uppercase; letter-spacing: 0.04em; }

        /* Topics rail — visually distinct from path cards (rust accent + dashed
           border) to signal "this is a free-text scan, not a register taxonomy". */
        .lob-topic-caveat {
            font-size: 0.83rem;
            color: var(--text-meta);
            line-height: 1.55;
            margin: 0 0 0.65rem;
            padding: 0.55rem 0.75rem;
            background: var(--orange-050);
            border-left: 3px solid var(--orange-700);
            border-radius: 0 8px 8px 0;
        }
        .lob-topic-caveat em { color: #0f3d5e; font-style: normal; font-weight: 600; }
        .lob-topic-card {
            background: #ffffff;
            border: 1px dashed var(--orange-700);
            border-top: 4px solid var(--orange-700);
            border-radius: 12px;
            padding: 0.75rem 1rem 0.75rem;
            box-shadow: 0 1px 2px rgba(17,24,39,0.06);
            min-height: 145px;
        }
        .lob-topic-icon { font-size: 1.6rem; line-height: 1; margin-bottom: 0.55rem; color: var(--orange-900); }
        .lob-topic-heading { margin: 0 0 0.3rem; font-size: 1.05rem; font-weight: 700; color: var(--text-primary); letter-spacing: -0.01em; }
        .lob-topic-body { margin: 0; font-size: 0.82rem; color: var(--text-meta); line-height: 1.5; }

        .lob-topic-banner {
            background: var(--orange-050);
            border: 1px solid #fed7aa;
            border-left: 5px solid var(--orange-700);
            border-radius: 12px;
            padding: 0.85rem 1.1rem;
            margin: 0.85rem 0;
        }
        .lob-topic-banner-heading {
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--orange-900);
            margin: 0 0 0.35rem;
        }
        .lob-topic-banner-body {
            font-size: 0.85rem;
            color: #7c2d12;
            line-height: 1.55;
            margin: 0;
        }
        .lob-topic-banner-body em { color: #0f3d5e; font-style: normal; font-weight: 600; }
        .lob-topic-keyword-row {
            display: flex; flex-wrap: wrap; gap: 0.35rem;
            margin: 0.2rem 0 0;
        }
        .lob-topic-keyword-pill {
            background: #ffffff;
            border: 1px solid #fed7aa;
            color: #7c2d12;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.15rem 0.55rem;
            border-radius: 999px;
            font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
        }

        .lob-topic-filter-banner {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 0.6rem;
            background: var(--orange-050);
            border: 1px solid #fed7aa;
            border-left: 5px solid var(--orange-700);
            border-radius: 10px;
            padding: 0.65rem 0.95rem;
            margin: 0.85rem 0 0.55rem;
            color: #7c2d12;
            font-size: 0.88rem;
        }
        .lob-topic-filter-label {
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--orange-900);
        }
        .lob-topic-filter-clear {
            margin-left: auto;
            background: #ffffff;
            border: 1px solid var(--orange-700);
            color: var(--orange-900) !important;
            font-size: 0.78rem;
            font-weight: 700;
            text-decoration: none;
            padding: 0.3rem 0.75rem;
            border-radius: 999px;
        }
        .lob-topic-filter-clear:hover {
            background: var(--orange-700);
            color: #ffffff !important;
        }

        .lob-revolving-callout {
            background: #fffbeb;
            border: 1px solid #fcd34d;
            border-left: 5px solid #d97706;
            border-radius: 12px;
            padding: 0.85rem 1.1rem;
            margin: 0.85rem 0;
        }
        .lob-revolving-heading { font-size: 0.72rem; font-weight: 800; letter-spacing: 0.08em; text-transform: uppercase; color: #92400e; margin-bottom: 0.3rem; }
        .lob-revolving-headline {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.15rem;
            font-weight: 700;
            color: #78350f;
            line-height: 1.35;
            margin: 0.1rem 0 0.35rem;
        }
        .lob-revolving-explain {
            font-size: 0.83rem;
            color: #78350f;
            line-height: 1.5;
            margin: 0 0 0.65rem;
        }
        .lob-revolving-list { margin: 0.55rem 0 0.45rem; border-top: 1px solid rgba(217,119,6,0.25); }
        .lob-revolving-row {
            display: flex; align-items: baseline; gap: 0.6rem;
            padding: 0.4rem 0;
            border-bottom: 1px solid rgba(217,119,6,0.18);
            font-size: 0.86rem;
        }
        .lob-revolving-row-rank {
            font-size: 0.7rem; font-weight: 800; color: #92400e;
            letter-spacing: 0.05em; min-width: 1.5rem;
        }
        .lob-revolving-row-name { flex: 1; font-weight: 700; color: #1f2937; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .lob-revolving-row-meta { color: #78350f; font-size: 0.78rem; white-space: nowrap; }

        /* Stage 2a prominent-cases sub-callout — sits inside the RD index
           hero zone to flag the highest-impact individuals. */
        .lob-rd-prominent {
            background: var(--orange-050);
            border: 1px solid var(--orange-300);
            border-left: 5px solid var(--orange-700);
            border-radius: 10px;
            padding: 0.7rem 1rem;
            margin: 0.5rem 0 1.1rem;
        }
        .lob-rd-prominent-heading { font-size: 0.7rem; font-weight: 800; letter-spacing: 0.09em; text-transform: uppercase; color: var(--orange-900); margin-bottom: 0.4rem; }
        .lob-rd-prominent-grid { display: flex; flex-wrap: wrap; gap: 0.55rem; }
        .lob-rd-prominent-pill {
            background: #ffffff; border: 1px solid var(--orange-300); border-radius: 999px;
            padding: 0.3rem 0.75rem; font-size: 0.82rem; color: #1f2937;
            display: inline-flex; align-items: baseline; gap: 0.4rem;
        }
        .lob-rd-prominent-pill strong { color: var(--orange-900); font-weight: 700; }

        .lob-activity-row { display: flex; align-items: flex-start; gap: 0.75rem; padding: 0.65rem 0; border-bottom: 1px solid var(--border); }
        .lob-activity-period { font-size: 0.73rem; font-weight: 700; color: #0f3d5e; white-space: nowrap; min-width: 5rem; padding-top: 0.1rem; }
        .lob-activity-body { flex: 1; min-width: 0; }
        .lob-activity-org { font-size: 0.88rem; font-weight: 700; color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .lob-activity-area { font-size: 0.75rem; color: var(--text-meta); margin-top: 0.1rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

        .lob-sidebar-label { font-size: 0.7rem; font-weight: 700; letter-spacing: 0.07em; text-transform: uppercase; color: var(--text-meta); margin: 0 0 0.4rem; }

        .lob-policy-pill {
            display: inline-flex; align-items: center; gap: 0.3rem;
            background: #ffffff; border: 1px solid var(--border);
            border-radius: 999px; padding: 0.2rem 0.7rem;
            font-size: 0.78rem; font-weight: 600; color: var(--text-meta);
            cursor: pointer; transition: border-color 0.12s, color 0.12s;
        }
        .lob-policy-pill:hover { border-color: #0f3d5e; color: #0f3d5e; }

        /* ── Legislation page ────────────────────────────────────────── */
        /* Status badge variants — extend the .signal base class */
        .leg-status-enacted  { background:#dcfce7; color:#15803d; border:1px solid #86efac; }
        .leg-status-active   { background:var(--accent-subtle); color:var(--accent); border:1px solid var(--accent-dim); }
        .leg-status-lapsed   { background:var(--surface); color:var(--text-meta); border:1px solid var(--border); }
        .leg-status-withdrawn { background:#fff1f2; color:#9f1239; border:1px solid #fda4af; }

        /* Stage timeline list */
        .leg-stage-list { display:flex; flex-direction:column; }
        .leg-stage-row {
            display: flex; gap: 1rem; padding: 0.55rem 0;
            border-bottom: 1px solid var(--border); align-items: baseline;
        }
        .leg-stage-row:last-child { border-bottom: none; }
        .leg-stage-num {
            font-size: 0.68rem; font-weight: 800; color: var(--text-meta);
            min-width: 1.6rem; text-align: right; flex-shrink: 0;
        }
        .leg-stage-label {
            font-size: 0.88rem; font-weight: 600; color: var(--text-primary); flex: 1;
        }
        .leg-stage-date { font-size: 0.78rem; color: var(--text-meta); white-space: nowrap; }
        .leg-stage-current .leg-stage-label { color: var(--accent); }
        .leg-stage-current .leg-stage-num   { color: var(--accent); }

        /* Bill identity strip in drilldown view */
        .leg-bill-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.65rem; font-weight: 700; color: var(--text-primary);
            line-height: 1.2; margin: 0.5rem 0 0.2rem;
        }
        .leg-bill-ref {
            font-size: 0.8rem; color: var(--text-meta); margin-bottom: 0.5rem;
        }
        .leg-bill-identity {
            padding: 0.75rem 0 0.5rem 0;
        }
        .leg-bill-badges {
            display: flex; gap: 0.4rem; align-items: center;
            flex-wrap: wrap; margin-bottom: 0.5rem;
        }
        .leg-hero-h2 {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.85rem; font-weight: 700; margin: 0.2rem 0 0.4rem;
            letter-spacing: -0.02em;
        }
        .leg-stage-chamber {
            font-weight: 400; color: var(--text-meta); font-size: 0.78rem;
        }
        .leg-long-title {
            font-size: 0.88rem; line-height: 1.6; color: var(--text-secondary);
        }
        .leg-stage-group {
            font-size: 0.65rem; font-weight: 800; letter-spacing: 0.09em;
            text-transform: uppercase; color: var(--accent);
            padding: 0.7rem 0 0.25rem; margin-top: 0.2rem;
            border-top: 1px solid var(--border);
        }
        .leg-stage-group:first-child { border-top: none; padding-top: 0.1rem; }

        /* Amendment-activity badge under the stage timeline (contestation proxy) */
        .leg-amend-badge {
            display: inline-flex; align-items: baseline; gap: 0.45rem;
            margin-top: 0.9rem; padding: 0.35rem 0.7rem;
            background: var(--accent-subtle); border-radius: 6px;
        }
        .leg-amend-count {
            font-size: 1.15rem; font-weight: 800; color: var(--accent);
            line-height: 1;
        }
        .leg-amend-label {
            font-size: 0.82rem; font-weight: 600; color: var(--text-primary);
        }
        .leg-amend-breakdown {
            font-size: 0.78rem; color: var(--text-meta); margin-top: 0.35rem;
        }

        /* Oireachtas link in bill identity strip */
        .leg-bill-oireachtas-link {
            display: inline-block;
            margin-top: 0.55rem;
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--accent);
            text-decoration: none;
        }
        .leg-bill-oireachtas-link:hover { text-decoration: underline; }

        /* Source link card */
        .leg-source-card {
            background: #ffffff; border: 1px solid var(--border);
            border-left: 4px solid var(--accent); border-radius: 2px;
            padding: 0.8rem 1rem; margin-bottom: 0.6rem;
        }
        .leg-source-label {
            font-size: 0.82rem; font-weight: 600;
            color: var(--text-primary); margin-bottom: 0.25rem;
        }
        .leg-source-link {
            font-size: 0.85rem; font-weight: 600;
            color: var(--accent); text-decoration: none;
        }
        .leg-source-link:hover { text-decoration: underline; }
        .leg-source-meta {
            font-size: 0.68rem; font-weight: 600; letter-spacing: 0.04em;
            color: var(--text-meta); text-transform: none;
        }

        /* ── Legislation: documents (versions / memos / amendments) ───── */
        .leg-doc-section { display: flex; flex-direction: column; gap: 0.35rem; }
        .leg-doc-group-label {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.95rem; font-weight: 700;
            color: var(--text); margin: 0.6rem 0 0.25rem;
        }
        .leg-doc-group-label:first-child { margin-top: 0; }
        .leg-doc-group-count {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.78rem; font-weight: 600;
            color: var(--text-meta); margin-left: 0.2rem;
        }

        /* ── Legislation: bill card list ─────────────────────────────── */
        .leg-bill-card {
            display: inline-flex;
            flex-direction: column;
            padding: 0.45rem 0.9rem;
            border: 1px solid var(--border);
            border-left: 3px solid var(--border-strong);
            border-radius: 12px;
            background: #ffffff;
            /* Uniform card width — sized to roughly match the phase
               segmented control row (All / Dáil / Seanad / Enacted),
               extending only modestly past it. */
            width: 600px;
            max-width: 100%;
            transition: border-left-color 0.12s, border-color 0.12s;
        }
        .leg-bill-card:hover {
            border-left-color: var(--accent);
            border-color: var(--accent-dim);
        }
        .leg-bill-card-header {
            display: flex;
            align-items: center;
            gap: 0.55rem;
            margin-bottom: 0.28rem;
        }
        .leg-bill-card-date {
            font-size: 0.73rem;
            color: var(--text-meta);
            white-space: nowrap;
        }
        .leg-bill-card-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 0.97rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.35;
            margin-bottom: 0.25rem;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .leg-bill-card-footer {
            display: flex;
            align-items: baseline;
            gap: 1.2rem;
        }
        .leg-bill-card-meta {
            font-size: 0.75rem;
            color: var(--text-meta);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .leg-bill-card-link {
            font-size: 0.73rem;
            font-weight: 600;
            color: var(--accent);
            text-decoration: none;
            white-space: nowrap;
            flex-shrink: 0;
        }
        .leg-bill-card-link:hover { text-decoration: underline; }

        /* Card row — card shrinks to fit its title, button sits immediately
           after. Vertical centering of the nav_button is handled by the
           reusable .dt-nav-btn rules above (no per-card override needed). */
        [data-testid="stHorizontalBlock"]:has(.leg-bill-card) {
            width: fit-content !important;
            max-width: 100%;
            gap: 0.4rem !important;
            margin-bottom: 0.3rem !important;
            justify-content: flex-start !important;
        }
        [data-testid="stHorizontalBlock"]:has(.leg-bill-card)
            > [data-testid="stColumn"] {
            flex: 0 0 auto !important;
            width: auto !important;
            min-width: 0 !important;
        }

"""
