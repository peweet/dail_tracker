## Design Context

### Project
**Dáil Tracker** — a civic transparency tool surfacing Irish parliamentary data: TD attendance, declared member interests (business dealings, directorships, shareholdings), lobbying connections, payments & expenses, and bills sponsored. Data is extracted from Oireachtas APIs and PDFs. Open source, publicly deployable.

### Users
Two distinct audiences sharing the same interface:

**General public** — Irish citizens checking up on their local TD. Occasional visitors, low data literacy, browsing on mobile or a laptop during the day. They want a quick, legible answer: "how often does my TD show up?", "what business interests have they declared?", "who is lobbying them?". They should not need a manual.

**Journalists & researchers** — Power users cross-referencing attendance against lobbying connections, member interests against legislation sponsored. They need filterable tables and CSV export for every view. They'll find the source code on GitHub and may run the pipeline themselves.

**The member interests dataset is the most politically potent** — publicly declared business dealings, directorships, and shareholdings that could signal conflicts of interest. This should be prominently surfaced, not buried.

### Brand Personality
**Three words: Direct. Civic. Accountable.**

- Direct — no jargon, no softening, data is presented as evidence
- Civic — feels like a public resource, not a startup product or a government bureaucracy
- Accountable — makes absence of action visible, treats the data as a record

Emotional goal: a citizen or journalist should feel *informed and empowered*, not overwhelmed or suspicious of what's been hidden.

### Aesthetic Direction
**Editorial accountability journalism** — think investigative newspaper crossed with a data reference tool. Strong typographic hierarchy. High contrast. Data tables as the hero, not charts as decoration. Ink-on-paper restraint with one sharp accent colour.

**Theme: Light.** This is used during the day, in offices, on phones over lunch, by journalists at desks. Dark mode would signal "cool tech product" — wrong register for a civic accountability tool.

**Anti-references**: 
- Do NOT look like the Oireachtas website (grey, bureaucratic, 2005)
- Do NOT look like a fintech dashboard (gradient accents, hero metrics, glassmorphism)
- Do NOT look like a generic Streamlit app (the existing work is a wireframe only)

**References in spirit**:
- The Guardian's data journalism pieces — legible, authoritative, data as story
- ProPublica's data tools — civic, functional, trustworthy
- A well-designed parliamentary record — serious without being sterile

### Design Principles
1. **Data is the evidence** — tables and numbers are the primary design element, not decoration. Never obscure data with visual noise.
2. **Accessible by default, powerful on demand** — the page loads showing something useful to anyone; CSV export, advanced filters, and deep-dives are available but not in the way.
3. **Member interests first** — the declared business interests dataset is the most politically significant. Surface it prominently; it is the thing that distinguishes this tool from a simple attendance tracker.
4. **Every row tells a story** — empty states and zero values are data too. A TD with zero declared interests and 40% attendance is a story. Don't hide it.
5. **Cheap and deployable** — Streamlit is the right choice. Design decisions must work within Streamlit's constraints: no custom JavaScript, CSS theming via `st.markdown`, standard components. Favour legibility over animation.

### Intentional rule overrides
These are documented exceptions to generic design-system rules. They exist because they earn their place in this specific civic-accountability register.

- **Side-stripe accent bar on cards (`border-left: 2–5px solid var(--accent)` and variants).** This is the project's editorial signature, modelled on a newspaper pullquote rule. Every card is a piece of evidence, and the stripe marks it as such. Apply through `info_card` / `card_row` in `utility/ui/components.py`; do not introduce ad-hoc stripes elsewhere. Generic alerts/callouts must use full borders, not side-stripes.
- **Pure `#ffffff` for card and pill backgrounds.** `var(--surface)` is a warm beige (oklch 94% 0.007 75); cards need to read as crisp white "paper" against that surface to keep the ink-on-paper aesthetic. Use `#ffffff` deliberately at card backgrounds; everything else uses tokens.
- **Tailwind-style blue/orange semantic pairs (`#1d4ed8`/`#3b82f6` good, `#c2410c`/`#f97316` bad).** Now formalised as OKLCH `--signal-good*` / `--signal-bad*` tokens in `:root`. Use the tokens; the raw hex values are being migrated and should not spread.

### Deferred design debt
Items the audit flagged that should not be one-shot fixed:

- **Typography scale collapse.** 12 heading sizes between 1.5–2.4rem with ratios as flat as 1.03. Should consolidate to a 6-step scale (0.72 / 0.85 / 1.0 / 1.25 / 1.65 / 2.1rem) but only with page-by-page visual verification — collapsing blind will flatten established hierarchy on hero blocks, stat strips, and member-overview tabs.
- **CSS architecture split.** `inject_css()` injects a single ~122KB stylesheet on every render across 9 pages. Should split into a core layer (always loaded) and per-page chunks, or migrate static rules to an external `.css` file referenced by `.streamlit/config.toml`. Pure architectural work; defer until the page roster is stable.
