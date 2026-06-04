# Election Money — UI design (impeccable pass)

> The visual layer on top of `SIPO_POLITICAL_FINANCE_SHAPE_BRIEF.md`. Register:
> **product** (serves the task), executed in the project's **editorial-accountability**
> aesthetic (PRODUCT.md). Committed design choices; no code yet (greenfield, build
> gated on gold promotion). Light theme is forced by the register, not chosen.

## Concept: the public ledger
Not a dashboard. An **editorial money-ledger** — the newspaper "follow the money"
page. Receipts and account books, ink-on-paper. The discipline this buys us: the
**figures are the hero** (PRODUCT principle 1), party identity recedes to a quiet
mark, and there is zero room for partisan-colour theatre.

**Scene sentence (forces the theme):** *a journalist at an 11am desk checking which
party spent most on Meta ads, and a citizen on the bus seeing who funded their local
TD's party.* → light, legible, dense-but-calm. Dark mode would signal "cool tech" —
wrong register.

## Category-reflex check (both altitudes — done deliberately)
- **First-order reflex to reject:** "election money → red/green/blue party bars, flag
  motifs, a results-night dashboard." We do **not** colour the money by party.
- **Second-order reflex to reject:** "civic data tool that isn't partisan-colour →
  generic Guardian-grey bar charts." We push past with the **ledger framing +
  tabular-figure money columns + a single proportional spend-ribbon**, which reads as
  an editorial graphic, not a chart library default.
- **The move:** money is **ink** (one tinted near-black); a party is a **3px side-stripe**
  (the project's editorial signature, via `card_row`). Nothing on this page is filled
  in party colours.

## Color — Restrained (product floor), OKLCH
- Paper surface: `var(--surface)` oklch(94% 0.007 75); cards crisp `#ffffff` (the
  documented override — ink-on-paper).
- **Ledger ink** (all money, every party): `oklch(0.25 0.012 75)` — warm near-black,
  never `#000`. This is the single most important colour decision: money is one ink.
- Accent (existing app `--accent`): active lens, links, the back affordance — nothing else.
- **Do NOT** repurpose `--signal-good/bad` here. Spending more or less is not good or
  bad; colour-coding it would be inference. Money stays ink.
- Party colour appears ONLY as the side-stripe and a 8px swatch in the card title.

## Typography
- Existing app scale. **Money = `font-variant-numeric: tabular-nums`, weight 600**, the
  heaviest thing on a card → columns of € align vertically like an account book.
- Category labels: small-caps, muted ink, letter-spaced (`0.04em`) — the ledger's row labels.
- Donor / candidate names: body weight. Party name: card title. Provenance: caption, muted.

## Signature viz — the "where the money went" ledger (NOT a clustered bar chart)
Two densities of the same object:
- **On a party card (compact):** a single 4px **proportional ribbon** — the 8 headings
  as segments in a *monochrome ink ramp* (tints of the ledger ink, light→dark), widths
  ∝ spend. One hairline + label calls out **Meta €77,725**. Below it, the top 3 headings
  as `LABEL ······ €amount` ledger lines. No legend (the ribbon + 3 lines self-explain).
- **On party drill (full):** the complete 8-row ledger — `4A Advertising  ▇▇▇▇▇  €171,981`
  with `└ incl. Meta ads  €77,725` nested under Advertising; tabular € right-aligned; a
  rule under the last row then `Total €626,576`. This is the editorial budget table.

Why a ribbon + ledger list, not Altair clustered bars: bars-by-party invite "who's
biggest" comparison-as-judgment; a per-party ledger answers "where did *this* party's
money go" — factual, self-contained, on-register.

## Money-in / money-out symmetry
- Lens control = a **ledger tab**, styled `st.segmented_control`: `Spending  ·  Donations`.
  Same card grammar both sides; a hairline directional glyph (`↓ spent` / `↑ received`)
  in the card corner, ink not colour.
- Identical grid + totals_strip on both lenses → the symmetry is felt, not labelled.

## Provenance as editorial sourcing (the verify mark)
The OCR seam is a **journalistic source-mark**, not a warning:
`.em-source-mark` → small-caps, muted ink, inline at the figure: `VERIFY · SIPO p.12`.
Never red, never an alert. Over-cap and low-confidence rows get this same calm mark —
it reads as "we're showing our working", which builds trust (theyworkforyou tone). The
page footer carries the full `source_links` provenance to the SIPO collection + PDFs.

## Donor cards (the receipt)
Ledger rows, never a table: `donor name   ·   12 Mar 2024   ·   EFT   ·   €2,500` with
€ tabular, right-aligned; method as a small-caps tag. **No address, ever.** An over-cap
donation just appends the `VERIFY · SIPO p.N` mark inline. Nil-return parties say so in
words, not an empty grid.

## Member panel (member_overview, below payments)
Tight, two-line ledger, no chart:
```
ELECTION FINANCE · GE2024
Campaign   €1,130  of €16,524 assigned     ▸ Advertising ▸ Posters ▸ Publicity
Gave       €2,800 → Labour · monthly S/O
                                   VERIFY · SIPO p.N
```
Omitted entirely if the member was not a 2024 candidate (no empty box).

## Motion
Product restraint: 150–250ms, state only. Lens switch = instant content swap, no
choreography (Streamlit rerun + PRODUCT principle 5 "favour legibility over animation").
The ribbon does not animate.

## Bans honoured / overrides used
- Honoured: no gradient text, no glassmorphism, no hero-metric-with-gradient template,
  no modal (drill is a query-param view, inline), no pie charts, no party-colour fills.
- Used (project override): the **side-stripe** on party cards, via `card_row` only.

## New CSS classes (add to `shared_css.py`; OKLCH; `#ffffff` card bg)
- `.em-amount` — `font-variant-numeric: tabular-nums; font-weight:600; color: oklch(0.25 0.012 75)`
- `.em-ledger-row` — grid `[label] [bar] [amount]`, baseline-aligned
- `.em-cat-ribbon` / `.em-cat-seg` — the 4px proportional ribbon + monochrome-ramp segments
- `.em-cat-bar` — single 4px ink-tint bar for the ledger rows
- `.em-meta-mark` — the called-out Meta figure (hairline + small-caps label)
- `.em-source-mark` — small-caps muted `VERIFY · SIPO p.N`
- `.em-lens-tab` — segmented-control ledger styling
- (party stripe + swatch reuse `party_colour` / `card_row`; no new stripe code)

## Reuse (no rebuilds)
`hero_banner`, `totals_strip`/`stat_strip`, `card_row` (+ side-stripe), `party_colour`,
`rank_card_row`, `pill`, `empty_state`, `evidence_heading`, `field_label`,
`fmt_civic_date`, `back_button`, `source_links`, `export_controls`.

## Next step to *see* it
A static HTML mockup (one self-contained file, real OKLCH tokens + the ribbon + the
ledger + the verify mark) would make the design viewable in a browser before the
Streamlit page can be built. Offered, not yet built.
