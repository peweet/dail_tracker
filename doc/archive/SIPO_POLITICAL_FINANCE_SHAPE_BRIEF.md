# Election Money — design brief (shape)

> Design brief only — no code. Greenfield feature (no existing page). Companion to
> `doc/SIPO_POLITICAL_FINANCE_FEATURE_PLAN.md` (decisions locked). Reference idiom:
> the Payments page (`utility/pages_code/payments.py`) + the per-TD panel pattern.
> **Data-readiness gates flagged throughout — build is blocked on gold promotion +
> `v_sipo_*` views; this shapes the UX so it's ready when the data lands.**

Two surfaces:
- **A. Standalone "Election Money" page** (party-level) — `utility/pages_code/election_money.py`
- **B. "Election finance · GE2024" panel** on member_overview — `ui/election_finance_panel.py`

---

## 1. User question each surface answers
- **Page:** *"What did each party spend getting elected in 2024 — and on what (incl. digital/Meta ads) — and who donated to them?"*
- **Member panel:** *"What did this TD's 2024 campaign cost, and did they personally give money to their party?"*

## 2. Current UI problems
**Greenfield — no existing page.** Design from the data + the user question. The one
trap to avoid: this is OCR-derived accountability data about named private donors and
political money — the design must lead with *facts + provenance*, never framing.

## 3. Bold layout — Election Money page

```
hero_banner(kicker="General Election 2024 · 34th Dáil",
            title="Election Money",
            dek="What parties spent getting elected, and the donations they declared.
                 Figures read from official SIPO returns — verify against source.")

[ ▌Spending▐   Donations ]            ← st.segmented_control (lens = primary control)
GE2024                                ← static election label (year-pills deferred; one election)

— LENS: SPENDING ———————————————————————————————————————————————
totals_strip:  €1.46m total campaign spend · 9 parties · NNN candidates

  card_row of PARTY SPEND CARDS  (clickable → ?party=<id>)
  ┌─────────────────────────────┐
  │ ▌FF  Fianna Fáil          → │   party_colour stripe
  │ €626,576  campaign          │   big number
  │ Posters    ▇▇▇▇▇▇▇  €184k   │   top-3 category mini-bars (.em-cat-bar)
  │ Advertising▇▇▇▇▇▇   €172k   │
  │ Publicity  ▇▇▇▇▇    €161k   │
  │ ▸ Meta ads  €77,725  [chip] │   .em-meta-chip — the signal viz
  │ 78 candidates               │
  └─────────────────────────────┘

  PARTY DETAIL (drill ?party=ff):
   - full 8-category horizontal bar (Altair) "where FF's money went"
   - candidate spend ranking (rank_card_row): candidate · constituency · spent / assigned
     · verify pill on flagged rows.  Winners link to member page; losers are plain.

— LENS: DONATIONS ——————————————————————————————————————————————
totals_strip:  €161,578 declared (> €1,500) · 7 parties · 74 donations

  card_row of PARTY DONATION CARDS (received)  (clickable → ?party=<id>)
  ┌─────────────────────────────┐
  │ ▌SF  Sinn Féin            → │
  │ €65,599 received            │
  │ 29 donations                │
  └─────────────────────────────┘

  PARTY DETAIL (drill): donor cards — donor name · €amount · date · method.
   NO ADDRESS. over-cap/low-conf rows carry a "verify vs PDF" pill, never a verdict.

provenance footer (source_links): SIPO GE2024 collection + per-party return PDF/page.
OCR caveat caption.
```

## 3b. Bold layout — member panel (below the payments panel)

```
evidence_heading("Election finance · GE2024")        ← only if member was a 2024 candidate

  stat columns:   Campaign spend €1,130    Assigned €16,524
  category chips:  ▸ Advertising  ▸ Posters  ▸ Publicity     (pills)
  if donation given:  "Gave €2,800 to Labour · monthly standing order"
  caption: "Source: SIPO National-Agent return p.N · OCR-read, verify vs official PDF."
```

## 4. Interaction model
- **Page primary view:** the two-lens (Spending / Donations) party-card grids. Lens =
  `st.segmented_control` (the one control that matters; election is fixed at GE2024).
- **Detail view:** click a party card → `?party=<id>` query-param drill (category bar +
  candidate ranking for spending; donor cards for donations). `back_button` to grid.
- **Cross-link:** winning candidates in the spend ranking link to their member page
  (`?member=<code>`); the member panel links *back* to the party's Election Money detail.
- **Member panel:** read-only, no controls — it's a per-TD slice, like the payments panel.

## 5. Temporal behaviour
- **One election event (GE2024) → NO year pills yet.** Show a static "GE2024" label.
- Build the views with an `election_event` key so GE2020/GE2016 (data exists, out of
  current 2024 scope) can later turn the static label into election pills — mirrors the
  Payments year-pill idiom but keyed on election, not reporting_year. Contract
  `temporal.mode: election_event`.

## 6. Source-link behaviour
**Provenance is REQUIRED here, never omitted** — this is OCR-derived accountability
data. Footer via `ui/source_links.py` → SIPO GE2024 collection + the specific party
return PDF (+ page for a record). Every figure inherits a confidence/flag; flagged rows
render a `.em-verify-pill` ("verify vs PDF"). Donor records show name+amount+date only.

## 7. Chart & table strategy
- **Category breakdown** (the one real chart): horizontal **Altair bar**, one bar per
  heading (4A–4H), answers "where did the money go" — factual, no inference. Meta ads
  surfaced as a labelled sub-figure/chip, not a separate claim.
- **Everything else = cards, not `st.dataframe`** (primary-view rule): party cards,
  candidate rank cards, donor cards. A CSV export lives behind `export_controls` for the
  donor/candidate full sets (secondary).
- No pie charts, no trend lines (single election), no "share of total" framing that
  invites comparison-as-judgment.

## 8. Empty-state copy
- Spending, no parties yet: *"No election-expenses returns are loaded for this election yet."*
- Donations lens, a party with nil return: *"This party declared no donations above the €1,500 threshold for 2024."*
- Party detail, candidate list empty: *"No candidate-level spending is itemised for this party yet."*
- Member panel, not a 2024 candidate: panel **omitted entirely** (no empty box).
- Member panel, candidate but no personal donation: show campaign spend only — no "gave" line.
- Flagged-only data: *"Amounts for this party are still being verified against the official SIPO PDF."*

## 9. Visual differentiators
- **Money-in / money-out symmetry** — one page, two mirrored lenses; rare and legible.
- **The category mini-bar + Meta-ads chip** is the signature viz: it makes "digital ad
  spend" visible as a *fact* without a single inferential word.
- **Provenance as first-class** — the `verify vs PDF` pill is a feature, not an apology;
  it signals the honest seam between OCR and official record (trust-building, theyworkforyou-tone).
- Party colour stripes (`party_colour`/`party_stripe_html`) tie the grid to the rest of the app.

## 10. TODO_PIPELINE_VIEW_REQUIRED (build blockers — pipeline, not UI)
- `v_sipo_expenses_candidates` — candidate · constituency · assigned · spend · statutory_limit ·
  flag · confidence · **unique_member_code** (NULL for losing candidates) · election_event.
- `v_sipo_expense_categories` — party · section(4A–4H) · category · category_total · election_event.
- `v_sipo_expense_items` — party · ref · item · cost · category (needs **all 8 parties**; 6 pending).
- `v_sipo_donations` — party · donor_name · value · date · method · description · flag ·
  **(donor_address intentionally NOT exposed)** · election_event.
- `v_sipo_sources` — per-party return PDF URL + page, for both spend and donations.
- **Pipeline pre-work:** promote the three sandbox facts → gold; normalize party-name
  spellings across expenses/donations; join candidate_name + donor_name → member registry
  (`normalise_df_td_name` → `unique_member_code`); carry the confidence/flag through.

## 11. Implementation plan (when data lands)
**New files**
- `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/election_money.yaml`
  (mirror payments.yaml: `temporal.mode: election_event`, `approved_registered_views: [v_sipo_*]`,
  SELECT/WHERE/ORDER/LIMIT only, provenance shown).
- `utility/pages_code/election_money.py` — page (lens segmented_control + party grids + party drill).
- `utility/ui/election_finance_panel.py` — `render_member_election_finance(conn, join_key)`.
- `utility/data_access/election_money_data.py` — view reads only (logic firewall).
- `sql_views/v_sipo_*.sql` — pipeline-owned (above).

**Wire-in**
- `member_overview.py`: import + call `render_member_election_finance` **below** the payments panel.
- App nav/sidebar: add "Election Money" entry.

**CSS (add to `shared_css.py`)**
- `.em-party-card` (card_row variant), `.em-cat-bar` (mini horizontal bar), `.em-meta-chip`,
  `.em-donor-card`, `.em-verify-pill`. Backgrounds `#ffffff` (not `var(--surface)`).

**Reuse (don't rebuild)**
- `hero_banner`, `totals_strip`/`stat_strip`, `party_colour`/`party_stripe_html`, `card_row`,
  `rank_card_row`/`ranked_member_card`, `pill`, `empty_state`, `evidence_heading`, `field_label`,
  `fmt_civic_date`, `back_button`, `export_controls`, `source_links`.

**Sequence:** finish Part-4 (6 parties) → consolidate → promote gold + views → normalize/join →
build contract → build page + panel → review (logic firewall + no-inference + privacy).
