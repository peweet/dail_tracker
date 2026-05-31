# Corporate page — feature plan (LOCKED via impeccable shape 2026-05-31)

Status: design brief LOCKED, ETL prerequisites done, awaiting go/no-go before build
Companion docs: [receiver_appointers_feature.md](receiver_appointers_feature.md) (will be absorbed into this page), [public_appointments_feature.md](public_appointments_feature.md) (live), [company_register_notices_feature.md](company_register_notices_feature.md) (superseded by this)
Supersedes: the parked broader business-gazette idea

## User question this page answers

> "Who's calling in Irish loans, who's being rescued, who's winding down — and what does Iris show about a specific named company?"

Two complementary audiences:
- **Recognition reader** (journalist, researcher): scan a ranked view of who's been most active appointing receivers, with brand-to-parent translation (Promontoria → Cerberus, Beltany → Goldman Sachs).
- **Specific-question reader** (anyone with a company in mind): search for a named company and see every Iris notice for it.

## Scope (agreed 2026-05-31)

IN: corporate_insolvency · corporate_notice · corporate_rescue (examinership + SCARP) · investment_vehicle_register_notice (ICAV strike-offs). ~35,894 notices after the personal-insolvency exclusion below.

OUT: **all personal insolvency** (the `bankruptcy` category, ~3,841 rows, plus the ~277 personal-bankruptcy-wording leaks into other buckets). Privacy rule — see [[feedback_personal_insolvency_privacy]].

## ETL prerequisites — done 2026-05-31

Before this brief was locked, four ETL improvements landed that directly affect this page:

- **Liquidator-firm leak fixed**: PKF / Friel Stafford / Hughes Blake / Baker Tilly / Grant Thornton / Mazars / Deloitte etc. no longer win the `entity_name` slot on MVL signature blocks. PKF-as-entity rows: 86 → 3.
- **`display_title` column added (A4)**: a clean first-meaningful-line headline for non-SI notices, replacing the bled "title1 | title2 | title3 | title4" string for card display. 100% fill on non-SI. The Corporate page reads `display_title` for card headlines; `title` is kept as raw provenance.
- **Section 181 subtype tag** (`si_section_181_state_development`, 20 rows) and **Foreshore Act tag** (`si_foreshore_act_decision`, 67 rows) — independent of this page, but unblocked the SI page "planning lens" follow-up.
- **Refresh orchestrator** (`iris_refresh.py`) now chains: poller → silver delta → SI gold → bill-SI gold → public-appointments gold. The fixes above are propagated end-to-end.

## Honest data caveats this page lives with

- **Receiver-appointer brand tagging covers 31% of receiverships.** 812 of 2,624 receivership notices name a known major loan-book buyer or Irish bank; the other 1,812 are appointed by individuals, smaller institutions, or under private debentures where no major fund is named (or the brand sits in the long tail beyond our starter map). The page must say this plainly.
- **`entity_name` is 75% clean on corporate scope.** 24% is title-fallback junk on record-split fragments where the company name simply isn't in that row — bounded by upstream record-splitting, not by extraction.
- **Per-company search may miss body-text-only mentions.** Search is on `entity_name` (primary) with `raw_text` fallback (secondary, flagged) — disclosed in editorial copy.

## Confirmed design decisions

- **Lead/centrepiece**: receiver-appointers ranking (brand → parent fund), the recognition story.
- **Brand-translation source**: curated `data/_meta/loan_book_fund_aliases.csv` (~25 starter entries, extensible). Deterministic, auditable.
- **Drill-in**: per-company search using `entity_name` + body-text fallback.

## Layout

```
HERO (editorial)
  Title + dek + explicit caveat:
  "Personal insolvency is excluded by policy. Corporate notices only."

FEATURED · who's calling in Irish loans (the recognition story)
  Subhead: "Of 2,624 receivership notices, 812 (31%) name a known
  loan-book buyer or Irish bank. The rest were appointed by smaller
  institutions or under private debentures."
  Ranked appointing parties (brand → parent fund), brown bar chart.
  Slim year sparkline of the receiver wave (peaks 2016/17, dip 2020,
  climbing back). Clickable to filter the whole page by year.

SEARCH BAR · find a named company
  "Search by company name…" — searches entity_name (primary) +
  raw_text (secondary, results-with-asterisk).
  Renders a per-company summary panel: every Iris notice for that
  company, chronological, typed.

SECTIONED FEED (the recent record)
  Tabs: All · Receiverships · Examinership · Liquidations · ICAV
  Cards: date · company (display_title fallback when entity_name junk) ·
  notice type pill · appointing/acting party (where applicable) ·
  Iris source link. Side-stripe evidence card pattern.
  Month-grouped dividers, paginated.

ACTIVE-FILTER CHIPS · search box / fund filter / type filter / year
DETAIL VIEW · selected notice — full record, original text, Iris source
CSV EXPORT · filtered set
```

## Design direction

- **Color**: Restrained (PRODUCT default + product register). One sharp accent for the receiver-appointer signal; subtle neutral pills for notice-type differentiation. No gradient bling, no hero-metric block.
- **Theme**: Light (PRODUCT-pinned). Scene: *"A finance journalist at a bright newsroom desk searching 'Promontoria' to see what they've done; a researcher tracing a fund's footprint across a decade."*
- **Anchor references**: The Guardian's data-journalism finance pieces, Cantillon-style ink-on-paper finance reporting, a well-set corporate register. Not Bloomberg-terminal density, not fintech.
- Sanctioned project signature: side-stripe evidence cards via `ui/components.py`, `#ffffff` paper, `--signal-*` tokens.

## Key states

- *Default*: featured receiver-appointer panel + recent feed across all types.
- *Filtered by fund*: page scopes to one appointing party (from clicking a row in the featured panel or `?fund=…`).
- *Company-search results*: per-company summary view, chronological notice list for that named company. Disclaimer about body-text-only misses.
- *Empty*: civic-voice "No notices match" with widen suggestion.
- *Partial 2026*: YTD tag.
- *Entity-name junk fallback*: card shows `display_title` if `entity_name` matches junk patterns; raw `entity_name` is hidden in those cases.
- *No brand mapping*: long-tail SPV brand displays as-is with quiet "parent fund not yet mapped" caption.

## Interaction model

Standard SI/PA pattern. Facet pills → URL params/session state → rerun. Click a ranked row in the featured panel → `?fund=Cerberus` filters the page. Click a card → `?ref=` opens detail. Search → on-the-fly filter. CSV download of filtered set. Per-company drill-in → `?company=ACME`. `?clear=<key>` handler for chip-clear.

## Content requirements

- Hero title + dek + explicit privacy line.
- Editorial intro paragraph on the receiver-appointer pattern (factual, recognition framing, no allegation).
- Featured-panel kicker + heading + the **honest 31% subhead**.
- Sub-type labels (Receivership / Examinership / Members' Voluntary Liquidation / Creditors' Voluntary Liquidation / Court Winding-Up / ICAV strike-off).
- Per-company-search disclaimer about body-text-only misses.
- Brand-translation provenance line: "Brand-to-parent mapping is curated in data/_meta/; long-tail brands display as-is."
- Civic empty-state copy.
- No em dashes.

## Build chain (logic firewall)

```
data/_meta/loan_book_fund_aliases.csv      ← curated brand → parent
pipeline_sandbox/corporate_notices_enrichment.py
  - read iris_notice_events_clean.csv, filter the 4 corporate categories
  - exclude personal-insolvency wording (~277 rows)
  - tag brand_mentions + parent_fund_mentions by joining text against alias CSV
  - emit `coverage_summary` to data/_meta/corporate_notices_coverage.json (A5 baked in)
  - write data/gold/parquet/corporate_notices.parquet
sql_views/corporate_corporate_notices.sql
  CREATE VIEW v_corporate_notices AS SELECT … FROM read_parquet(…)
  ORDER BY issue_date DESC NULLS LAST
utility/data_access/corporate_data.py
  @st.cache_data fetch_corporate_notices() → pd.DataFrame
utility/pages_code/corporate.py            ← display-only
utility/app.py                              ← register page
```

Page does only display-only aggregation on the loaded frame (ranking by parent fund, year trend, search). All business semantics (brand-join, personal-insolvency exclusion) live upstream of the view.

## Resolved defaults (asserting, not asking)

- A5 coverage gate baked into the corporate enrichment: emit a small `_meta/corporate_notices_coverage.json` with category counts, exclusion counts, brand-tag coverage %, junk rate, last-refreshed timestamp. Mirrors the SI taxonomy pattern.
- iris_refresh.py orchestrator extended to also chain corporate enrichment.
- Tightened pre-launch: refresh chain re-run after the build to confirm everything still mutually consistent.

## Recommended impeccable references for build

`layout.md` (featured + sectioned rhythm), `typeset.md` (editorial hierarchy), `clarify.md` (privacy + caveat copy); reuse `ui/components.py` and `shared_css.py`.

## Pre-build risks worth naming honestly

- **Brand-map curation** is small but editorial. The starter list is mainstream public-record content; long-tail SPV names will surface over time and need adding.
- **Per-company search reliability** is bounded by the entity_name junk tail (24% of corporate rows). The body-text fallback widens reach but isn't perfect — needs the on-page disclaimer to stay honest.
- **Audience overlap with citizens-checking-their-TD is low.** This page tilts journalist/researcher/finance-professional. PRODUCT.md tone still applies but the casual-citizen filter is weaker here than on Public Appointments.
