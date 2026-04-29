# Bold UI Redesign Protocol

## Reference products

Dáil Tracker is informed by **theyworkforyou.com** (UK) and **theyvoteforyou.eu**. Their UX decisions are proven, trusted, and user-tested. When a design decision is unclear, ask: would theyworkforyou ship this?

Characteristics of that style:
- Simple primary views — a clean list of members or records, not a dashboard
- Minimal hero — title and a short orientation line, no badge counts or stat strips
- Year navigation is prominent and direct, not buried in dropdowns
- Charts only when they answer a specific question; otherwise remove them
- Member profiles are the rich view — complexity is expected and acceptable there
- Tone is factual, not promotional

This is not copying their code or IP. It is keeping execution close to a proven template.

## Principle

The existing Streamlit page is a **functional reference**, not a design reference.

A redesign is not complete if it only:
- renames headings
- wraps old content in containers
- moves widgets slightly
- adds CSS around the same structure
- preserves the same table-first/default Streamlit flow without justification

## Required difference test

A redesigned page must differ from the old page in at least six of these ways:

1. Stronger editorial hero
2. Clearer primary user question
3. Different section order
4. More prominent temporal/date/year controls
5. Better filter placement
6. Stronger summary/evidence hierarchy
7. Better chart placement or removal of useless charts
8. Improved table configuration and labels
9. Single-member or single-record focus section
10. Better CSV export placement
11. Better government/source-link presentation
12. Better provenance/caveat display
13. Better empty/loading/error states
14. Better mobile section ordering
15. More polished shared CSS and reusable components

## Primary view noise budget

The primary view (browse / index / list) must be **free from noise**:

- **One leading table** — this is the main content; everything else serves it
- **No charts above the table** — a chart between the filter bar and the member table buries the content the user came to see
- **No stat strip duplicating hero badges** — if `members_count` and `sitting_count` are already in hero badges, do not repeat them in a stat strip below the hero
- **No stacked expanders above the fold** — "About" and "Data provenance" belong at the bottom, collapsed, in a single combined expander
- **Provenance at the bottom** — collapsed by default; one expander is enough

The secondary view (profile / detail / drilldown) may be more complex, because the user narrowed scope intentionally by selecting a record:

- A single selected record can support a chart, a breakdown table, and summary stats
- Default to two content sections; a third requires justification

## Forbidden excuse

Do not say “I preserved the existing layout for consistency” unless the contract or user explicitly asks for that.

Consistency should come from:
- shared CSS
- shared UI helpers
- shared interaction patterns
- shared typography/spacing

not from cloning boilerplate.
