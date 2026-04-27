# Reconciled Frontend Strategy for Dáil Tracker

This reconciles Claude frontend aesthetic guidance, Claude Code frontend-design guidance, Streamlit constraints, your existing CSS, YAML contracts, and DuckDB/Parquet analytical views.

## Borrow from frontend aesthetic guidance

Use:
- a clear design point of view before coding
- explicit avoidance of generic AI dashboard aesthetics
- directed attention to typography, colour, spacing, and hierarchy
- isolated prompts for typography/layout/UX when you want targeted changes

Modify:
- “bold” should mean editorial confidence, not visual noise
- motion should be minimal
- backgrounds should support legibility
- typography should be deployable and accessible

## Borrow from frontend-design skill

Use:
- purpose, tone, constraints, differentiation before coding
- cohesive aesthetic direction
- production-grade polish
- avoidance of cookie-cutter output

Modify:
- no custom JavaScript
- no page-specific CSS reinvention
- no excessive animation
- no generic component-library feel

## Borrow from UX prompt cookbook thinking

Use:
- repeatable review prompts
- UX friction audits
- empty-state checks
- accessibility checks
- performance checks
- design-drift checks

Modify:
- GA/event tracking is not priority unless deliberately added
- sitemap prompts are not relevant until a separate public website needs them
- reviews must be constrained by the page contract

## Dáil Tracker north star

A citizen should get a quick, legible answer. A journalist should get exportable evidence. A maintainer should see exactly which pipeline-owned view powers the page.

## Practical page recipe

1. Editorial hero.
2. Sidebar filters.
3. Metrics from summary views or visible-row counts.
4. Evidence table.
5. Optional chart only if named in the contract.
6. Provenance/caveat panel.
7. Empty states.
8. CSV export where enabled.
