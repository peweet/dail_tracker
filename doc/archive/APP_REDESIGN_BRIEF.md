# Dáil Tracker — full-app consolidation & bold-redesign brief

**Created:** 2026-06-10 · **Status:** Phase 0 (review & plan) in progress · **Owner decision log below**

## Intent
Total UI review + *consolidating* bold redesign across all 17 routes + hidden Home + Glossary.
Tidy and unify what exists — sharpen hierarchy, kill repetition, enforce the design system, AND
restructure the flat 17-item navigation into a coherent IA — **without** discarding the editorial
identity (`PRODUCT.md`), the side-stripe cards, the `#ffffff` paper cards, or any page's data.

The app is not broken: the 2026-06-06 full-app audit (`doc/APP_UI_SWEEP_AUDIT.md`) scored it
**15/20** and confirmed it does NOT look AI-generated. The problem is **clutter and
inconsistency**, not identity. Lead culprit already named: the **multi-stat top strip on 6 pages**.

## Owner decisions (2026-06-10)
- **Scope:** Visual consolidation **+ IA restructure** (group the flat 17-route nav).
- **Sequencing:** **Plan first, then approve** — no edits until the Phase 0 plan is signed off.
- **Auditing:** **Playwright screenshots** as the evidence base, not single-viewport inference.
- **Phase 0 SIGNED OFF 2026-06-10.** Direction set:
  - **S1 (stat strips):** replace all 8 with an **inline context sentence** (Lobbying-hero voice);
    numbers move into the data section where they're earned. No stat strip above the data.
  - **S3 (nav):** **grouped sections** — Members & Parliament / The Money / Law & Records /
    Influence / Glossary, via `st.navigation({section: [pages]})`, slugs unchanged. Verify the
    top-nav widget renders groups acceptably; reordered-flat is the fallback if not.

## Non-negotiable guardrails
- `PRODUCT.md` — constitution: brand, **intentional rule overrides** (side-stripe / `#ffffff` /
  signal tokens), deferred design debt (type-scale collapse, CSS-architecture split).
- `doc/APP_UI_SWEEP_AUDIT.md` — 15/20 baseline; do NOT re-litigate its "NOT A DEFECT" calls
  (em-dashes are house voice; stat strips are already non-overflowing/responsive).
- Logic firewall absolute: no business logic in UI, no new inference, no summing across the three
  money grains. Honour: no-inference-in-app, dataframes-secondary-only, member_overview-zero-
  dataframes, surface-trap (`#ffffff` not `var(--surface)` for cards), dual-config (path consts
  in BOTH config.py files), firewall-marker placement.
- All CSS in `utility/shared_css.py`; use `utility/ui/components.py` helpers; no ad-hoc `<style>`,
  no new hardcoded hex (use `var(--*)` / OKLCH signal tokens).

## Skill routing (which tool does what)
| Phase | Tool | Role |
|---|---|---|
| 0 Plan | `shape` | Design brief + IA proposal + per-page plan (no code) |
| 0 Plan | **Playwright** (`audit_screenshots/_sweep_capture.py`) | Screenshot evidence, desktop + mobile |
| 0 Plan | `civic-ui-review` + `impeccable audit` | Score the captures (PRODUCT.md loaded FIRST) |
| 2 Build (9 contract-backed pages) | `bold-redesign-page` → `streamlit-frontend` → `review-page` | attendance, committees, home_overview, legislation, lobbying, member_overview, votes, interests, payments |
| 2 Build (8 non-contract pages) | `shape` → `streamlit-frontend` → `civic-ui-review` + impeccable craft (`layout`/`typeset`/`adapt`/`quieter`) | election_spending, public_payments, procurement, appointments, corporate, statutory_instruments, judiciary, glossary |
| 3 Verify | Playwright + `civic-ui-review` + `impeccable audit` | Re-score on a FRESH server |

**Critical caveat:** `impeccable`'s absolute bans (side-stripe borders, `#fff`, hero-metric
template) contradict three documented PRODUCT.md signatures. PRODUCT.md MUST be loaded before any
impeccable design work; `civic-ui-review` is the authority on project-specific rules; use impeccable
for craft only, never to strip the signatures. (See memory: feedback_impeccable_product_override,
project_bold_ui_contract_coverage.)

## Phases
- **Phase 0 (read-only, current):** clean Streamlit restart → Playwright sweep all 16 visible
  routes + mobile → clutter ledger → IA proposal → prioritized P0/P1/P2 page plan → STOP for sign-off.
- **Phase 1 (post-approval):** systemic + IA — unify the stat strip into one house pattern; collapse
  the 12-size type scale → 6-step (per page, visually verified); page-local hex → tokens; nav
  restructure (preserve `url_path` slugs + `entity_links.PAGES`); clear open audit items.
- **Phase 2:** per-page bold pass in priority order (routing table above). Keep audit's best-in-app
  pages as references (Member Overview, Lobbying register, Appointments, Committees, Judiciary Courts).
- **Phase 3:** re-sweep + re-score on a fresh server; run firewall/lint/typecheck/test/SQL-contract
  CI; write results back into `doc/APP_UI_SWEEP_AUDIT.md`.

## Working rules
Consolidate, don't multiply (edit shared_css.py/components.py over per-page CSS). One page at a
time with a fresh-server Playwright screenshot per change. Don't touch `payments_original.py`. Flag
clutter that is actually a pipeline defect (hardcoded columns, duplicate rows) rather than masking
it in UI.
