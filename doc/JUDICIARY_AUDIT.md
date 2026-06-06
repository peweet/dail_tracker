# Courts & Judiciary — UI audit

**Date:** 2026-06-06 · **Page:** `utility/pages_code/judiciary.py` (4 tabs + profile drilldown)
**Method:** civic-ui-review 4-pass (logic firewall / API compliance / UI quality / Service Standard),
rendered live on a fresh server, every state screenshotted (`audit_screenshots/_jaudit_*.png`,
capture script `_jud_audit_capture.py`).

**Score: 16 / 20** — strong, civic, well-structured; no P0s. Loses marks on Legal-Diary
navigation (no search, long tail unreachable) and a couple of edge-render glitches.

---

## Pass A — Logic firewall ✅ PASS
`tools/check_streamlit_logic_firewall.py` clean. Every classification lives in the pipeline:
`plaintiff_kind` / category in the legal-diary extractor; `clearance_pct`, parsed weeks,
`is_elevation`, salary band, match confidence in the SQL views. The page does presentation
faceting only. All in-app counts (`value_counts`, `groupby`) are render-time displays over
already-classified columns and carry `# logic_firewall: display_only`. No parquet reads, no
joins, no metric definitions in Streamlit.

## Pass B — API compliance ✅ PASS (one documented deviation)
- `st.html` for all card/markup; the single `unsafe_allow_html=True` is the deliberate
  `<style>` injection in `_inject_jd_css` (st.html would iframe the style block).
- `width="stretch"` on the altair chart (not `use_container_width`); `st.segmented_control`
  and `st.pills` for temporal/filter controls (not `st.radio`); `_esc` (html.escape) on all
  dynamic text; white `#ffffff` card backgrounds; no emoji in headings.
- **Deviation (→ P2-3):** the ~100-line `jd-*` legal-diary CSS is page-local with hardcoded
  hex instead of the shared `var(--*)` tokens. Documented as intentional, but it duplicates
  the palette and risks drift from `shared_css.py`.

## Pass C / D — UI quality & Service Standard
Strong hero (h1) → section (h2) → subhead (h3) hierarchy; year **pills** on Courts, day
**segmented control** on the Diary; zero `st.dataframe` anywhere; human empty states; back-nav
in the main content on the profile; deuteranopia-safe colour throughout (clearance amber/teal,
plaintiff State-blue/company-amber/individual-grey, authority blue/amber) and **always
text-labelled**; provenance footers with source links + `source_sha256`.

---

## Findings (prioritised)

### P1 — should fix
1. **✅ FIXED 2026-06-06** — **Legal Diary search.** Added a "Search by party or judge" box to
   `_render_ld_cases` (display-only filter over the anonymised title + judge); matching groups
   auto-expand so hits show without clicking. The per-court cap remains for the unsearched view.
2. **✅ FIXED 2026-06-06** — **Waiting-times card rendering.** `_wait_card` now renders the big
   value from the parsed `weeks_2024` (zero → "Immediate", correct week/weeks singular) instead
   of the raw `wait_2024` phrase; dropped the redundant `(… weeks)` from the delta.

### P2 — polish
3. **Page-local `jd-*` CSS with hardcoded hex** (see Pass B). Migrate to shared tokens at the
   next CSS consolidation.
4. **No-spine profile is sparse + a data nuance.** "Record begins 2016" judges show only a note
   + salary + sources over a large empty page. Worse, the **Chief Justice (Donal O'Donnell)**
   shows salary "Ordinary Judge" and no Chief-Justice office label (a pipeline join nuance). →
   fold in any available gov.ie nomination / current assignment to fill the page; (pipeline)
   flag the CJ office + salary band.
5. **Two glossary strips stack on the Legal Diary tab** — the page-level one
   (Elevation / Ex-officio / Iris Oifigiúil) is irrelevant to the diary, sitting above the
   diary-specific one (DPP / For mention / Ex parte / JR). → one contextual glossary per tab.
6. **Hero badges are appointment-centric on every tab.** "154 appointments / 21 elevations"
   show on the Courts and Legal Diary tabs where they don't fit. → minor; vary per tab or keep
   generic ("194 judges / 5 courts").

### P3 — nice to have
7. Wait deltas use `▲▼` unicode rather than `:material/arrow_*` icons.
8. Court structure repeats within the Legal Diary tab (schedule-by-court, then cases-by-court).
9. Global header subtitle clips on mobile ("…searchable" cut) — cross-page, not judiciary-specific.

---

## Strengths (keep / replicate)
- **Courts tab** — clearance bars with a backlog hue (amber < 100% < teal) + a 100% break-even
  trend chart + area-of-law drilldown is the strongest civic data-viz on the page.
- **Legal Diary "Who's bringing these cases"** — the plaintiff split + named-institution ranking
  is a clean, privacy-safe accountability signal (individuals stay initials; named companies /
  State bodies in clear).
- **Bench cards** — post salary-dedup, a clean qualitative roster (name → appointed-by → chips).
- Honest coverage gaps ("Record begins 2016", "Needs review") shown, not hidden.

## Recommended order of work
P1-1 (diary search) → P1-2 (waiting-card render) → P2-4 (profile fill + CJ data) → P2-5/6 (glossary / badges) → P2-3 (CSS tokens, at consolidation).
