# Design Brief — "View Bill PDF" affordance on Legislation page

**Status:** Design only. No code shipped.
**Date:** 2026-05-06
**Scope:** Feature addition to the existing legislation page, not a full redesign. Existing layout (hero → pipeline strip → phase segments → bill cards → detail) is preserved. The brief covers only the PDF affordance and one adjacent improvement (stage progress tracker) directly inspired by the competitor reference.
**Reference product:** [oireachtas-explorer.ie/#/dail/34/legislation](https://oireachtas-explorer.ie/#/dail/34/legislation) — has a clickable "View Bill PDF" tile and an inline PDF modal.

---

## 1. User question this page answers

Existing: *"What legislation is visible in the dataset, and how can users inspect its official records?"*

The PDF feature sharpens **"how can users inspect"** from a single "Oireachtas ↗" link (which deep-links to a metadata page) to a direct, one-click route to the **bill text itself** — the actual legal artefact. A citizen wanting to read what a bill *says* should not have to click through three pages.

## 2. Current UI problems

- **The bill text is two clicks away.** Today's only outbound link goes to `oireachtas.ie/en/bills/bill/<year>/<no>/`. The reader must then locate the "Versions" tab and pick a PDF. The competitor compresses this to one click.
- **No visual signal that PDFs exist.** Users with low patience for government websites assume the data is locked away.
- **Versions are invisible.** Bills have multiple PDF versions (As Initiated, As Amended, As Passed, As Enacted) — and an Explanatory Memorandum. Today none of these surface anywhere in the UI.
- **Stage timeline is vertical and verbose.** The competitor's horizontal connected-dot tracker (1st · 2nd · Cmte · Report · 5th · Passed) is more legible at a glance than our vertical row list. Not strictly part of the PDF feature, but the tile in the reference shows the two patterns work together.

## 3. Bold redesigned layout — the changes

**Index card (Stage 1) — add one row:**

```
┌─────────────────────────────────────────────────────────────────┐
│ [IN PROGRESS]  [GOVERNMENT]                       29 Apr 2026   │
│                                                                 │
│ International Co-operation (Omagh Bombing Inquiry) Bill 2026    │
│                                                                 │
│ Sponsor: Minister for Justice  ·  Current stage: Fifth Stage    │
│                                                                 │
│ ─────────────────────────────────────────────────────────────   │
│ [📄 View Bill PDF]   Oireachtas ↗                               │
└─────────────────────────────────────────────────────────────────┘
   (whole card → /legislation?bill=2026_42)
```

The dark-green pill button is the new affordance. It is a `target="_blank"` anchor pointing at `official_pdf_url`. It does **not** intercept the card's own click target — the card stays clickable, and the button uses the existing `stretched-link`-overrides-`<a>`-inside-`<a>` pattern already used for the Oireachtas link (CSS class `dt-link-on-card` with higher z-index).

**Detail page (Stage 2) — add a "Bill Text" panel above Stage Timeline:**

```
EVIDENCE / BILL TEXT

  [📄 As Initiated · PDF]   [📄 As Amended · PDF]   [📄 Explanatory Memo · PDF]
                                            (only render the buttons that exist)
```

Each version button is a labelled link, opens the PDF in a new tab. Versions appear in chronological order. If only one version exists, render one button.

**Why no inline modal/iframe?** The competitor's modal embeds the PDF. We deliberately do not replicate this in v1:

- `data.oireachtas.ie` may set `X-Frame-Options: SAMEORIGIN` — an iframe will silently fail.
- Streamlit `st.dialog` cannot reliably size a PDF iframe across mobile/desktop.
- Custom JavaScript (CCv2 PDF viewer) is in-budget per `building-streamlit-custom-components-v2`, but is not justified for v1 — a labelled link to the canonical Oireachtas PDF is more trustworthy and accessible than a re-rendered viewer.
- v2 (optional, after v1 ships): a CCv2 component using PDF.js if the user reports the new-tab flow as friction.

## 4. Interaction model

- **Primary view:** Index of bill cards (unchanged). PDF button is now part of the card footer.
- **Click behaviour:**
  - Card body → opens detail (existing: `?bill=<bill_id>`).
  - "View Bill PDF" button → opens PDF in new tab (`target="_blank" rel="noopener noreferrer"`).
  - "Oireachtas ↗" link → opens metadata page in new tab (existing).
- **Detail view:** "Bill Text" panel sits above "Stage Timeline" so the bill text is the first thing reachable after the identity strip.
- **Keyboard:** PDF button is a real `<a>` — focusable, Enter activates, screen readers announce "View Bill PDF, link, opens in new tab" via `aria-label`.

## 5. Temporal behaviour

Unchanged. PDF feature is not temporal — bill PDFs are point-in-time artefacts at each stage. No year pills needed for this feature.

## 6. Source-link behaviour

- Contract `source_links.approved_url_columns` already lists `official_pdf_url` and `source_document_url` — this feature uses both.
- `display: labelled_links_not_raw_urls` — we render "View Bill PDF" / "Explanatory Memo · PDF", never raw URIs.
- Provenance expander adds one new line: *"Bill text PDFs are served from data.oireachtas.ie. Each version (As Initiated / As Passed / As Enacted) is a distinct PDF; this page links to all available versions."*

## 7. Chart and table strategy

No new charts or tables. One new visual element on the detail page only:

**Horizontal stage progress tracker** (replaces the existing vertical `_render_stage_timeline` *list* — the underlying data and stage-group logic stay; only the rendering changes):

```
●─────●─────●─────●─────◐─────○
1st   2nd   Cmte  Report 5th   Passed
✓     ✓     ✓     ✓      ●     ·
```

- Filled circle with check mark = completed stage
- Gold ring with filled centre = current stage (matches competitor)
- Empty circle = not yet reached
- Connecting line darker between completed stages, lighter between pending

This is a CSS-only treatment over the existing `v_legislation_timeline` data. No new metric, no new aggregation.

## 8. Empty state copy

- **No PDF URL on a card:** show only the "Oireachtas ↗" link (existing behaviour). Do **not** render a disabled/greyed-out PDF button — it adds noise. If `official_pdf_url IS NULL`, the PDF button is omitted.
- **No PDFs at all on detail page (rare — pre-First-Stage bills):** in the Bill Text panel: *"Bill text not yet published. The PDF becomes available when the bill is formally introduced."*
- **Explanatory Memo missing:** simply omit that button. Don't say "no memo".

## 9. Visual differences from old page

1. **New PDF pill button** on every index card with a non-null `official_pdf_url`, dark green, leading PDF icon.
2. **New "Bill Text" evidence section** on detail pages — appears above Stage Timeline.
3. **Stage timeline becomes horizontal** — connected dots replacing the vertical `leg-stage-row` list. Stage groups (Dáil / Seanad / Presidential) become section labels above the track, not inline rows.
4. **Detail header gains an explicit "Read the Bill" link** in addition to the existing "View Bill on Oireachtas.ie" link.
5. **Card footer split into two rows** (status row + actions row) so the PDF button has space without crowding sponsor/stage metadata.

## 10. TODO_PIPELINE_VIEW_REQUIRED items

The contract says these columns are *approved*. They currently exist in `versions.parquet` and `related_docs.parquet` but are returned as `NULL` in `v_legislation_sources`. The view itself already carries TODO comments to this effect (`sql_views/legislation_sources.sql:3-5`). Per CLAUDE.md, SQL views are safe to edit directly — these are not pipeline changes. The TODOs to resolve as part of this feature:

- **`TODO_PIPELINE_VIEW_REQUIRED`** — `v_legislation_sources.official_pdf_url`: populate from `versions.parquet → version.formats.pdf.uri`, picking the most recent version per bill (or the version matching `version.showAs = 'As Enacted'` if the bill is enacted, otherwise the latest by `version.date`).
- **`TODO_PIPELINE_VIEW_REQUIRED`** — `v_legislation_sources.source_document_url`: populate from `related_docs.parquet → relatedDoc.formats.pdf.uri` where `relatedDoc.docType = 'memo'`.
- **`TODO_PIPELINE_VIEW_REQUIRED`** — `v_legislation_index.official_pdf_url`: add column so the Stage 1 card can render the button without an extra fetch. Source same as above.
- **`TODO_PIPELINE_VIEW_REQUIRED`** — `v_legislation_versions` (new view, optional): one row per (bill_id, version_label, pdf_url, version_date) so the detail page can render multiple version buttons. If declined, the detail page falls back to the single `official_pdf_url` from `v_legislation_sources`.
- **No changes** to `v_legislation_detail`, `v_legislation_timeline`, `v_legislation_debates`.

## 11. Implementation plan

**Files to change (all permitted edits):**

| File | Change |
|---|---|
| `sql_views/legislation_sources.sql` | Replace NULL placeholders with `LEFT JOIN`s onto `versions.parquet` and `related_docs.parquet`. Keep one row per bill_id (latest version). |
| `sql_views/legislation_index.sql` | Add `official_pdf_url` column via subquery picking latest version PDF per bill. |
| `sql_views/legislation_versions.sql` | **New file.** One row per (bill_id, version_label, pdf_url). Optional — only if multi-version button row is wanted. |
| `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/legislation.yaml` | Add `v_legislation_versions` to `approved_registered_views` (only if new view created). Add `official_pdf_url` to `v_legislation_index.optional`. |
| `utility/data_access/legislation_data.py` | Add `fetch_bill_versions(bill_id)` (only if v_legislation_versions created). Update `fetch_legislation_index_filtered` SELECT to include `official_pdf_url`. |
| `utility/pages_code/legislation.py` | Render PDF button in card footer (Stage 1) and "Bill Text" panel (Stage 2). Replace vertical `_render_stage_timeline` body with horizontal track. |
| `utility/shared_css.py` | Add `leg-pdf-btn`, `leg-pdf-btn:hover`, `leg-bill-actions`, `leg-stage-track`, `leg-stage-dot`, `leg-stage-dot--done`, `leg-stage-dot--current`, `leg-stage-track-line`. |

**CSS classes to add (all in `shared_css.py`):**

- `.leg-pdf-btn` — dark-green (`#0f5132` on `#ffffff`) pill, 12px PDF glyph + label, white text. `display: inline-flex; align-items: center; gap: 6px;` `padding: 6px 12px; border-radius: 999px;`. Hover: background `#0a3d24`. Use **`#ffffff`** (per the surface-trap rule) anywhere a card/pill background is needed — not `var(--surface)`.
- `.leg-bill-actions` — flex row, `justify-content: space-between`, separator (`border-top: 1px solid var(--rule-soft)`) above.
- `.leg-stage-track` — flex row, equal-spaced dots and connector lines.
- `.leg-stage-dot` — 14px circle, base state.
- `.leg-stage-dot--done` — filled green, contains `✓` glyph.
- `.leg-stage-dot--current` — gold ring, filled green centre.
- `.leg-stage-track-line` — 2px line between dots; `--done` variant darker.
- `dt-link-on-card` raise `z-index: 2` so the PDF button receives clicks ahead of the stretched-link overlay (already the pattern for the Oireachtas link).

**Helpers to reuse (no new helpers needed):**

- `source_link_html(...)` — render the PDF button via this helper, passing the dark-green class.
- `provenance_expander(...)` — add the new provenance line.
- `evidence_heading(...)` — section header for "Bill Text".
- `empty_state(...)` — pre-First-Stage empty case.

**Order of work (to avoid a half-shipped feature):**

1. Update `sql_views/legislation_sources.sql` + `legislation_index.sql` so PDF URLs flow.
2. Add CSS classes to `shared_css.py`.
3. Add the PDF button on Stage 1 cards (smallest, highest-value change).
4. Add the "Bill Text" panel on Stage 2.
5. Replace vertical stage list with horizontal track.
6. Provenance text update.
7. (Optional) `v_legislation_versions` for multi-version button row.

**Acceptance — this brief satisfies the contract's bold-redesign rule:** changes touch `editorial_hero`-adjacent (action-row added), `information_hierarchy` (bill text now first-class evidence), `source_link_presentation` (labelled PDF button replacing raw text link), `chart_or_timeline_presentation` (vertical → horizontal track), `table_configuration` (card footer split), and `shared_css_polish` (new class family) — six dimensions, meeting `must_change_at_least.count: 6`.

---

## Open questions before code starts

1. **Multi-version button row** (As Initiated + As Amended + As Passed) — yes, or just the latest PDF? Multi gives more transparency but adds clutter on cards.
2. **Stage tracker redesign** — bundle now (matches competitor's tile exactly), or split into a separate ticket so this PR is scoped tightly to "View Bill PDF"?

## Data verification (already confirmed)

- `data/silver/parquet/versions.parquet` columns include `version.formats.pdf.uri`, `version.date`, `version.showAs`, `bill.billNo`, `bill.billYear`. Sample URI: `https://data.oireachtas.ie/ie/oireachtas/bill/2026/5/eng/initiated/b0526d.pdf`.
- `data/silver/parquet/related_docs.parquet` columns include `relatedDoc.formats.pdf.uri`, `relatedDoc.docType` (e.g. `memo`), `relatedDoc.showAs` (e.g. `Explanatory Memorandum`). Sample URI: `https://data.oireachtas.ie/ie/oireachtas/bill/2026/5/eng/memo/b0526d-memo.pdf`.
- Both parquet files are written by `legislation.py` (the loader, not the page) — no pipeline change needed; the SQL view simply needs to read them.
