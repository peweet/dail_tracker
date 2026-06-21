# App redesign sweep — full-page critique & work orders (2026-06-10)

Direction signed off in chat 2026-06-10. Supersedes the per-page "bold pass" ordering in
APP_REDESIGN_PHASE0.md §4 Phase 2 (the Phase 1 systemic items remain valid and are folded in
here). Three principles drive every work order below:

1. **Findings, not filters.** Every list page opens with 2–3 plain-English, pipeline-computed
   facts (the *lede*), not a stat strip and not a wall of controls. Stat strips are retired
   app-wide (Phase 0 decision S1, executed here as `finding_lede()`).
2. **Entity-first.** Browse pages are thin gateways into dossiers (TD, company, public body).
   The richest cross-references live on the dossier, not in extra tabs on register pages.
3. **Conduit-everywhere.** Every card that represents an official record links to that record
   at its official source. Where no per-record URL exists (verified, not assumed), the page
   says what the source is and links at dataset level. Never ship a dead or guessed link.

## Verified conduit capability (data checked 2026-06-10)

| Source | Per-record URL | Status |
|---|---|---|
| TED | `notice_url` column, 100% coverage, `ted.europa.eu/en/notice/-/detail/{id}` | ✅ render everywhere |
| Public-body payments | `source_file_url` (1,169 distinct PDFs) + `source_landing_url`, 100% coverage | ✅ render per row |
| Oireachtas (bills, votes, members, debates) | live patterns in entity_links / queries | ✅ extend per row |
| irishstatutebook.ie (SIs) | `/eli/{year}/si/{num}/made/en/html` | ✅ live |
| data.oireachtas.ie PDFs (interests, attendance, payments) | curated lists in `utility/ui/source_pdfs.py` | ✅ wire per-record |
| eTenders | **none** — etenders.gov.ie is behind CAS login; legacy eu-supply soft-404s (probed real vs bogus IDs, identical responses) | ⛔ dataset-level link only; use the TED link when the notice also exists in TED |
| CRO | **none** — core.cro.ie 403s; no verifiable public deep link | ⛔ show number as text |
| Iris Oifigiúil | month archive only today; per-PDF pattern unverified | ⚠️ verify pattern from extractor download URLs before shipping |

## Systemic work (do once, all pages inherit)

- S-1 `finding_lede()` component: kicker-less lede block under the hero; 1–3 sentences,
  `<strong>` numbers, optional inline source link. Replaces `stat_strip()` / `totals_strip()`
  everywhere. Values must come from registered views via `dail_tracker_core/queries` —
  the component renders only.
- S-2 `control_bar()`: one consolidated row (search + pills + toggles) with a `.dt-control-bar`
  grid; pages stop stacking 4–6 widget rows. Target: max 2 rows of controls before data on
  any page.
- S-3 Type-scale collapse to the 6-step scale (PRODUCT.md deferred debt) once S-1/S-2 land,
  since both delete most of the bespoke heading sizes.
- S-4 Conduit row on cards: a quiet `.dt-card-sources` line using `source_link_html()`;
  one helper, consistent placement (card footer, right-aligned).

## Per-page work orders

Ordered by impact. "Lacking" lists are the honest critique; orders are the fix.

### 1. procurement.py — P0, restructure (1,472 lines)
Lacking: ~6 rows of furniture before data (caveat, 7-stat strip, 2 expanders, year pills,
2 toggles); 4 tabs organised by register, not by question; the data model is taught before
any fact is shown; eTenders cards have no conduit; overlaps tab is a dead end (no causation,
no drill); supplier drill-down is the best content on the page and the least discoverable.
Order: collapse to two questions — "Who wins contracts?" (eTenders+TED cross-referenced
firm cards, counts-led) and "Who actually gets paid?" (payments). Lede: top repeat winner,
long-tail fact (top-10 firms = 4.5% of awards), safe-total with verb. The €570bn demolition
moves to a collapsed "How to read this data" explainer. Overlap badges (lobbying/charity)
move onto firm cards and the dossier. TED cards: per-notice conduit. Payments rows:
per-PDF conduit. eTenders: dataset-level source line.

### 2. NEW company dossier (supplier profile promoted to first-class page)
Lacking today: it's a query-param drill-down inside procurement; no URL identity of its own;
TED/payments/lobbying/charity cross-refs render as afterthought panels.
Order: `/company?supplier=<norm>` page: identity header (name, CRO number as text, class),
finding lede ("N awards from M bodies since YYYY; €X actually paid by K bodies"),
sections: awards (eTenders+TED, TED rows deep-linked), money actually paid (per-PDF links),
register overlaps (lobbying/charity badges with no-inference copy), provenance footer.
Reuses existing queries (`awards_for_supplier`, `ted_for_supplier`, `payments_for_supplier`,
overlaps). Procurement/public-payments/corporate pages link into it.

### 3. public_payments.py — P0 (475 lines)
Lacking: 7-stat strip; zero conduits despite 100% source-URL coverage in the data; tabs+sort
before any fact; no link into the company dossier.
Order: finding lede (top publisher, top supplier, tier totals with verbs, never summed);
per-row source PDF link (S-4); supplier names link to company dossier; keep the two-axis
tabs (publisher/supplier) — they are genuine questions.

### 4. statutory_instruments.py — P1 worst of the six (1,495 lines)
Lacking: 5-stat mixed-type strip (a department name as a "stat"); three stacked input rows;
dept-chip wall pushes content down a full viewport.
Order: finding lede (SIs this year, busiest department as a sentence); control_bar with
search + year pills; dept chips become a collapsed "filter by department" disclosure;
keep per-SI irishstatutebook conduit (already good).

### 5. interests.py — P1 (361 lines)
Lacking: the most politically potent dataset sits under 5 control rows + legend; no
per-declaration source link despite `source_pdf_url` on the detail view.
Order: finding lede (landlord count, most-declared category — counts only, no inference);
control_bar; per-declaration PDF conduit; legend collapses into the glossary expander.

### 6. votes.py — P1 (586 lines)
Lacking: 4 control rows (2 toggles + 2 dropdowns + year pills) before a single division;
stage pills truncate.
Order: control_bar consolidation; default view = latest divisions list (content first,
filter after); keep per-division Oireachtas conduits.

### 7. legislation.py — P1 (908 lines)
Lacking: 3-stat strip + stage-chip wall; bill cards half-empty (dead middle); hero copy
still claims Private-Members-only (Govt bills ARE indexed — stale TODO_GOVT_BILLS);
debate_url built inline.
Order: finding lede; fix hero claim; bill card grid tightened (2-col, fuller content);
centralise debate/bill URL builders into entity_links.

### 8. payments.py (TD allowances) — P1 (423 lines)
Lacking: 2-stat strip; per-month source PDFs exist in source_pdfs.py but rows don't link.
Order: finding lede; wire per-year/month PDF conduits onto cards.

### 9. committees.py — P1 (746 lines)
Lacking: 5 control rows; 4-stat strip; search Enter-trap; column truncation.
Order: finding lede; control_bar; keep party-composition bars untouched (best viz in app).

### 10. election_2024.py — P1 (792 lines)
Lacking: coloured-border stat strip (the loudest hero-metric tell in the app); SIPO conduit
only page-level.
Order: finding lede in the page voice; per-return SIPO PDF links where the OCR rows carry
them; keep 3-tab lens structure.

### 11. corporate.py — P2 (2,492 lines)
Lacking: 4-stat strip; month-level Iris links where the data knows the exact PDF.
Order: finding lede; verify + ship per-PDF Iris conduit (see table above); CRO numbers as
text (no link — verified unavailable).

### 12. attendance.py — reference, polish only
Lacking: per-year PDF conduits unwired.
Order: wire ATTENDANCE list from source_pdfs.py onto year cards. No structural change.

### 13. member_overview.py — reference, polish only
Lacking: questions/debates use generic links where per-record URLs exist.
Order: per-record Oireachtas links in those two sections. No structural change.

### 14. lobbying_3.py / judiciary.py / public_appointments.py — reference
Order: inherit finding_lede only if it removes something (lobbying has no stat strip —
leave alone). Judiciary/appointments: spot-polish only. Appointments: same Iris per-PDF
conduit as corporate when verified.

### 15. glossary.py — P2
Order: confirm masthead/toolbar parity. Add the "How to read this data" procurement
explainer section (moved from the procurement page).

## Data fixes shipped with this sweep (2026-06-10)

- eTenders extractor: authority whitespace collapse (OGP no longer ranks as two bodies);
  punctuation-initial truncation repair (941 distinct spellings remapped, 6,050 rows —
  Mazars +251 rows, O'Flynn Exhams consolidated to 97 awards and into the top 10);
  punctuation-initial names without a canonical twin now flagged `name_truncated`.
- Encoding mojibake ruled out (raw CSV is valid UTF-8; "Iarnród" is stored correctly).
- eTenders/CRO deep links verified impossible (login wall / 403) — recorded above so
  nobody re-attempts dead conduit work.
