# Streamlit Provenance Record — exposed vs. not exposed

**Date:** 2026-06-06
**Scope:** every page in `utility/pages_code/` and the provenance helpers in `utility/ui/`.
**Concept:** *provenance only* — the resources that let a user validate where a figure came from.
Freshness/currency is a separate axis and is **not** recorded here (see
[API_PROVENANCE_REVIEW.md](API_PROVENANCE_REVIEW.md) §0 for the distinction).

Provenance has two **layers**, used throughout this record:
- **Layer A — source-level attribution:** "this page draws on data.oireachtas.ie / lobbying.ie /
  SIPO." Delivered by `provenance_expander()` (the standard *About & data provenance* expander).
- **Layer B — per-record citation:** the clickable link to *this specific* document (bill PDF,
  division record, register entry). Delivered by `source_link_html()` / `render_source_links()` /
  the PDF registries.

A page can have A without B (a credit line but nothing to click through per row), B without A
(inline links but no unified footer), both, or neither.

---

## 1. The provenance toolkit (what exists to be used)

| Helper | File | Layer | What it renders |
|---|---|---|---|
| `provenance_expander(sections, source_caption, pdf_links)` | [source_pdfs.py:301](../utility/ui/source_pdfs.py#L301) | A | Standard "About & data provenance" expander |
| `render_pdf_source_links()` + `INTERESTS`/`ATTENDANCE`/`PAYMENTS` registries | [source_pdfs.py:42-298](../utility/ui/source_pdfs.py#L42-L298) | A/B | Curated lists of canonical oireachtas.ie PDF URLs |
| `render_source_links(df)` | [source_links.py:28](../utility/ui/source_links.py#L28) | B | Per-record official-source link chips from approved URL columns |
| `source_link_html()` / `oireachtas_profile_url()` | [entity_links.py](../utility/ui/entity_links.py) | B | Inline external/official link builder |

Note: `render_source_links` falls back to `todo_callout("source_url column on v_vote_sources")`
when no URL column is present ([source_links.py:59](../utility/ui/source_links.py#L59)) — a built-in
"provenance missing" signal.

---

## 2. Per-page record

Legend: ✅ exposed · ⚠️ partial / non-standard · ❌ not exposed

| Page | App nav | Layer A (expander) | Layer B (per-record links) | PDF registry | Sources credited | Key gap |
|---|---|---|---|---|---|---|
| **attendance** | rankings-attendance | ✅ `provenance_expander` ([:237](../utility/pages_code/attendance.py#L237)) | ❌ | ✅ `ATTENDANCE` | data.oireachtas.ie (TAA) | No per-member validation link; Dáil only, Seanad PDFs uncurated; `TODO_PIPELINE_VIEW_REQUIRED` per-year URL ([:8](../utility/pages_code/attendance.py#L8)) |
| **payments** | rankings-payments | ✅ `provenance_expander` ([:169](../utility/pages_code/payments.py#L169)) | ❌ | ✅ `PAYMENTS` | data.oireachtas.ie (PSA/TAA) | No per-transaction/ per-member source link; `TODO_PIPELINE_VIEW_REQUIRED` v_payments_sources |
| **payments_original** | (hidden) | ✅ ([:156](../utility/pages_code/payments_original.py#L156)) | ❌ | ✅ `PAYMENTS` | data.oireachtas.ie | Duplicate of payments.py |
| **interests** | rankings-interests | ✅ `provenance_expander` ([:152](../utility/pages_code/interests.py#L152)) | ❌ (PDF URL in views, not rendered) | ❌ (despite `INTERESTS` registry existing) | data.oireachtas.ie (Register PDFs) | No per-declaration source link; `TODO_PIPELINE_VIEW_REQUIRED` v_member_interests_sources ([:9-26](../utility/pages_code/interests.py#L9-L26)) |
| **votes** | rankings-votes | ✅ `provenance_expander` ([:334](../utility/pages_code/votes.py#L334), [:399](../utility/pages_code/votes.py#L399)) | ❌ | ❌ | Oireachtas Open Data API (text only, no link) | No per-division record link; credit is text-only, not clickable |
| **legislation** | rankings-legislation | ✅ ×2 ([:307](../utility/pages_code/legislation.py#L307), [:822](../utility/pages_code/legislation.py#L822)) | ✅ `source_link_html` bills→oireachtas.ie, SIs→irishstatutebook.ie | ❌ | Oireachtas Open Data API; irishstatutebook.ie; eISB | **Best-covered page** (A+B) |
| **lobbying_3** | rankings-lobbying | ✅ `provenance_expander` ([:271](../utility/pages_code/lobbying_3.py#L271)) | ✅ `render_source_links` ([:1121](../utility/pages_code/lobbying_3.py#L1121), [:2044](../utility/pages_code/lobbying_3.py#L2044)) + inline | ❌ | lobbying.ie, org websites | **Best-covered page** (A+B); only adopter of `render_source_links` |
| **committees** | rankings-committees | ⚠️ custom expander, not the helper ([:652-675](../utility/pages_code/committees.py#L652-L675)) | ⚠️ inline `committee_url` when present | ❌ | Houses of the Oireachtas | `TODO_PIPELINE_VIEW_REQUIRED` v_committee_sources ([:670](../utility/pages_code/committees.py#L670)); no link back to source document |
| **statutory_instruments** | rankings-statutory-instruments | ❌ no unified footer | ✅ `source_link_html` → irishstatutebook.ie, eISB, bill cross-ref | ❌ | irishstatutebook.ie, eISB, Iris Oifigiúil | Rich layer B but no layer-A footer |
| **member_overview** | member-overview | ❌ no unified footer | ✅ `source_link_html` ×6 (profile, debates, socials) + `st.caption` credits | ❌ | oireachtas.ie (profile/debates), socials | **Flagship page, no unified provenance footer**; credits scattered across section captions |
| **judiciary** | rankings-judiciary | ❌ | ⚠️ inline footer link → legaldiary.courts.ie (Legal Diary tab only) | ❌ | Courts Service Legal Diary, Iris Oifigiúil | Source link only in one tab footer; no page-level provenance |
| **corporate** | rankings-corporate | ❌ (custom *methodology* expander instead) | ⚠️ Iris link in feed; brand→parent table | Iris PDF in export only | Iris Oifigiúil | Methodology ≠ provenance; per-notice Iris link not on card |
| **public_appointments** | rankings-appointments | ❌ | ⚠️ Iris source PDF in detail cards / export | Iris PDF in export | Iris Oifigiúil | Credit is a constitutional-context line; no standard footer |
| **procurement** | rankings-procurement | ❌ | ❌ (footer HTML link only, not per-card) | Iris in export | eTenders/data.gov.ie, CRO, Register of Lobbying | Footer-only credit; no per-award source link |
| **glossary** | glossary | ❌ (reference page) | ❌ | ❌ | text caption: Oireachtas, lobbying.ie, SIPO | No data displayed — provenance not applicable |

---

## 3. Tallies

**Layer A — standard `provenance_expander()`: 6 of 15 pages**
✅ attendance · payments · payments_original · interests · votes · legislation · lobbying_3
*(7 calls, but payments_original is a hidden duplicate → 6 distinct surfaced pages)*
⚠️ committees uses a *custom* expander (same title, not the helper) → drifts from the standard.
❌ statutory_instruments · member_overview · judiciary · corporate · public_appointments ·
procurement have **no layer-A footer at all** (corporate has a methodology expander, not a
provenance one).

**Layer B — per-record clickable citation: 6 of 15 pages**
✅ legislation · lobbying_3 · member_overview · statutory_instruments
⚠️ committees · judiciary (partial — one context/tab only)
❌ attendance · payments · interests · votes · corporate · public_appointments · procurement
have **no per-record link a user can click to validate an individual figure**.

**Both layers (A + B): only legislation and lobbying_3.**
**Neither layer (data shown, nothing to validate against): procurement** (footer text only),
and effectively corporate / public_appointments (export-only links, nothing on-card).

**PDF document registries used: 3 pages** (attendance, payments, payments_original) — all the
others ignore the registries even where one exists (e.g. interests has an `INTERESTS` registry it
never renders).

---

## 4. The headline inconsistencies

1. **The flagship has no footer.** `member_overview` — the most-used page, fusing ~8 sources —
   has scattered inline links and captions but **no unified provenance footer**. (Mirrors the API,
   where the member dossier also carries no `sources` block.)

2. **Two patterns, no rule.** Layer A is split between the `provenance_expander()` helper (6
   pages), a hand-rolled look-alike expander (committees), a *methodology* expander (corporate),
   constitutional-context lines (public_appointments), and footer HTML (procurement). Same intent,
   five implementations.

3. **Layer B is the bigger gap than layer A.** Six pages credit a source (layer A) but give the
   user **no way to click through to the specific record** (attendance, payments, interests, votes
   especially). These are exactly the ranking/aggregate pages where "validate this number" matters
   most — and the validation path stops at a page-level credit.

4. **Registries underused.** Curated PDF URL registries exist (`INTERESTS`, `ATTENDANCE`,
   `PAYMENTS`) but only attendance/payments render them; interests has one and doesn't use it.

5. **Text credits that aren't links.** votes credits "Oireachtas Open Data API" as plain text with
   no clickable target; several Iris-sourced pages credit "Iris Oifigiúil" in prose with the actual
   PDF reachable only via data export.

6. **Pipeline-blocked rows are honestly marked.** `TODO_PIPELINE_VIEW_REQUIRED` markers
   (attendance:8, committees:670, interests:9-26, payments) and the `render_source_links`
   `todo_callout` fallback show where per-record provenance is *known-missing pending a view* — not
   silently dropped.

---

## 5. How this lines up with the API

| | Streamlit | API |
|---|---|---|
| Standard layer-A mechanism | `provenance_expander()` (6/15 pages) | one global licence line; no per-resource |
| Layer-B per-record links | 6/15 pages, `source_link_html`/`render_source_links` | 2/11 endpoints (`bill`, `vote` `sources{}`) |
| Flagship (member) | inline-only, no footer | no `sources` field at all |
| Shared inventory used? | curated registries + per-record URL columns | `source_registry.generated.json` unused |

Both surfaces are **bimodal and helper-fragmented**, and the member view is the weakest on both.
The corrective in [API_PROVENANCE_REVIEW.md](API_PROVENANCE_REVIEW.md) §4 (one canonical source
registry + `source_ids`/`source_links` everywhere) maps directly onto the Streamlit gap: a single
helper used on every page (layer A) plus per-record links wherever a record can be cited (layer B).

---

*This is a record, not a remediation plan. No fixes proposed here.*
