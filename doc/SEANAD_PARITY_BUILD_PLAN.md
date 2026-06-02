# Seanad Parity — Build Plan

**Goal:** Expose Senator data across Dáil Tracker at full parity with TDs — Senators become
first-class members with the same Member Overview profile depth, by (1) wiring already-extracted
Seanad data into the UI and (2) ingesting the three missing Seanad source domains (votes,
payments, attendance).

**Status:** Planning. No production code changed yet.
**Scope decision (locked by user):** Option B — full parity.
**Date:** 2026-06-01.

---

## 0. Working rules for this build

These are non-negotiable constraints that shape every phase below.

### 0.1 Prototyping goes to the sandbox, with `_experimental` tagging
- All **new Python/Polars enrichment** is prototyped in `pipeline_sandbox/` **only**. Never edit
  `pipeline.py`, `enrich.py`, or `normalise_join_key.py` during prototyping.
  (Project rule: see memory `project_pipeline_sandbox_rule.md`.)
- Every sandbox prototype file gets an `_experimental` suffix for tracking, e.g.
  `pipeline_sandbox/seanad_votes_etl_experimental.py`. This makes the throwaway/promote set
  greppable (`*_experimental*`).
- **Lifecycle:** prototype in sandbox → validate against real data → promote the logic into the
  main ETL script (parameterised, see §3) → **mark the sandbox `_experimental` file for deletion**
  (add a top-of-file `# DELETE AFTER PROMOTION: <target main script>` banner and an entry in
  §7 below). The `_experimental` file is never imported by production code.
- **SQL views** are written directly into `sql_views/` (not sandboxed) — that is the registered
  view layer and already the right home.

### 0.2 Never-touch / careful-touch files
- `pipeline.py`, `enrich.py`, `normalise_join_key.py` — do not edit during prototyping. Wiring a
  promoted script into `pipeline.py` is a deliberate final step, reviewed separately.
- Parquet writers must keep the project convention: `compression="zstd"`,
  `compression_level=3`, `statistics=True` (memory `feedback_parquet_write_convention.md`).

### 0.3 UI boundary (logic firewall)
- No business metrics or JOIN/GROUP_BY/HAVING/WINDOW in page files or the data-access layer.
  House-aware aggregation belongs in `sql_views/`, surfaced through registered views.
- No inference in app copy (memory `feedback_no_inference_in_app.md`). Where a domain is empty for
  Senators *by nature* (Questions), state the factual reason, do not editorialise.

---

## 1. Current state inventory (validated 2026-06-01)

| Domain | Seanad data exists? | Exposed in UI? | Work for parity |
|---|---|---|---|
| Members list | ✅ `flattened_seanad_members.parquet` (60) | ❌ not in registry | Phase 1 |
| Register of Interests | ✅ `seanad_member_interests_combined.parquet` (1,935) | ✅ Dáil/Seanad toggle live | none |
| Committees / offices | ✅ both chambers in ETL | partial — verify page | Phase 1 (verify) |
| Votes | ❌ `chamber=dail` hardcoded | ❌ | Phase 2 (API ingest) |
| Payments | ❌ "to-deputies" PDFs only | ❌ | Phase 3 (PDF ingest) |
| Attendance | ❌ "deputies-verification" PDFs only | ❌ | Phase 3 (PDF ingest) |
| Questions (PQs) | ❌ TD-only instrument by nature | n/a | Phase 4 (empty-state copy) |

### 1.1 Key references (file:line)
- Members fetch (Dáil **and** Seanad already pulled): `flatten_members_json_to_csv.py` →
  `flatten_members_to_csv("dail")` + `("seanad")`; API in `members_api_service.py:42` (dail/34) and
  `:45` (seanad/27).
- Member registry (the chokepoint, **Dáil-only**): `sql_views/member_registry.sql:22` reads only
  `flattened_members.parquet`.
- Member Overview picker: `utility/pages_code/member_overview.py:104` (`_member_list`),
  uniqueness assumption at `:1074` and `:1267`.
- Interests union pattern to copy: `sql_views/member_interests_detail.sql` (UNION ALL + `house`).
- Interests page toggle to copy: `utility/pages_code/interests.py:189`.
- Committees ETL (both chambers): `committees_long_format_etl.py:47`.
- Votes fetch (Dáil-only): `services/votes.py:11` `build_vote_url()` hardcodes `chamber=dail`.
- Vote transform: `transform_votes.py` (gold parquet `house_number` ∈ {31,32,33,34}).
- Attendance parser: `attendance.py` (assumes "deputies" PDFs).
- Payments parser: `payments_full_psa_etl.py` (assumes "to-deputies" PDFs).
- Hard-coded PDF URL list: `pdf_endpoint_check.py` (all `…to-deputies…`).
- Pipeline orchestration chains: `pipeline.py:53-70` (`members`, `payments`, `attendance`,
  `interests`, `legislation`).

---

## 2. Data validation findings (already-extracted Seanad data)

Run via `polars` against the silver parquets on 2026-06-01.

### 2.1 `flattened_seanad_members.parquet` — CLEAN, ready to wire
- 60 rows, **0 nulls** in `full_name`, `party`, `constituency_name`, `unique_member_code`,
  `membership_start_date`, `year_elected`, `ministerial_office`.
- `dail_number` = `27`, `dail_term` = `27th Seanad` for all rows (this is the reliable
  current-house discriminator).
- **`constituency_name` holds the electoral panel, not a geographic constituency**:
  Labour Panel (11), Agricultural Panel (11), Nominated by the Taoiseach (11), Industrial &
  Commercial (9), Administrative (7), Cultural & Educational (5), University of Dublin (3), NUI (3).
  → **UI implication:** the "Constituency" label must read **"Panel"** (or "Panel / Nomination")
  for Senators. Do not relabel data; relabel at the view/UI layer by `house`.
- Party split: FF 19, FG 18, Ind 12, SF 6, Lab 2, Green 1, Aontú 1, SocDem 1.

### 2.2 `seanad_member_interests_combined.parquet` — CLEAN
- 1,935 rows, years 2020–2025, 55 distinct senators with declarations, **0 null** `full_name` /
  `unique_member_code`, `registration_status` all `registered`.
- Note: 55 of 60 current senators appear — the 6-year PDF span covers prior Seanad cohorts, and the
  current-master left-join keeps only matched current senators. Newly elected 2025 senators may have
  no historic declarations yet. Not a defect; document in provenance.

### 2.3 `unique_member_code` is NOT globally unique across houses ⚠️
- Format: `First-Last.{D|S}.{first-election-date}`. The `D`/`S` infix marks the chamber of
  **first election**, **not** the current house — e.g. Catherine Ardagh sits in the Dáil file with
  a `.S.` code; Chris Andrews sits in the Seanad file with a `.D.` code.
- **1 code collides across both files today: `Seán-Kyne.D.2011-03-09`** appears in both the Dáil and
  Seanad parquets. This breaks the "registry is unique on `unique_member_code`" assumption at
  `member_overview.py:1074`.
- **Decision required (see §6 Q2/Q3):** key the registry and all member lookups on the composite
  `(unique_member_code, house)`, OR mint a canonical upstream `member_id`. The composite key is the
  smaller change and is recommended for Phase 1.

---

## 3. Phased build plan

### Phase 0 — Feasibility checks (blocks Phase 2/3; do first)
1. Confirm Oireachtas Seanad sources exist and shape:
   - `GET /votes?chamber_type=house&chamber=seanad&date_start=…&outcome=` returns Seanad divisions.
   - Seanad payments PDF naming, e.g. `…parliamentary-standard-allowance-payments-to-senators…`
     **and** confirm the allowance scheme/columns (Senators may draw a different allowance than the
     TD PSA — column layout may differ).
   - Seanad attendance PDF naming, e.g. `…senators-verification-of-attendance-for-the-payment-of-taa…`.
2. Lock the `house` discriminator = `dail_number` (34 → `'Dáil'`, 27 → `'Seanad'`). Do **not** derive
   house from the `D`/`S` in the member code.
3. Resolve the §2.3 collision strategy (composite key recommended).

**Deliverable:** a short findings note appended to this doc (URLs that 200, a sample Senator
payment/attendance PDF saved to `data/bronze/pdfs/...` for parser inspection, one Seanad vote JSON
sample). No production code.

---

### Phase 1 — Registry becomes house-aware (foundation; low risk, high value)
**This phase alone makes Senators real members with the data that already exists.**

1. **SQL (direct to `sql_views/`):** rewrite `sql_views/member_registry.sql` to `UNION ALL` the two
   parquets, adding `house` (`CASE dail_number WHEN '27' THEN 'Seanad' ELSE 'Dáil' END`) and
   re-expressing `constituency` so it is panel-aware for Seanad (keep raw value; UI relabels).
   Mirror the proven `member_interests_detail.sql` structure. Inject the second parquet path the
   same way the registry already injects `{MEMBER_PARQUET_PATH}` (add `{SEANAD_MEMBER_PARQUET_PATH}`
   in `utility/data_access/member_overview_data.py`).
2. **Composite key:** add `house` to the registry primary identity; update
   `member_overview.py:104` SELECT, and the two lookups at `:112`/`:133` and the dedup assumptions at
   `:1074`/`:1267` to key on `(unique_member_code, house)`.
3. **Picker filter:** add a Dáil/Seanad segmented control to the member picker (copy the live
   pattern at `interests.py:189`). Default = Dáil (preserve current behaviour).
4. **Label layer:** "Constituency" → "Panel" when `house = 'Seanad'`; "TD"/"Deputy" → "Senator" in
   hero/labels for Seanad members.
5. **Verify committees** already render for a Senator (ETL has the data; confirm the page query
   isn't house-filtered).

**Outcome:** Senator profile shows identity + committees + interests; votes/payments/attendance/
questions show existing empty states (handled in Phase 4 copy).

**No sandbox needed** — this is SQL + page wiring, no new ETL.

---

### Phase 2 — Seanad votes (API; medium)
**Prototype:** `pipeline_sandbox/seanad_votes_etl_experimental.py`
- Parameterise the vote URL builder by chamber; fetch `chamber=seanad` with the same skip/limit
  pagination and `resultCount` assertion as `services/votes.py`.
- Tag rows with `house`. Validate division counts against `head.counts.resultCount`.
- Inspect Seanad division schema vs Dáil — confirm member URI / `unique_member_code` resolution
  works for senators (reuse `normalise_join_key` semantics; do not edit that module).

**Promote into main ETL:**
- Generalise `services/votes.py` `build_vote_url(chamber)` and run both chambers.
- Thread `house` through `transform_votes.py` into the gold vote-history parquet (currently
  `house_number` ∈ {31..34}; Seanad terms are `27`).
- Update `sql_views/vote_td_summary.sql` and `vote_member_detail.sql` + the Votes page to filter and
  label by house. Confirm copy doesn't over-claim Seanad division semantics (no government-formation
  votes in the Seanad).

**Then:** mark `seanad_votes_etl_experimental.py` for deletion (§7).

---

### Phase 3 — Seanad payments + attendance (PDF; highest effort/risk)
**Prototype (two files):**
- `pipeline_sandbox/seanad_attendance_etl_experimental.py`
- `pipeline_sandbox/seanad_payments_etl_experimental.py`

Work:
- Add Senator PDF URLs to `pdf_endpoint_check.py` (currently all `…to-deputies…`). This is the
  documented hard-coded-URL debt (`DAIL-161`); keep it minimal and explicit.
- **Inspect a real Senator PDF first** (Phase 0 deliverable). The TD attendance parser
  (`attendance.py`) has a documented quirk (two categorical day columns; see memory
  `project_attendance_audit_2026_05_26`). The Senator layout may differ and need its own tuning —
  prototype the parser against the real PDF before promoting.
- Senator allowance scheme may differ from the TD PSA — confirm columns/amounts; the gript.ie
  reconciliation baseline is **TD-only**, so there is **no external cross-check** for Senator
  figures (state this as a known limitation).
- Add `house` to the fact tables; join against the unified registry; add/extend the
  attendance/payments views to filter+label by house.

**Promote:** parameterise `attendance.py` and `payments_full_psa_etl.py` by chamber (do not fork);
wire into the `payments`/`attendance` chains in `pipeline.py:54-55` as a final reviewed step. Then
mark both `_experimental` files for deletion (§7).

**Risk flags:** new bronze sources + manual URL maintenance; parser uncertainty until a real PDF is
seen; no external reconciliation for Senator amounts; separate allowance scheme.

---

### Phase 4 — Polish & guardrails
- Member Overview empty states: Questions has no Seanad data **by nature** — show civic-voice copy
  ("Senators raise Commencement Matters, not Parliamentary Questions"), not a bare empty card.
  No inference; factual only.
- Cross-page label audit: hardcoded "TD"/"Deputy" → house-aware. Grep `\bTD\b|Deputy|Deputies`.
- `doc/DATA_LIMITATIONS.md`: update the per-domain coverage table and add Senator allowance /
  reconciliation caveats.
- Provenance footers: note Seanad interests cover 2020–2025 and 55/60 current senators.

---

## 4. Sequencing & stop points
- **Phases 0 → 1 are the high-value, low-risk core.** If effort must be capped, ship Phase 1 and
  stop: Senators become real members with identity + committees + interests.
- Phase 2 (votes) is self-contained API work.
- Phase 3 (payments/attendance) is the long pole and should be gated on Phase 0 confirming the
  sources exist and a real PDF being inspected.

---

## 5. Data validation checklist (run at each phase)

**Phase 1 (registry):**
- `v_member_registry` row count = 176 (Dáil) + 60 (Seanad) = 236, minus any intentional dedup.
- Exactly one `(unique_member_code, house)` per registry row (composite key is unique).
- The Seán Kyne collision yields **two distinct rows** (one per house), not a merged/duplicated one.
- Every Seanad row has non-null `member_name`, `party_name`, `constituency` (panel), `house='Seanad'`.
- Picker filter: selecting "Seanad" lists 60; "Dáil" lists 176.

**Phase 2 (votes):**
- Seanad fetch: `len(results) >= head.counts.resultCount` (no silent truncation).
- Gold parquet contains `house_number` 27 rows with non-null `unique_member_code`.
- Senator vote rows resolve to a registry member (join-failure count = 0 or enumerated).

**Phase 3 (payments/attendance):**
- Parsed Senator rows: name-resolution failure count enumerated and ≤ agreed threshold.
- Per-Senator totals are non-negative and within the published PDF period bounds.
- Fact-table `house` column present; no Dáil rows mislabelled and vice-versa.
- (No external reconciliation available for Senators — record this.)

**Cross-cutting:**
- All new parquet writers use zstd / level 3 / statistics=True.
- No `read_parquet` in page files or data-access layer beyond the registered-view pattern.

---

## 6. Open decisions (need user input)
1. **Allowance scheme:** do Senators draw the same PSA/TAA structure as TDs, or a different scheme
   needing its own columns? (Affects Phase 3 parser + view design.)
2. **Registry key:** composite `(unique_member_code, house)` (recommended, smaller) vs a canonical
   upstream `member_id`?
3. **Dual-house people** (former TD now Senator, e.g. Seán Kyne): one merged profile or two
   house-scoped profiles? (Drives the key design and the picker UX.)
4. **Default picker house:** keep Dáil default (recommended) or show "All Oireachtas"?

---

## 7. `_experimental` sandbox tracker (delete-after-promotion)

| Sandbox file | Promotes into | Status |
|---|---|---|
| `pipeline_sandbox/seanad_votes_etl_experimental.py` | `services/votes.py` + `transform_votes.py` | not started |
| `pipeline_sandbox/seanad_attendance_etl_experimental.py` | `attendance.py` | not started |
| `pipeline_sandbox/seanad_payments_etl_experimental.py` | `payments_full_psa_etl.py` | not started |

Each file carries a top banner: `# DELETE AFTER PROMOTION: <target>`. Remove the file and tick this
table once the logic is parameterised into the main ETL and wired through `pipeline.py`.

---

## 8. Test suite & unit tests

Follow existing patterns in `test/` (pytest; see `conftest.py`, `fixtures/`). New tests live
alongside the domain they cover. Sandbox `_experimental` files get **lightweight** tests during
prototyping; the **durable** test suite targets the promoted production code.

### 8.1 Phase 1 — registry & page wiring
Extend / add:
- **`test/test_sql_views.py`** — `v_member_registry`:
  - returns both houses; Seanad count = 60, Dáil count = 176.
  - `house` column present with exactly {`Dáil`, `Seanad`}.
  - `(unique_member_code, house)` is unique (no duplicate identity rows).
  - the Seán Kyne code yields exactly 2 rows (one per house).
  - no null `member_name` / `party_name` / `constituency` for Seanad rows.
- **`test/test_silver_parquet.py`** — schema-lock `flattened_seanad_members.parquet`
  (mirror the existing Dáil assertions): required columns present, `dail_number == '27'`,
  zero nulls in the key columns listed in §2.1.
- **`test/test_page_imports.py`** — `member_overview` still imports/builds with a Seanad member
  selected (smoke). Add a fixture member from the Seanad registry.
- **New `test/test_member_registry_house.py`** (unit) — pure-function tests for the house-label /
  panel-label mapping and the composite-key lookup helper (no Streamlit, no DB).

### 8.2 Phase 2 — Seanad votes
Extend **`test/test_services_votes.py`**:
- `build_vote_url("seanad")` contains `chamber=seanad` and the required trailing `&outcome=`.
- pagination assertion fires on a stubbed short page (drift raises).
- transform: a Seanad division fixture yields rows with `house_number == 27` and a resolvable
  `unique_member_code`.
- view test (in `test_sql_views.py`): `v_vote_*` includes Seanad rows when present and labels house.

### 8.3 Phase 3 — Seanad payments & attendance
- **`test/test_member_interests.py`** is the parser-test template to copy (golden-row approach).
- **New `test/test_seanad_attendance.py`** — feed a small fixture derived from a real Senator
  attendance PDF text dump (store under `test/fixtures/`); assert parsed names, period, counts,
  and `house='Seanad'`. Include the column-layout quirk as an explicit fixture case.
- **New `test/test_seanad_payments.py`** — analogous; assert per-Senator totals, period bounds,
  non-negative amounts, `house` tag. Mirror `test_payments_golden.py` structure.
- **`test/test_silver_parquet.py` / `test_silver_layer.py`** — schema-lock the new fact tables;
  assert `house` column and zstd/level-3/statistics writer convention.

### 8.4 Cross-cutting / regression guards
- **`test/test_requirements_sync.py`** — unaffected unless new deps (none expected).
- **Logic-firewall guard** (extend whatever enforces it, e.g. `test_silver_layer`/review tooling):
  assert no new `read_parquet` / JOIN in page files or `utility/data_access/`.
- **Empty-state copy** (Phase 4): a render smoke test asserting the Questions card shows the
  Commencement-Matters explanation for a Seanad member, and that no inference phrasing leaks.
- Run the existing full suite as the regression baseline before/after each phase (current baseline
  noted in memory `project_reorg_plan` = 258/11; re-confirm at build time).

### 8.5 Test data / fixtures
- Add Seanad fixtures under `test/fixtures/`: a trimmed Seanad members parquet, a small Seanad
  interests slice, one Seanad vote JSON, and **text dumps** of one Senator attendance + one payments
  PDF (text only, not the binary, to keep the repo light).
- Keep fixtures minimal and deterministic; no network in unit tests (the API/PDF fetch is mocked,
  matching `test_http_engine.py` / `test_services_votes.py` patterns).

---

## 9. UI display plan — reuse the TD Member Overview (reconciled against validated data)

**Principle:** every per-member section in `member_overview.py` already renders generically off
registered SQL views keyed by `unique_member_code`. So the SAME page renders a Senator once
(a) senators are in the registry, and (b) the views include Senator gold. **No new page, no new
components.** Net new UI work = 1 registry union + 1 picker toggle + 3 view unions + label/empty
tweaks.

### 9.1 What the data supports (validated 2026-06-01 against pipeline_sandbox/_seanad_output/)

| Section | Senator data | Coverage | UI outcome |
|---|---|---|---|
| Identity / hero | members parquet | 60/60 | full — name, party, **panel** (not constituency), house |
| Votes (summary + detail) | `current_seanad_vote_history` | **60/60**, 0 null-metadata, 18,571 rows | full vote record (e.g. Ahearn 467 votes) |
| Attendance (by year) | `seanad_attendance_by_year` | **58/60** | full; 2 gaps = name-key (Craughwell middle initial, Ní Chuilinn Irish-name) — same class as TDs |
| Payments (rankings + detail) | `seanad_payments_full_psa` | **59/60** | full; 1 gap = Craughwell middle-initial key |
| Committees | committees long-format (both chambers) | full | already chamber-aware |
| Interests | `v_member_interests_detail` | already live | already has Dáil/Seanad toggle |
| **Questions (PQs)** | none — TD instrument | n/a | **civic-voice empty state** (Senators raise Commencement Matters) |
| Constituency demographics | none (panels aren't geographic) | n/a | hide for Seanad / show "Panel — national" note |

### 9.2 Layer 1 — registry (makes senators selectable)
- `sql_views/member_registry.sql`: `UNION ALL flattened_members + flattened_seanad_members`, add
  `house` (`CASE dail_number WHEN '27' THEN 'Seanad' ELSE 'Dáil' END`). Keep panel in
  `constituency`. Mirror `member_interests_detail.sql`.
- `utility/data_access/member_overview_data.py`: inject `{SEANAD_MEMBER_PARQUET_PATH}` next to the
  existing `{MEMBER_PARQUET_PATH}` substitution.
- Identity key must become `(unique_member_code, house)` — codes collide across houses (Seán Kyne).
  Update `_member_list` SELECT and the two lookups (`member_overview.py:104/112/133`) and the
  uniqueness assumptions at `:1074/:1267`.
- Picker: add a Dáil/Seanad segmented control (copy `interests.py:189`); default Dáil.

### 9.3 Layer 2 — domain views read Senator gold
Each view filters by the selected member's `unique_member_code`, so a plain `UNION ALL` of the two
houses' gold is sufficient — the senator's code resolves to senator rows automatically:
- `vote_td_summary.sql` / `vote_member_detail.sql`: read Dáil **+** `current_seanad_vote_history`.
- `payments_base.sql`: union `seanad_payments_full_psa`.
- attendance views: union `seanad_attendance_by_year` + enriched senator attendance.
- `member_overview_data.py`: add the Senator gold paths to the substitution dicts.

### 9.4 Layer 3 — labels & empty states (no new components)
- `constituency` label → **"Panel"** when `house = Seanad` (data already carries the panel string).
- "TD"/"Deputy" → **"Senator"** in hero + section copy (house-aware helper).
- Questions card: render the existing empty-state component with civic-voice copy — factual, no
  inference ("Senators raise Commencement Matters, not Parliamentary Questions").
- Constituency-demographics card: suppress for Seanad (panels have no Census denominator).
- Hero chip row: party + panel + house badge.

### 9.5 Standalone pages (later, optional)
Votes / Payments list pages can gain a Dáil/Seanad filter reusing the same card components once the
views are house-aware — not required for the per-member parity milestone.

### 9.6 De-risking preview
Before/independent of full ETL promotion, point the member-overview views at the sandbox
`_seanad_output/` parquets and seed the 60 senators into the registry to render a **live Senator
profile in the existing UI** — a throwaway spike proving the reuse with zero production ETL risk.
</content>
</invoke>
