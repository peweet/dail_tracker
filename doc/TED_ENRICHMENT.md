---
tier: CONTEXT
status: LIVE
domain: procurement
updated: 2026-06-08
supersedes: []
read_when: checking what TED enrichment already exists before proposing new TED ingestion/enrichment work
key: CONTEXT|LIVE|procurement
---

# TED enrichment — current state and how to factor it in

**Status:** reference + forward plan. Generated 2026-06-08.
**Scope:** how Tenders Electronic Daily (TED, the EU Official-Journal procurement
register) is *already* enriched and wired in Dáil Tracker, and the concrete
enrichment moves that remain. Companion to:

- `PROCUREMENT_MASTER.md` — authoritative procurement plan (TED uplift = Stage B, **shipped 2026-06-06**).
- `ENRICHMENTS.md` §B.2 — the original "is this worth investigating?" card for TED.
- `archive/ted_data_ingestion_links.md` — every official TED ingest endpoint (API, bulk XML, SPARQL, schema).

> **TED is not an unbuilt idea.** It is a production *silver* dataset with SQL
> views, a page tab, core queries, and tests already in the repo. This doc exists
> to record what's done and to scope the **remaining** enrichment, not to re-propose
> ingestion.

---

## 1. What already exists (the baseline)

| Layer | Artifact | Notes |
|---|---|---|
| Extractor | `extractors/ted_ireland_extract.py` | TED Search API v3 (zero-auth), `buyer-country=IRL AND notice-type=can-standard AND publication-date>=20240101`. Headless-safe; skips gracefully on API outage. |
| Bronze | `data/bronze/ted/ted_ie_awards_raw.json` | Raw API capture, regenerable. |
| Silver | `data/silver/parquet/ted_ie_awards.parquet` | **13,126 rows** (notice × winner), 2023–2026; gitignored, regenerable. |
| Meta | `data/_meta/ted_ie_awards_coverage.json` | Schema v1, coverage + CRO match rate. |
| Views | `sql_views/procurement_ted_awards.sql` → `v_procurement_ted_awards`; `sql_views/procurement_ted_supplier_summary.sql` → `v_procurement_ted_supplier_summary` | Read silver directly (no gold duplication, same precedent as lobbying-overlap). Strip the `_NNNNN` eForms org-id suffix from winner names in-view. |
| Core queries | `dail_tracker_core/queries/procurement.py` | `ted_corpus_stats`, `ted_supplier_summary`, `ted_for_supplier` — Streamlit-free, return `QueryResult`. |
| Data access | `utility/data_access/procurement_data.py` | `fetch_ted_corpus_stats_result`, `fetch_ted_supplier_summary_result`, `fetch_ted_for_supplier_result`. |
| Page | `utility/pages_code/procurement.py` | "EU-level awards (TED)" tab + per-firm TED cross-reference panel on the supplier profile. |
| Pipeline | `pipeline.py` — `("ted", "extractors/ted_ireland_extract.py")` | Committed chain; runs after the procurement/lobbying chains. |

**Verified value figures (from `PROCUREMENT_MASTER.md` §1):**

| measure | value | note |
|---|---|---|
| naive Σ of every TED `value_eur` | absurd | never display as a total |
| └ 375 pan-EU outliers | €586bn | GÉANT-type research frameworks; Ireland is one of dozens of participants — already flagged `is_pan_eu_outlier` |
| **sum-safe** (excl. outliers) | **€5.82bn** | award-grain, `value_safe_to_sum=true` |
| TED winners also in eTenders (by norm name) | 4,207 / 6,391 (**66%**) | ⇒ **never union/sum**; cross-reference per firm |

---

## 2. Enrichment already applied to TED

1. **Winner → CRO match** (in `ted_ireland_extract.py`): two-step — first by
   `winner_identifier` digits against CRO `company_num`, then by normalised name
   against CRO `name_norm`; method flag `identifier` / `name` / `none`. Match rate
   **~65%** (coverage meta) / ~69% (master plan, post-name-pass). A CRO match
   *upgrades* `supplier_class` (`sole_trader_or_individual` → `company`) and the
   `privacy_status` gate, because real firms often drop suffix words in TED naming.
2. **Value taxonomy tagging:** every row carries `value_kind`
   (`contract_award_value` vs `framework_or_dps_ceiling`) and a derived
   `value_safe_to_sum`. Pan-EU outliers flagged `is_pan_eu_outlier`.
3. **Privacy quarantine:** `supplier_class` + `privacy_status`; sole-traders /
   individuals withheld from rankings (company-class only).
4. **In-view name cleanup:** the `_NNNNN` org-id suffix stripped for display and
   to recover a join-norm for cross-reference.

---

## 3. Remaining enrichment moves (the actual backlog)

Ranked by leverage ÷ cost. All must keep the §4 honesty rails.

### 3.1 TED ↔ lobbying overlap — **cheap, high value, do next**
The lobbying cross-reference (`extractors/procurement_lobbying_xref.py`) currently
matches **eTenders** winners against lobbying clients/registrants by normalised
name. TED winners are *not* yet in it. Extend the same exact-norm-match pattern to
`v_procurement_ted_awards` so a TED winner's profile shows the same neutral
co-occurrence card ("appears on both registers" — never "influenced").
- **How:** add a TED branch to the xref (or a sibling view joining
  `v_procurement_ted_supplier_summary.winner_join_norm` to the lobbying norm key).
- **Risk:** L. Honesty rail: co-occurrence ≠ causation; reuse existing caveat copy.

### 3.2 Suffix cleanup at the extractor source — **tidy, blocks drift**
The `_NNNNN` suffix is stripped *in-view* only. Clean it at the silver source in
`ted_ireland_extract.py` so the join-norm is canonical everywhere and the view
logic simplifies. (Already noted as a follow-up in `PROCUREMENT_MASTER.md` Stage B.)

### 3.3 CRO match-rate uplift — **incremental**
~35% of winners are still unmatched. Levers: lean harder on eForms organisation
identifiers (national reg numbers carried in the notice), tighten name
normalisation, and reconcile against the CRO bulk register's alias table. Each
percentage point upgrades more rows from `review_personal_data` → `ok` and from
sole-trader → company (so they become rankable).

### 3.4 TED → realised-spend linkage — **gated behind Stage D**
`extractors/procurement_award_spend_link.py` already joins TED + eTenders awards to
public-body spend facts on a hybrid CRO-or-name key, **as a sandbox**. Promoting it
to production is blocked by the same privacy/`vat_status` gate as the payments tier
(`PROCUREMENT_MASTER.md` Stage D). Keep it sandbox until that gate clears.
- **Honesty rail:** award ceiling vs realised spend are **different tiers** — show
  side by side, never summed.

### 3.5 Historical backfill — **the API backfill is hollow; real backfill needs the bulk lane** (verified 2026-06-08)
Current silver is 2024+ **by choice**, not by API limit — but the choice is correct,
because the pre-2024 API data is missing the one thing this silver is built around:
**the winner.**

**Verified by probing `api.ted.europa.eu/v3/notices/search` directly** (250-notice
samples per year, fields checked against the API's full 1,373-field list):

| field | 2018 | 2021 | 2024 |
|---|---|---|---|
| `buyer-name` | 100% | 100% | 100% |
| `total-value` | 83% | 62% | 80% |
| **`winner-name`** | **0%** | **0%** | 51% |
| `winner-identifier` | 0% | 0% | 55% |
| `organisation-name-tenderer` | 0% | 0% | 55% |
| `tender-value` | 0% | 0% | 2% |

**Why a date-widen alone is the wrong move:** the API *does* reach back to 2016
(wall: 2016=550, 2017=1,019, 2018=1,190, 2019=1,301 awards; nothing before 2016),
and the award value is recoverable via `total-value` (not `tender-value`). **But the
winner name/identifier is genuinely 0% for legacy notices** — it isn't a wrong
field name (every winner/org field in the API was tested). This silver is
winner-centric: winner→CRO match, supplier rankings, per-winner `value_safe_to_sum`.
Backfilling ~10k winner-less rows corrupts that grain (and crashes the build on the
all-null `winner_name` column). **So the date filter stays at 2024.**

**The only source of pre-2024 winner+value is the bulk legacy TED_EXPORT lane** —
which I verified *does* carry the winner (e.g. *Enovation Solutions Ltd*, 2018) where
the API drops it. So the bulk parser, dismissed in earlier drafts as redundant, is in
fact the **only** path to a usable historical backfill.

**Optional cheap by-product (not the silver):** the API *can* cheaply feed a
*separate, buyer-side* historical layer for 2016–2023 — `buyer-name` (100%),
`total-value` (~62–83%), CPV, and `procedure-type` (100%). That answers "which Irish
authorities published the most/largest EU award notices, and via what procedure type,
over time" — with **no winner**. It must be a distinct artifact, never merged into the
winner-centric silver. Build only if buyer-side trend is wanted.

**Bulk legacy lane (the real backfill) — proven feasible but costly.**
Probed the bulk packages directly:
- Daily/monthly XML packages **download via plain `curl`, no bot-gating** (~6–9 MB
  per daily OJ S issue; `Content-Type: application/gzip`).
- A daily issue is **all-EU** (1,100–3,400 notices); Irish notices are only ~2–4
  per issue → you download the whole EU to filter to IE (~17 GB for 2011–2015).
  Use **monthly** packages (12/yr) over daily (~250/yr) to cut request count.
- **Format reality (matters for the parser):**
  - Pre-2024 packages = uniform **TED_EXPORT R2.0.8/R2.0.9** envelope. Irish notices
    found by `ISO_COUNTRY VALUE="IE"`; they carry everything needed —
    `TD_DOCUMENT_TYPE CODE="7"` (award), `OFFICIALNAME` (buyer + winner),
    `VAL_TOTAL`/`VAL_ESTIMATED_TOTAL` (+`CURRENCY`), `CPV_CODE`, and `NATIONALID`
    (often the CRO number, e.g. *Enovation Solutions Ltd* `6348876N`).
  - **Gotchas:** values use space thousand-separators (`"468 500"`); CPV codes
    repeat per lot (dedupe); `VALUE@TYPE` distinguishes `ESTIMATED_TOTAL` (→
    `estimate_advertised`) from `PROCUREMENT_TOTAL` (→ `contract_award_value`).
  - The **2024 bulk package is a mix** of converted-TED_EXPORT *and* native eForms
    UBL (`cac:`/`cbc:`, no `TD_DOCUMENT_TYPE`) — so bulk-parsing 2024+ is *harder*
    than the API. Clean split: **API for 2024+** (winner present), **per-notice XML
    for 2016–2023** (the only place the winner exists).
  - **Don't download the all-EU bulk.** Verified 2026-06-08: you don't need the
    14 GB of all-EU packages. The API cheaply enumerates the ~10k Irish award
    publication-numbers (2016–2023), and each notice's **individual XML**
    (`https://ted.europa.eu/{lang}/notice/{publication-number}/xml`, plain HTTP,
    `application/xml`, ~300 KB) carries the full `AWARDED_CONTRACT → CONTRACTOR`
    roster with `OFFICIALNAME` + `NATIONALID` + per-contract value — see the
    worked 2018 OGP framework example in §6. Targeted, ~3–5 GB, no EU-wide filtering
    waste. **This is the recommended winner-backfill path (full spec in §6).**

> **The buyer-side layer is built (2026-06-08):** `extractors/ted_ireland_buyer_history_extract.py`
> → `data/silver/parquet/ted_ie_buyer_history.parquet` (one row per notice, 2016+,
> buyer + total-value + CPV + procedure-type, **no winner**). It is a strict sibling
> to the winner silver and must never be merged or summed with it. The winner
> backfill in §6 remains the separate, larger build.

### 3.6 TD interest × TED award — **deep investigation, last**
RoMI declared shareholdings → CRO → TED/eTenders winners: surface where a TD's
declared interests received public contracts during their term. High editorial
value, highest matching/privacy care. Depends on 3.1 + 3.3 being solid first.
(Already flagged as a deep-investigation pattern in `PROCUREMENT_MASTER.md`.)

---

## 4. Honesty rails (non-negotiable — carried from `PROCUREMENT_MASTER.md` §3)

1. **Lead with counts, not euros** — `n_awards` is the trustworthy metric.
2. **Verb on every figure** — "awarded €X", "up to €X (shared framework ceiling)";
   never a bare €.
3. **One tier per section** — never blend `contract_award_value` /
   `framework_or_dps_ceiling` / committed / paid.
4. **Registers are siblings, never summed** — eTenders + TED overlap 66% by name;
   a firm's profile shows both, labelled, never added.
5. **Pan-EU default-hidden** — the 375 outliers (€586bn) sit behind a "show pan-EU
   frameworks" toggle that reveals the shared-ceiling mirage.
6. **Company-class only in rankings** — sole-traders/individuals quarantined.
7. **Co-occurrence ≠ causation** — lobbying overlap is "appears on both registers".
8. **Provenance on everything** — register named (eTenders national vs TED EU),
   landing link, `retrieved_utc`.

---

## 5. Architectural pattern for any new TED enrichment

- **Row-level enrichment** (CRO, name, privacy, value tags) → in the **extractor**,
  written to silver.
- **Cross-references and aggregates** (lobbying overlap, supplier summary,
  concentration) → as **SQL views reading silver directly** — no gold-parquet
  duplication, no gitignore dance (the extractor's own design: "gold only when a
  view exposes it").
- **No business logic in the page** — core/query/view computes; the page renders.
  Firewall: no `read_parquet` / JOIN / GROUP BY / window in `utility/pages_code/`.
- **Tests** at each layer (core query contract + view + UI smoke), as Stage B did.

---

## 6. Pre-2024 winner backfill via per-notice XML — ✅ BUILT 2026-06-08

**Result:** `extractors/ted_ireland_winner_history_extract.py` →
`data/silver/parquet/ted_ie_winner_history.parquet` — **23,263 winner rows across
10,667 notices (2016–2023)**; company 16,572 / sole-trader 4,947 / unknown 1,261 /
foreign 483; **CRO-matched 50%**; only 2/10,669 notices unrecovered (persistent 429).
Combined with the 2024+ API silver (`ted_ie_awards.parquet`, 13,230 rows) this gives a
**~36k-row winner history spanning 2016–2026**. Shares `extractors/ted_enrich.py` with
the API lane (identical classification/CRO/value flags) and reuses the buyer layer for
notice-level facts. **Next:** a `sql_views/ted_*.sql` view to `UNION` the two lanes
(select shared columns; the API lane's eForms competition fields are null pre-2024).

**Goal (original spec):** recover the winner + winner-`NATIONALID` + per-contract value
for Irish award notices **2016–2023**, which the Search API drops (§3.5), so the
winner-centric silver can extend back from 2024+ to 2016+ at the *same (notice × winner)
grain*.

**Why per-notice XML, not bulk packages.** Both carry the legacy `TED_EXPORT`
envelope with the full winner roster. But bulk = all-EU (~14 GB, then filter to ~2–4
Irish/issue); per-notice = targeted (~10k fetches, only Irish). Verified the
per-notice endpoint returns the winner where the API JSON does not.

**Worked example (verified):** `GET https://ted.europa.eu/en/notice/625-2018/xml`
→ 200, `application/xml`, 321 KB. The 2018 OGP fire-safety framework parses to **19
winners** — *Allied Fire Protection Ltd*, *Apex Fire*, *Chubb Ireland Ltd*, *G4S
Secure Solutions (Ire) Ltd*… — each under `AWARDED_CONTRACT → CONTRACTOR` with
`OFFICIALNAME`, `NATIONALID` (CRO `3213514AH`, `6365998B`… + VAT `IE4809551I`), and
per-contract `VALUE` (€2.688m, €0.672m…) plus the framework `VAL_TOTAL` €27m
(`TYPE="PROCUREMENT_TOTAL"`).

**Pipeline:**
1. **Enumerate** Irish award publication-numbers 2016–2023 via the existing API
   query (`buyer-country=IRL AND notice-type=can-standard`, paginate). ~10k PNs.
   (The buyer-side layer from §3.5 already produces this list — reuse it.)
2. **Fetch** each `…/en/notice/{pn}/xml` to a bronze cache (`data/bronze/ted/notices/`),
   resumable, polite rate-limit (429s observed — backoff + cap concurrency), skip
   already-cached. ~3–5 GB total.
3. **Parse** the `TED_EXPORT` envelope per notice → rows at (notice × `CONTRACTOR`):
   - winner ← `AWARDED_CONTRACT/…/CONTRACTOR//OFFICIALNAME`
   - winner_identifier ← sibling `NATIONALID` (feed the existing `clean_identifier`
     → CRO `company_num` match; CRO numbers and VAT both appear)
   - value ← per-`AWARDED_CONTRACT` `VALUE`; notice total ← `VAL_TOTAL`
   - buyer ← contracting-body `OFFICIALNAME`; CPV ← `CPV_CODE` (**dedupe** — repeats
     per lot); date ← `DATE_PUB` / dispatch
   - n_winners ← count of `CONTRACTOR` in the notice → `is_multi_supplier_framework`
     (this restores the *real* framework flag the buyer-side layer can't compute)
4. **Map onto the existing silver schema** so the rows union cleanly with the 2024+
   API rows: reuse `supplier_class` classification, `COMPANY_SUFFIX`/`FOREIGN_FORM`
   regexes, the CRO join, the value-flag block, and `value_safe_to_sum` from
   `ted_ireland_extract.py` (factor those into a shared module rather than copy).
5. **Tag provenance**: `source_lane = "per_notice_xml"` vs `"api"` so the two eras
   are auditable and the eForms-only competition fields stay null pre-2024.

**Parser gotchas (verified):** legacy values use space thousand-separators
(`"468 500"`); CPV repeats per lot; country is `ISO_COUNTRY VALUE="IE"` (2-letter) in
legacy vs `<COUNTRY>`/UBL in 2024 — irrelevant here since PNs are pre-filtered to IE;
multilingual notices inflate file size (parse one `FORM_SECTION`, prefer `LG="EN"`);
some `NATIONALID` are `N/A` (fall back to name-norm CRO match, as today).

**Effort:** medium. The fetch+cache+parse harness is the new work (~a day);
classification/CRO/value logic is reused. **Decide before building:** is supplier-side
history pre-2024 worth ~10k fetches + a parser? It deepens concentration/trend and
the TD-interest×award investigation (§3.6); it is *not* needed for the buyer-side
trends (§3.5 ships those now).

**Test fixtures:** sample legacy packages + the 625-2018 notice XML are kept in
`c:\tmp\ted_probe\` (2014/2018 daily packages, `notice_625_2018.xml`) — use them as
the parser's golden fixtures.

---

## 7. Poller architecture — remodeled on the open-source / official pattern (2026-06-08)

**Problem found:** both TED extractors had a bespoke `pull()` loop using
`paginationMode=PAGE_NUMBER`, which the TED API **hard-caps at 15,000 notices**, and
they self-capped lower (`PAGE_CAP=40` → 10k) with **no check against the declared
total** — so any pull over the cap *silently truncated*. This is exactly what bit the
2016+ buyer pull (≈16k expected, 10k landed) and crashed the build downstream.

**What others do (treated as superior to the bespoke loop):**

| Source | Pattern | Take |
|---|---|---|
| TED official ([ODS search-api](https://docs.ted.europa.eu/ODS/latest/reuse/search-api.html), [reusers Q&A](https://op.europa.eu/en/web/ted-reusers-workshops/questions_and_answers_2023_12_14)) | `paginationMode=ITERATION` + `iterationNextToken` → **no notice limit** | ✅ adopt |
| `flexponsive/tap-eu-ted` (Singer tap) | legacy `q/pageNum/pageSize`, **no retry** | ⚠️ outdated/fragile — don't copy |
| project `services/http_engine.py` + `member_paginated.py` | backoff on 429/5xx + **assert vs declared total** | ✅ reuse the robustness |

**The remodel:** new `services/ted_search.py` — a single reusable paginator that
combines TED's official ITERATION scroll with the project's own retry + truncation-
assertion idiom:
- ITERATION mode, `limit=250`; follows `iterationNextToken` until exhausted.
- Exponential backoff on 429 + 5xx + connection/timeout (mirrors `http_engine.py`).
- **Asserts** `len(notices) >= totalNoticeCount` — the anti-silent-truncation guard
  (mirrors `member_paginated.py`). `max_pages` gives a smoke-test bound that *skips*
  the assertion (explicit partial pull).
- Verified 2026-06-08: response carries `notices`, `totalNoticeCount`,
  `iterationNextToken`, `timedOut`; a 3-page smoke pull returned 750 notices / 750
  unique PNs (token advances correctly).

Both `ted_ireland_extract.py` and `ted_ireland_buyer_history_extract.py` now call
`fetch_ted_search(...)` instead of their own loops. `PAGE_NUMBER`, `PAGE_CAP`, the raw
`requests` calls, and the per-extractor headers are gone.

**Operational caveat (separate from this code):** running the *full* extractors still
requires a healthy `import polars`, which **hangs on this Windows box when WMI is
wedged** (see `[[feedback_polars_wmi_import_hang]]`). That import hang — not the
pagination — is what stalled the earlier runs and caused the python-process pileup.
Fix WMI (restart `Winmgmt` / reboot) or use the `platform.machine` pre-import patch
before doing the full 2016+ buyer pull. The paginator itself is polars-free and runs
fine.
