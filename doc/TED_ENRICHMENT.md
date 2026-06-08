# TED enrichment — current state and how to factor it in

**Status:** reference + forward plan. Generated 2026-06-08.
**Scope:** how Tenders Electronic Daily (TED, the EU Official-Journal procurement
register) is *already* enriched and wired in Dáil Tracker, and the concrete
enrichment moves that remain. Companion to:

- `PROCUREMENT_MASTER.md` — authoritative procurement plan (TED uplift = Stage B, **shipped 2026-06-06**).
- `ENRICHMENTS.md` §B.2 — the original "is this worth investigating?" card for TED.
- `ted_data_ingestion_links.md` — every official TED ingest endpoint (API, bulk XML, SPARQL, schema).

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

### 3.5 Historical backfill — **mostly a one-line win** (verified 2026-06-08)
Current silver is 2024+ **only by choice**: the extractor hard-codes
`QUERY = "... AND publication-date>=20240101"`. It is **not** an API limitation.

**Verified by probing `api.ted.europa.eu/v3/notices/search` directly:**

| range | how to get it | cost |
|---|---|---|
| **2016–2023 Irish awards** | **widen the date filter to `>=20160101`** — the API serves these with the *identical* eForms field set (buyer, winner, `tender-value`, CPV, `winner-identifier`). | **~1-line change + re-pull.** No new parser, no bulk download. |
| pre-2016 (2011–2015 … 1993) | bulk legacy TED_EXPORT XML packages (proven parseable — see below) | high — all-EU package downloads + a legacy parser; low relevance |

**The API wall is 2016, not 2024.** Year-by-year `can-standard` counts:
2014 = 0, 2015 = 0, **2016 = 550**, 2017 = 1,019, 2018 = 1,190, 2019 = 1,301.
No Irish notice of *any* type is indexed before 2016. So a date-filter widen takes
silver from ~13k (2024+) to roughly **+5,000–8,000** more Irish award notices
(2016–2023) for almost no work.

**Implementation (cheap win):** in `extractors/ted_ireland_extract.py`, change the
`QUERY` constant to `publication-date>=20160101`, run with `--refresh`, re-run the
view layer. Watch the bronze cache size (~21k notices vs 13k) and re-verify the
CRO-match rate holds on older names.

**Pre-2016 bulk lane (only if older history is wanted) — proven feasible but costly.**
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
    than the API. Another reason to use the API for ≥2016 and reserve bulk for
    pre-2016 only.

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
