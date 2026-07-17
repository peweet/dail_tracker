---
tier: PLAN
status: SUPERSEDED
domain: procurement
updated: 2026-07-17
supersedes: []
superseded_by: extractors/disclosed_bq_po_extract.py
read_when: historical only — this design SHIPPED; the live ingest is extractors/disclosed_bq_po_extract.py + disclosed_bq_po_newbodies_extract.py
key: PLAN|SUPERSEDED|procurement
---

> **⚠️ SUPERSEDED (2026-07-17).** The "NOTHING here ingests" note below is no longer true — this
> plan was **built**. Live ingest: **`extractors/disclosed_bq_po_extract.py`** (+ `_newbodies_extract.py`).
> Current state: [[project_disclosed_national_po_dataset]]. Kept for the build-history/call-chain map.

# Disclosed national "PO/payments over €20,000" — integration plan (DESIGN ONLY)

_Author: scoping pass 2026-06-22. This is an engineering map + plan. NOTHING here ingests,
modifies gold/silver, runs an extractor, or touches git. Provenance/licensing is assumed
cleared (out of scope). The disclosed source is `data/raw_bq/bq-results-20260619-122315-1781871808837.csv`
(582,119 rows, 216 bodies, 2011-q1..2026-q1; cols `PO, Supplier, Total, Description, QTR, Year, entity, year_quarter`)._

---

## 1. How `procurement_payments_fact` (gold) is built today — the call chain

The fact is **not** written by one extractor. It is a **Stage-D consolidation** that unions
several per-source SILVER facts (each emitted by its own extractor on a shared schema) and
backfills the regime/CRO/spend_category columns at the fold. The pipeline order
(`pipeline.py`, the `STEPS` list) is:

```
public_body_payments   -> extractors/procurement_public_body_extract.py     -> silver public_payments_fact.parquet
hse_tusla_payments     -> extractors/procurement_hse_tusla_materialize.py   -> silver hse_tusla_payments_fact.parquet
la_payments            -> extractors/procurement_la_payments_extract.py     -> silver la_payments_fact.parquet
procurement_consolidate-> extractors/procurement_payments_consolidate.py    -> GOLD procurement_payments_fact.parquet
```

(Other silver source facts folded by the consolidator: `nta_payments_fact`, `nphdb_payments_fact`,
`seai_payments_fact`, `dept_readingorder_payments_fact` — all emit the same 29-col schema.)

### 1a. The consolidation step — `extractors/procurement_payments_consolidate.py` (`main()`, lines 680-753)

This is the module/function that produces the gold fact. It is **mechanical — no re-parsing**.
Order of operations in `main()`:

1. `_load_facts()` (line 215) — reads each `SOURCE_FACTS` parquet from `data/silver/parquet/`,
   asserts **schema identity** (`set(df.columns)` must match the first fact, else `SystemExit`
   "schema drift"), then `pl.concat(..., how="vertical")`. So **all source facts must share an
   identical column set**; a publisher's rows are unioned by simple vertical concat (no join,
   no dedup across publishers — the facts are asserted **disjoint by publisher_id**).
2. `_load_la_fact(base)` (line 236) — conforms the LA silver fact (which carries `value_kind`,
   `region`, `entity_type`, `privacy_reason`, `supplier_is_id_code` natively) to the base 28-col
   schema: it aliases `value_kind -> amount_semantics` (same vocab), injects
   `extraction_status="extracted"`, `extraction_confidence="high"`, `caveat_text_detected=False`,
   remaps `privacy_status` `quarantined->review_personal_data` / `public->ok`, then
   `.select(base.columns).cast(dict(base.schema))`. Concatenated onto `base`.
   - **Listing-rot carry-forward** (lines 274-286): any `local_authority` publisher_id present in
     the EXISTING gold fact but ABSENT from current silver has its gold rows carried forward verbatim
     (so a council whose site newly blocks the harvester does not vanish).
3. `_canonicalise_split_entities` / `_clean_supplier_names` / `clean_paid_flag` — supplier de-frag.
4. `_conform(df)` (line 305) — derives `value_kind` + `realisation_tier` from `amount_semantics`
   via `SEMANTICS_TO_KIND` (`payment_actual->(payment_actual,SPENT)`, `po_committed->(po_committed,COMMITTED)`,
   default `(unknown,UNKNOWN)`); sets `vat_status`; **re-derives** `value_safe_to_sum` and `public_display`.
5. `_attach_regime(df)` (line 357) — **the regime backfill** (see §2).
6. `_attach_cro` / `_reclassify_missed_companies` / `_classify_id_codes` / `_apply_class_overrides`
   / `_derive_spend_category` / `_surface_sole_trader_contractors` — enrichment + re-classification.
7. **Reconciliation audit** (lines 709-720): per source label (keyed by that source's disjoint
   publisher_id set, captured at load via `_capture_stats`), gold's `(rows, €)` must equal the
   source's `(rows, €)` exactly (`reconciliation_violations`, €1.0 tolerance). A drift => `SystemExit`.
   **This is the gate that any new lane must pass**: rows + € must survive the fold to the cent.
8. `guard_payment_fact(df, name="procurement_payments_fact", hard=True)` — the data-contract drift
   gate (`services/data_contracts.py`, see §5).
9. Privacy invariant (lines 734-745), `value_plausible` flag, then
   `save_parquet(df, OUT, min_rows=MIN_FACT_ROWS)` where `MIN_FACT_ROWS = 150_000` (row floor;
   bypass with env `DAIL_SKIP_ROW_FLOOR=1`).

### 1b. Per-source extractor — where the per-row schema is actually set

The disclosed extract would join the corpus through the **public-body lane** pattern, so the
relevant write site is `extractors/procurement_public_body_extract.py`:

- `base(...)` helper (line 1415) builds each row dict. It stamps **per-row**:
  `publisher_id, publisher_name, publisher_type, sector` (all from the `cfg` config — see §3),
  `source_landing_url = cf["listing_url"]`, `source_file_url = file_url` (the harvested file URL),
  `source_file_hash = sha256(bytes)[:16]`, `period/year/quarter` (parsed from filename),
  `amount_semantics = cf["amount_semantics"]` (**config, not data — see §4**), and the parse fields.
- `classify_and_flag(df)` (line 1678) derives `supplier_normalised` (`name_norm_expr`, NFKD
  accent-fold from `shared/name_norm.py`), `supplier_class`, `privacy_status`, `public_display`,
  and `value_safe_to_sum`.
- The silver write is gated by a runtime privacy invariant + `save_parquet(..., min_rows=60_000)`.

---

## 2. `_publisher_regime.py` — how the regime columns are set, and the lookup key

File: `extractors/_publisher_regime.py`. Single source of truth for a publisher's disclosure regime.
Public entry: `regime_for(publisher_id, publisher_type) -> dict` returning the 5 keys
`disclosure_basis, disclosure_threshold_eur, threshold_vat, body_procurement_class, regime_note`.

Resolution logic (`regime_for`, lines 118-133):
- `_OVERRIDES.get(publisher_id, {})` — **the lookup key is `publisher_id`** (a per-id dict).
- `body_procurement_class` = override's `body_class` **else** `_CLASS_BY_TYPE.get(publisher_type, "contracting_authority")`
  — so it falls back on **`publisher_type`**.
- Everything else falls back to module defaults: `disclosure_basis="foi_s8_model_scheme"`,
  `disclosure_threshold_eur=20000`, `threshold_vat="unknown"`.

**Where it is applied:** in the consolidator, **`_attach_regime(df)`** (consolidate lines 357-377).
It takes the unique `(publisher_id, publisher_type)` pairs in the fact, calls `regime_for(...)` per
pair, builds a small lookup frame, and **left-joins on `["publisher_id", "publisher_type"]`**. So
**the join key is the (publisher_id, publisher_type) pair**, and the per-id override key inside
`regime_for` is **`publisher_id`**. This is confirmed by memory ("regime applied at consolidation,
keyed by publisher_id"). The vocabularies are closed sets `DISCLOSURE_BASIS` and `BODY_PROCUREMENT_CLASS`.

`amount_semantics`, `value_kind`, `value_safe_to_sum`, `realisation_tier` are **NOT** set by
`_publisher_regime.py`. They come from the per-source config (`amount_semantics`) and `_conform`
(`value_kind`/`realisation_tier` derived from it; `value_safe_to_sum` re-derived).

---

## 3. The publisher registry — where publisher_id / name / type / sector live

There is **no single registry CSV/dict consumed at gold-build time.** Identity is assigned **at the
per-source extractor** and simply carried through the union. Three registries exist, one per lane:

| Lane | Registry (source of truth) | publisher_id convention | type/sector set where |
|---|---|---|---|
| Public bodies / depts / agencies | `extractors/procurement_public_body_extract.py` → the **`PUBLISHERS` list of `cfg(...)` entries** (line 296+) | hand-chosen, e.g. `ie_opw`, `dept_finance`, `ie_hse` | `cfg(pid, name, ptype, sector, ...)` args |
| Local authorities | `extractors/procurement_la_payments_extract.py` → `SCHEMA_MAP` (per-council `_la(...)` cfg, slug-keyed) | `f"ie_la_{slug}"` (line 1050) | `publisher_type="local_authority"`, `sector="local_government"` (hardcoded, lines 1052-1053) |
| HSE / Tusla / NTA / NPHDB / SEAI / dept-readingorder | their own bespoke parser modules | per-module constants | per-module |

There is also a **seed CSV** `data/_meta/procurement_publishers/publishers_seed.csv` (written by
`extractors/procurement_publishers_seed.py`) and `extractors/procurement_la_seed.py` — but these are
**Phase-1 discovery seeds (landing URLs + status), NOT the runtime registry.** The runtime identity
is the `cfg()`/`SCHEMA_MAP` config in the extractor. The current 72-publisher set (with type) is
captured in `pipeline_sandbox/disclosed_po_spend/fact_publishers.csv`.

**How a NEW publisher is added today:** add a `cfg(...)` entry to `PUBLISHERS` (public-body lane) or
a `_la(...)` entry to `SCHEMA_MAP` (LA lane) — choosing `publisher_id`, `name`, `ptype`, `sector`,
`amount_semantics`/`value_kind`, and the source `listing`/`direct` URLs. The extractor then emits the
publisher's rows with those columns; the consolidator unions them and `_attach_regime` gives them a
regime via the `publisher_type` default (or a `_OVERRIDES[publisher_id]` entry if it differs).

---

## 4. How `amount_semantics` (payment_actual vs po_committed) is decided

**Per-publisher CONFIG, not derived from data.** In the public-body lane it is the `semantics=`
argument of each `cfg(...)` entry (stamped onto every row as `cf["amount_semantics"]`, line 1429).
In the LA lane it is `value_kind=` per `_la(...)` cfg (aliased to `amount_semantics` at the fold).
The human picks it from the source page title ("Purchase Orders over €20,000" => `po_committed`;
"Payments over €20,000" => `payment_actual`). Note the `dept_children` precedent (cfg line 803): the
page is titled POs but reports "Total Paid"+date, so it is configured `payment_actual` despite
`grain="purchase_order"` — i.e. semantics is a **deliberate human classification per body**, which is
exactly what the disclosed corpus needs (the prior workflow's per-body `body_regime.csv` /
`body_regime_crosswalk.csv` already drafted this for all 216 bodies).

---

## 5. The drift gate that any new lane must satisfy — `services/data_contracts.py`

`guard_payment_fact(df, name=..., hard=True)` runs at consolidate line 730. **HARD** closed-vocab
checks (a new out-of-vocab value HALTS the run) on:
- `amount_semantics` ∈ `{payment_actual, po_committed}` (no "unknown")
- `value_kind` ∈ `{payment_actual, po_committed}`, `realisation_tier` ∈ `{SPENT, COMMITTED}`
- `extraction_status` ∈ `{extracted}`, `extraction_confidence` ∈ `{high, medium, low}`
- `vat_status` ∈ `{incl_vat, excl_vat, unknown}`
- `supplier_class` ∈ `{company, foreign_company, sole_trader, sole_trader_or_individual, public_body, id_code, unknown}`
- `privacy_status` ∈ `{ok, review_personal_data, public, quarantined}`
- `paid_flag` (quarantine-only, escalates at >12% off-vocab)

Plus structural (required columns present; `publisher_id`/`amount_eur` non-null) and cross-column
invariants (no summable public-body recipient; CRO⇒company; value_kind⇔realisation_tier;
no summable non-positive amount; privacy). **Implication for the new lane:** every row it produces
must carry one of `{payment_actual, po_committed}` for `amount_semantics`. The disclosed corpus's
per-category **roll-up** rows and any "aggregated_rollup" bodies (e.g. Irish Water in the crosswalk)
have no clean per-line semantics — they must either be assigned a tier or excluded from the lane
(see open questions).

---

## 6. The precise hooks to register 141 new bodies + per-body regime

The disclosed extract is **not a self-fetching scraper** (it is a single static CSV with no per-file
URLs), so it does **not** fit the existing `cfg()`/harvest extractors cleanly. The clean engineering
shape is a **new dedicated silver extractor** that reads the CSV and emits the **same 29-col silver
schema**, then is added to `SOURCE_FACTS` in the consolidator. Hooks:

### Hook A — new silver fact + consolidator wiring
- New module e.g. `extractors/procurement_disclosed_bq_extract.py` →
  `data/silver/parquet/disclosed_bq_payments_fact.parquet`, emitting **exactly** the base 29-col
  schema (`SCHEMA_COLS` in the public-body extractor, lines 1870-1900) so `_load_facts()`'s schema-
  identity assert passes.
- Add the filename to `SOURCE_FACTS` (consolidate line 88) — **this is the one-line union hook.**
  The vertical concat then folds the new publishers automatically.
- **Disjointness requirement:** `_load_facts` + the reconciliation audit assume **no publisher_id
  overlap** between source facts. The 141 genuinely-new bodies are disjoint by construction, but the
  53 renames and the HSE overlap are NOT — see Hook D + §7.

### Hook B — publisher identity for the 141 new bodies
- Each new body needs a `publisher_id` (convention `ie_<slug>` or `dept_<slug>`; councils
  `ie_la_<slug>`), `publisher_name`, `publisher_type` (∈ the 7 existing values:
  `semi_state, department, education_body, local_authority, agency, hospital, state_body`), `sector`.
- Best engineering form: a **committed registry CSV** in `data/_meta/` (e.g.
  `procurement_disclosed_bodies.csv`, keyed on the raw `entity` string after stripping the
  `"Agency : "` prefix) mapping `entity_raw -> publisher_id, publisher_name, publisher_type, sector,
  amount_semantics`. The new extractor reads it; a row with no mapping HALTS (fail-closed) so an
  unmapped body can never silently ship un-typed. Seeds already drafted in
  `pipeline_sandbox/disclosed_po_spend/candidate_new.csv` (the 141) +
  `body_regime_crosswalk.csv` (entity->existing publisher) + `body_regime.csv`/`body_regime.csv`
  (per-body `po_committed` vs `payment_actual`).

### Hook C — per-body regime (`disclosure_basis` etc.)
- For the **141 new bodies**, regime is normally satisfied by the `publisher_type` **default** in
  `_publisher_regime._CLASS_BY_TYPE` (everything => `foi_s8_model_scheme`/€20k/`contracting_authority`).
- Bodies that genuinely differ get an **`_OVERRIDES[publisher_id]` entry** in
  `extractors/_publisher_regime.py` (the per-id dict). Concretely from the new corpus: the utilities
  (Irish Water/`ie_uisce` already present, EirGrid/`ie_eirgrid` present, Gas Networks/`ie_gni`
  present) need `basis="utilities_regime"`, `body_class="contracting_entity_utility"`; Central Bank,
  RTÉ (`ie_rte` present, `voluntary`/`commercial_state`), NAMA, IDA may need bespoke basis.
  **No new vocabulary is needed** unless a body falls outside `DISCLOSURE_BASIS`/`BODY_PROCUREMENT_CLASS`
  — adding a value there is a deliberate, separate change.
- Because `_attach_regime` joins on `publisher_id` (override) + `publisher_type` (default), simply
  giving each new body a correct `publisher_type` is sufficient for the common case.

### Hook D — the 53 renames (must map to EXISTING publisher_id, not create duplicates)
- These are NOT new publishers. In the registry CSV (Hook B), map the disclosed `entity` string to
  the **existing** `publisher_id` (e.g. disclosed "Cork County Council" -> existing council slug;
  the NTMA 6-way split -> `ie_ntma`; renamed departments -> existing `dept_*`). Crosswalk drafted in
  `body_regime_crosswalk.csv`.
- **CRITICAL conflict with disjointness:** if a rename maps to an existing publisher_id that is
  ALSO already in another source fact, the reconciliation audit (keyed by disjoint publisher sets)
  and the gold uniqueness break. Two engineering options:
  1. **Quarter-level supersede/dedup BEFORE the fold** — decide per (publisher_id, period) which
     source wins, so each publisher_id's rows come from exactly one lane. (Preferred for HSE — see §7.)
  2. Route renamed-but-already-held bodies **out of the new lane** entirely (keep our existing parse,
     only take the genuinely-new quarters/bodies from the disclosed extract).

---

## 7. The HSE overlap/dedup — concrete handling

Our gold holds 16,972 HSE rows for 2021-q4..2025-q3 from ONE source PDF
(`disclosure_basis=foi_s8_model_scheme`, single `source_file_hash`, via `hse_tusla` lane). The
disclosed extract covers the SAME quarters (more complete, +12 lines) AND adds 2017-q3..2020-q2 +
2025-q4 + 2026-q1; HSE has an internal gap 2020-q3..2021-q3; our parse has 190
`privacy_status=review_personal_data` rows to preserve.

Because the reconciliation audit is **per-source keyed by publisher_id set**, you cannot have HSE
rows in BOTH `hse_tusla_payments_fact` and the new disclosed fact — gold would double-count and the
audit would still pass (each source reconciles to itself) while the FACT is wrong. Engineering plan:
- **Quarter-level supersede:** for HSE, take the disclosed extract as the authority for the quarters
  it covers (it is more complete), keep our existing parse only for HSE's disclosed-gap quarters if
  any, and **re-attach our 190 review_personal_data classifications** by joining on a stable row key
  (supplier_raw + amount + period + po) so privacy is not regressed.
- This must happen in ONE place so a publisher_id lives in ONE source fact. Simplest: do the
  HSE merge **inside the new extractor** (emit the unioned/superseded HSE rows there) and **remove
  HSE from the `hse_tusla` lane's output for those quarters**, OR add an explicit
  `allowed_row_delta`-style documented carve-out. Do NOT rely on the generic concat.

---

## 8. The schema/engineering gaps to solve (the extract LACKS these)

The disclosed CSV has only `PO, Supplier, Total, Description, QTR, Year, entity, year_quarter`.
Everything else the silver schema requires must be re-derived in the new extractor:

| Missing column | How to fill |
|---|---|
| `source_file_url` / `source_landing_url` / `source_file_hash` | **No per-record source URL exists** (single BQ export). Engineering fix: set `source_landing_url` to each body's published-disclosures landing page (reuse the seed `landing_url` from `publishers_seed.csv`/`la_seed.py` where the body is known); set `source_file_url` to a stable synthetic provenance token (e.g. `bq-export:<filename>#<entity>/<year_quarter>`) so provenance is honest and the `source_file_hash` = hash of the CSV export. This makes the missing per-record URL a documented synthetic, not a null. |
| `amount_semantics` | per-body config from `body_regime.csv` (`po_committed`/`payment_actual`); map the raw `entity` (strip `"Agency : "`). |
| `value_kind` / `realisation_tier` / `value_safe_to_sum` | re-derived downstream by `_conform` (from `amount_semantics`) + `classify_and_flag`-style logic in the extractor (value_safe_to_sum needs supplier_class + non-blank normalised + non-public-body + amount<€100m). |
| `vat_status` | set by `_conform` at the fold (incl_vat only for the VAT_INCLUSIVE_PUBLISHERS set; else unknown). |
| `supplier_normalised` | `name_norm_expr("supplier_raw")` (NFKD accent-fold) — **run before any rollup** (41,990 raw -> ~37,397 normalised). |
| `supplier_class` / `privacy_status` / `public_display` | reuse the `classify_and_flag` logic (COMPANY_SUFFIX / FOREIGN_FORM / PUBLIC_BODY regexes + privacy quarantine). |
| `cro_company_num` / `cro_company_status` | attached at the fold (`_attach_cro` joins on `supplier_normalised`). |
| `spend_category` | derived at the fold (`_derive_spend_category` canonicalises `description`). |
| `publisher_type` / `sector` | from the new registry CSV (Hook B). |
| `period` / `year` / `quarter` | parse `year_quarter` ("2012-q1") directly — no filename guessing needed (cleaner than the existing lanes). |
| `parser_name` / `parser_version` / `extraction_status` / `extraction_confidence` / `caveat_text_detected` / `source_row_number` / `source_page_number` | constants (`disclosed_bq`, version, `extracted`, a confidence, `False`, CSV row index, null). |

`amount_eur` <- `Total` (read `schema_overrides={"Total": pl.Float64, "Year": pl.Int64}` per env note;
our fact `year` may be null so keep it nullable).

---

## 9. Build sequence (sandbox -> vet -> promote; no gold writes in this phase)

Per the project's data-anchored promotion rule (sandbox first, gold is the last step):
1. **Sandbox extractor** under `pipeline_sandbox/disclosed_po_spend/` that produces a candidate
   silver frame in the 29-col schema (write to sandbox, NOT `data/silver/`).
2. **Reconcile** the candidate against the prior-workflow findings (HSE to the cent; 141 new bodies;
   53 renames map to existing ids with zero new duplicate publisher_ids).
3. **Dry-run the contract** (`guard_payment_fact(df, name=..., hard=False)`) on the candidate to
   confirm zero out-of-vocab `amount_semantics`/`supplier_class`/etc. and zero invariant violations.
4. **Resolve the HSE supersede + rename disjointness** (§6 Hook D, §7) so each publisher_id is in one lane.
5. Only THEN (separate checkpoint, with the owner): create the real `extractors/procurement_disclosed_bq_extract.py`,
   the `data/_meta/procurement_disclosed_bodies.csv` registry, the `_OVERRIDES` additions, wire into
   `SOURCE_FACTS` + `pipeline.py`, and bump `MIN_FACT_ROWS` (gold will grow well past 150k).

---

## 10. Open questions / risks

- **Roll-up bodies.** Some utilities/regulators in the corpus publish per-category quarterly
  roll-ups (Irish Water flagged `aggregated_rollup` in `body_regime.csv`), which have no clean
  per-line `payment_actual`/`po_committed` semantics and would fail the HARD `amount_semantics`
  contract. Decide: exclude these bodies from the lane, or extend the vocab + `value_safe_to_sum`
  rules (a vocabulary change is a deliberate, separate decision — the contract is intentionally tight).
- **`MIN_FACT_ROWS` floor.** Adding ~hundreds of thousands of rows means the 150k floor must be
  raised once the new lane lands, or a future scoped rebuild trips the floor.
- **Rename disjointness vs reconciliation audit.** The audit is per-source by publisher_id set and
  will not catch a double-count if the SAME publisher_id appears in two source facts — it must be
  prevented structurally (one lane per publisher_id), not relied on the audit to detect.
- **Triple-count trap.** New councils (Dublin City €4.1bn, Louth, Kerry, Tipperary, Roscommon, etc.)
  add council->contractor legs; central->council transfers already exist (TII Road Grants). The
  existing guard (exclude `supplier_class='public_body'` from totals) handles it IF the new
  councils' transfer recipients are classed `public_body` — verify the classifier catches them.
- **Provenance token shape.** The synthetic `source_file_url` design (Hook §8) should be agreed with
  the owner so the UI's per-record source link degrades gracefully (link to the body landing page).

---

## 11. Layer layout, naming, registration & coverage convention (slot-in map)

This section maps the bronze/silver directory + naming conventions, the `pipeline.py`
registration + `--list` mechanics, the `data/_meta/*_coverage.json` convention, and the git-
tracking rules a new `disclosed_bq_po` source must follow.

### 11a. Bronze / silver directory + naming
- **Bronze** = `data/bronze/<source>/` (per-source subdirs: `bronze/cro/`, `bronze/ted/`,
  `bronze/iris_oifigiuil/`…). Payment scrapers cache fetched source files here so steady-state runs
  only download the newly-published file. The disclosed extract is **already-assembled tabular
  data**, not a fetched document corpus, so it has no per-file fetch step. It currently sits in
  `data/raw_bq/` and is `.csv`-gitignored (`.gitignore:12 *.csv`). **Plan:** treat the
  `data/raw_bq/<file>.csv` as the bronze input read-only (or move to `data/bronze/disclosed_bq_po/`);
  either way it stays gitignored (large, owner-supplied) — the *silver* parquet is the tracked artifact,
  mirroring how `la_payments` keeps PDFs in bronze but tracks `la_payments_fact.parquet`.
- **Silver** = `data/silver/parquet/<name>_fact.parquet`. Existing payment siblings:
  `public_payments_fact.parquet` (29-col base), `hse_tusla_payments_fact.parquet`,
  `nta_payments_fact.parquet`, `nphdb_payments_fact.parquet`, `seai_payments_fact.parquet`,
  `dept_readingorder_payments_fact.parquet`, plus `la_payments_fact.parquet` (31-col superset).
  **Proposed name:** `disclosed_bq_po_payments_fact.parquet` (source slug `disclosed_bq_po`).
- **Git tracking:** silver parquets are git-TRACKED only via explicit `.gitignore` negations
  (`!data/silver/parquet/la_payments_fact.parquet` … lines 369–384; the blanket `*.parquet` rule
  swallows everything else). A new fact needs **its own negation line**
  `!data/silver/parquet/disclosed_bq_po_payments_fact.parquet`, or it silently won't commit. Same for
  the coverage JSON (covered by the `!data/_meta/*.json` negation, line ~298) — no action needed there.

### 11b. pipeline.py registration + `--list`
- The registry is `pipeline.py:51 CHAINS: list[tuple[chain_name, script_path]]` (not "STEPS"; the
  current code calls it `CHAINS`). `--list` prints each via `_print_chain_list()` keyed on
  `_CHAIN_BLURBS[name]` (`pipeline.py:239`). Each chain is dispatched as
  `subprocess([sys.executable, script])` with `PYTHONIOENCODING=utf-8` forced on the child, and one
  chain's failure is isolated (try/except) so it never poisons the rest.
- To register: add `("disclosed_bq_po", "extractors/disclosed_bq_po_extract.py")` to `CHAINS`
  **after** the payment-silver chains (`public_body_payments`, `hse_tusla_payments`, `la_payments`)
  and **before** `("procurement_consolidate", …)` — the consolidate fold reads its silver output
  (the `SOURCE_FACTS` one-line hook from §6). Add a matching one-line `_CHAIN_BLURBS` entry so
  `--list` shows it. The extractor is a standalone `main()` script (like
  `procurement_la_payments_extract.py`) that should `sys.stdout.reconfigure(encoding="utf-8")`.

### 11c. Coverage-JSON convention (`data/_meta/*_coverage.json`)
- Every source writes one (`la_payments_coverage.json`, `public_payments_coverage.json`,
  `hse_tusla_payments_coverage.json`, `nta/nphdb/seai_payments_coverage.json`, and the gold
  `procurement_payments_fact_coverage.json`). The shape: `generated_utc`, `layer`,
  `source` (extractor path), `n_rows`, `n_publishers`, `n_suppliers`, per-dimension breakdowns,
  and free-text `*_note` caveats. **Plan:** the disclosed silver extractor writes
  `data/_meta/disclosed_bq_po_payments_coverage.json`. The gold fold already rewrites
  `procurement_payments_fact_coverage.json` (rows/publishers/suppliers/tier/vat/basis breakdowns) —
  those numbers grow automatically once the new lane is folded.

### 11d. source_health staleness registration
- `tools/build_source_registry.build_records()` adapts each source's in-code config into
  `source_registry.generated.json` → `data/_meta/source_health.json` (read by the freshness badge +
  scheduled report). It has per-group adapters: `adapt_public_body` (reads `PUBLISHERS`), `adapt_la`
  (`SCHEMA_MAP`), `adapt_hse_tusla`, and a `MANUAL_SOURCES` list (`build_source_registry.py:360`).
  The disclosed source is an owner-supplied **one-off file** (not a live-polled listing), so register
  it under `MANUAL_SOURCES` with `held_through = 2026-q1` (the extract's latest quarter) so the badge
  tracks when a newer BQ extract is owed. No new adapter needed.

### 11e. SQL view registration (no view files change — they read the file as-is)
- Views are `CREATE OR REPLACE VIEW v_…` files registered at runtime by
  `dail_tracker_core/db.py register_views()`, loaded by **domain-prefix glob, recursively, in
  alphabetical (= dependency) order**, `zz_`-prefixed views last (`sql_views/README.md`). The
  procurement views are loaded by `procurement_*.sql`. Because every consuming view does
  `read_parquet('data/gold/parquet/procurement_payments_fact.parquet')`, **no SQL file needs editing**
  — the wider fact flows through automatically. (Confirm scale/correctness per §11f.)

### 11f. Downstream dependents (what re-derives when the fact grows 247k → ~700k+ rows / 72 → ~210 publishers)

**SQL views over the gold fact** (`sql_views/procurement/` + `sql_views/housing/`):
- `procurement_payments.sql` → **`v_procurement_payments`** — the keystone display view; SELECTs
  `source_file_url` (so the §8 provenance-token gap must be solved or the per-record source footer
  shows null) and `paid_status` (strict paid_flag allowlist).
- `procurement_payments_publisher_summary.sql` → `v_procurement_payments_publisher_summary`
- `procurement_payments_supplier_summary.sql` → `v_procurement_payments_supplier_summary`
- `procurement_payments_by_category.sql` → `v_procurement_payments_by_category`
- `procurement_council_summary.sql` → `v_procurement_council_summary` (filters
  `publisher_type='local_authority'` — the +8 disclosed councils + 23 council renames land here;
  watch the triple-count guard, §10).
- `procurement_public_payments.sql` → `v_public_payments` / `_publisher_summary` / `_supplier_summary`
  (the non-council public-body register).
- `procurement_quarter_profile.sql`, `procurement_supplier_sector_breadth.sql`,
  `procurement_entity_chain.sql`, `procurement_afs_vs_po_coverage.sql`,
  `procurement_zz_entity_search.sql` (entity search; `zz_` loads last).
- `housing/housing_accommodation_spend_providers.sql` + `housing_accommodation_spend_by_year.sql` →
  **`v_accommodation_spend_providers`** etc. — filter on `spend_category LIKE '%asylum/ip/ukraine%'`.
  **RISK:** these UNION the gold fact with `dceidy_ipas_legacy_spend.parquet` (2023-2024) and exclude
  DCEDIY 2025+ to avoid double-counting Justice's 2025+ coverage. A new disclosed DCEDIY/Justice body
  could re-introduce a double-count — verify the de-dup window still holds after the fold.

**Query modules** (`dail_tracker_core/queries/`):
- `procurement.py` — reads `v_procurement_payments`, `v_procurement_council_summary`, the
  publisher/supplier summaries (the procurement page backbone).
- `public_payments.py` — reads `v_public_payments` + `_publisher_summary` / `_supplier_summary`
  (the public-body register page backbone).
- `housing.py` — accommodation-spend providers.
- `dossiers.py` — the entity dossier unifies `payments` + `procurement` + `public_payments`, so any
  body/supplier dossier surfaces the new rows.

**App pages** (`utility/pages_code/`):
- `procurement.py` (via `utility/data_access/procurement_data.py`)
- `public_payments.py` (via `utility/data_access/public_payments_data.py`)
- `council_spending.py` + `local_government.py` (council summary)
- `accommodation_spend.py` (asylum/Ukraine providers)
- `follow_the_money.py` (body⇄supplier⇄ledger trail)
- `company.py` (supplier dossier)

**MCP tools** (`mcp_server/server.py`):
- `public_body_payments` (line 704) — publisher/supplier rankings over the fact.
- `data_coverage` (847) — surfaces `pubpay.coverage_stats` (the public_body_payments coverage).
- `get_supplier` / supplier cross-refs — the de-fragmented supplier spine widens.

**API exports** (`api/routers/exports.py:97`): the `procurement_payments_fact` ExportSpec serves the
whole fact via `/v1/data`. The per-publisher VAT sidecar
`data/_meta/procurement_payments_vat_matrix.json` (built by `tools/build_vat_matrix.py`) should be
regenerated; `tools/build_runtime_manifest.py` records the fact in `runtime_data_manifest.json`.

**Scale watch-outs:** `@st.cache_data` caches every query (300–600s), so a ~3× larger fact means
larger cached frames — measure `server_ms` before/after per the scalability plan (memory:
`project_scaling_plans_2026_06_18`). And confirm the triple-count guard
(`value_safe_to_sum & supplier_class != 'public_body'`, `consolidate.py:330` + mirrored in views)
classes new transfer-type bodies (council→council, central→agency) as `public_body`.

### 11g. Output-regression / baseline guards to update in the same checkpoint
- `data/_meta/output_baseline.json` pins `procurement_payments_fact.parquet` at `rows: 219707` +
  its column list; `tools/check_output_regressions.py` flags silent row-thinning / column drops. A
  deliberate row growth needs `--update-baseline` (an *intended* change) committed in the same gold
  checkpoint. `data/_meta/gold_quality.json` / `gold_quality_baseline.json` similarly.
- After the lane lands, **raise `MIN_FACT_ROWS`** (currently 150_000, `consolidate.py:78`) to ~70% of
  the new row count, deliberately, so a future scoped rebuild still trips the floor on truncation.

---

## 12. INGEST / DEDUP-KEY / WRITE-SAFETY mechanics (verified in code 2026-06-22)

Pins the exact per-source ingest, merge granularity, dedup key, and write-safety facts the dedup
design hangs on — re-verified against source so the build phase needs no re-read.

### 12a. There is NO upsert into gold — gold is rebuilt by UNION every run
`procurement_payments_consolidate.main()` reads the silver lanes fresh, `pl.concat(how="vertical")`s
them, re-derives every enrichment, and `save_parquet`s the WHOLE fact. The only code that reads existing
gold is the LA listing-rot carry-forward (`_load_la_fact`, lines 274-286) — **publisher-keyed** (copies a
vanished council's gold rows forward), never row/period-keyed. **So the disclosed merge + ALL dedup must be
resolved at or upstream of silver.** The consolidator cannot dedup across lanes: its reconciliation audit
sums by each source's *disjoint publisher set*, so the same publisher_id present in two lanes
double-counts AND still passes the audit (each source reconciles to itself) — confirmed §7. This is why a
dedicated, period-aware disclosed lane (not a generic concat) is mandatory.

### 12b. `--only X --merge` (the sole upsert path) = WHOLE-PUBLISHER REPLACE; key = `publisher_id` only
In `procurement_public_body_extract.main()` (~lines 1915-1925):
```python
existing = pl.read_parquet(OUT_FACT)
sel_ids  = [p["id"] for p in pubs]                 # the --only publishers
kept     = existing.filter(~pl.col("publisher_id").is_in(sel_ids))
df       = pl.concat([kept, df.select(existing.columns).cast(dict(existing.schema))], how="vertical")
```
- **Dedup/upsert key = `publisher_id`, full stop.** It drops ALL existing rows for the reparsed
  publishers and appends the freshly parsed rows for those ids. There is **NO**
  `(publisher_id+period+po+supplier+amount)` row-level upsert anywhere in the codebase.
- It **REPLACES a publisher wholesale** — it cannot add one quarter while preserving the publisher's other
  quarters under a different parse. The whole publisher is re-emitted from the current run's parse.
- `--merge` requires `--only` (else `SystemExit`); a plain `--only` writes only that slice and is caught by
  the row floor (`RowFloorViolation`).
- The ONLY row-level dedup in that lane is `dedup_source_repeats()`:
  `DEDUP_SIG = [source_file_hash, supplier_raw, amount_eur, description, po_number, source_page_number,
  paid_flag, period]`, applied **PDF-only / within-file** (tabular readers pass through untouched — a
  tabular source emits each cell once, so identical tabular rows are genuine distinct invoices, the CHI
  lesson). It is NOT an across-source dedup.
- **Implication:** the disclosed data must be its OWN silver fact (§6 Hook A / §11), not folded via
  `--merge` — `--merge` literally cannot express "take only HSE's non-overlapping quarters" (§7); a
  dedicated extractor with explicit period/body subtraction can.

### 12c. The HSE/Tusla lane has NO row floor and NO merge — blind full rebuild
`procurement_hse_tusla_materialize.main()` parses the cached FOI PDFs each run and `save_parquet(df,
OUT_FACT)` with **no `min_rows=`** and **no `--only`/`--merge`** (the only args-bearing payment extractors
are public-body and LA). Consequences:
- If the HSE/Tusla PDF parse degraded, that silver could silently shrink/empty; only the gold 150k floor
  *might* catch it, and only if the other lanes also dropped below 150k (they won't — §12d).
- **For the §7 supersede:** whichever option moves HSE/Tusla quarters to the disclosed lane MUST (a) shrink
  the hse_tusla lane's output for those quarters AND (b) add a `min_rows=` floor to that lane so its
  now-smaller output cannot silently empty. Ground truth to preserve verbatim: `ie_hse` = **16,972 rows,
  1 `source_file_hash`, 190 `privacy_status=review_personal_data` rows, periods 2021-Q4..2025-Q3**;
  `source_file_url` = the HSE landing page (the dead deep-link was repointed by
  `tools/patch_hse_dead_source_url.py`).

### 12d. Row floors are MINIMUMS — they catch shrink, never a growing merge
`save_parquet(df, dest, min_rows=N)` raises `RowFloorViolation` BEFORE writing iff `rows < N` (bypass env
`DAIL_SKIP_ROW_FLOOR=1`; dest untouched on violation, atomic `.part`→`os.replace`). Current floors:
`public_payments_fact`=60k, `la_payments_fact`=60k, gold `procurement_payments_fact`=**150k**,
`hse_tusla_payments_fact`=**none**.
- A growing merge never trips a floor (fine).
- But the **gold 150k floor will NOT protect the disclosed lane specifically:** LA(~88k)+public(~109k)+
  hse_tusla(~32k) alone already exceed 150k, so a disclosed lane that came in empty would still let gold
  pass the floor. The disclosed lane therefore needs **its own floor** (sized ~70% of its first healthy
  run), and the gold floor + `output_baseline.json` must be RAISED once it lands (§11g / §10).

### 12e. Concrete dedup recipe — disclosed HSE/Tusla/councils without double-count, privacy preserved
Lowest-risk first cut (makes §7 mechanical):
1. Map disclosed `Health Service Executive`/`Tusla – Child and Family Agency` → existing
   `ie_hse`/`ie_tusla` (NOT new ids), and the 23 council renames → existing `ie_la_<slug>` (per the
   `body_regime_crosswalk.csv` draft). An unmapped `entity` HALTS the run (fail-closed — never mint a
   silent duplicate id).
2. Single-source the ownership in one committed table `data/_meta/disclosed_bq_period_authority.csv`
   (`publisher_id, period, owner_lane`), read by BOTH the disclosed extractor and the hse_tusla materialize
   (and consulted for council exclusion in the LA lane). The disclosed extractor EXCLUDES any
   `(publisher_id, period)` whose `owner_lane` is not `disclosed_bq`.
3. First cut sets `owner_lane`: HSE 2021-Q4..2025-Q3 = `hse_tusla` (keep our 16,972 + 190 privacy rows +
   per-record provenance authoritative); HSE 2017-Q3..2020-Q2 + 2025-Q4 + 2026-Q1 = `disclosed_bq`
   (net-new backfill). Tusla overlapping years = `hse_tusla`, net-new = `disclosed_bq`. The "+12 dropped
   PO lines" are forgone in the overlapping quarters — accept the small completeness loss to keep the
   curated privacy state and per-record source links.
4. The 23 renamed councils overlap the LA lane the same way — the same table decides per council whether
   `disclosed_bq` or `la_payments` owns it; the loser is excluded from its lane (the LA lane already
   supports per-council exclusion via `SCHEMA_MAP` `status`).
5. Tests (gold): for `ie_hse`/`ie_tusla`/each renamed council, every `(publisher_id, period)` group's rows
   share one `parser_name` family (no period appears from two lanes); HSE 2021-Q4..2025-Q3 € unchanged vs
   current gold; the 190 HSE privacy rows survive; the rename map mints zero duplicate publisher_ids.

### 12f. `aggregated_rollup` is a HARD blocker until the vocab is extended (write-safety, not just style)
`body_regime.csv` marks Irish Water / Gas Networks / EirGrid / RTE / ESB / Central Bank
`regime=aggregated_rollup` (per-category quarterly roll-ups, NOT line items — e.g. Irish Water 401 rows =
€11bn). The consolidator's `SEMANTICS_TO_KIND` only maps `payment_actual`/`po_committed`; an
`aggregated_rollup` value falls to `value_kind='unknown'`/`realisation_tier='UNKNOWN'` → `guard_payment_fact(
hard=True)` HALTS the whole consolidate (§5). **Before this lane can land, EITHER** extend
`SEMANTICS_TO_KIND` + the data-contract closed vocab to admit `aggregated_rollup` mapped to a NON-summable
tier (and ensure `_conform` leaves those `value_safe_to_sum=False`), **OR** exclude those bodies from the
disclosed lane. This is a required, deliberate upstream code change (the contract is intentionally tight) —
the extractor cannot paper over it.

---

## 13. Body-identity resolution — the renames in detail (§3/§6 Hook D expanded)

The 53 renames MUST map to an EXISTING `publisher_id` (no duplicate publisher). The
sandbox evidence files ARE the findings of the two prior workflows (there is no separate
FINDINGS.md/POTENTIAL.md yet — when one is written it should link this doc):
`pipeline_sandbox/disclosed_po_spend/body_regime_crosswalk.csv` (entity→existing publisher +
regime), `candidate_new.csv` (the 141 genuinely-new), `fact_publishers.csv` (our current 72,
the mapping targets), `per_body_coverage.csv` (per-body first/last quarter, blank-PO share,
semantics), `earlier_than_ours.csv` (the bodies the disclosed copy starts years before ours).

The `entity` string carries a `"Agency :  "` (note the DOUBLE space, confirmed in the raw CSV)
or `"Section 38 : "` prefix that MUST be stripped before any registry join — e.g.
`"Agency :  Legal Aid Board"`, `"Section 38 : Beaumont Hospital"`. The Section-38 voluntary
hospitals (Beaumont, KARE, Sunbeam House, National Maternity, Rotunda → `hospital`/`state_body`)
are their own identity question — map each to its existing id or mint a new `ie_<slug>`.

**Three rename families to call out by name:**
- **Councils (short vs "City and County").** Disclosed long names map to our short LA-lane ids
  (`ie_la_<slug>`): `Cork County Council`→`Cork County`, `Cork City Council`→`Cork City`,
  `Waterford City and County Council`→`Waterford`, `Limerick City and County Council`→`Limerick`,
  `Dún Laoghaire Rathdown County Council`→ our DLR id, etc. ~23 councils overlap the LA lane and
  contend for ownership per quarter (§5 / §12e). `earlier_than_ours.csv` is the evidence for which
  the disclosed copy should win (e.g. Wicklow disclosed-from 2012 vs ours 2025; Galway County 2012
  vs 2025; Mayo 2013 vs 2016) — there the disclosed lane should own the early quarters.
- **Renamed departments (8).** `Department of Children, Equality, Disability, Integration and
  Youth`→ our `Department of Children, Disability and Equality` (`dept_children`); `Department of
  Justice`→ `Department of Justice, Home Affairs and Migration`; `Department of Education`→
  `Department of Education and Youth`; `Department of Transport`/`Foreign Affairs`/`Social
  Protection`/`Finance`/`Agriculture, Food and the Marine` → their existing `dept_*` ids. Map to
  the existing id; never mint a second department for a rename.
- **The NTMA 6-way split.** The disclosed extract publishes the NTMA as SIX entities —
  `NTMA Administration Account`, `NTMA National Debt Account`, `NTMA Ireland Strategic Investment
  Fund`, `NTMA National Development Finance Agency`, `NTMA Future Ireland Fund`, `NTMA
  Infrastructure, Climate and Nature Fund` — whereas we hold ONE `National Treasury Management
  Agency (NTMA)` publisher (`fact_publishers.csv`). **Recommended:** collapse all six to the single
  existing NTMA `publisher_id` (the schema has no sub-account dimension; six publishers would
  fragment the register and the supplier rollups). The six fund names survive in `description`/a
  lane note, NOT as separate publishers. Alternative (keep them split as new ids) is an owner
  decision (§10 #3) and would need six `publishers_seed.csv` rows + regime defaults.
