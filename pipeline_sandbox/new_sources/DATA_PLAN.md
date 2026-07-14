# IPAS / accommodation — DATA PLAN

_Data only. No UI code, no visual design. This plans the CONTRACTS the UI will read, the
grains, the keys, and the decisions that are yours to make._

Extraction is complete: **16 documents · 5,330 classified facts · 556 explicit unknowns · quality
guard PASS.** Source register: `SOURCE_REGISTER.md`. Findings: memory `project_ipas_publishable_findings`.

---

## 1. The architecture decision (my recommendation, made explicit)

**Two layers, deliberately not one.**

| | `ipas_facts` (5,330 rows) | Serving contracts (7 small tables) |
|---|---|---|
| Role | **Citation backing store** | **What the UI actually reads** |
| Grain | one row per extracted fact | one row per thing the page shows |
| Keys | `doc_key` only | county / operator / standard / entitlement |
| Shape | 23 categories, 74 units, 285 free-text periods | clean, typed, gated |
| Used for | the provenance footer; "where did this number come from"; future-loop gap-finding | every panel |

**Why not one star schema?** `ipas_facts` has no county, no operator, no centre column; its `subject`
is 23% null and mixes grains (centre names, cohorts, institutions, towns). Normalising it into a
serving schema is weeks of work that would produce exactly the seven tables below — which already
exist, clean. Keep the archive as an archive. It is what makes every published figure traceable.

---

## 2. The serving contracts (what each surface reads)

Each is a `v_*` view over a gold parquet. **A gold parquet with no view is classified `dead` by
`build_runtime_manifest.py` and will not ship — so every one of these needs its view.**

| # | Surface | Reads | Grain | Status |
|---|---|---|---|---|
| 1 | **Spend headline** | `v_accommodation_spend_by_year` (EXISTS) + C&AG corroboration figures | year × stream | ✅ built; replace the page's 2 hardcoded C&AG constants with data |
| 2 | **County map** | `ipas_county_profile` (27) / `ipas_by_local_authority` (31) | **LA** (see D6) | new view needed |
| 3 | **County drill-down** | county profile + `ipas_sample_property_rates` (20) | county | new view |
| 4 | **Operator dossier** | `ipas_operator_money_compliance` (33) + `hiqa_centre_compliance` (2,668) + `national_standards_lookup` (40) | operator × centre × standard | new view; **gated — see D1** |
| 5 | **"The person" tab** | `ipas_entitlements` (11) | entitlement | new view |
| 6 | **Accountability / a decade** | `cag_2015_direct_provision_facts` recurrence rows + `ipas_legal_obligations` (16) + `ipas_si_amendment_chain` (6) | finding | new view |
| 7 | **Provenance footer** | `ipas_facts` filtered by `fact_id` | fact | new view (citation lookup) |

**The join that works:** `national_standards_lookup` → **100% of the 2,668 HIQA judgments join.** So
"Standard 4.3" renders as its binding statement. (Standards 6.3 and 9.2 were never judged in any of
the 101 reports — carried as an explicit unknown: *absence of judgment ≠ compliance*.)

---

## 3. Non-negotiable constraints (these are settled, not decisions)

- **`value_safe_to_sum=False` on every row of this corpus.** It is audit/report narrative grain. It
  must NEVER be unioned or summed with `procurement_payments_fact`, awards, or grants.
- **The `stream` filter is mandatory** on `dceidy_ipas_legacy_spend` (International Protection vs
  Ukraine). Unfiltered, Cape Wrath Hotel looks like a €46.4m IP provider; it is €10.9m. This nearly
  produced a false claim naming the wrong company.
- **Never causal.** Compliance window 2024-01→2026-03; payments DCEDIY 2023-24 + Justice 2025+.
  Co-occurrence only. The money is not "the price of that compliance record".
- **Unknowns are shown, not hidden.** 556 of them. Where the State does not publish a number, the
  page says so.
- **Privacy:** no resident is ever named, aged, located or quoted. Provider names inherit the
  accommodation-providers `public_display` gate.
- **Coverage is stated, not implied:** our IP 2024 total is €760.5m = **78% of the C&AG's €978m
  commercial**. The page must say 78%, not imply completeness.

---

## 4. DECISION POINTS (yours)

### D1 — Do we name the operators? ⭐ the big one
We can name ~32 operators and tie them to compliance records and money.
- **(a) Name them all.** Maximum accountability. Requires the house `name_norm` + a confidence gate
  first (variants persist: Onsite×3, Coziq×2, a Dídean typo; IGO fragments into two payee strings).
- **(b) Name only above a confidence threshold**, aggregate the rest as "other operators".
- **(c) Don't name; show compliance by county/centre only.**
_My view: (b). The evidence is strong enough to name, but only where identity is certain — one wrong
name is worse than ten omitted._

### D2 — Do we publish that HIQA's overview understates its own inspections?
Std 3.1: the overview says 12% not compliant; the 101 underlying reports say 26%. Triple-path
verified. This is a claim about a **regulator**.
- **(a) Publish with the full method shown** (three independent extraction paths, the divergence table).
- **(b) Footnote it** on the compliance panel.
- **(c) Hold it** pending a right-of-reply to HIQA.
_My view: (a) or (c) — not (b). It is either important enough to show properly or it should wait. A
buried footnote is the worst of both._

### D3 — How do we frame the €200m spent on people who already have status?
IGEES: ~5,500 people with status cost €200m in 2024 (~20% of the bill) while >3,000 entitled
applicants were unaccommodated. Politically charged; easily weaponised.
- **(a) Frame as a housing-supply failure** — the State paying twice for its own delay.
- **(b) State the number bare with sources and no framing.**
- **(c) Omit.**
_My view: (a). The number is real and material; leaving it out is its own distortion. But it must be
framed as what the auditor says it is — a consequence of processing delay and housing shortage._

### D4 — Map grain: local authority (31) or county (26)?
Our data is **per local authority** (South Dublin, Fingal, DLR and Dublin City are separate — and
South Dublin at 3,979 is the single largest). The existing choropleth is **constituency**-based.
- **(a) Build an LA choropleth** (truest to the data; matches "your council").
- **(b) Roll up to 26 counties** (loses the Dublin detail, which is the most interesting).
- **(c) Reuse the constituency map** (requires an unsafe split — constituencies span LAs).
_My view: (a). (c) is not safely possible._

### D5 — Per-capita: build it or omit it?
The C&AG's own map is per-1,000-population. We cannot compute it: our only population table is
constituency-level and constituencies span LAs.
- **(a) Ingest CSO Census-2022 county/LA population** (small, clean, one-off) → real per-capita.
- **(b) Show absolute counts only**, per-capita marked unknown.
_My view: (a) — it's a couple of hours and it unlocks the most honest version of the map._

### D6 — Scope: International Protection only, or IP + Ukraine?
The data carries both streams. Ukraine 2024 = €585.7m; IP = €760.5m.
- **(a) IP only** (matches the C&AG chapter and the entitlements law).
- **(b) Both, always split, never summed.**
_My view: (b) — the Ukraine stream is the reason the emergency estate exists at the scale it does;
hiding it distorts the picture. But it must be a visible split, never a total._

### D7 — "As at" date: the corpus refreshes at three different speeds
Weekly (IPAS stats) · rolling (HIQA inspections) · annual (C&AG) · episodic (Strategy).
- **(a) Per-panel "as at" stamps** (honest, slightly busy).
- **(b) One page-level date** = the oldest component (safe, understates freshness).
_My view: (a)._

---

## 5. Promotion prerequisites (from the architecture review)

1. **Wire an IPAS `PollSource` into `tools/build_source_registry.py`** — it reads five *hardcoded*
   configs; a source not in one is invisible to `source_health.json`/`freshness.json` and **the corpus
   silently rots**. ← blocking.
2. **Local/edge refresh lane only.** `assets.gov.ie` + `hiqa.ie` are gov.ie-family; their CDN 403s
   datacenter IPs. Not GitHub Actions. Pace ≥5s (~15 rapid requests → 405).
3. **Entity resolution** on provider names (house `name_norm` + confidence gate) — prerequisite for D1.
4. **A `v_*` view per serving contract** — no view ⇒ `dead` in the runtime manifest ⇒ does not ship.
5. **Fold the `ipas_facts_consolidate` guard into `check_extraction_quality.py::ADAPTERS`** (a 2-entry
   pilot today; this is its best use).
6. Fixture tests + `save_parquet(min_rows=…)` + rebaseline `output_baseline.json` /
   `gold_quality_baseline.json` + regenerate `runtime_data_manifest.json`.

---

## 6. Gaps a future loop should close
- **Rooms absorbed / housing-stock impact: NOT PUBLISHED anywhere** (only 2,612 rooms nationally, for
  the 25 tendered contracts). Needs an FOI or a new source.
- **The unaccommodated count is absent from the State's own weekly statistics** — the people the State
  failed to accommodate do not appear in its accommodation figures. Only the C&AG gives it (3,285).
- **Aramark has a compliance record but no matched payment** — likely a Vote 40 payee-name variant.
- **HIQA never publishes denominators** behind its percentages.
