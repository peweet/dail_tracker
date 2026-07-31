# Research scan — LLM code bias/quality, geospatial formats, EU planning law

**Date:** 2026-07-31. **Method:** 3 parallel web-research subagents (no code changes), scoped
deliberately narrow per instruction — heaviest effort on the LLM side, light dips into geospatial
and EU planning law. All findings below are **[Reported — research subagent, not independently
verified by primary-source read in this session]** unless a source says otherwise; treat citation
counts and specific numbers as indicative until re-checked if they become load-bearing for a build
decision. This file is a draft for review, not a decision record — nothing here has been adopted.

**Scope note:** context-engineering literature (context rot, position bias, harness engineering,
prompt caching, agent-memory staleness) was deliberately excluded — this project already ran a
deep pass on that ground, recorded in the persistent memory cards
`reference_context_engineering_literature_2026_07_25`,
`reference_token_optimization_literature_2026_07_31`, and
`reference_agent_memory_staleness_literature_2026_07_31` (memory index:
`~/.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/MEMORY.md`), plus
[doc/TOKEN_OPTIMIZATION_LITERATURE_2026_07_31.md](TOKEN_OPTIMIZATION_LITERATURE_2026_07_31.md) for
the full prior idea inventory. This scan looks for what's genuinely new beyond that.

---

## Part 1 — LLM code-generation bias and code-quality research (primary focus)

### 1a. Python/library favoritism in LLM code generation

| Source | Venue/year | Finding | Applicability to this project |
|---|---|---|---|
| Twist, Harman et al., "A Study of LLMs' Preferences for Libraries and Programming Languages" (arXiv:2503.17181) | ACL 2026 | Across 8 LLMs, models pick familiar/popular libraries over task-appropriate ones — NumPy used unnecessarily in ~45% of cases, Python chosen in 58% of performance-critical cases where it's suboptimal, models contradict their own stated language recommendations in 83% of cases. Co-authored by Mark Harman (established SE researcher). | Direct hit. This project enforces `polars for ETL, pandas only in the UI layer` as a never-break rule specifically because the two libraries look interchangeable to a model reaching for familiarity. This paper is evidence the failure mode is real and systemic, not project-specific paranoia — supports keeping that rule as a hard, machine-checked convention (`tools/check_conventions.py`) rather than prose guidance alone. |
| Cassano et al., "MultiPL-E" (arXiv:2208.08227) | IEEE TSE 2023 | Most code-gen benchmarks (HumanEval etc.) are Python-only; mechanically translating to 18 languages shows models often match or beat Python performance elsewhere. | Background/caveat only — cuts against a naive "LLMs are just worse outside Python" narrative. Low action value for this Python-only repo. |
| "Exploring Multi-Lingual Bias of Large Code Models" (arXiv:2404.19368) | ACM TOSEM 2024, 30 citations [checked — Semantic Scholar] | Quantifies multilingual generation-behavior bias beyond raw accuracy. | Tangential — this project is Python-only; relevant only if the codebase ever grows a second language. |
| Spracklen et al., "We Have a Package for You!" (package hallucination study) | USENIX Security 2025 | 2.23M samples, 16 models, Python+JS: 19.7% of generations hallucinate ≥1 fabricated package; commercial models 5.2%, open-source 21.7%. | Concrete and actionable. A hallucinated import is exactly what `tools/check_conventions.py`'s convention ratchet is positioned to catch, but that tool checks *placement* (extractors → `services/http_engine` etc.), not *existence* of imported packages. Worth checking whether agent-proposed new dependencies get any existence/hallucination check before landing — currently `uv sync` pruning undeclared deps is the closest thing on record (memory card `project_dependency_declaration_audit_2026_07_20`), which catches the symptom late (sync time) rather than at generation time. |

### 1b. LLM code quality / code review research

| Source | Venue/year | Finding | Applicability |
|---|---|---|---|
| Jin & Chen, "Are LLMs Reliable Code Reviewers? Systematic Overcorrection in Requirement Conformance Judgement" (arXiv:2603.00539) | Automated Software Engineering (Springer), 2026 | LLM reviewers systematically over-flag correct code as non-conformant, driven by requirement hallucination (inventing unstated constraints) rather than style nitpicking. More elaborate review prompts (asking for explanations/fixes) make it *worse*, not better. Proposes a "fix-guided verification filter" using the model's own proposed fix as counterfactual evidence, validated against tests. | The most load-bearing single finding in this scan for this project's own harness. `verifier` and `code-reviewer` agents, plus the `/code-review` and `security-review` skills, are exactly the "LLM as reviewer" pattern under test here. Two direct implications: (1) keep review prompts constrained rather than "explain your reasoning at length" — this paper found that backfires; (2) the fix-guided-verification idea (gate a review verdict on whether the model's own proposed fix actually passes tests) lines up with this project's existing CONFIRMED-via-reproduction-over-assertion discipline (`evidence.md`) and is a concrete pattern the `verifier` agent doesn't currently implement — it reports findings, it doesn't propose-and-test a fix as evidence. |
| Yetiştiren et al., "Evaluating the Code Quality of AI-Assisted Code Generation Tools" (arXiv:2304.10778) | 2023 | Multi-dimensional quality study (correctness, validity, security, reliability, maintainability, technical-debt minutes) across Copilot/CodeWhisperer/ChatGPT, not just pass@k. | Directly on-point for "code quality" as a broader concept than functional correctness. If this project ever wants a quality *score* for agent-generated diffs beyond pass/fail tests, this metric set (maintainability + technical-debt-minutes) is a more defensible starting taxonomy than inventing one. |
| Pearce et al., "Asleep at the Keyboard?" (arXiv:2108.09293) | IEEE S&P 2022 / CACM 2025 reprint | ~40% of 1,689 Copilot-generated programs across 89 CWE scenarios were vulnerable. | Base-rate prior: agent-authored code needs security scrutiny independent of whether it looks correct. Reinforces existing review/firewall habits rather than adding something new. |
| Li, Dutta, Naik, "IRIS: LLM-Assisted Static Analysis" (arXiv:2405.17238) | ICLR 2025 | Combines LLM-inferred taint specs with static analysis for whole-repo vulnerability detection, reducing hand-written specs. | Not currently wired into this project's stack (the firewall/convention checkers are rule-based, not LLM+static-analysis hybrids). Relevant only if the project later wants an LLM-assisted variant of `check_streamlit_logic_firewall.py`/`check_conventions.py` — not a near-term fit given those checkers currently work and are cheap/deterministic. |
| Liu et al., "EvalPlus/HumanEval+" (arXiv:2305.01210) | NeurIPS 2023, ~1,313 citations [reported, not independently re-verified] | HumanEval's test suites are weak; an 80x-larger test suite drops measured pass@k by 19–29pp and reorders model rankings. | Caveat for interpreting any future benchmark claim about code-gen capability — not a direct action item, since this project doesn't currently cite HumanEval-style scores anywhere. |
| Li et al., "CodeReviewer" (arXiv:2203.09095) | ESEC/FSE 2022, 276 citations [checked] | Pre-trained model for change-quality estimation, review-comment generation, code refinement; foundational but predates instruction-tuned LLM-as-judge. | Background only — superseded in relevance by the Jin & Chen 2026 finding above for this project's actual review pattern. |

**Not deeply chased, flagged only in passing:** "The End of Code Review: Coding Agents Supersede
Human Inspection" (arXiv:2606.13175) — surfaced incidentally, not vetted. Worth a look if the
project's review-agent strategy becomes a live design question, not before.

---

## Part 2 — Geospatial data formats (light dip, relevant to the `siting` module)

| Topic | Finding | Applicability to `PointScopedLayerStore` / the siting pipeline |
|---|---|---|
| **GeoParquet** (OGC incubating standard, geoparquet.org) | Stores geometry (WKB) inside Parquet with CRS metadata; 20+ tools across 7 languages support it; a "GeoParquet 2.0" push toward native Parquet `GEOMETRY` types is in progress (Apache Parquet blog, Feb 2026). | Fits the existing polars/DuckDB/parquet-zstd ETL convention directly — layer tables could live in the same `save_parquet` pipeline instead of a separate shapefile/GeoJSON lane. |
| **GeoParquet has no spatial index** — Cloud-Native Geospatial Forum guide (guide.cloudnativegeo.org, actually read in full, not snippet) | The spec explicitly states spatial indices are "not yet part of the standard"; the only current locality trick is row-group min/max pruning, which only helps if the file was physically sorted by that column at write time. | The load-bearing caveat: GeoParquet is a plausible **archival/interchange** format for layers (better compression, one ecosystem, no shapefile multi-file fragility) but would **not** replace `PointScopedLayerStore`'s in-memory bbox-prefilter/R-tree behaviour. Any adoption is "write layers as GeoParquet, still build an in-process index on load" — not a drop-in replacement for the existing 12–57x bbox-prefilter speedup. |
| **H3 / S2 / geohash vs R-tree** | H3 (hexagonal grid, Uber) avoids geohash's corner-adjacency distortion and does neighbour search via k-ring lookup; S2 (Hilbert-curve cell IDs) suits sharded/KV-store proximity search; R-tree indexes actual bounding boxes and is the classic in-process/PostGIS choice. | For a single-process, single-country workload, an R-tree (or the existing bbox-prefilter, a cheap flat approximation of one) is the natural fit — which is what the store already does. H3/S2 pay off when sharding/caching cell-keyed data across a distributed store or precomputing "which layers touch this hex" for report generation — not an obvious near-term win here. |
| **Shapefile format gotchas** (ESRI Shapefile Technical Description, 1998; still canonical) | .shp/.shx/.dbf triple, field names capped at 10 characters, no true null (sentinel values), fragile if any one of the three files goes missing. | Plausible root of some council schema-drift issues on top of the already-found Irish-Grid/ITM CRS bug — the 10-char field truncation is a distinct failure class from CRS mis-projection. |
| **GeoJSON's implicit-WGS84 assumption** | GeoJSON technically allows other CRS but is "effectively restricted to WGS84 in most web contexts" per vendor/FME comparisons. | A second, independent source of CRS confusion beyond the Irish-Grid/ITM bug already found — argues for an explicit CRS-assertion check at ingest regardless of source format (Shapefile `.prj`, GeoJSON's implicit WGS84, or File Geodatabase's stored CRS), rather than trusting the format's convention. |
| **OGC WFS `DescribeFeatureType`** | Standard WFS operation returns a feed's schema before querying features; OGC now recommends the newer, lighter **OGC API – Features** (REST/JSON) for new implementations over WFS 2.0. | `DescribeFeatureType` is the standards-native way to catch council schema drift before a query — worth checking whether the project's WFS scraper calls it, or infers schema only from `GetFeature` output (the latter would miss drift until a query already fails). |
| **Open-data freshness measurement** (arXiv:2106.09590, evaluated German open-data portals) | Proposes measuring freshness from portal metadata timestamps (crawl-time minus declared-modified) rather than trusting publisher claims, treating staleness as a quality dimension distinct from completeness/accuracy. | A generic, cheap pattern that could formalize the project's existing per-council vintage-tracking concern (source seed registry going stale silently, per `project_planning_source_seed_registry_2026_07_24`) instead of the current ad hoc per-CDP checking. |

**Depth flag:** only the Cloud-Native Geospatial Forum GeoParquet guide was read in full; the rest
are search-result syntheses — treat exact version numbers/benchmarks as
**[Indicative — search snippet]** until read directly if they become load-bearing.

---

## Part 3 — EU planning-law cross-transferability (light dip, for future internationalization)

**Framing, stated up front so it isn't oversold:** spatial/land-use planning is a **member-state
competence**, not an EU-harmonized one. There is no EU rulebook for zoning categories, plan-making
procedure, or development-consent decision criteria — only soft-law coordination (Territorial
Agenda 2030). The EU frameworks below bite in specific, bounded slices; everything else in the
current siting engine is Irish implementation detail with no EU counterpart at all.

| EU instrument | What it actually harmonizes | What transfers vs. what doesn't |
|---|---|---|
| **EIA Directive** (2011/92/EU, amended 2014/52/EU) | Sets project *categories* requiring environmental impact assessment (Annex I mandatory, Annex II discretionary) and the general screening/assessment process shape. | The screening *logic shape* (project-type trigger → threshold or case-by-case screening → EIA report → public participation) transfers. Ireland's specific size/type thresholds (Planning Regulations Schedule 5), competent authority (An Bord Pleanála/local authority), and appeal mechanics are 100% national — full rebuild per country. |
| **Habitats Directive** (92/43/EEC) + **Birds Directive** (2009/147/EC) | Establish the Natura 2000 network (SACs, SPAs) and mandate Art. 6(3) "appropriate assessment" for any plan/project that could affect a designated site, with a common "no significant effect" test. | The trigger logic (proximity/connectivity to a Natura 2000 site → appropriate-assessment screening) is EU-set and transfers almost directly — site boundaries are published EU-wide via the Natura 2000 Viewer/EEA data. Ireland's "stage 1 screening / stage 2 NIS" procedural labels and NPWS guidance are national gloss on an EU core — lighter rebuild than EIA. |
| **Floods Directive** (2007/60/EC) | Requires every member state to produce flood hazard/risk maps and management plans on a fixed 6-year cycle, coordinated with the Water Framework Directive. Mandates *that* maps exist and are public — not a common format, scale, or how planning authorities must weight flood risk. | "Does this site fall in a mapped flood zone" as a check-existence pattern transfers. Ireland's specific source (OPW CFRAM/FloodInfo, Zone A/B/C classification, sequential/justification test from Ministerial guidelines) is Irish-specific — full swap of data source and decision test needed per country. Cross-reference: memory card `project_opw_floodinfo_licensing_2026_07_30` already covers the OPW licensing angle; not re-verified this session. |
| **SEA Directive** (2001/42/EC) | Requires plans that frame future development consent — including town/country planning — to undergo strategic environmental assessment before adoption. A process obligation on the plan-*maker*, not a content rulebook. | Explains why Irish county development plans carry an environmental report/alternatives assessment; doesn't harmonize zoning categories or plan content. Low direct value to the siting engine — the zoning/land-use logic itself stays purely national. |
| **INSPIRE Directive** (2007/2/EC) | Mandates that certain spatial data themes (land use, protected sites, natural hazard zones) be discoverable and interoperable via common metadata/network services across member states — a data-plumbing standard, not a planning-law standard. | Other member states' planning GIS layers are more likely to be *reachable* via standard web services (WFS/WMS, common metadata) than a from-scratch discovery effort — but attribute schemas, zoning taxonomies, and update cadence still vary by country and need per-country mapping work regardless. |

**Net read:** of the roughly five major rule families the current engine encodes, three (EIA,
habitats/Natura 2000, flood-zone existence) have a genuine EU floor to build on — the *trigger*
transfers, the *procedure and thresholds* don't. Zoning, protected-structures, derelict-sites
levy logic, and the county-development-plan hierarchy have no EU counterpart and would need a
full per-country rebuild if this product were ever internationalized.

---

## Assessment — checked against current code, 2026-07-31 (post-draft follow-up)

The draft above was never checked against what the repo's checkers actually enforce. This pass
reads the real files instead of assuming applicability from a paper's abstract, per
[evidence.md](../.claude/rules/evidence.md)'s existence-vs-applicability rule. All claims below
are `[Verified — file:line]`, not `[Reported]`.

**Useable — confirmed gaps, buildable now, same shape as existing rules:**

1. **No import-existence check exists.** `check_conventions.py` has ten rules (R1–R10: raw HTTP,
   UA literals, raw parquet writes, `logging.basicConfig`, raw coverage JSON, retired formatters,
   `@dt_page`, file-size ratchets, split-gate watch) — none check whether an imported package is
   declared [Verified — tools/check_conventions.py, full read]. Spracklen et al.'s
   package-hallucination finding maps onto a genuinely empty slot: AST-walk changed files for
   `import X`, diff against `pyproject.toml`/`uv.lock`, same shape as R1–R5. `uv sync` pruning is
   the only current backstop, and it fires after the code has already landed in a diff.
2. **The polars-for-ETL rule is enforced by discipline, not a check.** Zero `extractors/` files
   currently `import pandas` [Verified — `grep -rl "import pandas" extractors/` → 0 hits], but no
   rule would catch one if added. R3 (`raw-parquet-write`) already proves the
   zero-baseline-hard-rule pattern works at 100% adoption — a `pandas-in-extractors` rule is the
   same shape. The Twist/Harman ACL-2026 finding (models default to familiar libraries under
   pressure) is the concrete reason this rule is worth having *before* it's needed, not after.

**Already substantially covered — the paper validates existing design, not a build item:**

3. Jin & Chen's fix-guided verification (gate a review verdict on the model's own proposed fix
   passing tests) — `verifier.md` already requires CONFIRMED findings to carry "file:line, failing
   output," keeps PLAUSIBLE as a separate bucket, and is explicitly told to "resist inventing style
   or scope objections" [Verified — .claude/agents/verifier.md]. The builder→verifier split is
   structurally the same pattern. Downgraded from the draft's "most load-bearing finding" — it's
   confirmation the existing design is right, not a gap.

**Narrower than the draft suggested — partial coverage found:**

4. WFS schema-drift checking: the scraper does capture typeNames/GetCapabilities confirmation, but
   as a one-time comment ("CONFIRMED against the live server 2026-07-13"), not an automated
   per-ingest check [Verified — planning/product/sandbox/planning_layers_wfs.py:17,40]. Automating
   it into a per-run diff is still reasonable, just smaller than "nothing exists today."
5. Spatial-indexing read holds up unchanged: the point-scoped layer store uses a flat float32
   bbox-column prefilter (vectorized polars/numpy, not an R-tree)
   [Verified — planning/product/tools/build_point_scoped_layers.py]. GeoParquet still has no
   spatial index, so it stays archival-format-only value — not a build candidate on its own.

**Not re-checked this pass (no code to verify against — genuinely speculative/strategic):**
CRS-assertion-at-ingest (#4 below), and the EU-planning internationalization framing — both stay
scoping material, not confirmed gaps.

---

## What this could concretely change (candidates only — nothing here is adopted)

1. **Package-hallucination check at generation time** — confirmed gap, see Assessment #1 above.
2. **`pandas`-in-extractors ratchet rule** — confirmed gap, see Assessment #2 above.
3. **GeoParquet as an archival format for layer tables**, explicitly *not* as a replacement for
   the point-scoped store's in-memory index — worth scoping only if shapefile/GeoJSON multi-file
   fragility or cross-tool friction becomes a live pain point, not speculatively.
4. **CRS-assertion check at ingest for every new council feed**, format-agnostic (don't trust
   Shapefile `.prj`, GeoJSON's implicit WGS84, or File Geodatabase's stored CRS) — the Irish
   Grid/ITM bug is a member of a broader class per this scan, not a one-off worth a one-off fix.
5. **`DescribeFeatureType` call on WFS ingest** to catch council schema drift before a query
   fails, if the current scraper doesn't already do this — worth a code check, not assumed.
6. **Internationalization scoping, if it ever becomes live:** treat EIA/Natura-2000/flood-zone
   *trigger* logic as the portable core and everything downstream (thresholds, competent
   authority, zoning taxonomy, protected-structures/derelict-sites logic, plan hierarchy) as a
   full per-country rebuild — useful as a scoping frame for a future estimate, not an immediate
   build item.

None of the above has been scoped, prioritized, or approved — this is raw material for a
follow-up decision, per your request for a draft to review.
