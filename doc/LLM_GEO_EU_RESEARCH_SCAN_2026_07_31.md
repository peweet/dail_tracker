# Research scan — LLM code bias/quality, geospatial formats, EU planning law

**Date:** 2026-07-31. **Method:** 3 parallel web-research subagents (no code changes), scoped
deliberately narrow per instruction — heaviest effort on the LLM side, light dips into geospatial
and EU planning law. All findings below are **[Reported — research subagent, not independently
verified by primary-source read in this session]** unless a source says otherwise; treat citation
counts and specific numbers as indicative until re-checked if they become load-bearing for a build
decision. This file is a draft for review, not a decision record — nothing here has been adopted.

**Verification pass, 2026-07-31 (later same day):** every cited paper, link and directive was
re-checked by two independent web-verification subagents. All 11 papers, the Feb-2026 Apache
Parquet blog post and all six EU directives are real; corrections found by that pass have been
applied inline below, each marked `[corrected 07-31 — <source>]`. Claims so marked are
[Reported — verification subagent, primary source fetched], one band above the original draft.

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
| Twist, Harman et al., "A Study of LLMs' Preferences for Libraries and Programming Languages" (arXiv:2503.17181) | **Findings of** ACL 2026 `[corrected 07-31 — arXiv comments field; doc previously implied main conference]` | Across 8 LLMs, models pick familiar/popular libraries over task-appropriate ones — NumPy used unnecessarily in up to 45% of cases, Python chosen in 58% of performance-critical cases where it's suboptimal, and models contradict their own stated language recommendations 83% of the time — the 83% figure is scoped in the paper to **project-initialisation tasks**, not all cases `[corrected 07-31 — paper body]`. Co-authored by Mark Harman (established SE researcher). | Direct hit. This project enforces `polars for ETL, pandas only in the UI layer` as a never-break rule specifically because the two libraries look interchangeable to a model reaching for familiarity. This paper is evidence the failure mode is real and systemic, not project-specific paranoia — supports keeping that rule as a hard, machine-checked convention (`tools/check_conventions.py`) rather than prose guidance alone. |
| Cassano et al., "MultiPL-E" (arXiv:2208.08227) | IEEE TSE 2023 | Most code-gen benchmarks (HumanEval etc.) are Python-only; mechanically translating to 18 languages shows models often match or beat Python performance elsewhere. | Background/caveat only — cuts against a naive "LLMs are just worse outside Python" narrative. Low action value for this Python-only repo. |
| "Exploring Multi-Lingual Bias of Large Code Models" (arXiv:2404.19368) | ACM TOSEM (journal record vol 35, ≈2026; arXiv preprint 2024 — the draft's "TOSEM 2024" conflated the two) `[corrected 07-31 — Semantic Scholar, DOI 10.1145/3786793]`, 30 citations [checked — Semantic Scholar, re-confirmed exact 07-31] | Quantifies multilingual generation-behavior bias beyond raw accuracy — specifically English-vs-non-English *instruction* bias (≥13% Pass@1 drop with Chinese instructions) `[corrected 07-31 — abstract]`. | Tangential — this project is Python-only; relevant only if the codebase ever grows a second language. |
| Spracklen et al., "We Have a Package for You!" (arXiv:2406.10279) | USENIX Security 2025 `[corrected 07-31 — venue confirmed, arXiv ID added]` | **576,000 code samples** producing **2.23M package references**, 16 models, Python+JS: **19.7% of generated packages** were hallucinated (440,445 of 2.23M) — the draft misstated 2.23M as the sample count and 19.7% as a per-generation rate `[corrected 07-31 — paper abstract]`; commercial models 5.2%, open-source 21.7% (confirmed verbatim in abstract). | Concrete and actionable. A hallucinated import is exactly what `tools/check_conventions.py`'s convention ratchet is positioned to catch, but that tool checks *placement* (extractors → `services/http_engine` etc.), not *existence* of imported packages. Worth checking whether agent-proposed new dependencies get any existence/hallucination check before landing — currently `uv sync` pruning undeclared deps is the closest thing on record (memory card `project_dependency_declaration_audit_2026_07_20`), which catches the symptom late (sync time) rather than at generation time. |

### 1b. LLM code quality / code review research

| Source | Venue/year | Finding | Applicability |
|---|---|---|---|
| Jin & Chen, "Are LLMs Reliable Code Reviewers? Systematic Overcorrection in Requirement Conformance Judgement" (arXiv:2603.00539) | Automated Software Engineering (Springer), 2026 — vol 33, art. 90, DOI 10.1007/s10515-026-00638-5 `[corrected 07-31 — venue now confirmed on link.springer.com; arXiv page alone carries no journal-ref]` | LLM reviewers systematically over-flag correct code as non-conformant, driven by requirement hallucination (inventing unstated constraints) rather than style nitpicking. More elaborate review prompts (asking for explanations/fixes) make it *worse*, not better. Proposes a "fix-guided verification filter" using the model's own proposed fix as counterfactual evidence, validated against tests. | The most load-bearing single finding in this scan for this project's own harness. `verifier` and `code-reviewer` agents, plus the `/code-review` and `security-review` skills, are exactly the "LLM as reviewer" pattern under test here. Two direct implications: (1) keep review prompts constrained rather than "explain your reasoning at length" — this paper found that backfires; (2) the fix-guided-verification idea (gate a review verdict on whether the model's own proposed fix actually passes tests) lines up with this project's existing CONFIRMED-via-reproduction-over-assertion discipline (`evidence.md`) and is a concrete pattern the `verifier` agent doesn't currently implement — it reports findings, it doesn't propose-and-test a fix as evidence. |
| Yetiştiren et al., "Evaluating the Code Quality of AI-Assisted Code Generation Tools" (arXiv:2304.10778) | 2023 | Multi-dimensional quality study (correctness, validity, security, reliability, maintainability, technical-debt minutes) across Copilot/CodeWhisperer/ChatGPT, not just pass@k. | Directly on-point for "code quality" as a broader concept than functional correctness. If this project ever wants a quality *score* for agent-generated diffs beyond pass/fail tests, this metric set (maintainability + technical-debt-minutes) is a more defensible starting taxonomy than inventing one. |
| Pearce et al., "Asleep at the Keyboard?" (arXiv:2108.09293) | IEEE S&P 2022 / CACM 2025 reprint | ~40% of 1,689 Copilot-generated programs across 89 CWE scenarios were vulnerable. | Base-rate prior: agent-authored code needs security scrutiny independent of whether it looks correct. Reinforces existing review/firewall habits rather than adding something new. |
| Li, Dutta, Naik, "IRIS: LLM-Assisted Static Analysis" (arXiv:2405.17238) | ICLR 2025 | Combines LLM-inferred taint specs with static analysis for whole-repo vulnerability detection, reducing hand-written specs. | Not currently wired into this project's stack (the firewall/convention checkers are rule-based, not LLM+static-analysis hybrids). Relevant only if the project later wants an LLM-assisted variant of `check_streamlit_logic_firewall.py`/`check_conventions.py` — not a near-term fit given those checkers currently work and are cheap/deterministic. |
| Liu et al., "EvalPlus/HumanEval+" (arXiv:2305.01210) | NeurIPS 2023, 2,002 citations `[corrected 07-31 — Semantic Scholar API; the draft's ~1,313 was stale]` | HumanEval's test suites are weak; an 80x-larger test suite drops measured pass@k by 19.3–28.9% (paper phrases the drop in %, not pp) `[corrected 07-31 — abstract]` and reorders model rankings. | Caveat for interpreting any future benchmark claim about code-gen capability — not a direct action item, since this project doesn't currently cite HumanEval-style scores anywhere. |
| Li et al., "CodeReviewer" (arXiv:2203.09095) | ESEC/FSE 2022, 276 citations [checked] | Pre-trained model for change-quality estimation, review-comment generation, code refinement; foundational but predates instruction-tuned LLM-as-judge. | Background only — superseded in relevance by the Jin & Chen 2026 finding above for this project's actual review pattern. |

**Not deeply chased, flagged only in passing:** "The End of Code Review: Coding Agents Supersede
Human Inspection" (arXiv:2606.13175) — surfaced incidentally, not vetted. Worth a look if the
project's review-agent strategy becomes a live design question, not before.

---

## Part 2 — Geospatial data formats (light dip, relevant to the `siting` module)

| Topic | Finding | Applicability to `PointScopedLayerStore` / the siting pipeline |
|---|---|---|
| **GeoParquet** (OGC incubating standard, geoparquet.org) | Stores geometry (WKB) inside Parquet with CRS metadata; the site now lists ~25+ tools across ~9 language bindings `[corrected 07-31 — geoparquet.org; the draft's "20+/7" was an older tagline]`; a "GeoParquet 2.0" push toward native Parquet `GEOMETRY` types is in progress (Apache Parquet blog, 2026-02-13 — post confirmed real by both verification passes; the types already landed in Parquet format 2.11.0, GeoParquet 2.0 itself is at rc.1). | Fits the existing polars/DuckDB/parquet-zstd ETL convention directly — layer tables could live in the same `save_parquet` pipeline instead of a separate shapefile/GeoJSON lane. |
| **GeoParquet has no spatial index** — Cloud-Native Geospatial Forum guide (guide.cloudnativegeo.org, actually read in full, not snippet) | The spec explicitly states spatial indices are "not yet part of the standard"; the only current locality trick is row-group min/max pruning, which only helps if the file was physically sorted by that column at write time. | The load-bearing caveat: GeoParquet is a plausible **archival/interchange** format for layers (better compression, one ecosystem, no shapefile multi-file fragility) but would **not** replace `PointScopedLayerStore`'s in-memory bbox-prefilter/R-tree behaviour. Any adoption is "write layers as GeoParquet, still build an in-process index on load" — not a drop-in replacement for the existing 12–57x bbox-prefilter speedup. |
| **H3 / S2 / geohash vs R-tree** | H3 (hexagonal grid, Uber) avoids geohash's corner-adjacency distortion and does neighbour search via k-ring lookup; S2 (Hilbert-curve cell IDs) suits sharded/KV-store proximity search; R-tree indexes actual bounding boxes and is the classic in-process/PostGIS choice. | For a single-process, single-country workload, an R-tree (or the existing bbox-prefilter, a cheap flat approximation of one) is the natural fit — which is what the store already does. H3/S2 pay off when sharding/caching cell-keyed data across a distributed store or precomputing "which layers touch this hex" for report generation — not an obvious near-term win here. |
| **Shapefile format gotchas** (ESRI Shapefile Technical Description, 1998; still canonical) | .shp/.shx/.dbf triple, field names capped at 10 characters, no true null (sentinel values), fragile if any one of the three files goes missing. | Plausible root of some council schema-drift issues on top of the already-found Irish-Grid/ITM CRS bug — the 10-char field truncation is a distinct failure class from CRS mis-projection. |
| **GeoJSON's WGS84 mandate** | The current spec (RFC 7946, 2016) **removed the `crs` member outright** — WGS84/CRS84 is the only legal CRS, other CRS permitted only by "prior arrangement" between parties; "technically allows other CRS" described the superseded 2008 spec `[corrected 07-31 — RFC 7946 §4]`. In practice council feeds may still carry 2008-spec files with non-WGS84 coordinates, which is exactly the trap. | A second, independent source of CRS confusion beyond the Irish-Grid/ITM bug already found — argues for an explicit CRS-assertion check at ingest regardless of source format (Shapefile `.prj`, GeoJSON's implicit WGS84, or File Geodatabase's stored CRS), rather than trusting the format's convention. |
| **OGC WFS `DescribeFeatureType`** | Standard WFS operation returns a feed's schema before querying features; OGC now recommends the newer, lighter **OGC API – Features** (REST/JSON) for new implementations over WFS 2.0. | `DescribeFeatureType` is the standards-native way to catch council schema drift before a query — worth checking whether the project's WFS scraper calls it, or infers schema only from `GetFeature` output (the latter would miss drift until a query already fails). |
| **Open-data freshness measurement** (arXiv:2106.09590, Wenige et al., evaluated German open-data portals) | Measures freshness as crawl-time minus `dcat:modified` (§4.3 of the paper) — but as one self-described *explorative* dimension inside a broader portal-quality framework, not the paper's thesis; the mechanism is real, the draft over-weighted it `[corrected 07-31 — full text via ar5iv; the abstract doesn't mention freshness at all]`. | A generic, cheap pattern that could formalize the project's existing per-council vintage-tracking concern (source seed registry going stale silently, per `project_planning_source_seed_registry_2026_07_24`) instead of the current ad hoc per-CDP checking. |

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
| **Habitats Directive** (92/43/EEC) + **Birds Directive** (2009/147/EC) | Establish the Natura 2000 network (SACs, SPAs) and mandate Art. 6(3) "appropriate assessment" for any plan/project that could affect a designated site — a two-stage test: *likely significant effect* triggers screening, and consent then turns on "no adverse effect on the site's **integrity**" `[corrected 07-31 — EC Art. 6 guidance; the draft's single "no significant effect test" compressed the two stages]`. | The trigger logic (proximity/connectivity to a Natura 2000 site → appropriate-assessment screening) is EU-set and transfers almost directly — site boundaries are published EU-wide via the Natura 2000 Viewer/EEA data. Ireland's "stage 1 screening / stage 2 NIS" procedural labels and NPWS guidance are national gloss on an EU core — lighter rebuild than EIA. |
| **Floods Directive** (2007/60/EC) | Requires every member state to produce flood hazard/risk maps and management plans on a fixed 6-year cycle, coordinated with the Water Framework Directive. Mandates *that* maps exist and are public — not a common format, scale, or how planning authorities must weight flood risk. | "Does this site fall in a mapped flood zone" as a check-existence pattern transfers. Ireland's specific source (OPW CFRAM/FloodInfo, Zone A/B/C classification, sequential/justification test from Ministerial guidelines) is Irish-specific — full swap of data source and decision test needed per country. Cross-reference: memory card `project_opw_floodinfo_licensing_2026_07_30` already covers the OPW licensing angle; not re-verified this session. |
| **SEA Directive** (2001/42/EC) | Requires plans that frame future development consent — including town/country planning — to undergo strategic environmental assessment before adoption. A process obligation on the plan-*maker*, not a content rulebook. | Explains why Irish county development plans carry an environmental report/alternatives assessment; doesn't harmonize zoning categories or plan content. Low direct value to the siting engine — the zoning/land-use logic itself stays purely national. |
| **INSPIRE Directive** (2007/2/EC) | Mandates that certain spatial data themes (land use and natural **risk** zones — the directive's Annex III term, not "hazard zones" — plus protected sites, Annex I `[corrected 07-31 — INSPIRE knowledge base]`) be discoverable and interoperable via common metadata/network services across member states — a data-plumbing standard, not a planning-law standard. | Other member states' planning GIS layers are more likely to be *reachable* via standard web services (WFS/WMS, common metadata) than a from-scratch discovery effort — but attribute schemas, zoning taxonomies, and update cadence still vary by country and need per-country mapping work regardless. |

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
5. Spatial-indexing read holds up, with one correction (2026-07-31 re-check): the flat float32
   bbox columns only window the *disk read*; the query path then builds a shapely `STRtree` (an
   R-tree variant) over the loaded rows and runs every spatial predicate against it
   [Verified — planning/product/core/layers.py:170,377]. The earlier "not an R-tree" phrasing
   misdescribed the query path. The conclusion is unchanged — strengthened, even: an in-process
   index is built at load regardless of file format, GeoParquet has no in-file spatial index
   (its only acceleration is bbox-covering + row-group statistics, and the covering shape is a
   struct of f64s, not this repo's flat outward-rounded f32 columns — conforming is a
   restructure, not a rename), so it stays archival-format-only value — not a build candidate.
   Re-check also found the runtime "shapely only — no geopandas/GDAL" rule excludes the standard
   GeoParquet toolchain, and DuckDB spatial already reads the WKB-column files via
   `ST_GeomFromWKB` — the adoption case is weaker than this draft suggested. Skipped, decided
   2026-07-31.

**Not re-checked this pass (no code to verify against — genuinely speculative/strategic):**
CRS-assertion-at-ingest (#4 below), and the EU-planning internationalization framing — both stay
scoping material, not confirmed gaps.

---

## What this could concretely change (candidates only — nothing here is adopted)

1. **Package-hallucination check at generation time** — confirmed gap, see Assessment #1 above.
2. **`pandas`-in-extractors ratchet rule** — confirmed gap, see Assessment #2 above.
3. **GeoParquet as an archival format for layer tables** — DECIDED 2026-07-31: skipped, see
   Assessment #5 (query path builds an STRtree regardless of format; covering shape doesn't
   match the repo's f32 bbox columns; runtime excludes the geopandas/GDAL toolchain; DuckDB
   already reads the WKB files). The ingest docstring's "GeoParquet" label was corrected to
   "parquet (WKB column)" the same day.
4. **CRS-assertion check at ingest for every new council feed**, format-agnostic (don't trust
   Shapefile `.prj`, GeoJSON's implicit WGS84, or File Geodatabase's stored CRS) — the Irish
   Grid/ITM bug is a member of a broader class per this scan, not a one-off worth a one-off fix.
   **SHIPPED 2026-07-31** after adversarial verification confirmed the gap (the country-bbox
   gate passes any layer-wide shift up to ~2°): response-SR probe (hard gate on positive
   mismatch), per-layer `Anchor` tripwires derived from the validated vintage (hard gate,
   6 layers), bbox-coherence fraction (warn+record) — `planning/product/ingest/
   planning_layers_ingest.py` CRS-guard section + `test_planning_layers_crs_guard.py`.
5. **`DescribeFeatureType` call on WFS ingest** to catch council schema drift before a query
   fails, if the current scraper doesn't already do this — worth a code check, not assumed.
6. **Internationalization scoping, if it ever becomes live:** treat EIA/Natura-2000/flood-zone
   *trigger* logic as the portable core and everything downstream (thresholds, competent
   authority, zoning taxonomy, protected-structures/derelict-sites logic, plan hierarchy) as a
   full per-country rebuild — useful as a scoping frame for a future estimate, not an immediate
   build item.

None of the above has been scoped, prioritized, or approved — this is raw material for a
follow-up decision, per your request for a draft to review.

---

## Part 4 — Precision, memory, and cross-reference bug classes (added 2026-07-31, same day, follow-on)

Triggered by a real CRS bug found this session (a council GeoJSON had Irish Grid coordinates
mislabeled/read as ITM). Deliberately **excludes** anything Part 2 already covered — GeoParquet,
H3/S2 vs R-tree, Shapefile field-truncation, GeoJSON's implicit-WGS84 assumption, WFS
`DescribeFeatureType`, open-data freshness. 3 parallel web-research subagents (blogs, GitHub
issues, mailing lists, docs — not literature-search); findings below are
**[Reported — subagent web synthesis]** unless marked `[Verified — file:line]` from this
session's own follow-up code read.

### 4a. Precision (float32/64, reprojection, serialization)

| Source | Mechanism | Applicability |
|---|---|---|
| Vuorinen, "Using Floating-Point Numbers to Represent Geographic Coordinates" (2023) | float32 ULP grows with coordinate magnitude (IEEE 754 exponent-dependent); worst case near ±180° ≈1.7 m, at Irish latitudes (~53°) ≈0.4 m `[Extracted — arithmetic on a Reported formula]` | The bbox-prefilter's actual handling is stronger than "worst case degrades gracefully" — see verified fix below. |
| pandas issue #43693 | `.astype(float32)`/`downcast='float'` silently corrupts integers ≥2^24 (16,777,216) | Irish Grid eastings/northings and lat/lon degrees stay well under 2^24 — not a live risk unless a future column stores Web Mercator or raw large-integer IDs as float32. |
| pyproj issue #232 | A pyproj 2.1.2 upgrade shifted the same transform's output by **1.025 m** vs 1.9.6/2.0.2, no error raised | **Genuine gap** — grepped for `golden|regression.*crs` project-wide, found no fixture pinning known Irish reference points through the transform chain. A pyproj/GDAL version bump today would ship silently if it shifted coordinates a few metres. |
| GDAL issue #2086 | Coordinate rounding on write (e.g. via a shapefile/WKB writer with implicit precision) can turn a valid polygon **self-intersecting** — precision loss altering topology, not just accuracy | Same failure class as the `buffer(0)` gap found below (4c) — any format-conversion hop needs a post-write `is_valid` check, not just the pre-write one this repo already has. |
| astropy #13708 / CliMA Insolation.jl #29 | float32-rounded values can land *outside* a boundary that was valid in float64 (nearest float32 to π/2 is slightly > π/2) | **Already mitigated** `[Verified — planning/product/tools/build_point_scoped_layers.py:8-69]`: the point-scoped store's bbox columns are deliberately OUTWARD-rounded via `np.nextafter` specifically to avoid the shrink-boundary version of this bug — the code comment already names the exact failure mode ("`.astype(float32)` rounds-to-nearest and SHRINKS the box on most rows — a silently dropped row"). No action needed; flagging because it confirms the existing design already anticipated this literature. |
| geojson (jazzband) issue #173 | `numpy.float32` fails the `geojson` package's JSON-compliance check (`float64` passes) | Cheap smoke test if any export path serializes float32-sourced columns straight to GeoJSON without casting to Python float first — would fail loud (crash), not corrupt silently, so low priority. |

### 4b. Memory and efficiency

| Source | Mechanism | Applicability |
|---|---|---|
| shapely Discussion #1828 | Millions of small GEOS calls (even ones returning empty) fragment glibc's heap; RSS stays elevated even though logical memory is flat; `malloc_trim(0)` recovers it | Hot query path uses the vectorized float32 bbox prefilter, not a per-candidate Python/GEOS loop, so this mostly doesn't bite there `[Verified — the store's own design, per memory card project_siting_point_scoped_store_shipped_2026_07_23]`. Relevant only to any exact-geometry check that runs in a loop *after* the prefilter narrows candidates. |
| GDAL/OGR `CoordinateTransformation` reuse; pyproj `Transformer` construction cost | Building a transform object per-row/per-feature instead of once per CRS pair is both slow (re-parses the PROJ pipeline) and, in the C API, a stale-reference risk | **Checked — already done right** `[Verified — extractors/epa_licensed_facilities_extract.py:112-124]`: `Transformer.from_crs(29902, 4326, always_xy=True)` is built once outside the per-feature loop and reused. Worth keeping as the template for any new extractor rather than a currently-missing guard. |
| shapely issue #2056 | `shapely.buffer()` (functional) and `geom.buffer()` (method) have different default `quad_segs` (8 vs 16) — a call-style switch silently doubles output vertex density | Not checked against every buffer call site this session; worth a grep (`buffer(` across `extractors/`, `planning/`) if buffer-heavy code is touched next, to pin `quad_segs` explicitly rather than rely on the inconsistent default. |
| GEOS issue #330; PostGIS `ST_MakeValid` docs | `make_valid()`/`buffer(0)` repair can occasionally balloon a geometry (one reported case: 1.11 → 4205.05 area units, ~3700×) or change geometry *type* (Polygon → GeometryCollection) instead of just fixing it | **Genuine, concrete gap** — see 4c, this is the most actionable finding in Part 4. |
| geopandas issues #955, #2171; `read_file()` full-materialization | GeoPandas-specific memory leaks (`sjoin` +9.3 GB not reclaimed) and `unary_union`-on-GeoSeries-object segfault | **Not applicable** `[Verified — pyproject.toml:83, extractor comment "runtime layer store stays shapely-only"]`: this repo has no geopandas runtime dependency at all — shapely + pyproj + polars/numpy only. These bugs live in a library this project doesn't use at runtime. |
| GDAL issue #640 | `GDAL_CACHEMAX` sizes off *host* memory by default, not a container's cgroup limit — can OOM-kill inside Docker even when the app's own memory guard (`guard_memory.py`) looks fine | Conditional — applicable only if ETL/extraction ever runs inside the repo's `Dockerfile`. Worth a one-line check (`echo $GDAL_CACHEMAX` in the container) rather than assumed fine. |

### 4c. Extraction and cross-reference (axis order, datum, silent-wrong-answer joins)

| Source | Mechanism | Applicability |
|---|---|---|
| macwright.com "lon lat lon lat lon"; GDAL RFC 20 | GeoJSON/Shapefile/WKB are lon-first; EPSG's own authority definition of 4326 is lat-first; WFS/WMS axis order flips by **protocol version** (WFS 1.0.0 lon/lat vs WFS 1.1.0+ lat/lon), not by CRS code | This is the general bug *class* the already-found Irish Grid/ITM incident belongs to. Since every council WFS endpoint is an independent server/version, the same `EPSG:4326` string can mean different axis orders from council to council. |
| GDAL RFC 20 + gdal-dev thread, EPSG:5514 shapefile regression | GDAL 3/PROJ 6 changed the *default* from ignoring authority axis order to honouring it — the identical file, same code, silently reprojected wrong after a routine library upgrade | Directly precedent for the pyproj-version-drift gap in 4a (#232) — a dependency bump is a live vector for silently changing axis/coordinate interpretation with zero error raised. Argues for the same fix: golden reference-point regression test gating GDAL/pyproj version bumps. |
| QGIS issues #57965 (2024), #33673 (2020) | #57965: QGIS→GeoServer WFS-T writes can axis-swap silently when the swapped longitude still falls in a valid range (no error, corrupted data at rest) | Sharpens the existing WFS gap already logged in Part 2 (`DescribeFeatureType` per-ingest check) — the risk isn't just schema drift, it's axis order flipping **silently** when the swapped value happens to look plausible. An Ireland-bbox sanity assert at ingest (lon ∈ [-10.7, -5.9], lat ∈ [51.4, 55.4]) catches the *loud* failure case; it does not catch a swap where both swapped values still happen to fall in range near the boundary. |
| killetsoft.de, WGS84 vs ETRS89 divergence | The two datums have drifted ~80-90 cm apart since 1989, diverging ~2.5 cm/year; some tools apply a hard-coded zero transform between them | Genuinely new risk, not previously logged: Irish national mapping (ITM, EPSG:2157) is ETRS89-based; a raw GPS/WGS84 site coordinate (e.g. from a mobile survey) joined against ITM-derived cadastral layers without an explicit datum step is off by a growing sub-metre amount — invisible for a flood-zone polygon, live for a near-boundary cadastral-parcel join. **Not checked this session** whether any ingest path treats WGS84 and ETRS89 as interchangeable — worth a grep for hard-coded WGS84↔ETRS89 passthrough before treating this as confirmed either way. |
| geopandas issue #2198 | Shapely 1.8 + PyGeos on GEOS 3.9.1 returned "within" for a point ~400 m outside a real polygon — silently wrong, no exception | **Not currently exposed** `[Verified — pyproject.toml:83, "shapely>=2.0"]`: this repo is pinned past the affected 1.8 combination and has no geopandas/PyGeos runtime dependency. Relevant as a cautionary precedent for future version pins, not a live bug here. |
| Esri ArcGIS Pro docs, "Unclosed Polygon" | Rings that don't close (common in non-geodatabase Shapefile exports, i.e. most council open-data dumps) load and join without error but corrupt analysis results | Combine with the GEOS #330 make_valid-can-balloon-area finding (4b): **the two together are the concrete gap** — `[Verified — reference/local_authority_boundaries_extract.py:142-143, reference/constituency_boundaries_extract.py:77-78]` both do `if not geom.is_valid: geom = geom.buffer(0)` with no post-repair area-delta or bbox sanity check. `[Verified — planning/civic/extractors/planning_decision_profiles.py:20,91]` shows this project *already has* the stronger pattern one file over — `make_valid()` + an Ireland-bbox guard, documented inline as lessons from `project_planning_arcgis_validation`. The two older extractors just never got upgraded to it. |
| GeoPandas docs, `sjoin_nearest` | Nearest-join distance is meaningless in an unprojected (degree-based) CRS — runs and returns a number anyway, no warning | **Checked, not present** `[Verified — grepped project-wide for sjoin_nearest/sjoin/always_xy]`: no `sjoin_nearest` usage found anywhere in the repo (consistent with the no-geopandas-at-runtime finding above). Worth keeping as a standing rule for any future distance-based join: reproject to EPSG:2157 (ITM, metres) first, never join by distance in EPSG:4326. |

### Assessment — what's already solid vs. genuinely open

**Already handled well, confirmed by this session's code read, not just assumed:**
- `always_xy=True` is set on the one live pyproj `Transformer` call, and it's built once and reused, not per-row `[Verified — extractors/epa_licensed_facilities_extract.py:110-124]`.
- The float32 bbox prefilter deliberately outward-rounds via `np.nextafter` specifically to avoid the shrink-boundary bug the literature warns about `[Verified — planning/product/tools/build_point_scoped_layers.py:8-69]`.
- No geopandas runtime dependency — the entire GeoPandas-specific bug family (sjoin memory leak, sjoin_nearest CRS trap, unary_union segfault, full-dataset `read_file`) doesn't apply `[Verified — pyproject.toml:83]`.
- `make_valid()` + Ireland-bbox guard is an established, documented pattern in this codebase already (`planning_decision_profiles.py`, `planning_applications_ingest.py`) — this isn't new territory, it's an existing convention.

**Genuinely open, not previously logged (status re-checked 2026-07-31, later same day):**
1. **`buffer(0)`-only validity repair with no bbox sanity check** in `reference/local_authority_boundaries_extract.py:142-143` and `reference/constituency_boundaries_extract.py:77-78` — the weaker pattern sat right next to the stronger one (`planning_decision_profiles.py`) in the same codebase. **SHIPPED 2026-07-31**: both sites now check the repaired geometry's bounds against the same `IRL` envelope `planning_decision_profiles.py` uses and print a warning (not a hard drop — these are locator-map builders with their own "all N canonical entries present" integrity check, so a silent drop would fail that check loudly instead; a raise was judged too strict for a manually-run `--write` script).
2. **No golden-coordinate regression test gating pyproj/GDAL version bumps.** **Substantially covered by the `Anchor`/`AnchorTracker` mechanism SHIPPED 2026-07-31** (see item 4 in "What this could concretely change" above, `planning/product/ingest/planning_layers_ingest.py:79,1874`) — 6 layers now carry a known-reference-point tripwire checked at every ingest, which would catch a pyproj/GDAL-version-driven coordinate shift for those layers. Not identical to a dedicated CI-gated regression test (it fires at ingest time for 6 specific layers, not on every dependency bump for the whole transform chain), so a narrower gap remains if that distinction matters, but the core mechanism this item asked for now exists.
3. **WGS84/ETRS89 datum-passthrough — checked, not found.** Grepped project-wide for `ETRS89`/`29903`/`29902`/`WGS84.*ETRS` — no ingest path treats a raw WGS84 coordinate as directly comparable to ITM/ETRS89-derived layers without an explicit transform step. Downgraded from "open" to "checked, no live instance found" — not proof none exists (per the proving-absence standard), but no longer an unchecked claim.
4. **Silent (in-range) axis-swap risk from council WFS servers.** Same status as item 2 — the `Anchor` tripwire mechanism would catch this for the 6 anchored layers (an axis swap moves the anchor point, which the tripwire compares against); layers without an anchor assigned yet remain exposed. Sharpens rather than replaces the existing Part-2 `DescribeFeatureType` gap.

Net: of the four items, only #1 was still a clean, unshipped gap — now fixed. #2 and #4 turned out to already be substantially addressed by same-day work on a different part of this scan; #3 was checked and not confirmed as live. This is why re-checking status before building matters — three of four "open" items had moved since they were first written.

---

## Part 5 — Scholarly literature pass (added 2026-07-31, follow-on to Part 4)

Part 4 leaned almost entirely on blogs/issue-trackers/mailing lists — that's a scoping gap, not
a finding that no scholarly literature exists (the original request named "papers, scholarly
papers" as co-equal targets alongside blogs/forums). This is the dedicated academic-literature
pass, 3 parallel subagents restricted to peer-reviewed journals, conference proceedings, and
citable preprints only. **Citation-count caveat throughout: the Semantic Scholar API returned
HTTP 429 (rate-limited) for nearly every query this session — counts below are `[Verified —
Semantic Scholar API]` only where explicitly marked; everything else is `[Reported — a
publisher/ScisPace listing, not independently cross-checked]`, so treat exact numbers as
indicative.**

### 5a. Coordinate precision & spatial computation correctness

| Source | Finding | Citations |
|---|---|---|
| Shewchuk, "Adaptive Precision Floating-Point Arithmetic and Fast Robust Geometric Predicates," *Discrete & Computational Geometry* 18(3), 1997 | Standard IEEE double precision gives topologically **wrong-sign** answers for orientation/incircle predicates near degeneracy; derives the adaptive-precision algorithms underlying GEOS's and CGAL's "robust predicates." | 401 `[Reported — Springer counter]` |
| Chrisman, "A theory of cartographic error and its measurement in digital databases," *Auto-Carto 5*, 1982 | The epsilon-band model: positional error in digitized vector data is a band around each vertex, not a point value, propagating into every downstream geometric operation. | not verified |
| Veregin, "Developing and testing an error propagation model for GIS overlay operations," *IJGIS* 9(6), 1995 | Formal probabilistic model for positional/attribute error propagating through overlay (AND/OR/XOR) — matches simulation for AND/OR, **breaks down for XOR**, a concrete divergence between the analytic model and reality. | not verified |
| Goodchild & Hunter, "A simple positional accuracy measure for linear features," *IJGIS* 11(3), 1997 | Measures positional accuracy as % of feature length within a buffer of a reference dataset — the standard metric still used to validate conflation/matching output. | not verified |

Coverage note (from the subagent, not softened): this was the thinnest of the three academic
slices — no peer-reviewed paper directly benchmarking "float32 vs float64 storage → spatial-join
accuracy delta" turned up. The above addresses precision's *downstream analytical* effect, which
is adjacent to but not identical to the raw-magnitude question Part 4 covered from blogs/issues.

### 5b. Spatial indexing / join performance (academic, not the H3/S2/GeoParquet ground already covered)

| Source | Finding | Citations |
|---|---|---|
| Guttman, "R-trees: A dynamic index structure for spatial searching," *ACM SIGMOD*, 1984 | The original R-tree paper — foundational structure behind nearly every spatial index in use today, including this project's own choice to use a flat bbox-prefilter as a cheap R-tree approximation (per Part 2). | 8,560 `[Verified — Semantic Scholar API]` |
| Brinkhoff, Kriegel & Seeger, "Efficient processing of spatial joins using R-trees," *ACM SIGMOD*, 1993 | Synchronized R*-tree traversal + plane sweep cuts spatial-join execution time roughly an order of magnitude over the naive approach; shows the join step is superlinear in object count without it. | not verified, but one of the most-cited spatial-join papers |
| Jacox & Samet, "Spatial join techniques," *ACM TODS* 32(1), 2007 | Survey decomposing spatial-join algorithms into a common framework (partitioning, in-memory join, refinement) — the standard reference taxonomy for comparing join families. | not verified |
| Kothuri, Ravada & Abugov, "Quadtree and R-tree indexes in Oracle Spatial: A comparison using GIS data," *ACM SIGMOD*, 2002 | Real-workload comparison: R-trees beat quadtrees ~2-3× on most queries up to a 10-mile radius, but quadtree build is ~4× faster and very tuning-sensitive. | not verified |
| Sowell et al., "An Experimental Analysis of Iterated Spatial Joins in Main Memory," *PVLDB* 6(14), 2013 | Empirical study of repeated spatial joins held entirely in memory — closer to a modern in-memory analytics workload than the classic disk-bound join literature. | not verified |
| Sidlauskas et al., "Trees or Grids? Indexing Moving Objects in Main Memory," *ACM SIGSPATIAL*, 2009 | Head-to-head R-tree vs. grid for in-memory, update-heavy workloads — grids become competitive with R-trees on query time once paired with a secondary object-ID index. | 82 `[Reported — ScisPace]` |

Applicability: this confirms (doesn't newly justify) the architectural choice already reviewed in
Part 2 — a flat bbox-prefilter is a cheap in-process approximation of exactly the R-tree
structure this literature treats as the baseline, appropriate for a single-process, national-
extent workload rather than the distributed/on-disk regime most of this literature targets.

### 5c. Spatial cross-referencing / record linkage under positional uncertainty

| Source | Finding |
|---|---|
| Cheng, Xia, Prabhakar, Shah & Vitter, "Efficient Indexing Methods for Probabilistic Threshold Queries over Uncertain Data," *VLDB*, 2004 | Standard exact-position index structures don't extend cleanly once input coordinates carry positional uncertainty rather than being treated as exact — motivates "probabilistic join" as a distinct problem from ordinary spatial join. |
| Cheng et al., "Efficient join processing over uncertain data," *ACM CIKM*, 2006 | Empirically shows naive exact-match join logic over- or under-counts matches once positional uncertainty is modeled explicitly. |
| Kim et al., "A new method for matching objects in two different geospatial datasets based on the geographic context," *Computers & Geosciences*, 2010 | Matches features across two independently-produced datasets at different levels of detail using buffer-growing/Voronoi/triangulation context rather than raw coordinate matching — a direct methodological answer to "how do you link two independently-sourced spatial datasets when their positional precision differs," which is structurally this project's council-feed cross-referencing problem. |
| Song et al., "A New, Score-Based Multi-Stage Matching Approach for Road Network Conflation in Different Road Patterns," *ISPRS IJGI* 8(2), 2019 | Multi-stage matching for linking two independently-sourced road networks, evaluated under differing digitization precision between sources. |
| Acheson, Volpi & Purves, "Machine learning for cross-gazetteer matching of natural features," *IJGIS* 34(4), 2020 | Random-forest matching between GeoNames and SwissNames3D — feature-type matching drives most accuracy gain; positional precision alone is a **weak** signal once categorical context is available. |

Gap flagged by the subagent: no peer-reviewed paper isolating "positional-uncertainty magnitude
→ spatial-join precision/recall" as its sole empirical variable turned up in the time available —
the Cheng et al. probabilistic-join line is the closest formal treatment but its empirical
sections are algorithmic-performance-focused, not accuracy-degradation curves.

### 5d. Spatial data quality standards (the strongest, deepest hit of the three passes)

Unlike 5a/5c, this area is **not thin** — a mature, continuously-revised standard with a real
academic lineage, most directly applicable to a pipeline cross-checking many independent council
open-data feeds.

| Source | Finding |
|---|---|
| ISO 19157:2013 / ISO 19157-1:2023, *Geographic information — Data quality* | Defines the canonical quality-element taxonomy — completeness, logical consistency, positional accuracy, thematic accuracy, temporal accuracy, usability — that every paper below either applies or critiques. |
| Ariza-López et al., "Geospatial data quality (ISO 19157-1): evolve or perish," *Revista Cartográfica* 100, 2020 | Critical review by working members of the standard's own community — argues it needs new elements (big data, BIM) and standardized evaluation methods or risks obsolescence. |
| FGDC, *National Standard for Spatial Data Accuracy* (NSSDA), FGDC-STD-007.3-1998 | RMSE-based positional accuracy statistic + independent check-point testing methodology, reported at 95% confidence — the US federal parallel to ISO 19157's positional-accuracy element. |
| Goodchild, "Citizens as sensors: the world of volunteered geography," *GeoJournal* 69(4), 2007 | Coined "Volunteered Geographic Information" (VGI) — origin point for the entire crowdsourced/open-data spatial-quality literature below. |
| Haklay, "How Good is Volunteered Geographical Information? A Comparative Study of OpenStreetMap and Ordnance Survey Datasets," *Environment and Planning B* 37(4), 2010 | First systematic VGI quality study — OSM road positional accuracy averaged ~6 m RMSE vs. UK Ordnance Survey ground truth, broadly comparable. Template design for every national OSM-vs-authoritative comparison since. |
| Girres & Touya, "Quality Assessment of the French OpenStreetMap Dataset," *Transactions in GIS* 14(4), 2010 | Applies a full ISO-19157-style multi-element assessment (geometric, attribute, semantic, temporal accuracy, logical consistency, completeness, lineage) to French OSM vs. IGN reference data — **the closest analogue in the literature to what a multi-council-feed cross-check would need to do**, since it evaluates the same feature set from independent producers across every quality dimension, not positional accuracy alone. |
| Fonte et al., "Assessing VGI Data Quality," ch. 8 in *Mapping and the Citizen Sensor*, Ubiquity Press, 2017 | Maps ISO 19157's indicators onto VGI methods, distinguishing extrinsic (compare-to-ground-truth) from **intrinsic** (internal-consistency-only, no reference dataset) assessment — the intrinsic distinction is the relevant one when there's no independent survey-grade dataset to check a council feed against. |
| Senaratne, Mobasheri, Ali, Capineri & Haklay, "A review of volunteered geographic information quality assessment methods," *IJGIS* 31(1), 2017 | Systematic review classifying every published VGI quality-assessment method by ISO 19157 element and extrinsic/intrinsic type — best single entry point for selecting a method. |
| Zacharopoulou, Skopeliti & Nakos, "Assessment and Visualization of OSM Consistency for European Cities," *ISPRS IJGI* 10(6), 2021 | Applies ISO 19157's logical-consistency element (topology-aware) to OSM across six European cities — a recent worked example of the specific quality dimension (consistency across independently-maintained layers) most relevant to this project's council cross-referencing. |

Gap: no paper specifically studying Irish council open-data GIS feeds or multi-authority
cadastral conflation quality turned up — the Girres & Touya (2010) and Zacharopoulou et al.
(2021) design pattern (compare independently-produced layers against ISO 19157 elements) is the
closest available anchor, applied by analogy rather than a direct study of this exact scenario.

### 5e. Empirical studies of GIS-specific software defects

Explicitly thin — reported as such rather than padded.

| Source | Finding | Citations |
|---|---|---|
| Pandey, van Renen, Kipf & Kemper, "How Good Are Modern Spatial Libraries?," *Data Science and Engineering* 6(2), 2021 | Empirical evaluation of JTS, GEOS, Google S2, ESRI Geometry API, JSI on real datasets — documents concrete correctness pitfalls (GEOS/JTS k-NN silently defaults to Euclidean distance when no metric given, wrong on lat/lon data) and a GEOS STRtree performance bug traced to 2.68M LLC misses on a Twitter dataset. Framed as a benchmark paper, not a bug-mining study, but the closest thing found to an empirical defect study of core GIS geometry libraries. | ~15 `[Reported — ScisPace]` |
| Smith, Lazzarato & Carette, "State of the Practice for GIS Software," arXiv:1802.03422, 2018 | 30 GIS products assessed against a 56-question, 13-quality rubric via Analytic Hierarchy Process — finds concrete deficiencies in correctness, maintainability, transparency, reproducibility across the surveyed population. Closest match to "empirical GIS software quality study," but rubric-based, not bug-tracker mining. | not verified |

**Explicitly not found, despite targeted queries** (per the subagent): no paper mining
GDAL/PostGIS/QGIS/GEOS/GRASS bug trackers or commit histories to characterize recurring bug
types; no formal GIS-specific bug taxonomy (CRS/topology/precision as a studied category); no
Stack-Overflow-or-app-store defect-mining study scoped to GIS/mapping apps. This is an
**[Indicative — negative search result, not proof the literature doesn't exist anywhere]** per
this project's own [proving-absence standard](../.claude/rules/evidence.md) — a scoping-limited
search result, not a citable claim that the gap is real.

### Net read across Part 5

Quality (5d) is the deepest, most directly usable academic ground — Girres & Touya (2010) and
Zacharopoulou et al. (2021) are worth an actual read if the council-feed cross-referencing
problem becomes a scoped build, since they're the closest peer-reviewed analogue to what this
project already does informally. Precision (5a) and empirical-defects (5e) are both genuinely
thin in the peer-reviewed literature — the practically useful material for those two topics
really does live in the practitioner sources (blogs, GitHub issues) that Part 4 already covered,
not because the search under-tried, but because that's where this class of concrete bug actually
gets documented. Indexing/joins (5b) and cross-referencing-under-uncertainty (5c) confirm
existing design choices (bbox-prefilter-as-cheap-R-tree) and name a real methodological pattern
(context-based matching beating raw positional matching) worth knowing about, without pointing to
an unbuilt gap the way 5d does.

None of Part 5 has been scoped, prioritized, or built — same status as Parts 2 and 4.
