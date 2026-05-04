# Dáil Tracker — Research-scoping report

## Policy-to-Action Trace: an evidence-led public-record timeline across Irish registers

---

### Decisions made since first draft

1. **eISB integration is link-out only.** No fetcher, no parser, no reconciliation. The pipeline now builds a per-SI URL via the stable ELI pattern; the eISB site itself is the canonical full-text view. Implemented as the `eisb_url` column in `pipeline_sandbox/iris_oifigiuil_etl_polars.py`.
2. **PQ topic spine uses Oireachtas's own editorial labels.** The `data.oireachtas.ie` payload includes `question.debateSection.showAs` (editorial section, e.g. *"European Union"*) and `question.to.showAs` (answering ministry, e.g. *"Foreign"*). v1 maps `debateSection` to the SI policy-domain taxonomy via a hand-built alias CSV. The rule-based text classifier originally proposed for PQs is demoted to fallback for unmapped sections only.

These supersede the v1 plans in §6.2, §6.6, §8.1–§8.2, and §9 (items 1 and 4). Inline notes flag the changes.

---

## 1. Core research question

### 1.1 The framing trap to avoid

The intuitive framing — "track how a policy issue moves through the Irish public record from lobbying through legislation" — embeds two unsafe assumptions:

1. **Causal flow.** That activity in one register *causes* activity in the next. It does not. Statutory Instruments are typically drafted over many months by departmental officials following EU transposition deadlines, sectoral consultations, primary-Act commencement schedules, or political programmes — not in response to an individual lobbying meeting or PQ.
2. **Intentional sequencing.** That registers are populated in a meaningful order. Many PQs are *reactive* (raised after a media story, a court case, or a published SI), not *anticipatory*. Lobbying.ie disclosures are filed in 4-month windows and may post-date the regulatory work they describe. Iris Oifigiúil publishes notices *after* the instrument is made.

The framing must be descriptive, not causal.

### 1.2 The safer measurable concept

> **Public-record topic activity timeline.** For a defined policy topic, surface the dated, sourced, public-record events across Oireachtas, lobbying.ie, Iris Oifigiúil, and the Irish Statute Book, and characterise the legal output (operation type, responsible actor, EU relationship, parent Act).

This is a *visibility* artefact, not an inference engine. It tells the user: *"In topic X between dates A and B, the following appears in the public record."*

### 1.3 Precise research questions the feature can answer

A. **Topic activity:** How frequent and where is public-record activity on topic X over the last N years?
B. **Legal output mix:** Of SIs published on topic X, what is the breakdown of operation type (substantive / amendment / commencement / fees / designation / restrictive measures), EU-relationship (full effect / further effect / instrument-referenced / none detected), and form (regulations / order / scheme / rules / bye-law)?
C. **Responsible-actor concentration:** Which ministers/departments author SIs on topic X, and at what cadence?
D. **EU pressure surface:** What share of topic X's SI output is EU-derived, and against which Directives/Council Regulations?
E. **Parent-Act footprint:** Which Acts of the Oireachtas are doing the most ongoing regulatory work in topic X (i.e., generating the most SIs)?
F. **Cross-register co-occurrence:** In a given quarter, are there public-record activities on topic X across more than one register? (This is co-occurrence, *not* coordination.)

### 1.4 What this feature explicitly cannot claim

- **It cannot claim** that lobbying influenced a regulation. Disclosure proves contact occurred, not that it shaped output.
- **It cannot claim** a PQ "led to" a policy change.
- **It cannot claim** that the absence of public-record activity means absence of policy attention. Most policy work happens in the civil service and is invisible to these registers.
- **It cannot claim** that two records on the same topic in the same window are connected. Co-occurrence is not coordination.
- **It cannot identify** the specific SI that "implements" a specific lobbying matter without a manual, named-instrument trace from the lobbying disclosure text.

These caveats must appear in-product (provenance footer) on every Trace view, not just in this report.

---

## 2. Conceptual model

### 2.1 The honest event model

```text
                  ┌──────────────────────────┐
                  │   PUBLIC-RECORD EVENT    │
                  │   (dated, sourced, typed)│
                  └────────────┬─────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
        OIREACHTAS        LOBBYING.IE       OFFICIAL OUTPUT
        - PQ              - Return          - Iris Oifigiúil notice
        - debate          - meeting/contact - eISB SI text
        - bill stage      - matter          - parent Act / EU basis
        - vote            - DPO contacted   - made/operative date
        - committee       - subject area    - responsible actor
              │                │                │
              └────────────────┼────────────────┘
                               │
                  ┌────────────▼─────────────┐
                  │  TOPIC ASSIGNMENT LAYER  │
                  │  (probabilistic, multi-  │
                  │   label, with confidence)│
                  └────────────┬─────────────┘
                               │
                  ┌────────────▼─────────────┐
                  │   TIMELINE VIEW (UI)     │
                  │   per-topic, dated,      │
                  │   linked to source       │
                  └──────────────────────────┘
```

Three things to notice:

1. **No directional arrows between registers.** The model is a co-located timeline, not a flow.
2. **Topic assignment is its own layer** with its own confidence — not a join key. A record can carry multiple topic labels with weights.
3. **The legal-output node is the only node where typed structure exists end-to-end.** Iris notices already feed `pipeline_sandbox/iris_oifigiuil_etl_polars.py` → `si_form`, `si_operation_primary`, `si_eu_relationship`, `si_policy_domain_primary`, `si_responsible_actor`, `si_parent_legislation`. eISB full-text would deepen this node, not the others.

### 2.2 Three time anchors per SI — they are not interchangeable

| Anchor | Source | Meaning |
|---|---|---|
| **Made date** | eISB (header / signature block) | When the Minister signed the instrument |
| **Operative / effective date** | SI body text or commencement clause | When it has legal force |
| **Iris publication date** | Iris Oifigiúil issue date | When it became publicly findable |

These can differ by months (esp. commencement orders that appoint a future date). The pipeline captures `issue_date` (Iris) and `si_effective_date_text` (regex-extracted prose).

**Per the v1 decision, made/operative dates are not ingested.** The Iris `issue_date` is the timeline anchor; the user clicks `eisb_url` to view the authoritative legal text including its formal made date. Ingesting structured made/operative dates is a v2 candidate only if the Iris regex prose proves insufficient in practice.

### 2.3 Topic as a probabilistic label, not a join key

Across registers the "topic" representation is heterogeneous:

- **Iris/SI:** structured `si_policy_domains` from a 17-domain taxonomy (token-rule classifier, multi-label).
- **Lobbying.ie:** `Public Policy Area` (free-ish vocabulary), `Relevant Matter`, `Specific Details` (free text).
- **Oireachtas PQs:** `question.debateSection.showAs` is an Oireachtas-assigned editorial section heading (e.g. *"European Union"*, *"Housing"*); `question.to.showAs` is the answering ministry (e.g. *"Foreign"*). Both are present in the JSON payload and are authoritative.
- **Oireachtas debates / bills:** topic tags inconsistent; rely on title + first paragraph + (where present) the same `debateSection` field.

There is no shared controlled vocabulary across registers. The pragmatic answer is to build the **trace at the topic level by mapping each register's native labels onto the SI taxonomy as the canonical spine**, because (a) it is the only domain taxonomy already in the pipeline, (b) it is rule-based and inspectable, and (c) it is multi-label by design. For PQs the mapping is `debateSection.showAs → policy_domain` via a hand-built alias CSV (see §6.2).

---

## 3. Data inventory and gaps

### 3.1 What the project already has (verified)

| Source | Status | Grain | Topic signal |
|---|---|---|---|
| Iris notice events | **Strong.** `iris_oifigiuil_etl_polars.py` produces clean / quarantined / SI-only outputs and an `eisb_url` link column. | One row per inferred notice record. | `si_policy_domains` (multi-label, 17 domains), `si_operation_flags`, `si_eu_relationship`, `si_responsible_actor`, `si_parent_legislation`, `notice_ref` (`[G-3]` etc.), `si_taxonomy_confidence`. |
| Parliamentary questions | **Present.** Oireachtas API confirmed to return `debateSection.showAs` (editorial section) and `to.showAs` (answering ministry) per question. | One row per question. | `debateSection.showAs` is the primary topic label (Oireachtas-assigned). `to.showAs` is the secondary department signal. Free text in `question.showAs` as fallback. |
| Debates, bills, votes, attendance, committees, payments, member interests | **Present.** The Streamlit pages already render these. | Per project memory (design principles, member overview rules). | Topic labels inconsistent for non-PQ Oireachtas data; rely on title + body text. |
| Lobbying.ie returns | **Manual CSV ingest** (per `project_lobbying_automation.md`); quality threshold 2020. | Per return × per matter. | `Public Policy Area`, `Relevant Matter`, `Specific Details`, designated public officials contacted. |
| Iris member-interests pages | **Captured as raw JSON** (`iris_member_interests_raw_pages.json`) for downstream parsing. | Page-range extracts. | Out-of-scope for the Trace feature. |

### 3.2 What is missing (the new build)

| Source | Gap | Required for |
|---|---|---|
| ~~eISB full text per SI~~ | **Resolved by link-out.** `eisb_url` column already added. No ingestion. | (n/a) |
| **PQ → policy-domain alias CSV** | The `debateSection.showAs` vocabulary (e.g. *"European Union"*, *"Housing"*, *"Other Questions"*) needs a hand-built mapping to the 17-domain SI taxonomy. | Cross-register topic spine. |
| **Lobbying disclosure ↔ SI named-instrument link** | Some lobbying disclosures explicitly name a draft regulation, an Act being amended, or a transposition file. Not extracted today. | Optional named-evidence layer (rare but high-confidence when present). |
| **Topic classifier on lobbying matter text** | Not present. The lobbying.ie native `Public Policy Area` field is too coarse and not aligned with the SI taxonomy. | Cross-register topic spine. |
| **Department / minister name resolver** | Two-sided: (a) `si_responsible_actor` regex extracts variants like *"The Minister for the Environment, Climate and Communications"*, with no controlled-list join; (b) PQ `to.showAs` returns abbreviated forms like *"Foreign"*, *"Health"* with no `roleCode`. A unifying alias table covers both. | Drilling from a Trace view into "all SIs by department X" and joining PQs by answering ministry. |

### 3.3 External dependencies and rate limits

- **eISB** — link-out only; no fetching from this project. The user opens the eISB page directly via `eisb_url`. No rate-limit exposure.
- **data.oireachtas.ie** has a documented JSON API; PQ payloads include the topic fields confirmed in §3.1. AKN XML is also available per record if richer structure is ever needed.
- **lobbying.ie** is manually exported per current pipeline; check DevTools for the XHR endpoint before committing to Playwright (per `project_lobbying_automation.md`).

### 3.4 Coverage windows

- **Lobbying.ie:** quality threshold ≥2020 per memory.
- **Iris pipeline:** depends on which PDFs have been ingested into `pipeline_sandbox/`.
- **eISB:** all years available via link-out; coverage is whatever the user clicks through to.
- **Oireachtas:** API depth varies by dataset.

The Trace UI must publish the effective date window per topic, derived from `min(date)` across the registers actually covered, not assume uniform coverage.

---

## 4. Linking strategy and keys

### 4.1 The exact-key joins (high confidence)

| Join | Key | Use |
|---|---|---|
| Iris notice ↔ eISB SI page | `eisb_url` column (built from `(si_year, si_number)` via the ELI template) | Click-out from any SI row in the timeline. |
| Iris notice ↔ Iris notice ref | `notice_ref` (e.g. `G-3`) | In-issue ordering, debugging. |
| Iris SI ↔ parent Act | Act citation regex-extracted from "in exercise of the powers conferred …" | `si_parent_legislation`. Sufficient for the v1 timeline; the user verifies on eISB by clicking through. |
| PQ ↔ policy domain | `debateSection.showAs` → `policy_domain` (alias CSV) | Topic spine for PQs. |
| PQ ↔ department | `to.showAs` → `department_key` (alias CSV) | Drill-down by answering ministry. |
| Lobbying return ↔ DPO contacted ↔ minister/department | DPO name → department lookup table (to be built) | Drilling from "lobbying activity touching department X" to "SIs from department X". |

### 4.2 The fuzzy-link layer (lower confidence, must be visible)

- **Topic label ↔ topic label:** SI taxonomy is the canonical spine (Section 2.3). PQs, debates, bills, lobbying matters get classified onto it.
- **Lobbying matter ↔ named SI:** when a disclosure mentions "S.I. No. X of Year" or names a Bill/Act, capture as a hard link with `link_type = 'named_in_lobbying_text'`. Otherwise no per-record link is asserted.
- **PQ ↔ SI:** never auto-linked. Only co-occurrence in the same topic + time bucket.

### 4.3 What never to join

- Do not infer "this lobbying meeting was about this SI" from topic + date proximity. State this explicitly in code review and in the UI provenance footer.
- Do not compute "lobbying → SI elapsed time" as a metric. It implies causality and will be misread.

---

## 5. Risks, biases, and the association-vs-causation firewall

### 5.1 Statistical and interpretive risks

| Risk | Description | Mitigation |
|---|---|---|
| **Post hoc ergo propter hoc** | Reader infers causation from temporal sequence. | Frame the page as "timeline of public-record activity," never "from lobbying to law." Caveat in provenance footer on every Trace view. |
| **Survivorship / coverage bias** | Iris ingest may have gaps; lobbying.ie excludes pre-2020. | Show effective date range per register at the top of each Trace. Grey out periods with no coverage. |
| **Topic taxonomy drift** | The 17-domain SI taxonomy is rule-based and was tuned for SI text — not for PQs or lobbying matters. | Report per-register topic-classification confidence. Allow user to expand / collapse the SI-only view. |
| **Volume ≠ importance** | One sweeping primary Act amendment can matter more than 50 commencement orders. | Distinguish operation types in the UI; don't show a single "SI count" headline metric. |
| **Selection bias in lobbying disclosure** | Only disclosable lobbying is captured. Informal contact, party donations, civil-service consultation are invisible. | Provenance footer must say so. |
| **Reactive PQ misreading** | A PQ tabled *after* an SI is published is often *about* it, not anticipatory. | When showing PQs near SIs, render the visual order strictly chronologically and label PQs as "tabled after publication" / "tabled before publication" — but draw no inference. |
| **Multi-domain SIs over-counted** | The pipeline correctly preserves multi-domain tags; naive counting double-counts. | Aggregate by primary domain by default, with an opt-in multi-domain view. |
| **Minister/department renaming** | Departments restructure; "Minister for the Environment, Climate and Communications" did not always exist under that name. | Build a date-aware department alias table. Display the contemporaneous name. |

### 5.2 Editorial / reputational risks

- **Naming individuals.** The pipeline already detects person titles (`person_title_detected`). For the Trace feature, do not surface individual lobbyist names in the primary view; restrict to organisation/client level. Names belong in drill-down with explicit context.
- **Implying corruption.** Co-occurrence visualisations can be screenshotted as "look, lobbying then regulation." The UI copy must pre-empt this. Consider a literal disclaimer pill on screenshot-prone views.
- **Pre-empting policy disputes.** The Trace shows what is in the public record; it does not adjudicate whether a regulation is good or bad.

### 5.3 The firewall, restated

Every Trace view ships with:

1. A **dated coverage strip** at the top: "Lobbying.ie data from 2020-01-01. Iris notices from {min} to {max}. eISB linked from {min}."
2. A **provenance expander** at the bottom listing data sources, refresh dates, and the canonical disclaimer:
   > *Co-occurrence in the public record does not imply causation. Lobbying disclosure proves contact, not influence. Statutory Instruments are typically drafted by officials over many months and may have no relationship to lobbying activity in the same period.*
3. **Per-row source links.** Every event must have a click-through to its primary source (Iris PDF, eISB page, lobbying.ie return, oireachtas.ie URL).

---

## 6. Methodology

### 6.1 Topic taxonomy (canonical spine)

Adopt the existing 17-domain SI taxonomy from `pipeline_sandbox/iris_oifigiuil_etl_polars.py` (lines 113–131). Extend by adding the headline topics from the prompt as either aliases or sub-domains:

| Prompt topic | Map to existing domain | Refinement needed |
|---|---|---|
| Housing | `housing_planning_local_gov` | None initially. |
| Fisheries | `fisheries` | None. |
| Renewable energy | `environment_climate_energy` | Add a sub-tag `renewable_energy` (token rule on RENEWABLE / WIND / SOLAR / OFFSHORE). |
| Online safety | `communications_digital_online` | Add sub-tag `online_safety` (token rule on ONLINE SAFETY, MEDIA COMMISSION, COIMISIÚN NA MEÁN). |
| Pharma / medicines / health regulation | `health_medicines_care` | None. |
| Finance / banking / sanctions | `finance_banking_tax` + `eu_restrictive_measures` flag | Already two-axis; combine. |
| Ukraine temporary protection | `migration_international_protection` (token rule already covers UKRAINE TEMPORARY PROTECTION) | None. |
| Solid fuels / air pollution | `environment_climate_energy` (already covers SOLID FUELS, AIR POLLUTION) | None. |

This is a low-risk extension — sub-tags are additive, do not displace existing labels, and stay rule-based and inspectable.

### 6.2 Topic classification on non-SI text

**For PQs (revised):** use `question.debateSection.showAs` directly via a hand-built `pq_section_to_policy_domain.csv` alias table in `pipeline_sandbox/`. The Oireachtas section vocabulary is small and bounded; a one-pass review of distinct values across the available PQ corpus produces the table. Persist with versioned columns (`pq_section`, `policy_domain_primary`, `policy_domain_secondary`, `notes`).

- `to.showAs` (answering ministry) is recorded as a secondary signal for department-level drill-down, mapped via the same department alias table used by `si_responsible_actor` (§3.2 row).
- A **rule-based text tagger** (token rules over `question.showAs`) is the fallback only for sections that do not clean-map (e.g. *"Other Questions"*, *"Topical Issue Matters"*, generic admin sections). Implement only if the alias table leaves >5% of PQs unclassified.

**For debates and bills:** the rule-based tagger applied to title + first paragraph remains the v1 approach, since the editorial section signal is less reliable outside PQs.

**For lobbying matter text:** rule-based tagger over `Relevant Matter` + `Specific Details`, mapped to the same SI taxonomy.

**Out of scope for v1:** embedding-based nearest-neighbour and LLM classification. Both are deferred (opaque, costly, harder to defend in a public-facing civic tool).

### 6.3 Time bucketing and "near-by" semantics

- **Default bucket: quarter.** Aligns with lobbying.ie's reporting periods.
- **Drill-down: month.**
- The Trace never computes "elapsed time between X and Y" as a numeric metric. The visualisation is a vertical timeline with chronological pips, not a duration arrow.

### 6.4 Multi-label aggregation

When counting events for a topic:

- Use `si_policy_domain_primary` for headline counts.
- Make `si_policy_domains` (full multi-label set) available behind a toggle that says "include secondary topics" — so a power user can see the full reach of a multi-domain SI like the EU ETS aviation amendment (which carries `health_medicines_care|environment_climate_energy` in the smoke output).

### 6.5 Confidence handling end-to-end

Every event in the Trace carries:

- `topic_confidence` — for SI rows, from `si_taxonomy_confidence`. For PQ rows mapped via `debateSection`, fixed to 0.95 (Oireachtas-assigned label). For PQ rows that fall through to the rule-based fallback, from the tagger output. For lobbying / debates / bills, from the rule-based tagger output.
- `record_confidence` — from `si_taxonomy_confidence` or analogue per source.
- `link_confidence` — 1.0 for exact joins (`eisb_url`, `notice_ref`, member URI), lower for fuzzy.

The UI shows a small confidence pip on each row. Below a threshold (suggest 0.5), events are shown in a collapsed "lower-confidence matches" expander, not in the primary timeline.

### 6.6 eISB integration (link-out only)

- **No fetcher, no parser, no cache.** The pipeline emits `eisb_url` per SI row using the stable ELI pattern `https://www.irishstatutebook.ie/eli/{si_year}/si/{si_number}/made/en/html`. Implemented as a `pl.format(...)` `with_columns` in `enrich_records`, gated on `notice_category == 'statutory_instrument'` and both `si_year` / `si_number` non-null.
- **The eISB site itself is the canonical full-text view.** Users click out from any SI row in the Trace timeline.
- **Trade-offs accepted:** no automated reconciliation of parent Act, made date, or operative date; the timeline pip uses Iris `issue_date` as the date anchor. If a future need arises (e.g. structured operative-date filtering), revisit as a v2 fetcher in `pipeline_sandbox/`.

---

## 7. UI scope — the "Policy-to-Action Trace" page

### 7.1 Page placement and navigation

This is a **secondary/exploratory** page, not a member-overview-style primary surface. It belongs alongside committees, attendance, payments — not as the landing page. The primary user journey is "I have a topic in mind → show me the public record."

### 7.2 Page anatomy (per project design memory: TheyWorkForYou spirit, primary view simplicity, year pills, cards-not-dataframes, provenance footer)

```
┌─────────────────────────────────────────────────────────────┐
│  Policy-to-Action Trace                                     │
│  See what the public record shows about a topic — and       │
│  what it doesn't.                                           │
│                                                             │
│  Topic: [ Housing ▼ ]   Window: [ 2020 · 2021 · 2022 …  ]   │
│                                                             │
│  Coverage: Lobbying.ie 2020+ · Iris {min}–{max} · eISB ✓    │
├─────────────────────────────────────────────────────────────┤
│  Snapshot — what's in the record on this topic              │
│                                                             │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│   │ N SIs        │  │ N PQs        │  │ N lobbying   │      │
│   │ since YYYY   │  │ since YYYY   │  │ returns YYYY │      │
│   └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                             │
│   Operation mix (SI):  ████ amendment  ██ commencement      │
│                        ██ fees  █ designation  …            │
│   Responsible actors:  Minister for Housing (N) ▸           │
│                        Minister for Local Govt (N) ▸        │
│   EU relationship:     N EU-derived · N domestic            │
├─────────────────────────────────────────────────────────────┤
│  Timeline                                                   │
│                                                             │
│  2024 ─┬─ Q4 ◆ S.I. 723/2024  Residential Tenancies (Amdt)  │
│        │       Min. for Housing · amendment · domestic      │
│        │       [Iris notice ↗] [eISB text ↗]                │
│        │                                                    │
│        ├─ Q4 ● PQ 39512/24   Howlin   "Rent caps"           │
│        │       Tabled 2024-11-12                            │
│        │                                                    │
│        ├─ Q3 ▲ Lobbying     IPAV / various TDs              │
│        │       Subject: Residential rental market           │
│        │       [lobbying.ie ↗]                              │
│   …    │                                                    │
├─────────────────────────────────────────────────────────────┤
│  Lower-confidence matches ▾                                  │
├─────────────────────────────────────────────────────────────┤
│  Provenance & caveats ▾                                      │
│   • SI taxonomy v{n}, rule-based, multi-label.              │
│   • Co-occurrence is not coordination. (full text)          │
│   • Coverage windows per source.                            │
│   • Source code: pipeline_sandbox/iris_oifigiuil_etl_polars │
└─────────────────────────────────────────────────────────────┘
```

Notes against the project's UI memories:

- **Card pattern, not dataframes.** Per `feedback_dataframes_secondary_only.md` and `feedback_member_overview_no_dataframes.md`, every primary section is card-based. Rows in the timeline are cards with a glyph (SI / PQ / lobbying / debate), title, date, actor, and source link.
- **Year pills** for window selection (already an established pattern).
- **`#ffffff` backgrounds** for cards, not `var(--surface)` (per `feedback_css_surface_trap.md`).
- **CSS in `utility/shared_css.py`; helpers in `utility/ui/components.py`.** No bespoke per-page CSS.
- **`st.html` over `unsafe_allow_html`; `width="stretch"` not `use_container_width`** (per `feedback_streamlit_api_patterns.md`).
- **`segmented_control` not radio** for the topic / window selectors (per `project_audit_findings_2026_04_30.md`).
- **No emoji icons** (per audit findings); use SVG glyphs in a small icon component.

### 7.3 What the page does *not* show

- No "lobbying → SI" lines/arrows.
- No "elapsed days from PQ to SI" metric.
- No causal-language headlines ("how housing policy moved through Government").
- No individual-lobbyist names in primary view.
- No predicted topic labels above confidence threshold mixed silently with rule-based ones — always disclose method.

---

## 8. MVP definition and success criteria

### 8.1 MVP scope (one engineer, ~1.5–2 weeks of focused work)

The original 3-week estimate has shrunk: eISB ingestion is replaced by a one-line URL formatter (already merged), and PQ classification is replaced by an alias-CSV lookup (no model to train or tune).

**In:**

1. **`eisb_url` column** in the SI taxonomy output. ✅ Already done.
2. **`pq_section_to_policy_domain.csv`** and **`department_alias.csv`** alias tables in `pipeline_sandbox/`, hand-built by reviewing the distinct `debateSection.showAs` and `to.showAs` values across the available PQ corpus.
3. **Rule-based topic tagger** applied to lobbying matter text and (as fallback only) to PQs whose `debateSection` does not clean-map. Use the SI taxonomy as the spine plus the four sub-tags from §6.1.
4. **New Streamlit page** rendering the Trace per §7.2 for a 3-domain pilot (housing, fisheries, online safety), on the existing 2020+ window.
5. **Provenance & caveats expander** wired to a single canonical block.

**Out (defer to v2):**

- Cross-register link inference (named-SI extraction from lobbying text) beyond a simple SI-citation regex.
- Embedding/LLM topic classification.
- eISB structured fetching (made/operative date, parent-Act verification).
- Bills/votes layer (PQs, debates, lobbying, SI is enough for v1).
- Member-interests overlay.

### 8.2 Success criteria

| Criterion | Measure |
|---|---|
| `eisb_url` correctness | 100% of sampled SI rows resolve to a live eISB page (HTTP 200) on click. Manual spot-check, n≥30. |
| PQ alias coverage | ≥95% of PQ rows in the date window are mapped to a policy domain via `debateSection`; the remaining ≤5% fall through to the rule-based fallback. |
| Topic precision on lobbying matters | ≥80% of returns auto-tagged to a topic survive a manual spot-check on a 100-row sample. |
| UI defensibility | A civic journalist or a parliamentary researcher can describe in one sentence what the page shows *and* what it does not claim, after 30 seconds of reading. (Internal usability test, n≥3.) |
| Caveat surfacing | The provenance/caveat block is visible in any screenshot of the primary timeline (not buried below the fold). |

### 8.3 Success criteria the feature must *not* be measured against

- Number of "matches" between lobbying and SIs.
- Average elapsed time from a lobbying meeting to a related SI.
- Any "influence score" or composite.

These metrics would falsely imply causation and should be explicitly forbidden in the spec.

---

## 9. Open research questions

These are scoping items to resolve before — or during — the MVP:

1. ~~**Does eISB expose a structured XML / Akoma Ntoso layer for SIs?**~~ **Resolved (out of scope).** eISB integration is link-out only via `eisb_url`; no parsing is built. Revisit only if v2 requires structured made/operative dates.
2. **Is there a stable, machine-readable mapping from "Minister for X" titles to a department key over time?** If not, build a small alias table in `pipeline_sandbox/`. The DfPER (gov.ie) restructure log is the likely authoritative source. **The same table also resolves the abbreviated `to.showAs` values from the Oireachtas PQ payload (e.g. *"Foreign"* → Department of Foreign Affairs).**
3. **What share of lobbying.ie disclosures cite a named SI, Bill, or Act?** A 200-row manual sample answers this and tells us whether the named-evidence layer is worth building in v2.
4. ~~**Does the Oireachtas API expose any topic taxonomy at the PQ level?**~~ **Resolved.** Yes — `question.debateSection.showAs` is an editorial section heading and `question.to.showAs` is the answering ministry. Both are used as the v1 PQ topic spine via alias CSVs (§6.2, §8.1).
5. **Is "Iris non-SI notice" worth surfacing in the Trace?** International-agreements-entered-into-force, fisheries management notices, and bankruptcy notices are tagged but heterogeneous. Default: include only `statutory_instrument` and `fisheries_notice` in v1 timelines; everything else is drill-down only.
6. **Where do EU transposition deadlines come from?** If we can ingest a list of Directive transposition deadlines, the EU-relationship layer becomes far stronger ("this SI transposes Directive 2022/X due 2024-Y"), and provides a non-Irish causal anchor that displaces the misleading "lobbying → SI" reading entirely.
7. **Is the PQ `debateSection.showAs` vocabulary stable across Dáil terms?** The 33rd Dáil sample shows clean section names. If it varies meaningfully across the 32nd / earlier, the alias CSV needs date-aware versioning. Cheap to verify by counting distinct values per Dáil term in the Oireachtas API.

Item 6 is the most under-rated. **EU transposition deadlines are the most powerful single antidote to the post-hoc misreading the feature is at risk of.** A user looking at "renewable energy: 4 SIs in Q3 2024" who also sees "RED III transposition deadline 2024-07-01" understands the SI cluster instantly, without any false lobbying inference. Worth treating as a strong v2 candidate.
