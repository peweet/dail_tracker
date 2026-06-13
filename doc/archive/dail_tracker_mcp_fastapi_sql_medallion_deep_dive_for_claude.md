# Dáil Tracker — MCP / FastAPI / SQL / Medallion-Layer Deep-Dive Brief for Claude

**Prepared:** 2026-06-07  
**Repository:** `https://github.com/peweet/dail_tracker`  
**Purpose:** Help Claude scope a planning and investigation pass around (1) a possible MCP/commercial-agent angle, (2) FastAPI/API readiness, (3) current workflow and CI maturity, (4) SQL-view logic soundness, and (5) bronze/silver data that may deserve promotion.

This brief is grounded in a fresh repository pass plus public documentation/blogs on MCP, FastAPI, and real-world agent/data-connector patterns.

---

## 1. Executive reassessment

Dáil Tracker is now closer to a **public-record intelligence platform** than a pure political dashboard.

The current repo shows a meaningful stack:

```text
source extractors / refresh chains
  -> bronze/silver/gold medallion data
  -> DuckDB SQL views
  -> dail_tracker_core query layer
  -> Streamlit pages
  -> potential future FastAPI / MCP / dossier-builder layer
```

The strongest current product surfaces are:

```text
Members / attendance / votes / payments
Lobbying
Legislation / statutory instruments
Corporate notices
Procurement
Public Payments
Courts & Judiciary
```

The strongest commercial/data-intelligence surfaces are:

```text
procurement awards
public payments / purchase-order facts
supplier-to-CRO matching
procurement-to-lobbying overlap
corporate notices
CBI register/corporate-notice cross-reference
judiciary bench/legal-diary public records
local authority AFS
CSO PxStat housing/government-finance denominators
TED Ireland procurement notices
```

The repo has **not** yet implemented a FastAPI server or MCP server, but the `dail_tracker_core` layer is now explicitly moving toward interface-agnostic query functions and `QueryResult` state handling. That makes MCP/FastAPI plausible without rewriting all data logic.

---

## 2. Real-world MCP use cases and commercial relevance

### 2.1 What MCP is, based on primary sources

Anthropic describes the Model Context Protocol as an open standard that lets developers build secure, two-way connections between data sources and AI-powered tools. Developers can expose data through MCP servers, and AI applications connect to those servers.

Source:

```text
https://www.anthropic.com/news/model-context-protocol
```

The MCP specification states that MCP enables integration between LLM applications and external data sources/tools.

Source:

```text
https://modelcontextprotocol.io/specification/2025-06-18
```

Anthropic’s engineering blog frames MCP as a way to connect agents to tools and data without building custom integrations for every pairing, reducing fragmentation and duplicated integration work.

Source:

```text
https://www.anthropic.com/engineering/code-execution-with-mcp
```

### 2.2 Real MCP usage patterns

Public examples and documentation show MCP being used for:

```text
developer workflow tools
GitHub / repository operations
database connectors
enterprise data connectors
ERP / CRM / business-system access
file/search/document systems
agent-accessible APIs
```

Relevant examples:

```text
GitHub MCP practical guide:
https://github.blog/ai-and-ml/generative-ai/a-practical-guide-on-how-to-use-the-github-mcp-server/

Anthropic reference servers repository:
https://github.com/modelcontextprotocol/servers

CData enterprise MCP blog:
https://www.cdata.com/blog/how-cdata-mcp-servers-connect-ai-to-enterprise-data

Fast-moving MCP ecosystem / reference implementations:
https://mcpservers.org/servers/iamadk/reference-servers
```

Important caveat: the official `modelcontextprotocol/servers` repository describes itself as reference/educational implementations, not production-ready solutions, and says developers should evaluate security requirements and implement safeguards appropriate to their threat model.

Source:

```text
https://github.com/modelcontextprotocol/servers
```

### 2.3 Real-world security concerns

MCP is commercially interesting, but security cannot be an afterthought.

Recent public security reporting has raised concerns around MCP server misuse, unsafe tool exposure, prompt-injection paths, and supply-chain risks. News coverage described research claiming systemic MCP-related remote-code-execution risks in SDK/server deployments. These reports should not be treated as definitive design guidance by themselves, but they are enough to justify conservative security assumptions.

Sources:

```text
https://www.tomshardware.com/tech-industry/artificial-intelligence/anthropics-model-context-protocol-has-critical-security-flaw-exposed

https://www.techradar.com/pro/security/this-is-not-a-traditional-coding-error-experts-flag-potentially-critical-security-issues-at-the-heart-of-anthropics-mcp-exposes-150-million-downloads-and-thousands-of-servers-to-complete-takeover
```

### 2.4 What this means for Dáil Tracker

Dáil Tracker is well suited to MCP because its value is **structured source-linked retrieval**, not just a UI.

A credible MCP server would expose opinionated, safe tools such as:

```text
search_entity
get_supplier_footprint
get_procurement_awards
get_public_payments
get_lobbying_overlap
get_cbi_register_status
get_corporate_notices
get_judiciary_public_records
get_source_appendix
generate_source_linked_brief
```

It should **not** expose unrestricted raw SQL or unbounded table dumps as a first commercial surface.

The best commercial framing is:

```text
Irish public-record intelligence MCP server
for analysts, researchers, journalists, public-affairs teams,
law firms, consultants, and due-diligence workflows.
```

The strongest MCP use case is not “ask the politics app questions.” It is:

```text
Ask an AI agent to prepare a source-linked due-diligence brief
using official Irish public records.
```

---

## 3. FastAPI readiness

### 3.1 Is FastAPI currently implemented?

No actual FastAPI app was found in the repo pass.

Evidence:

- `pyproject.toml` core dependencies include Streamlit, Altair, Plotly, DuckDB, pandas, NumPy and PyArrow, but not FastAPI.
- The only enabled console script is `dail-pipeline = "pipeline:main"`.
- Repository search for `FastAPI` / `app = FastAPI` did not surface a FastAPI application.
- `dail_tracker_core/db.py` explicitly says a future API layer would build a read-only connection per request, but currently this is an interface-agnostic query layer, not a server.

Relevant repo sources:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/pyproject.toml
https://raw.githubusercontent.com/peweet/dail_tracker/main/dail_tracker_core/db.py
```

### 3.2 Is the repo API-ready?

Partly yes.

The repo has an emerging separation:

```text
sql_views/*.sql
  -> dail_tracker_core.db.connect_with_views()
  -> dail_tracker_core.queries.*
  -> dail_tracker_core.results.QueryResult
  -> Streamlit data_access wrappers
```

`QueryResult` is particularly important. It distinguishes:

```text
success with rows
success with no rows
source unavailable / view missing / DuckDB error
```

This is exactly the distinction a FastAPI or MCP layer needs, because an API response must not confuse “no records” with “source failed”.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/dail_tracker_core/results.py
```

`dail_tracker_core/db.py` is also explicitly Streamlit-free and says query functions take a DuckDB connection as an argument so they are unit-testable and interface-agnostic. It says a future API layer could build a connection per request while Streamlit caches one per session.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/dail_tracker_core/db.py
```

### 3.3 Why FastAPI is a reasonable later interface

FastAPI’s own README describes it as a modern Python API framework based on standard Python type hints, with automatic interactive docs, OpenAPI compatibility, and JSON Schema compatibility.

Source:

```text
https://github.com/fastapi/fastapi
```

FastAPI is therefore a good fit if Dáil Tracker wants:

```text
stable JSON endpoints
OpenAPI documentation
typed response models
API keys / auth later
integration with a Next.js frontend
integration with an MCP server
report-builder workflows
```

But the repo does not need FastAPI before shipping the Streamlit beta. FastAPI becomes useful once there is a defined data contract for external consumers.

### 3.4 FastAPI risk areas if added later

Claude should investigate:

```text
- Which dail_tracker_core query functions are truly interface-agnostic?
- Which Streamlit data_access modules still contain business logic?
- Which pages depend on Streamlit-only cache or session state?
- Which views can fail open via swallow_errors=True?
- Which endpoints would expose privacy-sensitive rows?
- Which endpoints need match-confidence and source-state fields?
- Which endpoints should return QueryResult-style status instead of bare arrays?
```

---

## 4. Workflow and CI review

### 4.1 CI workflow

The repo has a GitHub Actions CI workflow with:

```text
ruff check
ruff format --check
basedpyright
pytest
```

The test job skips these markers in CI:

```text
integration
sql
sources
bronze
```

The workflow comments say integration/sql/bronze tests need committed or built pipeline outputs and are local-only for now. Source URL health is scheduled separately via a nightly workflow, according to comments.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/.github/workflows/ci.yml
```

### 4.2 Dependabot

The repo has Dependabot configured for `uv` dependencies and GitHub Actions, scheduled weekly.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/.github/dependabot.yml
```

### 4.3 Workflow implication

CI is good for lint/type/import/unit-level integrity, but **not yet sufficient for a due-diligence data product**.

The following are currently not fully proven in CI from the inspected workflow:

```text
SQL views execute against current generated data
bronze/silver/gold row counts are complete
external source availability is current
production pipeline creates all gold facts
privacy gates hold on live generated outputs
no row-thinning happened after source-layout drift
```

There are late-chain freshness/source-health/output-regression tools in `pipeline.py`, which is positive, but Claude should verify whether their outputs are generated, reviewed, and enforced anywhere beyond local/scheduled runs.

Pipeline source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/pipeline.py
```

---

## 5. Pipeline and data-chain review

### 5.1 Current pipeline breadth

The current `pipeline.py` chain list is much broader than the first pass suggested. It includes:

```text
bootstrap
members
payments
attendance
seanad
interests
lobbying
iris
legislation
afs
cbi
cro
procurement
procurement_lobbying
ted
public_body_payments
hse_tusla_payments
cso
freshness
source_health
output_regressions
```

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/pipeline.py
```

This is an important update: Procurement, TED, public-body payments, HSE/Tusla payments, AFS and CSO are now explicitly part of the pipeline chain list.

### 5.2 Chain-level strengths

The pipeline has several strong data-product ideas:

```text
- procurement awards promoted to gold
- procurement-to-lobbying overlap promoted to gold
- TED Ireland award notices retained as silver
- public-body payments over €20k promoted to gold
- HSE/Tusla payment facts promoted to gold
- CSO PxStat housing/HAP + government-finance denominators promoted to gold
- freshness and source-health metadata generated at the end
- output regression checks against baseline generated at the end
```

### 5.3 Chain-level risks

The pipeline is still a subprocess dispatcher. That is okay for a research/beta pipeline, but Claude should investigate:

```text
- whether each script is idempotent;
- whether each script writes atomically;
- whether partial gold outputs can be left behind after failure;
- whether data/_meta output_regressions.json is reviewed/enforced;
- whether every Streamlit page has a corresponding chain dependency;
- whether every chain has a source coverage JSON;
- whether missing parquets are treated as unavailable, not empty.
```

---

## 6. Streamlit UI and coupling review

### 6.1 App navigation

Current `utility/app.py` now routes:

```text
Member Overview
Attendance
Votes
Interests
Payments & Donations
Lobbying
Legislation
Statutory Instruments
Appointments
Corporate Notices
Procurement
Public Payments
Committees
Courts & Judiciary
Glossary
```

This means Procurement, Public Payments and Judiciary are currently top-nav pages in the inspected version.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/app.py
```

### 6.2 Page-boundary pattern

The strongest new pages explicitly document a “logic firewall”:

- Procurement page: no modelling, no value_counts/groupby/merge/parquet reads; every aggregation, CRO join and value gate lives in SQL views.
- Public Payments page: no modelling; aggregation, value gate and privacy gate live in SQL views.
- Judiciary page: joins/classifications live in SQL views; page does presentation faceting only.

Repo sources:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/procurement.py
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/public_payments.py
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/judiciary.py
```

This is a strong pattern and should be preserved if building API/MCP.

### 6.3 Coupling risk

The UI is still Streamlit-specific, but not fatally coupled if the SQL/core boundary remains intact.

The risk is not “Streamlit exists.” The risk is:

```text
page-local pandas shaping becomes business logic
Streamlit cache hides source-state bugs
custom HTML/CSS obscures accessibility/security review
views fail open with swallow_errors=True
```

For MCP/FastAPI, Claude should focus on **core query functions and SQL views**, not page rendering code.

---

## 7. SQL logic review

### 7.1 SQL registry and view registration

The shared SQL registry rewrites `read_parquet('data/...')` and `read_csv('data/...')` paths to absolute repo-root paths before DuckDB executes them. This solves a real CWD problem when Streamlit runs from a subdirectory.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/data_access/_sql_registry.py
```

The core DB module duplicates this logic for the Streamlit-free query layer, and explicitly says it is transitional until every data-access module is migrated.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/dail_tracker_core/db.py
```

### 7.2 View-registration risk

Both registry implementations support `swallow_errors=True`.

This is useful for optional enrichment, but risky for required MCP/API endpoints. If a view fails to register and the query layer returns “unavailable,” that is acceptable; if the page silently renders an empty state, that is not.

Claude should classify views as:

```text
required for page / endpoint
optional enrichment
experimental / sandbox
```

### 7.3 Procurement SQL logic

The procurement SQL is logically strong.

`v_procurement_awards` is a raw display layer over `procurement_awards.parquet`. The view header explicitly says value is not spend, frameworks/DPS are ceilings, multi-supplier frameworks repeat one ceiling across suppliers, and only `value_safe_to_sum` rows may be summed.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/procurement_awards.sql
```

`v_procurement_supplier_summary` has the right default ranking: `n_awards` is the trustworthy metric, while `awarded_value_safe_eur` sums only `value_safe_to_sum`. It folds in CRO match and lobbying overlap but describes overlap as co-occurrence, not influence.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/procurement_supplier_summary.sql
```

This is exactly the kind of SQL logic a due-diligence product needs. It prevents the common mistake of summing framework ceilings or implying lobbying causation.

### 7.4 Procurement payments SQL logic

`v_procurement_payments` explicitly distinguishes payment/purchase-order facts from eTenders/TED awards. It says SPENT and COMMITTED tiers are never summed together, only `value_safe_to_sum` rows should sum, and VAT-status differences must not be silently mixed.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/procurement_payments.sql
```

This is strong. The main issue is a privacy-policy tension:

- `public_payments.py` says likely personal suppliers are withheld at the view boundary.
- `procurement_payments.sql` says suppliers, including sole traders/individuals, are named because the source is already published and the fact carries no address/PII beyond source fields.
- `procurement_payments_consolidate.py` repeats that owner decision and says rows are not suppressed.

Repo sources:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/public_payments.py
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/procurement_payments.sql
https://raw.githubusercontent.com/peweet/dail_tracker/main/extractors/procurement_payments_consolidate.py
```

Claude should reconcile this before public launch or MCP exposure.

### 7.5 Judiciary SQL logic

`v_judiciary_legal_diary_cases` has a strong privacy contract: statutory in-camera categories are dropped upstream, natural persons are reduced to initials, organisations and State bodies are kept in clear, case references and solicitor names are stripped, and rows carry source URL and SHA.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/judiciary_legal_diary_cases.sql
```

The Judiciary page also documents that it does not rate judges, infer bias, or imply misconduct, and that coverage gaps are shown rather than hidden.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/judiciary.py
```

This is logically sound, but Claude should still verify extractor-level tests around:

```text
raw-name column absence
protected-category removal
case-reference stripping
solicitor-name stripping
natural-person anonymisation
```

### 7.6 SQL view inventory maturity

The `sql_views` directory now contains a broad semantic layer, including:

```text
appointments
attendance
charity
committees
corporate
judiciary
legislation / statutory instruments
lobbying
member profile
payments
procurement
public payments
```

Directory source:

```text
https://github.com/peweet/dail_tracker/tree/main/sql_views
```

This is the right direction for MCP/FastAPI because SQL views are more stable than Streamlit pages.

---

## 8. Bronze/silver/gold scan and promotion candidates

### 8.1 Current visible data folders

The repo’s `data` directory exposes:

```text
data/_meta
data/bronze
data/gold/parquet
data/silver
```

Directory source:

```text
https://github.com/peweet/dail_tracker/tree/main/data
```

The visible `data/silver` tree on GitHub shows only a small subset:

```text
data/silver/lobbying
data/silver/aggregated_td_tables.csv
data/silver/flattened_members.csv
data/silver/flattened_seanad_members.csv
data/silver/seanad_aggregated_tables.csv
```

Directory source:

```text
https://github.com/peweet/dail_tracker/tree/main/data/silver
```

However, many extractors write silver/gold outputs not fully visible in GitHub tree listings, likely because generated data is gitignored or produced locally.

### 8.2 Candidate: TED Ireland award notices

`pipeline.py` describes TED as EU award notices for Ireland, cleaned to silver, not yet exposed to UI. It caches raw bronze, depends on the CRO silver register, and skips gracefully on API outage.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/pipeline.py
```

Promotion argument:

```text
TED should probably remain silver until de-duplicated/reconciled against eTenders.
It becomes gold when there is a stable v_ted_awards or v_procurement_ted_awards view
with value-kind semantics and clear "EU journal award notice" caveats.
```

Why useful:

```text
EU-level procurement awards may catch above-threshold notices and cross-border suppliers.
```

### 8.3 Candidate: local-authority payments fact

`procurement_la_payments_extract.py` says it builds a 31-council per-transaction fact for purchase orders/payments over €20k, writes to `data/silver/parquet/la_payments_fact.parquet`, and is not wired into `pipeline.py` yet in its docstring.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/extractors/procurement_la_payments_extract.py
```

Note: `pipeline.py` currently has separate public-body payments and HSE/Tusla payments, but not an explicit `la_payments` chain. Claude should verify whether LA payments are now included indirectly elsewhere or remain standalone.

Promotion argument:

```text
Promote after coverage tests show all 31 local authorities, value_kind consistency,
privacy/public_display policy, and source-file coverage metadata.
```

Why useful:

```text
This is one of the strongest Local Authority & Housing / due-diligence layers.
```

### 8.4 Candidate: AFS local-authority finance

`pipeline.py` includes `afs` as amalgamated Local Authority Annual Financial Statements from gov.ie PDFs to a silver spend-by-service-division fact.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/pipeline.py
```

Promotion argument:

```text
AFS should be promoted to gold-backed SQL views once service-division fields,
years, local-authority names, and value-kind definitions are stable.
```

Why useful:

```text
AFS is the audited local-government finance spine for local authority reports.
```

### 8.5 Candidate: CSO PxStat housing/HAP/government-finance denominators

`pipeline.py` includes a `cso` chain for CSO PxStat housing/HAP plus government finance tables GFA01/GFQ01/NA012, writing gold outputs.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/pipeline.py
```

Promotion argument:

```text
If outputs are already gold, the missing step is likely UI/report integration:
housing/local-government denominators should feed Local Authority & Housing reports.
```

Why useful:

```text
This gives denominator context and prevents procurement/housing figures being
presented without population/finance context.
```

### 8.6 Candidate: CBI sandbox authorised firms

`cbi_registers_extract.py` writes `cbi_authorised_firms.parquet` to sandbox and only promotes corporate-notice xref to gold.

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/extractors/cbi_registers_extract.py
```

Promotion argument:

```text
Do not simply promote the raw authorised-firm table.
First add register-family metadata, extraction-confidence, duplicate handling,
and false-positive filtering. Then expose a regulated-entity lookup view.
```

Why useful:

```text
Regulated-entity status is highly valuable for due-diligence reports, but the extractor
itself warns that extraction is heuristic.
```

### 8.7 Candidate: procurement payments sandbox facts

`procurement_payments_consolidate.py` consolidates several sandbox facts into gold:

```text
public_payments_fact.parquet
hse_tusla_payments_fact.parquet
nta_payments_fact.parquet
nphdb_payments_fact.parquet
seai_payments_fact.parquet
```

Repo source:

```text
https://raw.githubusercontent.com/peweet/dail_tracker/main/extractors/procurement_payments_consolidate.py
```

Promotion status:

```text
Already promoted through consolidation into procurement_payments_fact.parquet.
Claude should check whether each upstream sandbox fact has its own source coverage JSON
and schema/freshness tests.
```

---

## 9. MCP scope candidates for Claude

### 9.1 Recommended first MCP tool family: Supplier / public money

Most realistic tools:

```text
search_supplier(query)
get_supplier_procurement_awards(supplier_norm)
get_supplier_public_payments(supplier_norm)
get_supplier_lobbying_overlap(supplier_norm)
get_supplier_cro_match(supplier_norm)
get_supplier_source_appendix(supplier_norm)
```

Why this is ready:

```text
procurement awards + supplier summary + CRO match + lobbying overlap exist in SQL/views
public payments views exist
value semantics are explicit
```

### 9.2 Recommended second MCP tool family: Public body / publisher

```text
search_public_body(query)
get_procurement_by_authority(authority)
get_public_payments_by_publisher(publisher_id)
get_public_body_source_coverage(publisher_id)
```

Why useful:

```text
public buyers and publishers are the natural unit for public-body reports.
```

### 9.3 Recommended third MCP tool family: Judiciary public records

```text
search_judge_or_court(query)
get_judiciary_roster()
get_judicial_appointments(judge_key)
get_court_clearance()
get_legal_diary_schedule(date)
get_legal_diary_anonymised_cases(date)
```

Caveat:

```text
Must preserve no-ranking/no-misconduct/no-bias language.
```

### 9.4 Recommended fourth MCP tool family: Entity / corporate-regulated status

```text
search_corporate_entity(query)
get_corporate_notices(entity)
get_cbi_register_match(entity)
get_cro_match(entity)
```

Caveat:

```text
Current CBI logic is register-centric and heuristic; enforcement/warnings are not yet integrated.
```

---

## 10. MCP safety contract

Any Dáil Tracker MCP tool should return:

```text
data
source_url
source_name
retrieved_at
dataset_layer
match_confidence
value_kind
realisation_tier
privacy_status
caveat
```

Response language should separate:

```text
confirmed official record
possible match
derived aggregate
inference
manual review required
unknown / unavailable
```

Avoid:

```text
caused
influenced
corrupt
risky
paid X
conflict
```

unless a cited source directly supports the statement.

Prefer:

```text
appears in
matched to
co-occurs with
source records show
awarded value, not actual spend
ordered or paid, not a single spend figure
requires manual review
```

---

## 11. Key investigation questions for Claude

### 11.1 MCP

```text
1. Which public-record questions can be answered safely by existing SQL views?
2. Which tool outputs need row-level sources versus aggregate caveats?
3. Which tools require auth/rate limits before exposure?
4. Which tools risk privacy leakage?
5. Which tools require entity-match confidence?
6. Which tools should be internal-only at first?
7. Should the MCP sit directly over dail_tracker_core or over a FastAPI layer?
8. How should source-unavailable states be surfaced to the agent?
9. How should an agent be prevented from overstating co-occurrence?
10. What should the first paid MCP report workflow be?
```

### 11.2 FastAPI

```text
1. Confirm there is no FastAPI app currently.
2. Identify which dail_tracker_core query modules are API-ready.
3. Identify which Streamlit data_access modules still contain logic.
4. Design a minimal read-only API contract around QueryResult.
5. Define required versus optional SQL views.
6. Decide whether DuckDB connections are per-request or pooled/cached.
7. Define response models with caveats and source metadata.
8. Decide which endpoints are public, private, or internal.
```

### 11.3 SQL

```text
1. Verify every SQL view referenced by each page exists.
2. Verify every view can be registered with generated data present.
3. Check whether any view depends on swallowed failures.
4. Classify views as required, optional, or experimental.
5. Check value-kind semantics across procurement, payments, TED, AFS and CSO.
6. Check privacy-state consistency across payments.
7. Check whether CBI/CRO/lobbying/procurement entity match semantics are consistent.
8. Check whether source URLs and coverage metadata flow to all reportable views.
```

### 11.4 Medallion data

```text
1. List every bronze/silver/gold output generated by the pipeline.
2. Compare actual outputs against views and pages.
3. Identify silver outputs that are not surfaced but report-worthy.
4. Identify sandbox facts already consolidated into gold.
5. Identify gold outputs without source coverage JSON.
6. Identify silver outputs that need privacy review before promotion.
7. Identify outputs that are source-cache only and should never be promoted.
```

---

## 12. Most important findings

### 12.1 Strong positives

```text
- The pipeline now includes procurement, TED, public-body payments, HSE/Tusla payments, CSO and AFS.
- Procurement SQL is logically careful about award value vs actual spend.
- Public-payments SQL is logically careful about ordered vs paid.
- Judiciary SQL and page copy are privacy/caveat-aware.
- QueryResult provides a strong future API/MCP status model.
- Streamlit pages increasingly read SQL/core outputs rather than doing business logic.
- CI has lint, format, typecheck and tests.
```

### 12.2 Main fragility

```text
- CI skips integration/sql/bronze/source tests.
- SQL view registration can swallow errors.
- CBI is register-centric and heuristic, not full regulatory-history coverage.
- Public-payments privacy policy is inconsistent between page docstring and SQL/extractor comments.
- LA payments may still be silver/standalone and not a first-class pipeline chain.
- TED is silver and not yet productized.
- Housing/local authority reports need promotion of AFS/CSO/LA payments plus new housing datasets.
- No FastAPI or MCP implementation exists yet.
```

### 12.3 Commercial interpretation

The most realistic commercial path is:

```text
free Streamlit app = proof/trust layer
manual due-diligence reports = near-term paid testing
FastAPI = stable data interface
MCP = analyst/agent workflow interface
```

The MCP angle is credible because MCP is being used as a standard way to connect AI assistants to external systems and data, but production MCP requires stronger security, auth, tool scoping, and hallucination/overclaiming safeguards than a simple dashboard.

---

## 13. Suggested source citations for Claude to keep handy

### MCP

```text
Anthropic MCP announcement:
https://www.anthropic.com/news/model-context-protocol

MCP specification:
https://modelcontextprotocol.io/specification/2025-06-18

Anthropic MCP code-execution blog:
https://www.anthropic.com/engineering/code-execution-with-mcp

Official MCP reference servers:
https://github.com/modelcontextprotocol/servers

GitHub MCP practical guide:
https://github.blog/ai-and-ml/generative-ai/a-practical-guide-on-how-to-use-the-github-mcp-server/

CData MCP enterprise connector blog:
https://www.cdata.com/blog/how-cdata-mcp-servers-connect-ai-to-enterprise-data
```

### FastAPI

```text
FastAPI GitHub README:
https://github.com/fastapi/fastapi

FastAPI docs:
https://fastapi.tiangolo.com/
```

### Repo anchors

```text
Pipeline:
https://raw.githubusercontent.com/peweet/dail_tracker/main/pipeline.py

App navigation:
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/app.py

pyproject:
https://raw.githubusercontent.com/peweet/dail_tracker/main/pyproject.toml

QueryResult:
https://raw.githubusercontent.com/peweet/dail_tracker/main/dail_tracker_core/results.py

Core DB:
https://raw.githubusercontent.com/peweet/dail_tracker/main/dail_tracker_core/db.py

SQL registry:
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/data_access/_sql_registry.py

Procurement page:
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/procurement.py

Public Payments page:
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/public_payments.py

Judiciary page:
https://raw.githubusercontent.com/peweet/dail_tracker/main/utility/pages_code/judiciary.py

Procurement awards SQL:
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/procurement_awards.sql

Procurement supplier summary SQL:
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/procurement_supplier_summary.sql

Procurement payments SQL:
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/procurement_payments.sql

Judiciary legal diary cases SQL:
https://raw.githubusercontent.com/peweet/dail_tracker/main/sql_views/judiciary_legal_diary_cases.sql

eTenders extractor:
https://raw.githubusercontent.com/peweet/dail_tracker/main/extractors/procurement_etenders_extract.py

Payments consolidation:
https://raw.githubusercontent.com/peweet/dail_tracker/main/extractors/procurement_payments_consolidate.py

LA payments extractor:
https://raw.githubusercontent.com/peweet/dail_tracker/main/extractors/procurement_la_payments_extract.py

CI:
https://raw.githubusercontent.com/peweet/dail_tracker/main/.github/workflows/ci.yml

SQL views directory:
https://github.com/peweet/dail_tracker/tree/main/sql_views

Data directory:
https://github.com/peweet/dail_tracker/tree/main/data
```

---

## 14. Bottom line for Claude

Dáil Tracker is not yet an MCP/API product, but it is now structurally close enough to justify planning one.

The main thing Claude should avoid is designing MCP around raw tables. The correct scope is:

```text
opinionated, source-linked, caveat-aware public-record tools
over stable SQL/core query contracts.
```

The best first commercial MCP workflow is:

```text
“Create a source-linked Irish public-record supplier footprint report.”
```

The best second workflow is:

```text
“Create a public-body / local-authority payments and procurement briefing.”
```

The safest implementation path is to preserve the current logic boundary:

```text
source extractors
-> gold/silver facts
-> SQL views
-> dail_tracker_core queries
-> QueryResult
-> API/MCP/report-builder interface
```

Before any paid MCP/API exposure, Claude should focus investigation on:

```text
required vs optional SQL views
source-unavailable handling
privacy consistency
entity-match confidence
value-kind semantics
output-regression enforcement
silver outputs ready for promotion
```
