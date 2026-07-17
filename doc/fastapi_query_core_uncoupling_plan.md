---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-07-17
supersedes: []
read_when: decoupling business/data-access logic from Streamlit, or exposing it via FastAPI/other interfaces
key: PLAN|LIVE|infra
---

# FastAPI / Query-Core Uncoupling Plan for `dail_tracker`

## Goal

Uncouple business logic and data-access logic from Streamlit so that Streamlit becomes a thin UI layer, while the same backend logic can later support:

- FastAPI;
- React / Next.js;
- CLI exports;
- scheduled reports;
- newsroom datasets;
- paid/professional API endpoints;
- notebook analysis;
- static reports.

The goal is **not** to rewrite the whole app in FastAPI.

The goal is:

> Create a Streamlit-free core, then expose it through Streamlit and optionally FastAPI.

---

## Key Design Principle

The architectural win is not FastAPI itself.

The win is:

> `dail_tracker_core` must contain the reusable data/query/business logic and must not import Streamlit.

FastAPI is then just one interface.

Target shape:

```text
data/gold/parquet/
sql_views/
        ↓
dail_tracker_core/
  DuckDB + SQL views + business rules + provenance
        ↓
interfaces:
  utility/        Streamlit thin UI
  api/            FastAPI thin API
  reports/        optional report/dossier generator
```

---

## Why This Fits the Project

The project already uses DuckDB, Parquet, SQL views, source metadata, pytest, Ruff, basedpyright, Pandera, CI, and Streamlit.

The current weakness is that many `utility/data_access/*.py` modules import Streamlit and use `st.cache_data` / `st.cache_resource`. That makes Streamlit part of the data layer.

The fix is to move reusable logic into `dail_tracker_core`, then keep Streamlit caching only in thin wrapper modules.

---

## Real-World Pattern: Streamlit + FastAPI

The common pattern is:

```text
Streamlit frontend
FastAPI backend
shared business logic or service layer
```

But for this project, the first version should not make Streamlit call FastAPI over HTTP. That adds latency and deployment complexity.

Better first pattern:

```text
Streamlit app  → imports dail_tracker_core directly
FastAPI app    → imports dail_tracker_core directly
React later    → calls FastAPI
```

Avoid this initially:

```text
Streamlit → local HTTP call → FastAPI → same local DuckDB data
```

---

## Alternatives Considered

### FastAPI

Good for:

- custom API;
- typed endpoints;
- OpenAPI docs;
- React frontend later;
- paid API;
- reports/dossiers;
- authentication and rate limits later.

Main burden:

- deployment;
- auth;
- rate limits;
- pagination;
- query timeouts;
- API versioning;
- schema stability.

Verdict:

> Good candidate after core extraction.

### Django / Django REST Framework

Good for:

- full SaaS;
- user accounts;
- admin workflows;
- permissions;
- saved searches;
- billing;
- manual review queues.

Less ideal first step because the project’s core data is analytical DuckDB/Parquet, not Django ORM models.

Verdict:

> Consider later if the product becomes a full SaaS with users, teams, admin, and manual-review workflows.

### Litestar

Good for:

- FastAPI-like API;
- more structured ASGI app;
- built-in batteries.

Main downside:

- smaller ecosystem than FastAPI.

Verdict:

> Plausible alternative, but FastAPI is the safer default.

### Datasette

Good for:

- public data browsing;
- JSON API over tabular data;
- journalism-friendly dataset access.

Less ideal for:

- custom paid workflows;
- dossiers;
- authentication;
- business logic;
- complex entity joins.

Verdict:

> Good possible companion for public datasets, not the main product API.

### Static exports / reports

Good for:

- public transparency;
- grant-funded outputs;
- journalist deliverables;
- low maintenance.

Less ideal for:

- interactive search;
- API monetization;
- saved searches;
- watchlists.

Verdict:

> Good short-term product output even before FastAPI.

---

## Target Package Layout

Add:

```text
dail_tracker_core/
  __init__.py
  db.py
  registry.py
  results.py
  errors.py
  provenance.py
  models.py
  schemas/
    procurement.py
    payments.py
    lobbying.py
    corporate.py
    si.py
    members.py
  queries/
    suppliers.py
    buyers.py
    procurement.py
    public_money.py
    lobbying.py
    corporate.py
    statutory_instruments.py
    members.py
    payments.py
    attendance.py
    appointments.py
    committees.py

api/
  __init__.py
  main.py
  deps.py
  models.py
  routers/
    health.py
    datasets.py
    suppliers.py
    buyers.py
    procurement.py
    lobbying.py
    corporate.py
    statutory_instruments.py
```

Keep existing Streamlit files:

```text
utility/app.py
utility/pages_code/
utility/data_access/
```

But turn `utility/data_access/` into thin Streamlit cache wrappers.

---

## Hard Rules

### `dail_tracker_core` must not import Streamlit

Forbidden in `dail_tracker_core`:

```python
import streamlit as st
```

Also avoid importing Streamlit page modules from core or API.

### Streamlit pages must not own business semantics

Move these rules into core:

- award value is not spend;
- framework/DPS ceiling values are not safe to sum;
- lobbying/procurement overlap is co-occurrence only;
- null SI legal state means not checked;
- OCR-derived SIPO data requires manual review;
- sole-trader / individual-like rows require quarantine;
- CRO/charity/company matches require confidence labels;
- source links/provenance are required for public-facing enriched rows.

### API should not return raw DataFrames

Core may return DataFrames internally, but FastAPI responses should use Pydantic models or controlled JSON serialization.

### Pandera validates data contracts

Pandera should validate internal DataFrames and public-facing marts.

### Pydantic validates API contracts

Pydantic should validate API request/response shapes.

### basedpyright should focus on clean boundaries

Use basedpyright on:

- `dail_tracker_core`;
- `api`;
- pure utility modules;
- API models;
- schema/result types.

Do not immediately force the entire pandas-heavy pipeline into strict type checking.

---

## Dependency Plan

Do not add FastAPI to the base Streamlit runtime immediately.

Add an optional dependency group / extra.

Example:

```toml
[project.optional-dependencies]
api = [
  "fastapi>=0.115",
  "uvicorn[standard]>=0.30",
]
```

For tests:

```toml
[dependency-groups]
dev = [
  "httpx>=0.27",
]
```

Reason:

- Streamlit Cloud/runtime should stay lean.
- API dependencies should be optional.
- Pipeline/OCR/dev dependencies should remain separate.

---

## DuckDB Connection Handling

This is a key technical risk.

Do not use a single shared global DuckDB connection for all FastAPI requests.

Safer first pattern:

```python
from collections.abc import Iterator
import duckdb

def get_conn() -> Iterator[duckdb.DuckDBPyConnection]:
    conn = connect_with_views(PROJECT_ROOT)
    try:
        yield conn
    finally:
        conn.close()
```

Rationale:

- API servers may handle concurrent requests.
- Shared DuckDB connections can create thread-safety and cache issues.
- Cache registered SQL text or metadata, not the connection itself.

Streamlit can still use `st.cache_resource` in wrapper modules, but the cached object should not leak into the core API design.

---

## Result Type

Create a shared result wrapper.

```python
from dataclasses import dataclass
import pandas as pd

@dataclass(frozen=True)
class QueryResult:
    data: pd.DataFrame
    ok: bool
    error: str | None = None
    unavailable_reason: str | None = None
    source_caveat: str | None = None
```

This lets every interface distinguish:

- successful query with no matching records;
- source unavailable;
- SQL registration failure;
- parquet missing;
- source not checked;
- coverage incomplete;
- manual review required.

Do not silently return empty DataFrames for failures.

---

## Provenance and Caveats

Create:

```text
dail_tracker_core/provenance.py
```

Example constants:

```python
PROCUREMENT_CAVEAT = (
    "Award value is not spend. Framework and DPS values may be ceiling values. "
    "Only value_safe_to_sum should be aggregated."
)

PUBLIC_MONEY_CAVEAT = (
    "Observed disclosed value reflects records collected and parsed by this project. "
    "It is not a complete measure of all public money received."
)

LOBBYING_OVERLAP_CAVEAT = (
    "Procurement/lobbying overlap means entity co-occurrence only. "
    "It is not evidence that lobbying influenced any award."
)

SI_LEGAL_STATE_CAVEAT = (
    "This is a source-linked discovery aid, not legal advice. "
    "Not checked does not mean in force."
)
```

Every public-facing API response should include relevant caveats.

---

## Pandera Plan

Use Pandera for internal tabular contracts.

Good targets:

```text
ProcurementAwardsSchema
PublicPaymentsSchema
SupplierSummarySchema
LobbyingOverlapSchema
CorporateNoticesSchema
SIStateSchema
```

Example checks:

- required columns exist;
- source URL present;
- value columns numeric;
- `value_safe_to_sum` present where relevant;
- no public rows missing provenance;
- confidence columns present for enrichment joins;
- privacy-quarantine columns present for public-money/procurement rows.

Suggested file layout:

```text
dail_tracker_core/schemas/procurement.py
dail_tracker_core/schemas/public_money.py
dail_tracker_core/schemas/lobbying.py
dail_tracker_core/schemas/corporate.py
dail_tracker_core/schemas/statutory_instruments.py
```

Pandera should validate DataFrames returned by core queries or generated marts.

Avoid exposing Pandera DataFrame types directly in FastAPI public response models at first. Use Pydantic for API responses.

---

## Pydantic / API Model Plan

Create stable response models.

Example:

```python
from datetime import datetime
from pydantic import BaseModel

class ApiMeta(BaseModel):
    dataset: str
    generated_at: datetime | None = None
    source_coverage: str | None = None
    caveat: str | None = None

class SupplierSummary(BaseModel):
    supplier_id: str
    supplier_name: str
    observed_payment_value_eur: float | None = None
    observed_award_value_safe_eur: float | None = None
    framework_ceiling_value_eur: float | None = None
    n_buyers: int
    n_awards: int
    has_lobbying_overlap: bool
    has_corporate_notices: bool
    coverage_note: str | None = None
    caveat: str | None = None

class SupplierSummaryResponse(BaseModel):
    data: SupplierSummary
    meta: ApiMeta
```

Do not expose implementation details like DataFrame column names unless they are part of the public contract.

---

## basedpyright Plan

Keep basedpyright focused.

Initial include list should add:

```toml
[tool.basedpyright]
include = [
  "services",
  "shared/normalise_join_key.py",
  "config.py",
  "manifest.py",
  "dail_tracker_core",
  "api",
]
```

Keep:

```toml
typeCheckingMode = "standard"
```

Use basedpyright to catch:

- missing imports;
- optional field errors;
- incorrect response model types;
- unhandled `None`;
- incompatible return types;
- accidentally returning DataFrame where API model expected;
- Streamlit leakage into core/API.

Do not start by type-checking every pandas-heavy pipeline script.

---

## First API Surface

Keep the first API tiny.

```text
GET /v1/health
GET /v1/datasets
GET /v1/suppliers/search?q=
GET /v1/suppliers/{supplier_id}/summary
GET /v1/procurement/lobbying-overlap
```

Then add:

```text
GET /v1/suppliers/{supplier_id}/dossier
GET /v1/buyers/search?q=
GET /v1/buyers/{buyer_id}/summary
```

Do not expose every Streamlit page through API.

Start with the commercially plausible surface:

- suppliers;
- public bodies / buyers;
- procurement;
- public money;
- lobbying overlap;
- corporate/entity context.

---

## Streamlit Wrapper Pattern

After extraction, Streamlit data-access modules should look like this:

```python
import streamlit as st
from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries.procurement import get_supplier_summary

@st.cache_resource
def get_conn():
    return connect_with_views(PROJECT_ROOT)

@st.cache_data(show_spinner=False)
def load_supplier_summary(supplier_id: str):
    conn = get_conn()
    result = get_supplier_summary(conn, supplier_id)

    return result
```

Streamlit pages should then do:

```python
result = load_supplier_summary(supplier_id)

if not result.ok:
    st.warning(result.unavailable_reason or "Data unavailable.")
else:
    st.dataframe(result.data)
```

The page should not contain SQL or business interpretation.

---

## API Implementation Pattern

Example:

```python
from fastapi import APIRouter, Depends, HTTPException
from duckdb import DuckDBPyConnection

from api.deps import get_conn
from api.models import SupplierSummaryResponse
from dail_tracker_core.queries.suppliers import get_supplier_summary

router = APIRouter(prefix="/v1/suppliers", tags=["suppliers"])

@router.get("/{supplier_id}/summary", response_model=SupplierSummaryResponse)
def supplier_summary(
    supplier_id: str,
    conn: DuckDBPyConnection = Depends(get_conn),
) -> SupplierSummaryResponse:
    result = get_supplier_summary(conn, supplier_id)

    if not result.ok:
        raise HTTPException(status_code=503, detail=result.unavailable_reason)

    return to_supplier_summary_response(result)
```

---

## Unit Test Plan

Add tests at four levels.

### 1. Import / firewall tests

Files:

```text
test/test_core_no_streamlit_imports.py
test/test_api_imports.py
```

Checks:

- `dail_tracker_core` imports without Streamlit;
- `api` imports without Streamlit;
- no `dail_tracker_core` module imports `utility.pages_code`;
- no `api` module imports Streamlit pages;
- Streamlit pages still import.

### 2. Core query tests

Files:

```text
test/test_core_procurement_queries.py
test/test_core_supplier_summary.py
test/test_core_public_money_semantics.py
test/test_core_provenance.py
```

Checks:

- no-data vs unavailable distinction;
- `value_safe_to_sum` respected;
- framework/DPS ceilings not summed as spend;
- payment / PO / award lifecycle values remain separate;
- observed disclosed value is labelled coverage-limited;
- lobbying overlap caveat present;
- source URL fields present.

### 3. Pandera contract tests

Files:

```text
test/test_contract_procurement_awards_schema.py
test/test_contract_public_money_schema.py
test/test_contract_supplier_summary_schema.py
test/test_contract_lobbying_overlap_schema.py
```

Checks:

- required columns;
- nullable rules;
- numeric value columns;
- source/provenance columns;
- confidence/manual-review columns;
- privacy/quarantine columns;
- no public rows missing required source metadata.

### 4. API tests

Files:

```text
test/test_api_health.py
test/test_api_datasets.py
test/test_api_suppliers.py
test/test_api_procurement_overlap.py
```

Use FastAPI `TestClient`.

Checks:

- endpoint returns 200 where expected;
- JSON shape matches response model;
- empty search returns structured empty result;
- unavailable data returns structured error;
- caveats appear in `meta`;
- source coverage appears in `meta`;
- pagination works when added.

---

## CI Plan

Keep existing CI.

Add jobs or steps gradually.

### Step 1

Add core/API import tests to normal pytest.

### Step 2

Add basedpyright coverage for:

```text
dail_tracker_core
api
```

### Step 3

Add Pandera schema tests for stable public-facing datasets.

### Step 4

Add API tests with TestClient.

Do not add live API deployment tests yet.

Do not add network tests to normal CI.

---

## Deployment Plan

### Phase 1

No deployment.

Only local API proof:

```bash
uv run uvicorn api.main:app --reload
```

### Phase 2

Deploy API separately from Streamlit.

Do not run FastAPI inside Streamlit.

Possible deployment models:

```text
Streamlit Cloud / public app
Separate API service on Render/Fly/Railway/VPS
```

or later:

```text
React frontend
FastAPI backend
static parquet/data storage
```

### Phase 3

Add:

- API keys;
- usage logging;
- rate limits;
- response-size limits;
- query timeouts;
- pagination;
- billing hooks if needed.

---

## Monetizable API Endpoints Later

Possible paid endpoints:

```text
GET /v1/suppliers/{id}/dossier
GET /v1/buyers/{id}/profile
GET /v1/reports/public-money
GET /v1/reports/lobbying-overlap
GET /v1/reports/foi-leads
GET /v1/corporate/entities/{id}
GET /v1/statutory-instruments/{id}/legal-context
```

The API should sell:

> source-linked Irish public-record intelligence

not:

> raw parliamentary data.

Most valuable paid API surface:

```text
supplier / public-body / public-money / lobbying / corporate context
```

---

## Recommended PR Sequence

### PR 1 — Core extraction only

Scope:

1. Create `dail_tracker_core/`.
2. Move DB connection and SQL registry logic.
3. Add `QueryResult`.
4. Add provenance/caveat constants.
5. Add import/firewall tests.
6. Add basedpyright include for `dail_tracker_core`.
7. Do not add FastAPI yet.

Acceptance:

- `dail_tracker_core` imports without Streamlit.
- Existing Streamlit pages still import.
- SQL registration still works.
- CI passes.

---

### PR 2 — Procurement/public-money core

Scope:

1. Move procurement query functions from Streamlit data-access into core.
2. Keep Streamlit wrapper thin.
3. Add Pandera schema for procurement/public-money outputs.
4. Add tests for value semantics and provenance.

Acceptance:

- framework/DPS values are not summed as spend.
- observed payment/PO values are labelled coverage-limited.
- lobbying overlap caveat is attached.
- public rows include source/provenance fields where expected.

---

### PR 3 — Tiny FastAPI proof

Scope:

1. Add optional API dependencies.
2. Add `api/main.py`.
3. Add `/v1/health`.
4. Add `/v1/datasets`.
5. Add `/v1/suppliers/search`.
6. Add `/v1/suppliers/{id}/summary`.
7. Add Pydantic response models.
8. Add TestClient tests.
9. Add basedpyright include for `api`.

Acceptance:

- API imports cleanly.
- API tests pass.
- response models are stable.
- no Streamlit import in API.

---

### PR 4 — Dossier endpoint

Scope:

1. Add `/v1/suppliers/{id}/dossier`.
2. Include:
   - observed payments;
   - procurement awards;
   - unsafe/framework values separately;
   - lobbying overlap;
   - corporate/entity signals;
   - source coverage;
   - caveats.
3. Add tests.

Acceptance:

- dossier endpoint includes provenance and caveats.
- no causation language.
- no total-spend overclaim.
- values are separated by lifecycle stage.

---

### PR 5 — Deployment experiment

Scope:

1. Run API separately from Streamlit.
2. Test concurrent reads.
3. Add query timeouts.
4. Add response-size limits.
5. Add pagination.
6. Add basic API docs page.

Acceptance:

- API can run independently.
- Streamlit app still works without API.
- no production commitment yet.

---

## What Not To Do

Do not:

- rewrite all Streamlit pages;
- move all business logic into FastAPI route handlers;
- make Streamlit call FastAPI locally just for architecture purity;
- expose raw DataFrames as public API;
- add auth/billing before proving endpoint value;
- directly auto-publish API data without freshness/provenance guards;
- type-check every pandas-heavy pipeline script at once;
- treat Pandera as a substitute for API response models;
- merge public money, awards, and framework values into one misleading total.

---

## Final Recommendation

Build this in small steps.

The strategic feature is:

> `dail_tracker_core`: a Streamlit-free, tested, typed, provenance-aware query/business layer.

FastAPI should be a proof that the data layer is reusable and potentially monetizable.

The best near-term stack is:

```text
DuckDB / Parquet / SQL views
Pandera dataset contracts
dail_tracker_core pure query/business logic
Pydantic API models
FastAPI thin API
Streamlit thin UI
basedpyright on core/API boundaries
pytest/TestClient for API tests
```

This makes Streamlit disposable without forcing an immediate frontend rewrite.
