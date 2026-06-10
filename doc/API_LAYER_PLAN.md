# Dáil Tracker — API Layer Plan

Status: **Phases 0–1 + a slice of 2–3 BUILT & validated (2026-06-06).** Behind the
`[api]` extra (`uvicorn api.main:app`): scaffold, member dossier wedge, legislation +
statutory-instruments resources, and `/v1/catalog`. **15 API TestClient tests + 187
total green; firewall clean; basedpyright 0; existing Streamlit app unaffected.**
Live counts: 236 members, 1,632 bills, 5,924 SIs. Remaining Phase 2 (bulk parquet
downloads) + Phase 3 (votes/lobbying/payments resources) + Phase 4 (harden) are
demand-gated (see §3). Prereq is DONE — the Streamlit-free core
(`dail_tracker_core`) is the seam an API consumes (152 query fns, 15/15 data
modules routed through it, firewall-guarded). This doc designs the read-only
JSON API over that core, informed by how comparable services actually do it.

Scope note: this is the **exposition layer** the uncoupling plan
(`doc/fastapi_query_core_uncoupling_plan.md`) always deferred. Build only on
demand — see "When to build" at the end.

---

## 1. What comparable services actually do (researched, cited)

The single most relevant precedent is the **Oireachtas API** — the upstream this
project already ingests — because consumers of our data likely already know it,
and mirroring it gives them zero learning curve.

| Service | Style | Version | Auth | Pagination | Envelope | Bulk | License |
|---|---|---|---|---|---|---|---|
| **Oireachtas API** ([swagger](https://api.oireachtas.ie/v1/swagger.json)) | REST nouns | `/v1` in path | **open, none** | `skip`/`limit` (0/50) | `{head:{counts}, results:[]}` | metadata API + static file store; XML back to 1919 | PSI = **CC-BY 4.0** ([licence](https://www.oireachtas.ie/en/open-data/license/)) |
| **UK Parliament** ([Members](https://members-api.parliament.uk/swagger/v1/swagger.json), [Bills](https://bills-api.parliament.uk/swagger/v1/swagger.json)) | REST nouns, OpenAPI | mixed (Bills `/v1`, Members metadata-only) | **open, none** | `skip`/`take` (max 20) | per-resource | — | Open Parliament Licence |
| **TheyVoteForYou** ([docs](https://theyvoteforyou.org.au/help/data)) | REST, JSON-only | `/api/v1/` | free self-service key | 100-cap + filter | — | XML dumps on data host | mySociety family |
| **TheyWorkForYou** ([docs](https://www.theyworkforyou.com/api/docs)) | RPC functions (`getMP`) | arg, no path | free key, **metered** (£20–£300/mo) | per-function | xml/json/php | ParlParse bulk | CC-BY-SA 2.5 + OPL |
| **OpenSanctions** ([openapi](https://api.opensanctions.org/openapi.json)) | **REST on FastAPI** | path | key; **free for non-commercial / journalists** | `limit`(≤500)/`offset`(≤9499) | `{limit, offset, total, results}` | **free no-auth bulk dumps** | CC-BY-**NC**; compute metered €0.10/query, only 200s billed ([metering](https://www.opensanctions.org/faq/7/metering/)) |
| **OpenCorporates** ([API ref](https://api.opencorporates.com/documentation/API-Reference)) | REST | `api_version` meta | key; free **share-alike** / paid proprietary | `page`/`per_page` (max 100) | `{results, api_version}` | enterprise only | **ODbL** (share-alike gates free/paid) |
| **TED (EU procurement)** ([docs](https://docs.ted.europa.eu/api/latest/index.html)) | REST search | path | **open for published**; key only for submitting | — | — | **bulk XML packages first**, SPARQL | EU reuse (open) |
| **Datasette** ([json_api](https://docs.datasette.io/en/latest/json_api.html)) | **auto-generated from schema** | — | none (delegate to host) | **keyset (`_next` token)** | `{ok, rows, truncated, next}` | the DB file itself | tool |
| **ProPublica / GovTrack** | REST (GET) | `/v1` | key, 5k/day | offset×20 | json/xml | bulk on `@unitedstates` (CC0) | **both DISCONTINUED** |

### Patterns that recur (and what we take from each)

1. **Official/public-mission sources are open, no-auth** (Oireachtas, UK Parliament, TED, CSO). Civic *re-publishers* add a free key to fund themselves (TWFY, TVFY). → **We default to open, no key.** Our data is already CC-BY upstream.
2. **`/v1` in the path** is the modern-winner choice (Oireachtas, TVFY, UK Bills). → **Adopt.**
3. **Offset pagination (`skip`/`limit`) dominates**; everyone caps hard. Datasette's keyset is the scalable upgrade. → **`skip`/`limit` like Oireachtas, hard cap + `truncated` flag like Datasette; keyset only if deep-paging ever hurts.**
4. **Thin envelope with metadata** — never a bare array. Oireachtas `{head, results}` and OpenSanctions `{limit, offset, total, results}`. → **Mirror Oireachtas's `{head, results}` for familiarity, fold in Datasette's `truncated` + our existing freshness meta.**
5. **Bulk file downloads are first-class**, not an afterthought (OpenSanctions, TED, ProPublica). → **We already sit on parquet — publishing it directly is the highest-leverage, lowest-effort bulk channel, and a better format than the XML these emit.**
6. **FastAPI is proven for exactly this** (OpenSanctions runs it over an analytical/search store). **Datasette is the architectural template** for read-only-analytical→JSON. → **FastAPI + the Datasette discipline (read-only, capped, CDN-cached).**
7. **The "dossier" is composed, not served** — every service keys on a stable person ID and fans it across resources; TWFY's only concession is `getMPInfo` with sparse `fields`. → **We have an advantage: `moq` already composes the member dossier server-side. A single `/v1/members/{code}/dossier` is a genuine differentiator.**
8. **Don't run an unfunded metered API** — ProPublica and GovTrack both died. → **Open + bulk, no metering, unless/until a funded commercial need appears.**

### How the free/paid line is drawn (if we ever need one)
Three mechanisms seen: **by compute/delivery** (OpenSanctions — records free, matching paid), **by license reciprocity** (OpenCorporates — ODbL share-alike free, proprietary paid), **by user type via NC license** (OpenSanctions — journalist/academic carve-out). Public-mission services (ProPublica, TED, CSO) draw **no line**. → **Default: free-everything + bulk parquet. If commercial free-riding ever bites, adopt OpenSanctions' license carve-out (NC + journalist exemption) — *not* per-query metering.**

---

## 2. Architecture (tailored to this codebase)

```
                 ┌──────────────────────────────────────────┐
   React / curl  │  api/  (FastAPI, [api] optional extra)    │
   notebooks  ──▶│   routers/  — thin: parse → call core →   │
   LLM agents    │              serialize → envelope         │
                 │   deps.py   — read-only conn (lifespan)   │
                 │   models are in CORE (firewall)           │
                 └───────────────────┬──────────────────────┘
                                     │ calls (no Streamlit)
                 ┌───────────────────▼──────────────────────┐
   Streamlit  ──▶│  dail_tracker_core/                       │
   (same core)   │   queries/*  — 152 fns → QueryResult(df)  │  ◀ DONE
                 │   serialize.py (NEW) — df → JSON-safe      │
                 │   models/*.py  (NEW) — Pydantic contracts  │
                 │   db.py — connect_with_views (read_only)   │
                 └───────────────────┬──────────────────────┘
                                     │ DuckDB over parquet (read-only)
                 ┌───────────────────▼──────────────────────┐
                 │  sql_views/*.sql  (the firewall)          │  ◀ DONE
                 │  data/gold,silver/*.parquet               │  ◀ also the BULK product
                 └──────────────────────────────────────────┘
```

### Decision register

- **D1 — REST, noun resources, JSON-only, `/v1` path.** Mirror Oireachtas nouns (`members`, `votes`, `legislation`, `questions`, `parties`, `constituencies`) + our extras (`lobbying`, `payments`, `statutory-instruments`, `procurement`). FastAPI auto-emits OpenAPI/Swagger (matches the Oireachtas/UK precedent for free).
- **D2 — Envelope:** `{ "head": { "limit", "offset", "total", "truncated", "mart_version", "generated_at" }, "results": [ ... ] }`. `head` mirrors Oireachtas (familiar); `truncated` from Datasette; `mart_version`/`generated_at` reuse the freshness meta **already in views** (`v_payments_summary.latest_fetch_timestamp_utc`, `mart_version`). Errors: RFC-ish `{ "error": { "code", "message" } }`.
- **D3 — Pagination:** `skip`/`limit` (default 50, **max 500**), exactly Oireachtas. Always cap + set `truncated`. Keyset is a documented future upgrade, not v1.
- **D4 — Auth: OPEN, no key.** Matches every official source + our CC-BY licence; sidesteps the GovTrack unfunded-metering death. Abuse control via CDN rate-limiting, not app code (Datasette's stance).
- **D5 — Connection model (the one real decision):** build **one read-only DuckDB connection at app startup** (FastAPI `lifespan`) via `core.db.connect_with_views(..., read_only=True)` over the gold/silver parquet; hand each request a `conn.cursor()` (DuckDB cursors give independent, concurrent reads off one connection). If a single connection bottlenecks under load, swap to a tiny fixed pool — behind the same `deps.py` provider, so routes don't change. **Never rebuild views per request** (expensive). Mirrors how `@st.cache_resource` gave Streamlit one conn/session.
- **D6 — Serializer (the one real new module), in CORE:** `dail_tracker_core/serialize.py` — `QueryResult → JSON-safe list[dict]` handling the exact traps the parity harnesses already surfaced: `NaN/NaT → null`, `Timestamp → ISO-8601`, `numpy int/float → native`, `Decimal → str|float`, array cols (`flags`, `beneficiary_tags`, `party_seats_json`) → lists. Lives in core (not `api/`) so the same serializer powers the future "dossier pack" file product and the firewall still holds. This is where **caveat attach + PII suppression** belong (the serializer is the single chokepoint for both).
- **D7 — Pydantic models, in CORE, hand-picked projections:** `dail_tracker_core/models/*.py`. **Not 1:1 with marts** (that re-couples the contract to churn) — ~3–5 published surfaces to start. A `mart → model` mapper is the shock absorber; version bumps only on a deliberate published-shape change, never a mart rewrite. Pandera validates the marts feeding them (published + load-bearing only, not all 90 views).
- **D8 — Bulk = the parquet files themselves**, served as static no-auth downloads + a `/v1/catalog` manifest (name, rows, schema, `mart_version`, download URL, licence). Immutable → caches trivially on a CDN. This is the cheapest, highest-leverage channel and the accountability-mission default (TED/OpenSanctions/ProPublica all bulk-first).
- **D9 — Caching/CDN:** set `Cache-Control` + `ETag` (ETag from `mart_version`); put a CDN in front; let it absorb load and rate-limit. App stays stateless. (Datasette delegates exactly this way.)
- **D10 — Packaging/deploy:** FastAPI + `uvicorn[standard]` in a **`[api]` optional-dependency extra** so Streamlit Cloud's `uv sync` stays lean (per `project-streamlit-cloud-deploy`). API deploys as its own container/host; core + sql_views + parquet are the shared payload.
- **D11 — Licence:** **CC-BY 4.0** on responses + bulk (matches Oireachtas PSI upstream; code stays AGPL). Attribution string: "Data via Dáil Tracker". Keep the OpenSanctions-style NC carve-out in reserve only if commercial free-riding appears.
- **D12 — basedpyright** extended to `api/` at creation (start clean), as the plan specified.

### What maps to what (the build is mostly mechanical)
- The **133 `fetch_*` wrappers are the endpoint spec** — each is ~3 lines → one route handler.
- **`moq` already composes the member dossier** → `/v1/members/{code}/dossier` is nearly free and is the differentiator (no comparable service serves a composed dossier).
- **`QueryResult.ok/is_empty/unavailable`** maps cleanly to HTTP: `unavailable → 503`, ok-empty → `200 {results:[]}`, found → `200`.

---

## 3. Phased build (ship the wedge, let demand pull the rest)

**Phase 0 — Scaffold ✅ BUILT.**
`[api]` extra in pyproject; `api/main.py` (app + `lifespan` conn), `api/deps.py`
(`get_cursor`), `/v1/health` + `/` root; `dail_tracker_core/serialize.py`
(df→JSON + `{head, results}` envelope); `dail_tracker_core/connections.py`
(the 4-phase member-view builder, **extracted from the Streamlit wrapper** so
both share it Streamlit-free); basedpyright includes `api`. FastAPI 0.136.

**Phase 1 — The wedge: member dossier ✅ BUILT.**
`GET /v1/members` (list; `skip`/`limit` cap 500, filters house/party/constituency/
`fuzzy_name`) and `GET /v1/members/{code}/dossier` (`dail_tracker_core/dossiers.py`
composes the `moq` queries — identity, headline, attendance, payments, legislation,
ministerial, SIs, revolving-door, questions, links, constituency). Pydantic
`MemberSummary`/`MemberDossier` in `dail_tracker_core/models/`. 8 `TestClient`
tests (`test/test_api_members.py`): envelope shape, limit-cap→422, house filter,
dossier roundtrip (real data — Aengus Ó Snodaigh: €177k, 1607 votes, 22 bills),
unknown-code→404, auto-OpenAPI. **The serializer + conn model are now solved once;
everything after is mechanical addition.**

**Phase 2 — Bulk + catalog.** `/v1/catalog` ✅ BUILT (`api/routers/catalog.py`) —
curated manifest (PII-safe: only reviewed endpoints, never a raw-parquet dump) with
live counts. **Static parquet downloads still TODO** — needs a vetted allow-list
(SIPO donor addresses / personal insolvency must never be exposed).

**Phase 3 — Noun resources (mechanical, demand-ordered).**
`/v1/legislation` (list + `{bill_id}` composed dossier) and `/v1/statutory-instruments`
(list, filters year/operation/department/eu_only) ✅ BUILT on a second lifespan
connection (`legislation_conn`, all 111 views are `CREATE OR REPLACE` so registration
is idempotent). `dossiers.build_bill_dossier` composes detail + timeline + amendment
intensity + sources + PDFs + debates + SIs-under-bill. `/v1/votes` (list + `{vote_id}` division dossier: vote + party
breakdown + each member's vote + sources), `/v1/payments` (all-time TAA ranking),
`/v1/lobbying/organisations` + `/v1/lobbying/revolving-door` ✅ BUILT. **All
endpoints now run off a SINGLE union connection** (`api_conn()` — `register_member_views`
+ the other domain globs; only 3 files need substitution, all handled). The five
per-domain connections collapsed to one; every router uses one `get_cursor`.

**FastAPI↔core parity pass ✅ BUILT (2026-06-10).** Brought the HTTP surface up to
the composition layer that the MCP server (`C:\tmp\dail_mcp`, out-of-repo) already
consumed — the routers were the only gap (composition + serialize + caveats were
all done in `dossiers.py`). Added: **procurement** router (`/v1/procurement/suppliers`
list + `/{supplier_norm}/dossier` — the commercial wedge — plus `/competition` and
`/lobbying-overlap`, both carrying the no-inference caveat from core and mapping
`unavailable→503`); **committees** (`/v1/committees` + `/{committee}`); **ministers**
(`/v1/ministers?department=&on_date=` — the who-held-X-when primitive); member
sub-resources `/v1/members/{code}/questions` + `/interests` (speeches already
existed); votes `/v1/votes/{vote_id}/interest-breakdown` + `/v1/cross-reference/votes-interests`;
and `/v1/payments/{year}`. Surface is now **~25 routes** (was 12). `/v1/catalog` +
the root resources list updated. New `test/api/test_api_extra.py` (16 TestClient
tests, data-conditional skips). Verified: 211 core+api tests pass, firewall clean
(37 files), basedpyright 0 on `api/`, ruff clean; every new endpoint returns live
data (suppliers 7,530 / committees 61 / payments-2025 219 / Finance min 2023-06-01
→ Michael McGrath). Still demand-gated, not pre-expanded: charities / public-body
payments / SIPO / appointments nouns (core fns exist; add when a consumer asks).

**Phase 4 — Hardening (only as traffic appears).**
Pandera on published marts; Pydantic versioning policy; CDN + cache headers + ETags; CDN rate-limiting if abused.

**Deferred (demand-gated):** API keys / metering / billing (only if a *paid* product — the bigger, separate lift); keyset pagination (only if deep offsets hurt); the OpenSanctions NC carve-out (only if commercial free-riding).

### Effort shape
Front-loaded into **two components — the serializer (D6) and the conn model (D5)** — both solved once in Phase 0–1. The wedge is genuinely small (a few focused sessions); full coverage is moderate and linear because the brain + spec already exist. The thing that usually makes Streamlit→API migrations hard (logic trapped in UI) is already done.

---

## 4. When to build (demand gate)
Per the benefit analysis: the API's value is **opening channels** (machine/programmatic/React/bulk/revenue) you have **no consumer for today**. Build **Phase 0–2 when** any one of these is real: a data-journalist/NGO wants bulk; an LLM/agent or research consumer appears; a React/SEO push is committed; a buyer asks. Until then, Phase 0–1 has only architectural value (it *proves* the core is interface-agnostic and gives a demo artifact) — a small, optional hedge, not urgent.

## 5. Open questions
- Single read-only connection + cursors vs a small pool — settle empirically under a load test in Phase 1 (D5).
- Dossier shape: one fat composed object vs sub-resource links (`/members/{code}/votes`) — start fat (the differentiator), add sub-resources if payloads get heavy.
- Bulk hosting: same host as API vs object storage + CDN (object storage is cheaper/cacheable for immutable parquet).
- Do we expose a Datasette-style read-only `?sql=` escape hatch? Powerful for researchers, but a bigger safety surface — defer past v1.
