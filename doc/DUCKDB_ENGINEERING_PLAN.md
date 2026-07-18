---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-07-17
supersedes: []
read_when: refactoring or hardening the DuckDB layer — sql_views registration, dail_tracker_core queries/connections, data_access wrappers, api routers
key: PLAN|LIVE|infra
---

# DuckDB layer — engineering assessment & phased plan

## Implementation status (2026-07-18) — Phases 0–3 SHIPPED

Implemented in-session 2026-07-18 (all gates green; see the working tree). What
shipped, and the two deliberate deviations from the plan text below:

- **Phase 0** — health/catalog cursors; `QueryResult.require()` + `SourceUnavailable`
  (gates raise, sections degrade VISIBLY via `unavailable_sections` on the three
  dossier models); votes 400/503 typed; `top_n` bound. Bonus find: `HeadlineStats.divisions`
  never matched the dossier key `divisions_participated` → the API always returned 0;
  field renamed.
- **Phase 1** — `analytics_loading`/`sql_queries/test_sql.py` archived to
  `pipeline_sandbox/_archive/`; `news_data.py` moved next to its only (sandbox)
  consumer; 4 orphaned procurement fetchers removed (grep-verified); `_sql_registry.py`
  deleted (2 test imports repointed); stale docs fixed; `section_map.py --write` now
  REFRESHES an existing map.
- **Phase 2** — `connections.DOMAIN_REGISTRATIONS` policy table + `domain_conn()`;
  16 data_access modules migrated; SIPO trio now shares ONE conn; the API glob list
  is DERIVED from the table (adds committee_evidence_* + minister_briefs.sql to the
  union — previously a gap). `test/sql_views/test_registration_graph.py` enforces
  reachability + same-dir order + cross-dir unit order from real SQL ASTs — and on
  first run caught `constituency_la_agenda_items.sql` registered NOWHERE (the
  your_councillors agenda-items feature had silently returned unavailable since it
  shipped; now wired).
- **Phase 3** — 24 `_run` shims → `make_runner`; the SPENT/COMMITTED never-sum
  fragments centralized as derived columns `amount_spent_safe_eur` /
  `amount_committed_safe_eur` on `v_procurement_payments` (parity-verified on
  396,599 real rows: €25,174,333,145.70 / €29,920,525,688.48 — old vs new identical);
  cross_ref's interest-classification predicates graduated to
  `v_member_interests_flags` (parity-verified across all 4 interest kinds);
  `queries/procurement.py` split by money grain into a package
  (awards/payments/ted/tenders_live/afs/signals + _shared), import surface
  unchanged; the 3 legacy non-`v_` views renamed (40 references; the v_-prefix
  allowlist is now EMPTY).
- **Deviation 1:** the payments/afs GROUP-BYs were NOT graduated to views wholesale —
  the derived-column approach centralizes the correctness-critical never-sum logic
  without 15 new grain-sensitive views (house rule: consolidate for correctness,
  not tidiness).
- **Deviation 2:** `constituency_housing_context_with_ssha`'s pandas merge stays in
  core: a SQL LEFT-JOIN view would LOSE the documented supply-only degradation
  (an absent SSHA view kills a referencing view at registration; the Python path
  degrades gracefully). Single-site, not duplicated — below the consolidation bar.
- **Phase 4 remains open** (envelope/pagination/Pydantic width are product
  decisions); the `read_only`-is-convention-only posture is now documented in
  `api_conn`'s docstring.

Audit date 2026-07-17. Evidence: four file:line-cited subagent surveys (queries,
data_access, api, everything-else), the `view_deps` SQL-AST graph
(`mcp_server/sql_index.py`: 266 views / 111 dependency edges / 100% parsed by
DuckDB's own parser), and test runs. Findings below are CONFIRMED unless marked
PLAUSIBLE. Implementation is deliberately left to a later session (Opus): this
doc is the plan, not the diff.

## Verdict

**The core is well engineered — materially better than its own comments claim.**
The three-tier design (registered `sql_views/` → `queries/*` returning
`QueryResult` → two thin consumers, Streamlit `data_access/` and FastAPI
`dossiers.py`/routers) is executed with unusual discipline:

- One error boundary: `queries/__init__.py:run_query` holds the **only**
  `.execute()/.df()` call in the query tree; 402/405 functions return `QueryResult`.
- Injection posture clean: values `?`-bound throughout (375 occurrences);
  f-strings carry only allow-listed ORDER BY dicts and module constants. Single
  non-bound caller value: `int(top_n)` in `procurement.supplier_concentration`
  (int-coerced, safe).
- Logic firewall holds everywhere: zero pages open connections or run SQL
  (enforced by `test_firewall_no_raw_db_in_ui.py`, empty allowlist); zero
  data_access modules contain SQL or read parquet directly.
- No SQL implemented twice: Streamlit and API both sit on the one `queries.*` core.

**The weaknesses are at the seams, not the core:** fragile registration-order
conventions checked by convention rather than by machine (until `view_deps`);
policy decided per-module instead of per-layer (`swallow_errors`, pagination,
error envelopes); idiom-level duplication (25 identical `_run` shims, repeated
tier-aggregate fragments, three SIPO connections over one glob); stale
documentation that misdescribes the current architecture; and a handful of
genuinely dead modules.

## Phase 0 — Correctness fixes (small, all CONFIRMED)

1. **`/health` and `/catalog` race the shared connection.** `api/routers/health.py:16`
   and `catalog.py:231-247` execute on `app.state.conn` directly instead of the
   cursor-per-request pattern (`api/deps.py:get_cursor`) every other endpoint
   uses; catalog holds it across ~24 sequential counts. A DuckDB connection
   object is not safe for concurrent `.execute()` across threads. Fix: route both
   through `Depends(get_cursor)` (catalog already imports it).
2. **~8 dossier builders collapse `unavailable` into "empty".** `dossiers.py`
   `_identity`, `build_member_dossier`, `list_members`, `list_bills`,
   `build_bill_dossier`, `list_statutory_instruments`, `list_votes`,
   `build_division_dossier` read `.data` without checking `.ok` — a source outage
   renders as 404/no-data. Honor `QueryResult.ok` uniformly (the other ~25
   dossier functions already do).
3. **String-matched status code.** `api/routers/votes.py:72-75` picks 400 vs 503 by
   substring-matching the error text; a rewording upstream silently flips the
   code. Replace with a typed reason on `QueryResult`.
4. **Bind `top_n`.** `procurement.py:216-220` — close the one non-parameterized value.

## Phase 1 — Dead code & stale docs (archive, don't delete outright)

1. `shared/analytics_loading.py` — orphaned pre-registry builder that registers
   every parquet as a raw stem, bypassing all grain/privacy views; only reference
   is the firewall test that FORBIDS it. Archive; keep the forbidden-name string.
2. `sql_queries/test_sql.py` — dev scratch, import-time side effects (opens DuckDB
   and creates views at import), never collected (`testpaths=["test"]`). Archive.
3. PLAUSIBLE dead — **verify liveness before touching**: `utility/data_access/news_data.py`
   (only consumer is a sandbox page) and the four bare-`.data` procurement
   fetchers (`fetch_supplier_summary/authority_summary/cpv_summary/lobbying_overlap`)
   superseded by `_result` twins.
4. Stale docs that misdescribe reality:
   - `dail_tracker_core/db.py:8-15` "TRANSITIONAL NOTE" claims `_sql_registry.py`
     is a live ~25-line duplicate. It is already a 34-line pure re-export shim
     with two test importers. Recommend: delete the shim (repoint
     `test/sql_views/test_sql_views.py:169`, `test/seanad/test_seanad_views.py:45`
     at `dail_tracker_core.db`) and rewrite the note. Also `.gitignore:421`,
     `doc/AGENT_CUSTOMIZATION_PLAN.md:137`.
   - `connections.py:318` "All 111 views" → 266 actual.
   - `tools/section_map.py` docstring says `--write` "(re)generates"; it is
     insert-only (refuses existing maps). Either implement refresh or fix the doc;
     also `--check` did not flag server.py's drifted map — verify what check
     actually detects (PLAUSIBLE gap).

## Phase 2 — Registration hardening (the order trap, made checkable)

Context: registration order is encoded in filename alphabetics (sorted globs,
`zz_` prefixes, sort-first naming) plus hand-ordered lists in `connections.py`.
`view_deps` proved the constituency lists are load-bearing (their deps sort
after their consumers — glob registration would break) and found 13
cross-directory edges relying on caller ordering. `swallow_errors=True` turns
any ordering mistake into a silently half-empty page.

1. **New fast-suite test powered by the graph** (import `mcp_server.sql_index` —
   stdlib+duckdb only): (a) every `sql_views/**/*.sql` is reachable by at least
   one production glob/list (closes the ship-dark gap: `test_view_group_registers`
   only iterates the hand-maintained group list); (b) every same-directory edge
   is alphabetically satisfied OR the consumer appears in an explicit ordered
   list; (c) every cross-directory edge is satisfied by the documented caller
   order in `connections.py` builders.
2. **Do NOT rewrite registration to topo-sort yet.** The convention works today
   and is now machine-checked; a topo-sort loader is justified only if (1)
   starts failing regularly. (Matches the house rule: consolidate for
   correctness-critical shared logic, never for tidiness.)
3. **`swallow_errors` becomes declared policy, not per-module judgement.** Today:
   payments/committees/lobbying/interests/SIPO register loud; procurement/
   appointments/etc. soft; the loud-core/soft-enrichment rule is applied within
   two modules but not across the layer (and one loud registration caused a
   recorded prod incident — `interests_data.py:41-45`). Encode a per-glob policy
   table in `connections.py` consumed by every builder.
4. **Consolidate connection builders:**
   - Three SIPO modules each cache their own conn over the identical
     `["sipo_*.sql"]` glob → one shared `sipo_conn()` in core (mirror `housing_conn`).
   - `votes_data.py:28-35` re-hand-rolls the vote substitution dict that
     `connections._member_view_substitutions()` owns → import it.
   - The domain→glob mapping is enumerated twice (per Streamlit module and
     `_API_DOMAIN_GLOBS`) → one `DOMAIN_GLOBS` table in `connections.py`, both
     consumers read it.

## Phase 3 — Query-layer harmonization (idiom dedup; procurement decomposition)

1. **Hoist the 25 identical `_run` shims** into a factory in `queries/__init__`
   (`make_runner(label, log)`); keep `member_overview`'s `conn is None` guard as
   an option.
2. **Shared micro-helpers** for the re-typed idioms: dynamic-WHERE builder
   (generalize `votes._and_clauses` — currently the only module using one),
   the `LIMIT ?` append (~20 sites), the year-scope branch (3 sites), and merge
   the byte-identical ORDER-BY whitelist dicts (`_SUPPLIER_ORDER`==`_RANK_ORDER`).
3. **Graduate correctness-critical embedded SQL into views** (this passes the
   consolidation bar because it centralizes never-sum semantics):
   - The SPENT/COMMITTED `SUM(...) FILTER (WHERE value_safe_to_sum AND
     realisation_tier=…)` fragment repeated 5+× in `procurement.py`.
   - `cross_ref.py`'s two Python-built cross-domain CTEs (votes × interests).
   - `constituency.constituency_housing_context_with_ssha`'s pandas `.merge` in core.
   - The procurement `payments_*` / `afs_*` GROUP BYs (~15 functions, 200-250 lines).
   Each new view: run `view_deps` first; respect the sort-first naming rule.
4. **Split `procurement.py` (1,638 lines) by money grain** — awards / payments /
   TED / AFS / signals — which also localizes the duplication above and aligns
   with the never-sum boundaries.
5. **Legacy names**: `td_vote_summary`, `td_vote_year_summary`,
   `party_vote_breakdown` are the only non-`v_` relations queried. Investigate
   origin (gold parquet stems from the lobbying-style ETL vs legacy view names),
   then rename to `v_*` or document the carve-out. `view_deps` + grep before any
   rename — renames hit the ordering trap.

## Phase 4 — API contract surface (lower priority while Streamlit-first)

1. Uniform response envelope (`serialize.envelope` or a Pydantic `Meta`) so
   `total`/`truncated`/`caveat` appear on every list endpoint (today ~15 of ~45).
2. One pagination convention (defaults today span 20-200; caps 500 vs 2000).
3. Decide Pydantic posture: 3 endpoints carry models today; either widen or
   declare serialization-only as the contract.
4. Document `read_only` as convention-only (in-memory DBs can't be opened
   read-only; only `exports.py` legitimately writes).

## What NOT to do (already investigated and rejected — do not re-derive)

- No CPV/awards mega-base view; no merging the `v_public_payments` /
  `v_procurement_payments` families (deliberate `public_display` gate). See
  memory `project_sql_view_consolidation`.
- No bronze/silver/gold rework; no merging `dossiers.py` with `data_access/`
  (the Streamlit-free firewall is the point — doc/fastapi_query_core_uncoupling_plan.md).
- No topo-sort registration rewrite ahead of the Phase-2 check earning it.
- `sql_queries/` is NOT legacy — it is the live gold producer for lobbying
  (loaded by `lobby_processing.py:888-898`, upstream of `sql_views/`). Its
  ad-hoc loader may be harmonized with a shared runner, but the directory stays.

## Working method for the implementing session

- Every phase independently shippable; gate each on: fast suite green
  (`-m "not integration and not sql and not sources and not bronze"`),
  `tools/check_streamlit_logic_firewall.py`, `tools/check_conventions.py`
  (ratchet: never add to a baseline).
- Scan cheaply: `search_project` (incl. `kind='code'`), `code_outline`,
  `view_deps` (all in `mcp_server/`) before any full-file read.
- PLAUSIBLE findings above require verification before acting; user performs
  all git pushes.
