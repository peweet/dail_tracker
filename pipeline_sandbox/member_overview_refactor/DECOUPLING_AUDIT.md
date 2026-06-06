# member_overview — decoupling audit (sandbox refactor)

Scope: extract the page's business/retrieval layer into `dail_tracker_core` so the
page becomes a thin render layer, **without touching the originals** until the
sandbox version is parity-validated. Originals (committed, clean):
`utility/pages_code/member_overview.py`, `utility/data_access/member_overview_data.py`.

## What is "business data" here vs rendering

The page mixes two concerns that the other 12 migrations kept separate:

1. **Connection build** — `member_overview_data.py::get_member_overview_conn`.
   A bespoke **4-phase** registration (NOT a glob): `_DOMAIN_FILES` (~25 files in
   explicit dependency order, no substitution, per-file swallow) + `_REGISTRY_FILES`
   ({MEMBER_PARQUET_PATH},{SEANAD_MEMBER_PARQUET_PATH}) + `_EXTERNAL_LINKS_FILES`
   ({EXTERNAL_LINKS_PARQUET_PATH}) + `_VOTE_FILES` ({PARQUET_PATH},{SEANAD_VOTE_PARQUET_PATH}).

2. **Retrieval SQL** — **25 functions / 18 views live in the PAGE FILE itself**
   (the `_q` helper + 24 `@st.cache_data` fns, lines ~90–498). This is the part the
   other pages put in their data-access module. This is the decoupling target.

Everything below line ~500 (the `_section_*` / `_render_*` renderers, ~1400 lines)
is **pure rendering** — HTML/Streamlit, no SQL. It stays in the page untouched.

## Seam: retrieval (→ core) vs shaping (→ stays in page wrapper)

Each page `_xxx(_conn, …)` splits into a core retrieval fn returning `QueryResult`
plus thin shaping that stays in the page wrapper (SAME name+signature, so render
call-sites are unchanged):

| page fn | core retrieval | shaping kept in page wrapper |
|---|---|---|
| `_q` | `_run` (→ QueryResult) | — (removed) |
| `_member_list` | `member_list` | `.data` |
| `_join_key_by_name` | `join_key_by_name` (2 branches) | `str(iloc0) if not empty else None` |
| `_member_house` | `member_house` | `str(iloc0) or "Dáil"` |
| `_identity` | `identity_attendance` + `identity_registry` | attendance-first fallback + `.to_dict()` |
| `_att_all_years` | `att_all_years` | `.data` |
| `_att_rank_for_year` | `att_rank` + `att_rank_total` | compute `(rank, total)` tuple |
| `_external_links` | `external_links` | drop-null dict comprehension |
| `_votes_summary` | `votes_summary` | `.data` |
| `_pay_overview` | `pay_overview` | `.data` |
| `_pay_grand_total` | `pay_grand_total` (SUM row) | `float` + NaN/empty guard |
| `_lobbying_rd` | `lobbying_rd` | `.data` |
| `_legislation` | `legislation` | `.data` |
| `_si_signed` | `si_signed` | `.data` |
| `_ministerial_roles` | `ministerial_roles` | `.data` |
| `_constituency_context` | `constituency_context` | empty-name guard + `.to_dict()` |
| `_q_profile` | `question_profile` | `.to_dict()` else `{}` |
| `_q_focus_shift` | `question_focus_shift` | `.to_dict()` else `{}` |
| `_q_years` | `question_years` | `[int(y) …]` |
| `_q_ministries` | `question_ministries` | `astype(str).tolist()` |
| `_q_top_topics` | `question_top_topics` | `.data` |
| `_q_feed` | `question_feed` (dynamic WHERE) | `.data` |
| `_debate_years` | `debate_years` | `[int(y) …]` |
| `_debate_topics` | `debate_topics` (dynamic) | `[str(t) …]` |
| `_debate_sections` | `debate_sections` (dynamic) | `.data` |

## Behaviour-preservation notes (the traps)

- **Swallow → QueryResult.** Old `_q` returned an empty DataFrame on ANY error
  (and on `conn is None`). Core `_run` returns `unavailable`; the page wrapper reads
  `.data` (empty on unavailable) → same no-crash behaviour, but the 3-state is now
  available. `conn is None` is also mapped to `unavailable`.
- **SUM NaN trap.** `_pay_grand_total` guards `pd.isna` already — preserved in the
  wrapper (`.df()` yields NaN for a NULL SUM, unlike a tuple `None`).
- **Cross-page render lifts are orthogonal.** The page also calls
  `render_member_{attendance,lobbying,payments,interests,votes,committees}` — those
  use their OWN (already-migrated) data-access conns, NOT this conn. Untouched.
- **identity_resolver** imports `get_member_overview_conn`; the rebuilt builder keeps
  the same name + return type, so the resolver is unaffected.
- **`test_member_overview_connection_builds`** imports `_DOMAIN_FILES` etc. BY NAME —
  the rebuilt builder keeps those module-level lists, so the test still passes.

## Validation results (2026-06-06 — sandbox build complete)

- **Query parity: 121/121** — original page fns vs refactored sandbox page fns, on
  the SAME live connection, across 5 member archetypes (any / questions / minister /
  payments / debates) × all 24 query helpers + `_member_list`. Byte-identical
  (order-insensitive multiset compare on frames; repr-compare on dict/list/scalar).
- **Conn-builder parity: identical 22-view set** — the rebuilt `register_views`-based
  builder registers exactly the same views as the original `_load_sql` builder
  (`information_schema` diff = ∅ both directions).
- **Core tests:** `test/test_core_member_overview_queries.py` (8) + full core suite
  **107 pass**. Firewall guard scans the new core module (Streamlit-free ✓).
- **basedpyright:** 0 errors on `dail_tracker_core/queries/member_overview.py`.
- **Firewall checker:** 33 utility pages clean (originals untouched).
- **UI/API compliance (sandbox page):** 0 `unsafe_allow_html`, 0 `use_container_width`,
  0 `st.radio`, 0 `var(--surface)`, 0 `st.markdown` (all `st.html`), no inline styles
  except the dynamic party-colour swatch (accepted). Render code untouched, so the
  prior civic-ui audit (subsection_heading nesting, walker house-scoping, etc., already
  committed) is preserved. Removed dead `_log`/`logging` left over from deleting `_q`.

## ⚠️ Pre-existing finding surfaced (NOT fixed — would change behaviour)

`v_attendance_year_rank` is **not registered on the member-overview connection**
(absent from `_DOMAIN_FILES`), so `_att_rank_for_year` always returns `(None, None)`
and the hero "Rank X of Y TDs" sub-label silently never renders on this page. Old `_q`
swallowed the Catalog Error into an empty frame; new `_run` returns `unavailable` (now
visible in logs). Both behave identically → parity holds. **Fix is a one-line add of
`attendance_year_rank.sql` to `_DOMAIN_FILES`** — but it's a behaviour change (the
sub-label would start appearing), so it belongs in a separate follow-up, not this
behaviour-preserving refactor.

## Integration (after greenlight)
1. `dail_tracker_core/queries/member_overview.py` → already a real core file (shipped, tested).
2. Copy sandbox `member_overview_data.py` over `utility/data_access/member_overview_data.py`
   (drop the sandbox `_REPO/_UTIL` bootstrap back to the original `parent` one-liner).
3. Copy sandbox `member_overview.py` over `utility/pages_code/member_overview.py`
   (restore the original `_UTIL = parent.parent` bootstrap; keep the `moq` import).
4. Run: core tests, firewall, basedpyright, `test_member_overview_connection_builds`,
   `test_seanad_views`, fresh-server visual check. Then delete this sandbox dir.
5. Optional follow-up: the `v_attendance_year_rank` finding above.
