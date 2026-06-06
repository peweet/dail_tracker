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

## Integration (after greenlight)
1. `dail_tracker_core/queries/member_overview.py` → already lands in core (real file).
2. Copy sandbox `member_overview_data.py` over `utility/data_access/member_overview_data.py`.
3. Copy sandbox `member_overview.py` over `utility/pages_code/member_overview.py`.
4. Run: core tests, firewall, basedpyright, `test_member_overview_connection_builds`,
   `test_seanad_views`, fresh-server visual check. Then delete the sandbox dir.
