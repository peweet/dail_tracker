---
tier: SPEC
status: LIVE
domain: ui
updated: 2026-07-14
read_when: working anywhere under utility/ (the Streamlit presentation layer)
key: SPEC|LIVE|ui
---

# utility/ — the Streamlit presentation layer

> This file was previously a pasted ChatGPT prompt. It is now the navigation map.
> (Its firewall rules survive below — they were the only load-bearing part.)

## ⚠️ Size warning — check before you open

| File | tok | Note |
|---|---:|---|
| `static/dailtracker.css` | **6.6k lines** | Order-dependent shared stylesheet. Search for the selector; do not reorder families. |
| `pages_code/procurement.py` | **59k** | See `pages_code/MANIFEST.md` |
| `ui/components.py` | 17k | The shared component library — **audit it before hand-rolling any HTML** |

## Layout

| Path | What it is |
|---|---|
| `app.py` | Nav registration + routing (`st.Page`). Where a page becomes visible. |
| `pages_code/` | The 27 page renderers. **Read `pages_code/MANIFEST.md`, not the pages.** |
| `data_access/` | The **only** layer a page may call. Framework-neutral cached wrappers over `dail_tracker_core.queries`. |
| `ui/components.py` | Shared editorial components (`hero_banner`, `info_card`, `card_row`, `evidence_heading`, `empty_state`, `totals_strip`, …) |
| `static/dailtracker.css` | The reusable design system. `shared_css.py` is only its Streamlit injection adapter. |
| `../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/` | Legacy Streamlit-specific contracts; reconcile before treating them as live |

## The rules (a checker enforces #1)

1. **Pages render; they do not compute.** No joins, merges, fuzzy matching, KPI maths, parsing or
   parquet reads in `pages_code/`. Allowed: widget state, display-only filtering/sorting of
   already-fetched rows, formatting, CSV export, empty states.
   → `python tools/check_streamlit_logic_firewall.py`
   If you think you need `groupby`/`merge`, **do not implement it** — it belongs in the SQL view.
2. **The domain triple.** To add data, walk **right-to-left**:
   `sql_views/<domain>/X_*.sql` → `dail_tracker_core/queries/X.py` → `data_access/X_data.py` → `pages_code/X.py`
3. **Reuse the component system.** Audit `ui/components.py` + `static/dailtracker.css` *before* writing inline
   HTML. (Hand-rolling cards instead of reusing components is a recorded, repeated mistake.)
4. **Caveats live in `dail_tracker_core/caveats.py`** — the single source. Never inline one.
5. **Restart the server after view/data changes** — Streamlit does not hot-reload by-name imports,
   nor re-run view DDL on a cached connection.

## To do X, open Y

| I want to… | Go to |
|---|---|
| Add or find a page | `app.py` + `pages_code/MANIFEST.md` |
| Add a metric | the SQL view → `queries/` → `data_access/`. **Not a page.** |
| Style something | `static/dailtracker.css` (search by selector). Eight page/component modules also emit local CSS; see the manifest. |
| Reuse a card | `ui/components.py` |
| Check the firewall | `python tools/check_streamlit_logic_firewall.py` |
