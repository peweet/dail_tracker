---
tier: SPEC
status: LIVE
domain: ui
updated: 2026-07-14
read_when: you need to find, change or add a Streamlit page — READ THIS INSTEAD OF OPENING A PAGE
key: SPEC|LIVE|ui
---

# utility/pages_code — page renderers

## ⚠️ READ THIS BEFORE OPENING ANY FILE HERE

**These files are enormous. `procurement.py` alone is ~59,000 tokens — opening it "just to look"
costs more than most tasks are worth.** Use the size column below, then:

1. `Grep` for the section or function you need (all the big pages carry `# ────` banners), **then**
2. `Read` with `offset`/`limit` on just that range.

**Never `Read` a page over ~20k tokens whole.**

## The layering rule (the "domain triple") — learn this once, skip three directories

A domain `X` has the *same file stem* in each layer, and you walk it **right-to-left**:

```
sql_views/<domain>/X_*.sql   →  dail_tracker_core/queries/X.py  →  utility/data_access/X_data.py  →  utility/pages_code/X.py
   (SQL: all joins/aggregation)      (Streamlit-free retrieval)        (thin @st.cache_data wrapper)      (render ONLY)
```

**Pages contain NO business logic** — no `groupby`, no `merge`, no parquet reads, no metric maths.
Enforced by `python tools/check_streamlit_logic_firewall.py`. To add a metric: **start at the SQL
view**, never here.

## The pages

`!` = do not read whole. Sizes are approximate tokens.

| Page | tok | ln | Renders |
|---|---:|---:|---|
| `procurement.py` **!!** | **59k** | 4,444 | Public Procurement explorer over `v_procurement_*`. **Also owns the COUNCIL spending dossier** (running/building/paying lanes) |
| `corporate.py` **!** | 28k | 2,396 | Corporate notices (insolvency/distress) browser. ⚠️ 727 lines of it are page-local CSS |
| `member_overview.py` **!** | 27k | 2,389 | The single-politician accountability record |
| `judiciary.py` **!** | 24k | 1,882 | Courts & Judiciary — bench, appointments, Legal Diary |
| `lobbying_3.py` **!** | 23k | 2,176 | Lobbying register (3-stage: landing → org → cross-views) |
| `statutory_instruments.py` **!** | 19k | 1,498 | SI browser |
| `public_appointments.py` | 16k | 1,428 | State-board / public appointments browser |
| `public_payments.py` | 13k | 965 | Public-body payments over `v_public_payments_*` |
| `election_2024.py` | 12k | 1,033 | Election-finance hub (⚠️ never-sum across sources) |
| `legislation.py` | 11k | 1,032 | Bills / legislation |
| `committees.py` | 10k | 898 | Committee register (committee-first two-stage) |
| `constituency.py` | 9k | 867 | Per-constituency civic dossier ("Your Area") |
| `local_government.py` | 9k | 823 | **Who Runs Your County** — the unelected executive layer (CE, NOAC, derelict, audit) |
| `ministerial_diaries.py` | 7k | 635 | Who Ministers Meet |
| `follow_the_money.py` | 7k | 575 | Guided trail: public body → supplier |
| `votes.py` | 6k | 587 | Dáil/Seanad divisions |
| `your_council.py` | 6k | 510 | ONE consolidated dossier per local authority (`?yc=` deep-links) |
| `your_councillors.py` | 6k | 494 | Who represents you on your council (roster, votes, pay, plan overrides) |
| `glossary.py` | 6k | 289 | Acronym / term reference |
| `siting_check.py` | 6k | 466 | What planning issues does a site trigger? |
| `company.py` | 5k | 417 | One firm's full public-money footprint |
| `attendance.py` | 5k | 426 | Attendance & Participation ("Showing up") |
| `payments.py` | 5k | 431 | TD payments |
| `housing.py` | 4k | 431 | National social-housing waiting list |
| `what_they_own.py` | 4k | 343 | Register of Members' Interests |
| `accommodation_spend.py` | 2k | 244 | What the State pays accommodation providers |
| `council_spending.py` | 1k | 108 | Council spending directory (thin shell over `procurement.py`) |

## ⚠️ CSS is split-brain — check both places

Most pages use `utility/shared_css.py` (~72k tokens — has a SECTION MAP header; jump, don't read).
But **five pages inject their own CSS**, so a rule may live in either:

| Page | Local CSS injector |
|---|---|
| `corporate.py` | `_inject_corp_css()` — **727 lines** (`.con-*` *also* exists in shared_css) |
| `judiciary.py` | `_inject_jd_css()` — 218 lines |
| `statutory_instruments.py` | `_inject_si_css()` |
| `public_appointments.py` | `_inject_pa_css()` |
| `siting_check.py` | `_css()` |

## To do X, open Y

| I want to… | Go to |
|---|---|
| Add a metric / change a number | the **SQL view** first → `queries/` → `_data.py`. **Not here.** |
| Find which page shows dataset D | `Grep "v_D"` in `utility/data_access/` — not here |
| Know where a page gets its data | same stem: `pages_code/X.py` ← `data_access/X_data.py` |
| Change a caveat / disclaimer | `dail_tracker_core/caveats.py` — the single source. Never inline it in a page. |
| Style something | `utility/shared_css.py` (jump via its section map) — but check the 5 local injectors above |
| Reuse a card / component | `utility/ui/components.py` (~17k tok) — audit it **before** hand-rolling HTML |
| Check I haven't broken the firewall | `python tools/check_streamlit_logic_firewall.py` |
