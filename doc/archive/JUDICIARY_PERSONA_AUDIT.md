# Courts & Judiciary — Persona Surfacing Audit

**Date:** 2026-06-11 · **Page:** `utility/pages_code/judiciary.py` (`?page=rankings-judiciary`)
**Method:** code-grounded walkthrough of every render path against the gold parquets and
SQL views, plus data probes (`c:/tmp/probe_judge_diary_join*.py`). Live re-verification on a
fresh Streamlit server happens after the rebuild (Phase 4 of this effort).

Three personas, each with the questions they bring to the page. Verdicts:
**✅ at a glance** · **🟡 answered after digging** · **✗D data exists, not surfaced** ·
**✗S not answerable, source/privacy limit**.

---

## Persona 1 — Devilling barrister / junior counsel

| Question | Verdict | Detail |
|---|---|---|
| Which judge, courtroom, time am I before tomorrow? | ✅ | Legal Diary → "Today on the bench" cards carry court/courtroom/judge/time/list. |
| How long is the list (how many matters before mine)? | ✅ | `n_items` chip on each sitting card; "Most active lists" ranking. |
| What's listed before Judge X this week (across days)? | **✗D** | The diary is day-locked. `judicial_legal_diary_cases.parquet` carries `judge` on 1,737/2,376 rows but **no view joins it to the roster**, so a judge's profile shows their 2017 appointment and nothing about their current lists. Root cause of "the appeals page for each judge has no plaintiff data": Court of Appeal has 294 + 206 (criminal) case rows with plaintiff/defendant — none reachable from a judge profile. |
| What kind of work does this judge do (specialist list)? | 🟡 | `assignment` renders on the profile, but with no diary evidence beside it. |
| Status of each matter (for mention / for hearing / judgment)? | 🟡 | `status` is in the cases view but is **not rendered** in the case rows — only party names + category chip. |
| Courtroom + time on the actual case rows? | **✗D** | Cases section drops courtroom/time (they live in the schedule view); a barrister must cross-reference two sections. |

**Join feasibility (probed):** diary names are surname-only ("Ms Justice Butler") vs roster
forename+surname keys — exact `judge_key` match covers only 42/64 distinct diary judges.
Surname matching **scoped by court** resolves most of the rest; 28 bench-wide surname
collisions mostly split across courts. Two hard cases get refused, not guessed:
"Mr Justice Mcdonald" vs "Ms Justice Mcdonald" (honorific conflict on one surname) and
"The President" (office, not name — roster doesn't identify court presidents).

## Persona 2 — Solicitor (client expectations)

| Question | Verdict | Detail |
|---|---|---|
| Realistic wait for my matter / venue? | 🟡→**✗D** | Waiting cards render, but the labels lost their report-section context at extraction: "Full hearing" appears 4×, "Appeals" 2×, "Pre-leave/Post-leave" 2× with **no court attached** (only an unrendered `page` number). As shown, a solicitor cannot tell Supreme Court appeals (22 wk) from Circuit appeals (10 wk). Fix: curated section map (`data/_meta`), not guesswork. |
| Circuit vs High Court — backlog context? | ✅ | Clearance bars + per-area drilldown + trend chart are strong. |
| Who is suing — funds/banks/State concentration? | **✗D** | "Who's bringing these cases" exists but only inside ONE diary day. The accountability signal (repeat institutional plaintiffs) needs the full multi-day corpus — data exists across 5 days and accumulates daily. |
| Which judge runs the relevant specialist list this term? | 🟡 | Buried: profile-only `assignment`; not browsable as "lists → judges". |

## Persona 3 — Courts journalist

| Question | Verdict | Detail |
|---|---|---|
| Who appointed this judge; prior career; vacancy cause? | ✅ | Profile career arc + gov.ie vacancy lifecycle is the page's strength. |
| Elevation patterns / appointing authority split? | ✅ | Appointments & Government tab. |
| What's before Judge X this week, with parties? | **✗D** | Same missing judge↔diary join as Persona 1. |
| Which institutions dominate as plaintiffs over time? | **✗D** | Same day-locked plaintiff breakdown as Persona 2. |
| Outcomes, counsel, case refs, per-judge caseload stats | **✗S** | Privacy/source walls (by design): diary is forward-looking, counsel stripped, no per-judge performance metrics. Not gaps to fix. |

---

## Gap → fix mapping (Phases 2–3)

| Gap | Fix | Layer |
|---|---|---|
| Judge profile has no diary/party data | `extractors/judiciary_diary_link.py` → `judiciary_diary_judge_map.parquet` (cleaned-string + surname-court matcher, honorific-conflict guard) + `v_judiciary_judge_diary`, `v_judiciary_judge_sittings`; "Before the court" profile section | pipeline + UI |
| Plaintiff signal day-locked | `v_judiciary_plaintiff_league` (orgs/State only, GROUPING SETS overall + per-court) + "Who is suing" section on Legal Diary tab | pipeline + UI |
| Waiting times ambiguous ("Full hearing" ×4) | curated `data/_meta/courts_waiting_context.csv` (page+seq → jurisdiction & list context, label-verified at build) → `jurisdiction`/`list_context` cols in gold + grouped matter-first cards | pipeline + UI |
| `status` not on case rows | render `status` chip in `_case_party_html` | UI |
| Courtroom/time missing from case section | joined in `v_judiciary_judge_sittings`; diary cases keep schedule cross-ref via judge map | pipeline + UI |

**Privacy walls honoured:** map publishes nothing new — it links already-public officials to
already-anonymised listings. No counts framed as judge workload/performance; league names
organisations and State bodies only; in-camera categories remain dropped.

---

## After-state (shipped 2026-06-11, verified on fresh server :8631 via `audit_screenshots/_jud_persona_verify.py`)

| Gap | Status |
|---|---|
| Judge profile has no diary/party data | ✅ SHIPPED — `extractors/judiciary_diary_link.py` map (68/72 judge-court pairs; row-level **cases 1,720/1,736 = 99.1%**, schedule 406/423 = 96%); honorific-conflict guard withdrew both McDonald entries rather than guess; `v_judiciary_judge_diary` + `v_judiciary_judge_sittings`; "Before the court" section on the profile (sittings with courtroom/time/list + anonymised matters, newest day first, honest empty state + surname-match transparency note). |
| Plaintiff signal day-locked | ✅ SHIPPED — `v_judiciary_plaintiff_league` (GROUPING SETS overall + per-court; mechanical canonicalisation merges Pepper/Mars suffix+case variants; mixed initials-led sides excluded); "Who is suing — across all captured days" section with court pills. |
| Waiting times ambiguous | ✅ SHIPPED — `data/_meta/courts_waiting_context.csv` transcribed from AR 2024 pp.135–140 (label drift-guard at build); gold gains jurisdiction/list_context; **+3 Central Criminal Court rows the regex extraction missed** (Priority/Custody 44 wk, Bail 49 wk, Juvenile 24 wk); section reworked court→list grouped; the 2 mojibake rows (Jury 17 wk, Common-law motions 13 wk) now render under their curated list names instead of being hidden. |
| `status` not on case rows | ✅ SHIPPED — status chip in `_case_party_html` (profile + diary tab). |
| Courtroom/time missing | ✅ SHIPPED — on profile sitting lines via `v_judiciary_judge_sittings`. |

**Bonus privacy fix found during the work:** "The Child **&** Family Agency" bypassed the
in-camera PROTECTED_KEYS drop (keys match "child and family", not the ampersand form) — 3
Tusla rows were in committed gold. Fixed (`protected_reason` normalises `&`→`and`, parser
1.1.1), regression-tested, gold rebuilt (2,376 → 2,373 kept; 626 → 629 dropped).

**Known residue (deliberate):**
- Diary list-codes glued to plaintiff names ("SPMARS CAPITAL…") stay separate league rows —
  upstream `strip_refs` fix, inconsistent prefix lengths make blind stripping unsafe.
- "The President" / "Ms Justice Egan (not on roster snapshot)" stay unmatched (honest).
- Tier A list_type occasionally carries practice-direction text verbatim (parse noise, no
  party data) — visible on some profile sitting lines.
- Outcomes / counsel / case refs / per-judge caseload remain out of scope (privacy/source walls).
