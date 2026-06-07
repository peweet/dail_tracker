# Iris Oifigiúil — Irish-language notices: value assessment & plan

**Date:** 2026-06-06
**Question:** the `unclassified_other` quarantine pool (14,039 rows) contains ~591 Irish-language
notices the English-only SI regex misses. Are they **net-new value** or **useless duplicates**?
**Method:** profiled the substantive Irish notices (≥60 chars) against the gold SI table
(`statutory_instruments.parquet`, 5,924 rows) by order-family and title-token overlap.

## TL;DR verdict — **essentially no net-new value**

A full "reclassify Irish notices → SI" would **inject duplicates and boilerplate into gold and is
net-harmful.** Every order family the Irish notices announce is already held in one of the project's
**two** SI sources. The only safe action is a free boilerplate cleanup.

> **Two SI tables matter here** — and checking only the first is what created a false "net-new"
> signal mid-analysis:
> - `statutory_instruments.parquet` (5,924) — **Iris-derived** (the `S.I. No.` regex on Iris text). This is what the SI page uses.
> - `si_current_state.parquet` (7,367) — **legislation-directory extract** (`[[project_si_legal_state_c1]]`), broader and authoritative.

| Slice | Count | Value | Why |
|---|---|---|---|
| Pure boilerplate cover notices | **206 (35%)** | **Zero** | "The Government today made Orders entitled as above. Copies … Government Publications." No subject. Just misfiled `publication_admin`. |
| Bilingual announcements of SIs **already in the Iris-derived gold** | majority of the substantive ~379 | **~Zero (duplicate)** | Delegation-of-Functions, Transfer/Alteration-of-Department, Global Valuation all already captured via their English `S.I. No.` notice. The Irish text is the press-style announcement, not a new record. |
| Gaeltacht language-planning orders | ~36–58 | **~Zero (already held)** | Looked net-new vs the *Iris-derived* table (only 3) — but **`si_current_state` already has 26 "language planning" + 63 "gaeltacht" entries.** They're registered SIs the legislation directory already captures. |
| Not SIs at all | a handful | **Out of scope** | Central Bank collective-investment authorisations, Revenue zoned-land-tax consultations, Defence Forces commissioning — different domains. |

**The decisive evidence:** every recurring order family is already in `si_current_state`
(`delegation of ministerial functions` 135, `transfer of departmental administration` 51,
`language planning` 26, `gaeltacht` 63). The English-notice path captures the first families into
the Iris-derived gold; the legislation-directory path captures the Gaeltacht ones. **There is no
family the Irish Iris notices would add.** (The mid-analysis "~36 net-new Gaeltacht" figure was an
artifact of comparing only against `statutory_instruments.parquet` — corrected here.)

## What these notices *are* in reality (decoded)

Translating the recurring Irish titles shows they are overwhelmingly **machinery-of-government
statutory instruments** — the legal paper trail of how executive power is distributed and
reorganised:

| Order type (Irish → English) | What it actually does | In our data? |
|---|---|---|
| `Feidhmeanna Aire a Tharmligean` → **Delegation of Ministerial Functions Order** | Formally delegates specific powers from a Cabinet Minister to a **Minister of State** (junior minister), under the senior Minister's supervision — i.e. *which junior minister legally wields which powers* | `si_current_state`: **135** |
| `Riaracháin Roinne a Aistriú` → **Transfer of Departmental Administration & Ministerial Functions Order** | The legal mechanism for **machinery-of-government reshuffles** — moving functions between departments when government restructures | `si_current_state`: **51** |
| `Ainm na Roinne … a Athrú` → **Alteration of Name of Department & Title of Minister Order** | Renames a department / ministerial title | in `si_current_state` |
| `Limistéir Pleanála Teanga … a Ainmniú` → **Gaeltacht Language Planning Area designation** | Designates Gaeltacht areas under the Gaeltacht Act 2012 | `si_current_state`: 26 |

So the dig did **not** reveal hidden Irish-only data of value. It revealed that these notices belong
to a **machinery-of-government SI category** that is (a) genuinely on-mission for an
executive-accountability tracker, and (b) **already held** in `si_current_state` — just never
surfaced as a distinct, meaningful view. **The value is in the category, not the language.**
Sources: [Justice (Delegation of Ministerial Functions) Order 2023](https://www.irishstatutebook.ie/eli/2023/si/91/made/en/print),
[Integration and Reception (Transfer …) Order 2025](https://www.irishstatutebook.ie/eli/2025/si/159/made/en/html).

## How other countries surface this kind of data

| Tool / source | Relevance |
|---|---|
| **The Gazette (UK, thegazette.co.uk)** | The direct analog to Iris: a national gazette turned into a **structured, searchable civic product** — categorised notices (insolvency, appointments, honours, state) + global search by keyword/company/person/date. **Proof the model works.** Note: it surfaces *personal* insolvency too — the opposite of our deliberate privacy suppression, a conscious and defensible divergence. |
| **Canada Gazette** | Officially **bilingual** (English/French): Part I notices/appointments, Part II enacted regs/SIs, Part III Acts. Shows bilingual gazette publishing is normalised — **but at the official level, not as a third-party civic feature.** No notable civic-tech tool surfaces the second-language version. → supports treating Irish titles as optional polish, not a value driver. |
| **Institute for Government (UK)** | Tracks **machinery-of-government changes** (departments created/abolished, functions moved) as analysis + the Whitehall/Parliamentary Monitor trackers — but there is **no structured per-instrument tracker of delegated ministerial functions**. → a Machinery-of-Government view built from the delegation/transfer SIs would be relatively **novel and on-mission**. |

Sources: [How to search The Gazette](https://www.thegazette.co.uk/all-notices/content/116),
[About the Canada Gazette](https://gazette.gc.ca/cg-gc/lm-sp-eng.html),
[IfG — Machinery of government changes](https://www.instituteforgovernment.org.uk/explainer/machinery-government-changes).

## Irish → English mapping (Iris legal/notice vocabulary)

Built from the actual notice text. Most notices are **bilingual** — the English title sits in
parentheses after the Irish — so this mapping is used mainly to (a) detect the notice *type* and
(b) strip boilerplate, not to translate.

| Irish | English | Signals |
|---|---|---|
| `Ionstraim Reachtúil` / `I.R. Uimh.` | Statutory Instrument / S.I. No. | SI number (rare in these notices) |
| `An tOrdú` / `Ordú` / `Orduithe` / `hOrduithe` | The Order / Order / Orders | SI (Order) |
| `Rialacháin` / `Rialachán` | Regulations / Regulation | SI (Regulation) |
| `Rialacha` | Rules | SI (Rules) |
| `Acht` / `Achtanna` | Act / Acts | parent legislation |
| `Uimh.` | No. (number) | enumerator |
| `Aire` / `an tAire` / `Aire Stáit` | Minister / the Minister / Minister of State | actor |
| `an Roinn` | the Department | actor |
| `Rinne` / `Do rinne an Rialtas inniu` | made / The Government today made | **announcement opener** |
| `dar teideal thuas` / `thuasluaite` | entitled as above / aforementioned | **boilerplate pointer** |
| `Féadfar cóipeanna … a fháil` / `Foilseacháin Rialtais` / `Oifig an tSoláthair` | Copies may be obtained / Government Publications / Stationery Office | **boilerplate colophon** |
| `Feidhmeanna Aire a Tharmligean` | Delegation of Ministerial Functions | order subject (in gold) |
| `Riaracháin Roinne a Aistriú` / `Ainm na Roinne agus Teideal an Aire a Athrú` | Transfer of Departmental Administration / Alteration of Name of Department & Title of Minister | machinery-of-gov order (in gold) |
| `Limistéir Pleanála Teanga Ghaeltachta a Ainmniú` | Designation of Gaeltacht Language Planning Areas | **Gaeltacht order (net-new gap)** |
| `Tháinig i bhfeidhm` | came into force | commencement |
| `Fógra` | Notice | notice |
| `Ceapachán` / `a cheapadh` / `a athcheapadh` | Appointment / to appoint / to reappoint | appointment |
| `Comhairleoir (Speisialta)` | (Special) Adviser | appointment |
| `Banc Ceannais na hÉireann` | Central Bank of Ireland | regulator (not SI) |
| `Scéimeanna Comhinfheistíochta a Údarú` | Authorisation of Collective Investment Schemes | Central Bank notice (not SI) |
| `Cáin ar Thalamh Cónaithe Criosaithe` | Zoned Residential Land Tax | Revenue notice (not SI) |
| `Óglaigh na hÉireann` / `an tUachtarán` | Defence Forces / the President | commissioning (appointment-adjacent) |
| `Fodhlíthe` / `Bye-Laws` | Bye-laws | local authority |
| `Baile Átha Cliath` / `Arna fhoilsiú ag` | Dublin / Published by | boilerplate |

## Plan (value-driven, narrow — NOT a bulk reclassify)

- **P1 — Stop the boilerplate polluting `other`.** Route the 206 "entitled as above / Government
  Publications" cover notices to `publication_admin` (extend the existing `has_admin` rule with the
  Irish markers `DAR TEIDEAL THUAS` / `FOILSEACHÁIN RIALTAIS`). Zero gold impact; cleans the
  quarantine pool. Safe, cheap.
- **P2 (dropped) — no Irish→SI promotion.** The cross-check shows nothing to promote: every family
  is already in `statutory_instruments.parquet` or `si_current_state.parquet`. A promotion path
  would only risk duplicates.
- **P3 (nice-to-have, deferred) — bilingual enrichment.** The genuinely-unique thing the Irish
  notices carry is the **Irish-language title** of orders we already hold in English. Optionally
  add an `si_title_irish` field by joining the bilingual notice to the existing SI on English title.
  Cosmetic / bilingual-completeness only; low priority.
- **Out of scope:** Central Bank / Revenue / Defence notices — separate domains, separately scoped.

## Bottom line

The Irish-language notices are **not a hidden trove of missing SIs**. A third is boilerplate; the
rest are bilingual echoes of SIs the project already holds — via the English Iris notice
(Delegation/Transfer/Valuation) or the legislation directory (Gaeltacht/language-planning, in
`si_current_state`). **Net-new SI recovery ≈ 0.** Recommend P1 (free `publication_admin` cleanup)
now; skip the bulk reclassify entirely; treat bilingual titling (P3) as optional polish.
