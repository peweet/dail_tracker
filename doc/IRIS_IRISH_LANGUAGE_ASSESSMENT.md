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
