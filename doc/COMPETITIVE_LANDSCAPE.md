# Competitive Landscape & Source Reference

**Snapshot date:** 2026-05-06
**Purpose:** Reference for future strategic decisions. Lists every Irish civic-tech project, dataset, and journalism outlet that touches the same data Dáil Tracker covers, with a note on overlap and uniqueness. Refresh annually — civic-tech changes slowly but it does change.

> Heuristic for adding entries here: if a journalist might reasonably ask "isn't this just X?", X belongs in this document.

---

## Headline finding

- **Source-by-source:** active competitors exist for votes/debates, lobbying, and payments.
- **Cross-source joins:** unoccupied. Iris Oifigiúil, CRO, charities-register, and Register of Members' Interests as a *structured dataset* are wide open.
- **Unique product surface:** the unified per-TD profile combining all sources is, as of this snapshot, the only one in existence.

---

## Direct competitors (overlapping product)

### KildareStreet.com — votes, debates, member pages
- **URL:** https://www.kildarestreet.com/
- **TD profile example:** https://www.kildarestreet.com/td/cathal_berry/kildare_south
- **Built by:** mySociety (TheyWorkForYou codebase, Irish branch)
- **Covers:** Dáil and Seanad debate transcripts, voting records, committee memberships, parliamentary speeches with linguistic / readability analysis, current and former members.
- **Does not cover:** lobbying register, Register of Members' Interests, payments / PSA, CRO companies, charities register, Iris Oifigiúil.
- **Maturity:** ~20 years, reliable, citable, well-known among Irish journalists.
- **Strategic note:** **Do not try to win on votes/debates surface.** Treat their work as table-stakes; integrate-by-link rather than recreate. Possibly reach out to mySociety for collaboration rather than competition.

### Lobbyieng.com — lobbying register dashboard
- **URL:** https://www.lobbyieng.com/
- **Built by:** Rob McElhinney
- **Covers:** lobbying.ie returns analysis. As of Jan–Apr 2026 snapshot: 108,985 returns, 4,047 officials, 2,873 lobbyists. Six features: Explore Insights, Find a TD, Browse Officials, Browse Lobbyists, Compare Officials, Data & Limitations.
- **Does not cover:** declared interests, CRO, Iris Oifigiúil, votes, payments.
- **Maturity:** production-quality, currently maintained, free.
- **Strategic note:** **Do not try to win on standalone lobbying dashboard.** This competitor is good and active. Differentiate by *cross-referencing* lobbying with declared interests + CRO + Iris notices on a single profile — that is what they do not do.

### Gript Datahub — TD payments tracker
- **URL:** https://data.gript.ie/tds
- **Built by:** Gary Kavanagh (Gript)
- **Covers:** Parliamentary Standard Allowance (PSA = TAA + PRA) per TD since 2019. Filterable by Dáil session, party, constituency, year. Cumulative since 2019: €28.7M; 2023 alone: €5.95M.
- **Does not cover:** secretarial allowances, committee payments, party whip supplements, ministerial payments, lobbying, declared interests, votes, CRO.
- **Maturity:** solid, active, public-records + FOI sourced.
- **Strategic note:** **Don't try to win on payments standalone.** Their historical depth is greater than yours. Differentiate by linking payments to the same TD's declared interests, lobbying contacts, and CRO directorships.

---

## Adjacent / partial overlap

### Oireachtas Connect
- **URL:** https://oireachtasconnect.ie/
- **Tagline:** "Email Your TD" — markets coverage of social, economic, and lobbying metrics.
- **Coverage:** unverified at snapshot date (page content was not retrievable). Re-check periodically.
- **Strategic note:** monitor — could be a partner or a competitor depending on direction.

### Oireachtas Explorer (unofficial)
- **URL:** https://oireachtas-explorer.ie/
- **Covers:** members, voting records, speeches, debates. Unofficial wrapper around the Oireachtas Open Data PSI Licence.
- **Does not cover:** lobbying, interests, payments, CRO, Iris.
- **Strategic note:** narrow scope, lower visibility than KildareStreet. Not currently a major threat.

### ronan-mch/lobbying (GitHub)
- **URL:** https://github.com/ronan-mch/lobbying
- **What it is:** dormant Ruby research scripts for downloading/normalising lobbying register into SQLite. 2 commits, no releases.
- **Strategic note:** abandoned. Useful as prior-art reference, not a live competitor.

### Better Regulation — paywalled Iris aggregator
- **URL:** https://service.betterregulation.com/document/624706
- **What it is:** commercial regulatory-publications aggregator that includes Iris Oifigiúil curation. Paywalled / login-gated.
- **Strategic note:** they target compliance professionals, not journalists. Pricing structure makes them irrelevant to Dáil Tracker's audience.

---

## Official sources (the data Dáil Tracker ingests)

| Source | URL | Format | Notes |
|---|---|---|---|
| Oireachtas Open Data API | https://api.oireachtas.ie/ | JSON | Members, debates, votes, questions, legislation. PSI Licence. |
| Oireachtas Open Data policy | https://www.oireachtas.ie/en/open-data/ | — | Licensing terms. |
| Find a Vote (official) | https://www.oireachtas.ie/en/debates/votes/ | HTML | Division records. |
| Constituency dashboards (official) | https://www.oireachtas.ie/en/constituency-dashboards/ | HTML | Demographic / constituency-level. Not member-level. |
| Register of Members' Interests | https://www.oireachtas.ie/en/members/register-of-members-interests/ | PDF | Annual declarations. |
| Lobbying.ie register | https://www.lobbying.ie/ | CSV / Excel | Returns + officials + lobbyists. |
| Companies Registration Office | https://cro.ie/ | Search + bulk export | Directorships, ownership, address history. |
| Iris Oifigiúil official site | https://www.irisoifigiuil.ie/ | HTML + PDF (presentation only) | Twice-weekly state gazette. No structured download. |

---

## Journalism that uses this data (proven demand)

These outlets do the cross-reference work *manually* today. Each story below is evidence that the workflow Dáil Tracker automates is already valuable.

### The Ditch
- **URL:** https://www.ontheditch.com/
- **Wikipedia:** https://en.wikipedia.org/wiki/The_Ditch_(website)
- **Founded:** 2021 (McNeill, Shortall, Bowes, Cosgrave). Receives funding from Web Summit.
- **Track record:** broke the Robert Troy and Damien English stories, leading to two ministerial resignations. Both stories turned on undeclared property/business interests cross-referenced manually against the Dáil register and CRO.
- **SIPO follow-up on Troy:** https://www.thejournal.ie/sipo-investigation-hearing-robert-troy-6379415-May2024/
- **Strategic note:** **highest-priority outreach target.** They are doing the work Dáil Tracker would automate. A single case study in their feed is worth more than 1,000 social shares.

### TheJournal.ie / Noteworthy
- **TDs with undeclared rental income (2022):** https://www.thejournal.ie/td-register-interests-land-property-rental-foley-5864805-Sep2022/
- **Strategic note:** longer reach than The Ditch. More cautious editorially. Approach after a clean dataset is shipped.

### Radio Kerry
- **Healy-Rae undeclared CRO directorship:** https://www.radiokerry.ie/insights/dail-register-members-interests-omission-kerry-td-214384
- **Strategic note:** local newsrooms are exactly the audience for one-click cross-reference tooling — they don't have data teams.

---

## Comparable tooling outside Ireland (prior art / inspiration)

| Project | URL | Relevance |
|---|---|---|
| TheyWorkForYou (UK) | https://www.theyworkforyou.com/ | Parent project of KildareStreet; design and tone reference. |
| simonw/register-of-members-interests-datasette | https://github.com/simonw/register-of-members-interests-datasette | UK MP financial interests as structured Datasette. Pattern worth copying for Ireland. |
| OpenDataServices/oroi-scrape | https://github.com/OpenDataServices/oroi-scrape | Multi-jurisdiction scraper for politicians' interest declarations. |
| OpenKamer (NL) | https://github.com/openkamer/openkamer | Parliamentary scraper, listed as inspiration in README. |
| How They Vote EU | https://howtheyvote.eu/ | Cross-source joins at EU level — closest spiritual analogue. |
| OireachtasAPI (Python) | https://github.com/Irishsmurf/OireachtasAPI | Python wrapper for the Oireachtas API. Worth checking against Dáil Tracker's own API client for parity. |

---

## Summary: where Dáil Tracker is unique

| Capability | Competition? | Position |
|---|---|---|
| Votes / debates / speeches | KildareStreet | Behind. Don't compete. |
| Lobbying dashboard standalone | Lobbyieng | Behind. Don't compete. |
| Payments tracker standalone | Gript Datahub | Behind. Don't compete. |
| Register of Members' Interests as **structured data** | None | **Open.** |
| CRO ↔ declared interests cross-reference | None (journalists do it by hand) | **Open.** |
| Iris Oifigiúil parser | None open-source; Better Regulation is paywalled | **Open.** |
| Charities register cross-reference | None | **Open.** |
| Unified per-TD profile across all sources | None | **Open. Highest-value surface.** |
| Constituency-level dashboard | Oireachtas itself | Don't compete. |

---

## How to use this document

- **When asked "isn't this just X?":** look it up here and have a one-line answer ready.
- **When designing a new feature:** check the unique-vs-occupied column. Build into the open columns.
- **When pitching a journalist:** quote the proven-demand evidence (Ditch / Journal / Radio Kerry stories).
- **When evaluating a fork or competitor:** add them here. Update the "Position" column for any row they overlap.

## Refresh schedule

Re-verify every entry every 6 months. Mark obvious changes (new launches, abandoned projects, paywall changes) with a dated note rather than overwriting history.
