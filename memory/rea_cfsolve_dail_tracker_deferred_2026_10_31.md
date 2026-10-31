---
description: Deferred Dail Tracker uses for REA and cfsolve; current tool work is confined to SpecPlan.
updated: 2026-10-31
---

# REA / cfsolve - Dail Tracker restart note

## User direction, 2026-10-31

Record these ideas for the return to Dail Tracker. For now, confine further REA /
cfsolve work to SpecPlan. This is the current scope of this tool investigation;
a later explicit user instruction can change it. Do not automatically resume the
public-data trials or expand Dail Tracker ingestion from this queue.

The private SpecPlan restart entry is
`planning/product/claude/memory/reference_rea_cfsolve_scope_2026_10_31.md`.

## Deferred candidates

1. Central Bank enforcement notices: trace the browser requests supplying the
   document list, filters and pagination. Source:
   https://www.centralbank.ie/news-media/legal-notices/enforcement-actions
   Project candidate: `doc/ROADMAP_SOURCES.md`.
2. Council financial libraries: Carlow, Cavan and Roscommon are marked
   `NEEDS-RENDER` in `extractors/procurement_la_payments_extract.py:611-643`.
   Recheck ordinary HTML first: Cavan year links were visible in the retrieved
   page during this session, so the old labels are not proof of a current block.
3. Public-body board minutes: revisit the historically WAF-blocked IDA,
   Gas Networks Ireland and other entries in
   `doc/PUBLISHED_MINUTES_AND_CAPITAL_PIPELINE_REGISTER.md:69-70`.
   Publication and present access remain source-specific open questions.
4. Broken collectors after redesigns: compare actual browser requests and script
   changes to recover document URLs, filters and pagination.
5. Separate optional REA use: inspect client-side software features/interfaces or
   file handling. Native analysis requires a suitable provider; none was added.

## Dated trial evidence - observations, not general reliability claims

- PPR: current-path downloads succeeded 3/3; median download 11.299 seconds;
  814,413 CSV records. No Cloudflare challenge was encountered.
- SEAI publications: ordinary Windows requests and same-UA Linux curl received
  Cloudflare challenges. Four cfsolve diagnostic captures contained identical
  real publications HTML, while every solver CLI invocation exited 1 with
  no Turnstile container and no clearance token.
- REA 6.8.0 parsed three retained first-party scripts in 84.054 seconds with
  no syntax failures. Semantic coverage was partial. No financial-report API
  was identified in those three scripts; application-route coverage is unknown.
- Reports and reproducible scripts are laptop-local, ignored artifacts:
  `.cache/cfsolve-ppr-trial-20261031/SUMMARY.md` and
  `.cache/rea-combined-trial-20261031/SUMMARY.md`.
  The compact observations above remain useful if those caches disappear.
- cfsolve source was pinned to `98240dd73f1a6cdffbba241d94c2f82521d679c9`.
  The REA trial used isolated Node 24.22.1. No collector, production data,
  deployment, MCP server or agent registration was changed by the trials.

## Resume method

When the user returns to Dail Tracker, choose one candidate and confirm its
current ordinary-fetch behavior. Ask one concrete question of live browser /
network observation before broad static bundle analysis. Add cfsolve only for a
reproduced relevant access problem. A useful captured page is not evidence of a
successful token solve, a complete API map, or production readiness.

Source: user instruction and measured artifacts from session
`01a1920e-9137-7283-b6d0-1e99ff67f11f`, 2026-10-31.
