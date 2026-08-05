# South Dublin planning weekly-list fallback

**Verified 2026-08-05.** South Dublin County Council's official Search and View page links
planning applications from 2000 onward to the Agile Citizen Portal and separately links its
hosted weekly lists. The council's Online Planning page also identifies the Agile site as its
Citizen Portal.

## Source map

- Official Search and View: `https://www.sdcc.ie/en/services/planning-building-control/planning-applications/search-and-view/`
- Agile Citizen Portal: `https://planning.agileapplications.ie/southdublin/search-applications/`
- Weekly-list index: `https://planning.southdublin.ie/Home/WeeklyLists`
- Hosted decisions archive: `https://planning.southdublin.ie/`

## Adapter finding

`planning/product/core/decision_docs.py:list_documents()` resolves current South Dublin
references through Agile when input whitespace is stripped. The public document endpoint may
then return an explicit empty list. This means **application found; no public document metadata
returned**, not that no council file exists.

The weekly-list host publishes current *Applications Received* PDFs. Its PDF rows include
personal data and free-text descriptions, so `fetch_south_dublin_weekly_applications()` retains
only: planning reference, received date, application type, submission type, and official source
URL. It intentionally does not bulk-download the archive, infer an outcome, or retain applicant,
address, or proposal text.

## Reproduction and guardrails

- Run `fetch_south_dublin_weekly_applications(max_lists=4)` for a bounded current slice.
- Keep Agile scanned-file state and weekly-list state separate in UI/reporting.
- A blank Agile status or empty document list is an explicit unavailable/evidence-gap state, not a
  validity, grant/refuse, or file-absence conclusion.
- The source is public but must never become a people or premises search.
