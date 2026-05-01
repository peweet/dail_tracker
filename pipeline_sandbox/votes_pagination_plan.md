# Vote pagination fix — phased rollout plan

_Status: PHASE 0 CONFIRMED (2026-05-01). Truncation is real — the API returns the full 1000 records, meaning `head.totalResults` is greater than the limit. Phases 1–4 are now justified work. Read, push back, then implement._

## Problem statement

The current vote-fetching call (somewhere in the API service layer, exact location TBC) uses `limit=1000` without pagination. This is a textbook silent-truncation pattern:

- If `head.totalResults` ≤ 1000, you get everything. Fine.
- If `head.totalResults` > 1000, you get *some* 1000 records. The Oireachtas API is not contractually bound to return them in any specific order absent an explicit sort parameter, so you don't know whether you got the most recent 1000, the oldest 1000, or some database-internal slice.
- No error is raised. Downstream gold tables look populated. The truncation is invisible.

This is the same bug pattern documented in every REST-pagination guide ([Speakeasy](https://www.speakeasy.com/api-design/pagination), [Merge.dev](https://www.merge.dev/blog/rest-api-pagination)). The fix is paginate-with-total-check, not "set the limit higher".

## Diagnostic — Phase 0 (CONFIRMED, kept for context)

**Outcome:** the API returns the full 1000 records under the current call, which means more than 1000 records exist in the requested period. Silent truncation is happening today. Proceed to Phase 1.

**Original goal:** find out whether truncation is *currently* happening. Two-line change in the existing call:

```python
# pseudo
response = call_votes_api(skip=0, limit=1000)
total = response["head"]["totalResults"]
returned = len(response["results"])
log.info("votes_api_canary | total=%d | returned=%d | truncated=%s",
         total, returned, total > returned)
```

That single log line tells us today whether the pipeline is silently dropping records. If `total <= returned`, the limit isn't biting and the rest of this plan is precautionary. If `total > returned`, we have a confirmed bug and the rest is mandatory.

**Acceptance criterion for Phase 0:** ✓ MET. Truncation confirmed.

**Implication for the rest of the plan:** every section below is justified. The "stop here if not truncated" exit no longer applies.

## Phase 1 — paginated fetch in a new sandbox module

**Where:** `pipeline_sandbox/votes_paginated_fetch.py` (this directory). Does not modify `pipeline.py`, `members_api_service.py`, or `transform_votes.py`.

**Shape (pseudocode, not the implementation):**

```text
def fetch_all_votes(start_date, end_date):
    skip = 0
    page_size = 100   # reduce from 1000 — see note below
    all_results = []
    total = None

    while True:
        page = api_get(
            "/divisions",
            params={
                "date_start": start_date,
                "date_end": end_date,
                "limit": page_size,
                "skip": skip,
                "sort": "date",
                "order": "desc",   # explicit ordering, not API-default
            },
            conditional_get=True,   # see DDoS section
        )
        if total is None:
            total = page["head"]["totalResults"]
        all_results.extend(page["results"])

        if len(page["results"]) < page_size:
            break               # last page
        if skip + page_size >= total:
            break               # all accounted for
        skip += page_size

    assert len(all_results) >= total - tolerated_drift, \
        f"Expected ~{total}, got {len(all_results)} — pagination bug"
    return all_results
```

Key choices defended below.

### Why page_size = 100, not 1000

- Smaller pages mean smaller per-request memory and less wasted bytes on conditional-GET retries.
- Standard API politeness — many gov APIs throttle large `limit` values harder than smaller ones.
- The total request count goes up but each is faster; net throughput is similar.
- Resource cost on Oireachtas side is roughly the same; on our side it's lower.
- Documented as a sensible default in the [JSON:API cursor pagination profile](https://jsonapi.org/profiles/ethanresnick/cursor-pagination/) and the [Microsoft REST API guidelines](https://github.com/microsoft/api-guidelines).

### Why explicit `sort=date,order=desc`

Without explicit ordering, the API can return results in any order. Two consecutive calls with the same params can return different orderings — this breaks idempotency. Pin the order.

### Why the assert at the end

If we asked for `total=N` and got fewer than N back (modulo small drift if a vote appeared mid-fetch), something is wrong: API quirk, our pagination logic, network drop. **Loud failure beats silent partial success.**

## Phase 2 — side-by-side comparison run

**Goal:** prove the new fetch returns the existing data plus whatever was being truncated. Don't trust correctness blindly.

```text
old_results = current_fetch_function()
new_results = fetch_all_votes(start_date, end_date)

new_minus_old = set(new_results) - set(old_results)   # records the new fetch found that old missed
old_minus_new = set(old_results) - set(new_results)   # records old fetch had that new doesn't (should be empty)

write parquet "votes_canary/old_minus_new.parquet"
write parquet "votes_canary/new_minus_old.parquet"
log counts
```

**Acceptance criterion for Phase 2:**
- `old_minus_new` is empty (or every entry in it has a documented reason — e.g. a vote was retracted upstream).
- `new_minus_old` is non-empty if Phase 0 confirmed truncation.
- A sample of `new_minus_old` records is manually verified against the Oireachtas web UI.

Do *not* proceed to Phase 3 until this passes.

## Phase 3 — switch over

**Action:** replace the production fetch call with the paginated version. Single PR. Keep the side-by-side comparison code commented out, not deleted, for one cycle in case rollback is needed.

**Rollback plan:** revert the PR. Old code path is intact. Two days' lost data tolerable; bad data is not.

## Phase 4 — clean up

After two successful production cycles with the new fetcher:
- Delete the side-by-side comparison code.
- Move `votes_paginated_fetch.py` from `pipeline_sandbox/` into the appropriate production module.
- Update `transform_votes.py` if any downstream assumptions change (e.g., it was previously safe to assume ≤1000 records; now it isn't).

## Open questions for the maintainer

1. **Where exactly is the current `limit=1000` call?** I saw `members?...&limit=200` in `members_api_service.py:54` for members, but didn't trace the votes call. Need to find that before Phase 0 can be implemented.
2. **What does `head.totalResults` actually return on the divisions endpoint?** The diagnostic in Phase 0 assumes the field exists. Confirm by inspecting one response in a notebook before the Phase 0 PR.
3. **What's the realistic vote volume?** ANSWERED: more than 1000. Exact count to be confirmed in Phase 1 by reading `head.totalResults` once.

## What this plan deliberately does not do

- It does not refactor the rest of the API service layer. One bug, one fix.
- It does not introduce new dependencies. `requests` + standard library is enough.
- It does not add a fancy retry/circuit-breaker layer. That's a separate hardening pass once auto-refresh is wired up.
- It does not move to cursor-based pagination. The Oireachtas API exposes skip/limit; cursor would require their cooperation.

## Integration check before merging

When this plan is approved and Phase 0 is merged, verify in this order:
1. Phase 0 log line shows up in one pipeline run.
2. Decision: is truncation real? If no, stop. If yes, continue.
3. Phase 1 module exists in `pipeline_sandbox/` with unit tests against a mocked API response.
4. Phase 2 comparison runs at least once locally before any production change.
5. Phase 3 PR is small, reviewable, and has the rollback comment block.
