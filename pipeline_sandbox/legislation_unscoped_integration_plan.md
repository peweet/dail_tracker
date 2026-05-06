# Unscoped legislation fetch — integration plan

Pairs with [legislation_unscoped_fetch.py](legislation_unscoped_fetch.py).

## 1. The gap

[services/urls.py:17-39](../services/urls.py#L17-L39) builds one
`/v1/legislation` URL per TD with `member_id=<uri>`. That filter excludes
Government bills entirely — a Minister sponsoring a bill "in capacity as
Minister" carries no member URI. Bronze proof: 100 % of 642 unique bills in
[data/bronze/legislation/legislation_results.json](../data/bronze/legislation/legislation_results.json)
have `bill.source = "Private Member"`.

The sandbox script fixes the gap by fetching the same endpoint **without**
`member_id`, paginating on `skip`. ~5–10 calls covers the full 2014→present
corpus. Output: `legislation_results_unscoped.json` next to the existing
per-TD file.

## 1a. Sponsor-shape gotcha — must fix during graduation

Confirmed by [legislation_unscoped_validate.py](legislation_unscoped_validate.py):

| source | rows | sponsor.by.showAs present | sponsor.as.showAs present |
|---|---|---|---|
| Private Member | 2,449 | 2,449 | 0 |
| Government | 552 | **0** | 552 |
| Private | 1 | 0 | 0 |

Government bills carry sponsor identity in `sponsor.as.showAs`
("Minister for Finance", "Minister for Public Expenditure …") and have
`sponsor.by.showAs = NULL`. Two production filters would silently drop
every Government row if the input file is swapped without code changes:

- [legislation.py:126](../legislation.py#L126):
  `dropna(axis=0, subset=["sponsor.by.showAs"], how="all")`
- [sql_views/legislation_index.sql:24](../sql_views/legislation_index.sql#L24):
  `WHERE sponsor_by_show_as IS NOT NULL`

Fix in graduation PR: coalesce. Replace the dropna with
`dropna(subset=["sponsor.by.showAs", "sponsor.as.showAs"], how="all")`
and the SQL filter with
`WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL`.
The `sponsor` column in `v_legislation_index` already does the right
thing once that change is made (it's already a COALESCE candidate via
the existing `sponsor` derivation).

## 1b. Empirical impact on existing silver/gold

From the validation run against current bronze + silver:

- **1,621 unique bills** in unscoped vs **642 unique bills** captured by
  the per-TD harvest → **979 newly visible bills**, of which 548 are
  Government, 430 are Private Member (sponsored by TDs no longer
  members), and 1 is `Private`.
- **Gold cross-check**: 7 of 20 sampled Government bills already have
  matching rows in
  [data/gold/parquet/current_dail_vote_history.parquet](../data/gold/parquet/current_dail_vote_history.parquet)
  via `debate_title` substring match — e.g. Climate Action and Low Carbon
  Bill 2021 (579 vote rows), Garda Síochána (Functions and Operational
  Areas) Bill 2021 (126 vote rows). The votes are *already* in gold; the
  bills they relate to are the missing piece.

## 2. Graduation — minimal merge into production

Scope is small enough to fit the existing scenario pattern. No new abstraction.

1. **services/urls.py** — add a sibling builder:

   ```python
   def build_legislation_unscoped_url(skip: int = 0) -> str:
       return (f"{API_BASE}/legislation?date_start=2014-01-01"
               f"&date_end=2099-01-01&limit=1000&skip={skip}"
               f"&chamber_id=&lang=en")
   ```

2. **services/legislation_unscoped.py** (new, ~30 lines) — copy the
   `fetch_all_bills` loop verbatim from the sandbox script. Mirrors
   [services/votes.py](../services/votes.py) shape and naming.

3. **services/storage.py** — extend `result_file_path()`:

   ```python
   if scenario == "legislation_unscoped":
       return LEGISLATION_DIR / "legislation_results_unscoped.json"
   ```

4. **services/oireachtas_api_main.py** — one new call after the existing
   per-TD legislation scenario:

   ```python
   run_paginated_scenario(
       scenario_name="legislation_unscoped",
       fetch_fn=fetch_all_bills,
       overwrite=overwrite_legislation,
   )
   ```

   (Either factor a tiny `run_paginated_scenario` next to
   `run_member_scenario`, or just inline the save — votes already inlines.
   Don't over-engineer this.)

5. **legislation.py** — switch the source from `legislation_results.json`
   to `legislation_results_unscoped.json`. Schema is identical (same API,
   same bill envelope). The per-TD file becomes redundant and can be
   retired in a follow-up; keep it during the changeover so a rollback is
   one-line.

## 2a. SQL-view + page consistency checklist

Two of the five legislation views currently filter on
`sponsor_by_show_as IS NOT NULL`; the other three don't. Once Government
bills reach silver, the views will disagree about which bills exist —
`fetch_bill_debates(bill_id)` will return rows whose `bill_id` doesn't
appear in `v_legislation_index`, breaking the click-through. The page
also assumes Dáil-origin and hardcodes a "Private Members only" caveat
in three places. All of this must land in one PR with the data flip.

| # | Where | Change | Why |
|---|---|---|---|
| 1 | [legislation.py:12-29](../legislation.py#L12-L29) (`BILL_META`) | Add `["bill", "originHouse", "showAs"]` (note: `originHouse` is itself a `{showAs, uri}` struct — drilling to the leaf is required). Add `"bill.originHouse.showAs": "origin_house"` to `rename_bill_fields`. | Needed for chamber-aware `bill_phase` (item 5). Govt bills can originate in the Seanad. |
| 2 | [legislation.py:126](../legislation.py#L126) | Change `dropna(subset=["sponsor.by.showAs"])` → `dropna(subset=["sponsor.by.showAs", "sponsor.as.showAs"], how="all")` | Govt rows have `by.showAs = NULL`, identity is in `as.showAs` ("Minister for X"). Without this, all 548 Govt bills are dropped at silver. |
| 3 | [legislation_index.sql:24](../sql_views/legislation_index.sql#L24) and [legislation_detail.sql:30](../sql_views/legislation_detail.sql#L30) | `WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL`; also change the `sponsor` derivation in both views to `COALESCE(sponsor_by_show_as, sponsor_as_show_as, '—')` | Mirror the silver coalesce. Aligns the index/detail views with timeline/debates/sources, which already have no sponsor filter. |
| 4 | `v_legislation_index` SELECT in [legislation_index.sql](../sql_views/legislation_index.sql) | Add `source` and `origin_house` columns to the projection | The index doesn't currently expose `source`. Without it, no Govt-vs-PM filter or card meta is possible at the listing level. `bill_type` already gets COALESCE'd over `source` so the type badge stays "Public" for both — a separate `source` column is required. |
| 5 | `bill_phase` CASE in [legislation_index.sql:38-45](../sql_views/legislation_index.sql#L38-L45) | Make origin-aware: `WHEN origin_house ILIKE '%Seanad%' AND stage_number < 6 THEN 'seanad'`, `WHEN origin_house ILIKE '%Seanad%' AND stage_number >= 6 THEN 'dail'`, then existing fallthroughs. Note: `origin_house` values are "Seanad Éireann" / "Dáil Éireann", **not** the bare strings. | Current logic assumes every bill starts in the Dáil. A Seanad-origin Govt bill at stage 2 currently shows up in the Dáil bucket. |
| 6 | [utility/pages_code/legislation.py:56-60, 110-117, 207-212](../utility/pages_code/legislation.py#L56-L60) | Strip the "Pipeline todo" callout, generalise hero copy ("Bills before the Oireachtas — Private Members and Government"), and update the provenance footer text | Three hardcoded "Private Members only" caveats now contradict the data. |
| 7 | [utility/pages_code/legislation.py](../utility/pages_code/legislation.py) | Add a Government/Private Member/All segmented control next to the existing phase selector; render `source` on the bill card meta strip | Surfaces the new dimension to users. Cheap once item 4 lands. |

Item ordering matters: 1–2 unblock the data, 3–5 fix the views, 6–7 are
UI polish. Don't ship 1–2 alone — Govt bills would land in some views
and not others, and the broken click-through is worse than the current
"missing entirely" state.

### Verification once items 1–7 are in

- `SELECT bill_id FROM v_legislation_index EXCEPT SELECT bill_id FROM v_legislation_detail` returns 0 rows.
- Every distinct `bill_id` in `v_legislation_debates` exists in `v_legislation_index`.
- Spot-check one Seanad-origin Government bill — bill_phase reflects origin chamber, not Dáil.
- Spot-check the `Climate Action and Low Carbon Bill 2021` (already validated to have 579 vote rows in gold) — appears in the index, click-through to detail works, debates panel populated.

### Sandbox proof — items 1–5 already verified

[legislation_unscoped_silver_views.py](legislation_unscoped_silver_views.py)
implements items 1–5 against a parallel silver path
(`pipeline_sandbox/out/silver/`) and a DuckDB connection — production
silver and SQL views are not touched. Run with:

    python -m pipeline_sandbox.legislation_unscoped_silver_views

Empirical results from the latest run (2026-05-06):

| measurement | value |
|---|---|
| sponsor rows (silver) | 3,001 |
| stages rows (silver) | 7,326 |
| debates rows (silver) | 6,937 |
| bills in `v_legislation_index` | 1,620 (1,072 PM + 548 Govt) |
| `v_legislation_index` ↔ `v_legislation_detail` drift | 0 rows |
| Seanad-origin bills correctly placed in 'seanad' phase | confirmed |
| `enacted` Government bills | 504 / 548 = 92 % (sanity matches: govt bills usually pass) |
| `enacted` Private Member bills | 21 / 1,072 = 2 % (sanity matches: PM bills rarely enacted) |
| Climate Action and Low Carbon Bill 2021 (`2021_39`) end-to-end | index ✓, detail ✓, phase = enacted ✓ |

Items 6 (page copy) and 7 (UI segmented control) are out of scope for the
sandbox — they need a Streamlit run to verify and don't change any data
contract. They land with the production graduation PR.

## 3. Sponsor-link side-effect

Per-TD calls returned the bill **only** for the TD whose `member_id` matched
a sponsor row. Unscoped calls return each bill **once**, with all sponsors
inside `bill.sponsors[]`. The existing sponsor flattener
([legislation.py:60-66](../legislation.py#L60-L66)) already iterates that
array via `record_path=["bill", "sponsors"]`, so the per-TD ↔ bill join is
preserved through `sponsors_df.unique_member_code`. No join logic changes.

Net effect on row counts (measured against current bronze):

- `bills` (unique): 642 → 1,621.
- Newly visible: 979 bills (548 Government, 430 Private Member sponsored
  by TDs no longer in the Dáil, 1 Private).
- `sponsors_df`: drops slightly (no longer multi-counts a co-sponsored bill
  once per TD's API call) — verify with a count diff before/after.

## 4. Verification before flipping production

1. Run sandbox script, confirm `bill.source` Counter shows both
   `Government` and `Private Member` — already done, see
   [legislation_unscoped_validate.py](legislation_unscoped_validate.py).
2. Spot-check 5 known Government bills (e.g. Finance Bill 2024, Planning
   and Development Bill 2023) — present in unscoped, absent in per-TD.
3. Confirm the Private Member bill set is a strict superset of the
   per-TD set (key on `(billYear, billNo)`).
4. Run `legislation.py` against the unscoped file in a scratch branch;
   diff `silver/sponsors.csv` and `silver/stages.csv` row counts and unique
   `unique_member_code`s. The TD ↔ bill join must not regress.
5. After items 1–7 of section 2a land, run the four checks in the
   "Verification once items 1–7 are in" sub-section.

## 5. Out of scope

- Retiring the per-TD `build_legislation_urls` / removing the 174 calls.
  Do that as a separate cleanup PR after the unscoped path has been live
  for one refresh cycle.
- Splitting Government bills by sponsoring department. The
  `sponsor.as.showAs` field already carries "Minister for Finance" etc. —
  surface that on the legislation page once the data lands, no new fetch.
- Backfilling pre-2014. Date floor matches the existing builder; widening
  is a one-line change deferred until there's a UI need.
