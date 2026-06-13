# Logic Firewall — Pass 2 Audit

**Date:** 2026-05-27 (post-implementation sweep)
**Status:** `tools/check_streamlit_logic_firewall.py` returns **OK — scanned 23 files, no violations**. This doc catalogs what the checker *cannot* catch — pipeline-territory drift that hides in patterns the AST scanner doesn't model.

Successor to the pass-1 `LOGIC_FIREWALL_AUDIT.md` + `LOGIC_FIREWALL_PLAN.md` (both removed 2026-06-04 once their violations were remediated and the checker passed clean). This is now the sole firewall doc, tracking only the residual drift the AST scanner cannot catch.

---

## P0 — Real architectural drift the checker misses

### A. Duplicated civic taxonomies across pages

[`lobbying_2.py:197`](../utility/pages_code/lobbying_2.py#L197) defines `_CURATED_TOPICS` (Immigration & asylum, Housing crisis, Climate, …) — each topic carries a substring-keyword tuple used to bucket lobbying returns. [`lobbying_3.py:112`](../utility/pages_code/lobbying_3.py#L112) defines **a second copy of the same constant**. Two civic-classification taxonomies, one rename away from silent drift.

[`votes.py:46`](../utility/pages_code/votes.py#L46) defines `_TD_PICKER_TOPICS` (topic → SQL `ILIKE` pattern) for the vote-topic picker. Same conceptual shape, third file.

**Why this matters:** "Housing" on /lobbying must mean the same thing as "Housing" on /lobbying-3 and on /votes. Today it doesn't have to — three Python files own the definition independently.

**Fix shape:** Phase 2.2 in the plan covers the vote side. The two `_CURATED_TOPICS` need to be either (a) extracted to a single shared constant in `utility/civic_topics.py` and imported in both pages, or (b) moved to `config/topic_taxonomy.yaml` and surfaced via a `v_lobbying_topic_taxonomy` view. The two-copy state is the worst of both worlds.

### B. Hardcoded URL construction in page code

[`statutory_instruments.py:204-212`](../utility/pages_code/statutory_instruments.py#L204-L212) `_eisb_url()`:
```python
return f"https://www.irishstatutebook.ie/eli/{int(yr)}/si/{int(no)}/made/en/print"
```
The PIPELINE_VIEW_BOUNDARY doc explicitly lists "official URL construction" as pipeline-owned. The view should always populate `eisb_url`; the page-side fallback masks a pipeline gap.

**Fix:** add a `COALESCE(eisb_url, 'https://www.irishstatutebook.ie/eli/' || si_year || '/si/' || si_number || '/made/en/print')` expression to `v_statutory_instruments`. Remove `_eisb_url()` from the page.

### C. Type coercion on view output (the view contract should already be typed)

The four `pd.to_datetime(df[col], errors="coerce")` calls in [attendance.py:314, 473](../utility/pages_code/attendance.py#L314), [attendance_overview.py:221](../utility/pages_code/attendance_overview.py#L221), and [statutory_instruments.py:195](../utility/pages_code/statutory_instruments.py#L195) all coerce a date column *after* reading it from a registered view.

The view should expose `sitting_date::DATE` / `si_signed_date::DATE`. If the underlying parquet has the date as a string, the coercion belongs in the view definition, not the page. This is a small but systematic boundary leak — every time the page coerces a view column, it asserts a type contract the view should already enforce.

**Fix:** add `CAST(sitting_date AS DATE)` in `sql_views/attendance_*.sql` and `sql_views/legislation_si_index.sql`. Page-side coercion becomes a no-op and can be removed.

### D. In-page lookup-dict-by-iterrows (row-level join in disguise)

[`lobbying_2.py:2269-2275`](../utility/pages_code/lobbying_2.py#L2269-L2275):
```python
org_idx = fetch_org_index()
org_lookup: dict[str, dict[str, str]] = {}
if not org_idx.empty and "lobbyist_name" in org_idx.columns:
    for _, r in org_idx.iterrows():
        org_lookup[str(r["lobbyist_name"])] = {
            "sector": str(r.get("sector", "") or ""),
            "website": str(r.get("website", "") or ""),
        }
```
This is the **same anti-pattern as the DPO dict-join** fixed in Phase 1.4 — a row-by-row dict build over a DataFrame, used downstream to enrich card rendering by lobbyist_name. The V1 checker doesn't recognise this shape (it's not a `pd.merge` and not a `.groupby()`), but it's a join.

**Fix:** the cards consume `sector` and `website` per return — fold them into `v_lobbying_contact_detail` (or `v_lobbying_contact_detail_with_dpo` which we just built). One more LEFT JOIN in the view; the `org_lookup` dict goes away.

---

## P1 — Constants and stats that should be annotated

### E. Civic-fact constants embedded in pages

[`statutory_instruments.py:264`](../utility/pages_code/statutory_instruments.py#L264):
```python
_COMMITTEE_FORMED = pd.Timestamp("2025-12-01")
```
This is the formation date of the Seanad Committee on EU Scrutiny & Transparency — used as the filter pivot for the "EU scrutiny" callout. It IS a fixed historical fact (per the PIPELINE_VIEW_BOUNDARY exception list), but it should be annotated with the source URL like `_YEAR_SITTING_DAYS` in attendance.py:
```python
# Source: Cathaoirleach Mark Daly's Feb 2026 statement to the Seanad
# (Irish Times article linked at _ARTICLE_URL below).
_COMMITTEE_FORMED = pd.Timestamp("2025-12-01")
```
Currently uncommented. Same shape as `_OTHER_MIN` annotation in Phase 3.2.

### F. Bare-Series stat aggregations missing display_only annotations

[`committees.py:256-258`](../utility/pages_code/committees.py#L256-L258):
```python
member_count = int(filtered["name"].nunique())
active_memberships = int((filtered["status"] == "Active").sum())
chair_total = int(filtered["is_chair"].sum())
```
Plus mirrors at [`:509-512`](../utility/pages_code/committees.py#L509) and [`:789-791`](../utility/pages_code/committees.py#L789).

These are stat-strip numbers computed on the active filter set — same shape as `_OTHER_MIN`. The checker doesn't flag bare `.sum()`/`.nunique()` on Series (only `.groupby(...).sum()` chains), so these silently pass. Worth adding `# logic_firewall: display_only` for the next reviewer's benefit.

### G. Display-only aggregations on per-TD slices in member_overview

[`member_overview.py:508-510`](../utility/pages_code/member_overview.py#L508-L510):
```python
depts = [d for d in df["si_department_label"].dropna().unique().tolist()]
dept_str = ", ".join(depts) if depts else "—"
eu_n = int(df["si_is_eu"].fillna(False).astype(bool).sum())
```
Display-only — these summarise SIs signed by one TD for the section caption. Annotate.

### H. Display-only `.max()` / `.min()` denominators

Lobbying pages use `.max()` for progress-bar denominators: [`lobbying_2.py:1321, 1353, 1556, 1733`](../utility/pages_code/lobbying_2.py#L1321), and `.min()` / `.max()` for period spans: [`lobbying_2.py:1435-1436`](../utility/pages_code/lobbying_2.py#L1435). All display-only. Same pattern.

---

## P2 — Smaller smells

### I. Manual year extraction from view dates

[`attendance_overview.py:486-487`](../utility/pages_code/attendance_overview.py#L486-L487):
```python
first_year = pd.Timestamp(first_date).year if pd.notna(first_date) else years_asc[0]
last_year = pd.Timestamp(last_date).year if pd.notna(last_date) else years_asc[-1]
```
The view returns full dates and the page extracts the year. If `first_year` / `last_year` are consumed columns, the view should expose them.

### J. Cross-page duplication of one-line filter expressions

`all_dpos["chamber_display"].dropna().astype(str).replace({"": pd.NA}).dropna().unique().tolist()` appears verbatim in both [`lobbying_2.py:905`](../utility/pages_code/lobbying_2.py#L905) and [`lobbying_3.py:1252`](../utility/pages_code/lobbying_3.py#L1252). Move to a shared helper.

### K. `dpo_count` int-cast in lobbying_2

[`lobbying_2.py:2281`](../utility/pages_code/lobbying_2.py#L2281):
```python
dpo_count = int((detail["dpo_count"] > 0).sum())
```
Introduced in Phase 1.4. Display-only (counts rows with at least one DPO). The `.sum()` slips past the checker because it's bare. Annotate.

### L. SI mojibake quarantine still silent

[`statutory_instruments.py:197`](../utility/pages_code/statutory_instruments.py#L197):
```python
df = df[~df["si_title"].astype(str).str.contains("�", na=False)]
```
Per Phase 3.3 in the original plan: this was tagged as "skip per scope cut" — purely cosmetic, current behaviour is fine. Still worth noting it's silent (no provenance, no count surfaced).

---

## Deliberately deferred (re-stated for completeness)

These were caught in pass 1 and consciously scoped out by the user:

- **Phase 1.3** — SI facet `value_counts` (now all annotated `display_only`)
- **Phase 1.5** — lobbying topic `value_counts` panel (annotated)
- **Phase 2.2** — vote topic bucket classification (`_TD_PICKER_TOPICS` still in code)
- **Phase 2.3** — lobbying canonical-position resolver (`lobbying_2.py:544` `.str.contains` fuzzy match still in code)
- **Phase 3.3** — formalise SI title quarantine with view-side `quarantine_flag`

---

## What's actually clean

After pass 1 + pass 2:

- **No `pd.merge` / `df.merge` anywhere** in `utility/pages_code/` or `utility/data_access/`.
- **No `.apply(...)` / `.map(lambda ...)` derivations** in pages.
- **No `np.where` / `np.select`** column derivations.
- **No `json.loads/dumps`** in pages (DuckDB list/struct types are decoded inside data-access functions, not pages).
- **All 29 `iterrows()` loops** in pages are card-rendering loops (`for _, row in visible.iterrows(): cards.append(_card_html(row))`) — legitimate display code, not ETL.
- **Zero raw `read_parquet` / `read_csv` calls** outside `pipeline_sandbox/` and `sql_views/`.
- **Zero `duckdb.connect(":memory:")` + register patterns** in pages or data-access.

---

## Priority order if you do another pass

1. **A — duplicated `_CURATED_TOPICS`** (concrete drift risk, ~30 min to consolidate)
2. **D — `org_lookup` dict-join in lobbying_2** (same shape we fixed in Phase 1.4; trivial view extension)
3. **C — date-column type coercion** (4 sites, ≤30 min, makes the view contract honest)
4. **B — `_eisb_url` fallback** (one view edit + one page deletion)
5. **F/G/H/K — display-only annotations** (mechanical, 5 min, marks the boundary explicitly for next reviewer)
6. **E — annotate `_COMMITTEE_FORMED` with source** (one line)

Items I–L are P2; pick up opportunistically.
