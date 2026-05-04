# Citation friendliness + first-class data export

_Last revised: 2026-05-04. Owner: solo. Status: planning._

This doc plans two features that share most of the same plumbing and should ship together:

1. **"Cite this view" on every chart and page** — copy-button that yields a permalink, a stable citation ID, a parquet hash, and a retrieved-at timestamp. Designed to be paste-able into a news article footnote, a journalist's spreadsheet, an academic citation manager, or a Slack thread.
2. **A first-class `/data` page** — every gold mart available as Parquet + CSV download, each with a permalink, a hash, a manifest, and a "cite this dataset" block. The dashboard is the trust artefact; the data page is the product newsroom developers and academic researchers actually use.

Both features rest on the same primitive: **a deterministic, content-addressed, versioned reference to a slice of the analytical layer.** Build that primitive once; expose it twice.

---

## Table of contents

1. [Why this matters](#1-why-this-matters)
2. [How the two features share infrastructure](#2-how-the-two-features-share-infrastructure)
3. [The four citation artefacts](#3-the-four-citation-artefacts)
4. [Data model — manifest extensions](#4-data-model--manifest-extensions)
5. [URL state encoding (permalinks)](#5-url-state-encoding-permalinks)
6. [Citation ID format](#6-citation-id-format)
7. [Parquet hash strategy](#7-parquet-hash-strategy)
8. [Zenodo / DOI integration](#8-zenodo--doi-integration)
9. [UX — the cite block](#9-ux--the-cite-block)
10. [UX — the /data page](#10-ux--the-data-page)
11. [Implementation phases](#11-implementation-phases)
12. [Open decisions](#12-open-decisions)
13. [Integration with the 8-week plan](#13-integration-with-the-8-week-plan)
14. [What this plan deliberately does not solve](#14-what-this-plan-deliberately-does-not-solve)

---

## 1. Why this matters

The thesis from the audit ([`feedback.md`](../feedback.md)):

> Aggressive citation friendliness is what gets you cited in news articles, which is what gets you used. The data export is the product, not the dashboard.

Four concrete reasons to invest here early:

1. **Citations beat metrics.** A single footnote in an Irish Times piece — "(Source: Dáil Tracker, dailtracker.ie/lobbying?...)" — is worth more than 1,000 unique-visit pings. It legitimises the project, drives referrals, and triggers further journalist contact.
2. **Reproducibility is trust.** A journalist who can't reproduce a chart in a week's time will not cite it. A chart whose URL renders the same numbers in 3 years' time is citable. This is the *only* feature that addresses the "is this data going to silently change under me?" objection.
3. **Newsrooms use the data, not the dashboard.** Investigations are written in spreadsheets, Datawrapper, and Flourish. The dashboard's job is to convince a journalist the data is clean enough to download. Once they trust it, they leave the dashboard.
4. **Academia needs structured IDs.** Politics researchers at TCD/UCD/DCU using your dataset for a paper need something that survives Zotero/Mendeley round-trip. A bare URL doesn't.

The cost is small relative to the leverage: ~3–5 days of focused work for the v1, sitting on top of infrastructure (manifests, versioned releases) the existing plan already commits to.

---

## 2. How the two features share infrastructure

| Primitive | Used by "cite this view" | Used by `/data` page |
|---|---|---|
| Manifest with parquet sha256 | yes — surfaces hash in cite block | yes — drives "verify integrity" hint |
| Versioned release tag (e.g. `data-v2026.05.07`) | yes — anchors the citation in time | yes — the unit of download |
| Stable URL state encoding | yes — permalink for the chart | yes — deep-linkable filter for download |
| Citation ID format | yes — paste-able string | yes — dataset-level cite |
| Methodology page | linked from cite block | linked from per-mart download |
| Zenodo deposit per release | yes — DOI for the cite | yes — DOI for the dataset |

Build order is therefore: **manifest hash → release tag → URL encoding → citation generator → cite block UI → /data page → Zenodo automation.** Cite block and /data page are 1–2 days each *after* the foundations are in.

---

## 3. The four citation artefacts

A "cite this view" copy yields four things, in this order, in one paste-able block:

### 3.1 The permalink

A stable, deep-linkable URL that reproduces the exact view.

```
https://dailtracker.ie/lobbying?as_of=2026-05-07&filter.year=2024&filter.policy=health
```

- `as_of=` pins the data version. Without it, citing a view is meaningless — tomorrow's data will give different numbers.
- Filter state in query params, namespaced (`filter.foo`).
- Survives renames via aliases (§5.4).
- See §5 for encoding rules and length budget.

### 3.2 The citation ID

A short, structured, human-readable, machine-parseable identifier. Designed to look like a DOI but not be one.

```
dailtracker:lobbying-summary:v2026.05.07:m4q2-yt8h
```

- Format: `dailtracker:<view-slug>:<release-tag>:<filter-fingerprint>`
- `view-slug` = the SQL view or page contract name (kebab-case).
- `release-tag` = the data release the view was rendered from.
- `filter-fingerprint` = first 8 chars of base32-encoded sha256(canonical filter JSON). Optional, omitted if no filters.
- Looks DOI-ish enough to read as a citation but is unambiguously *ours*. We can graduate this to a real DOI later (§8) without breaking compatibility.

### 3.3 The parquet hash

The sha256 of the parquet file underlying the view.

```
sha256:5f4c...e3a2  (mart_lobbying_summary.parquet, release v2026.05.07)
```

- Truncated to 16 chars in display, full 64 in copy.
- Comes from the per-mart manifest (§4).
- A reader who downloaded the parquet can independently verify their copy matches the cited version.
- This is the single thing that makes the citation reproducible against future data updates.

### 3.4 The retrieved-at timestamp

```
Retrieved 2026-05-04T14:23Z
```

- ISO-8601 UTC.
- Convention from web citations.
- Captured at the moment the cite block was generated, server-side.

### 3.5 Composed cite block

The copy button puts all four into a single paste-able block in three formats (user picks, default = plain):

**Plain text:**
```
Dáil Tracker (2026), "Lobbying summary by policy area, 2024".
dailtracker:lobbying-summary:v2026.05.07:m4q2-yt8h
https://dailtracker.ie/lobbying?as_of=2026-05-07&filter.year=2024&filter.policy=health
sha256:5f4c8a1b2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a
Retrieved 2026-05-04T14:23Z
```

**Markdown:**
```markdown
[Dáil Tracker (2026), "Lobbying summary by policy area, 2024"](https://dailtracker.ie/lobbying?as_of=2026-05-07&filter.year=2024&filter.policy=health) (`dailtracker:lobbying-summary:v2026.05.07:m4q2-yt8h`, retrieved 2026-05-04).
```

**BibTeX:**
```bibtex
@misc{dailtracker_lobbying_summary_2026_v20260507,
  author       = {{Dáil Tracker}},
  title        = {Lobbying summary by policy area, 2024},
  year         = {2026},
  howpublished = {\url{https://dailtracker.ie/lobbying?as_of=2026-05-07&filter.year=2024&filter.policy=health}},
  note         = {dailtracker:lobbying-summary:v2026.05.07:m4q2-yt8h, sha256 5f4c8a1b...},
  urldate      = {2026-05-04},
}
```

These three formats cover ~95% of citation contexts (news article footnote, blog post, academic paper). A "Chicago" or "APA" formatter can come later if anyone asks.

---

## 4. Data model — manifest extensions

The manifest schema in [v4 §3.5](dail_tracker_improvements_v4.md#35-manifests-and-freshness-ui) is the right shape but missing four fields needed for citation:

```json
{
  "dataset_name": "mart_lobbying_summary",
  "layer": "mart",
  "grain": "one row per (member, policy_area, year)",
  "built_from": ["fact_lobbying_return", "dim_member", "dim_policy_area"],
  "row_count": 8423,
  "run_id": "2026_05_07_001",
  "git_commit": "c7a67de",
  "release_tag": "v2026.05.07",                          // NEW
  "parquet_sha256": "5f4c8a1b2d3e4f5a...",               // NEW
  "parquet_bytes": 1842311,                              // NEW
  "view_slug": "lobbying-summary",                       // NEW — citation key
  "source_versions": {
    "oireachtas_api": "fetched 2026-04-30T04:12Z",
    "lobbying_ie": "manual export 2026-03-10"
  },
  "caveats": [...],
  "schema": [                                            // NEW — column-level
    {"name": "unique_member_code", "type": "VARCHAR", "nullable": false},
    {"name": "policy_area",        "type": "VARCHAR", "nullable": false},
    {"name": "return_count",       "type": "INTEGER", "nullable": false}
  ]
}
```

The four new top-level fields and the schema array are mandatory. The `view_slug` is what a user sees in citation IDs — keep it short (~3 words, kebab-case). Each contract YAML in `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/` should declare its `view_slug`.

**Pydantic strict model** for the manifest belongs in `utility/data_access/manifest_schema.py`, building on the [pydantic-at-boundaries example](../pipeline_sandbox/pydantic_manifest_example.py) already in the sandbox. CI fails the build if a manifest is missing any of the four new fields.

---

## 5. URL state encoding (permalinks)

### 5.1 Current state

`st.query_params` is already used for member selection ([member_overview.py:567](../utility/pages_code/member_overview.py#L567), [:780](../utility/pages_code/member_overview.py#L780)). The pattern works but is ad-hoc per page. Citation needs a uniform, reversible mapping between *all* page filter state and the URL.

### 5.2 Convention

| URL key | Meaning |
|---|---|
| `as_of` | Release tag, e.g. `v2026.05.07`. Required on cited URLs; defaults to "latest" otherwise. |
| `filter.<name>` | One value per filter. Multi-value uses comma: `filter.policy=health,housing`. |
| `view` | Optional sub-view slug for pages with multiple chart panes (e.g. `view=revolving-door`). |
| `member` | Reserved (already in use). |

Filter values are URL-encoded, lowercased where the underlying view allows it, and order-stabilised (alphabetical) so two equivalent filter sets produce the same canonical URL. Canonical URL is what gets hashed for the filter fingerprint (§3.2).

### 5.3 Length budget

Streamlit Cloud and most browsers tolerate ~2,000-character URLs. With multi-select filters this can blow out. Mitigation:

- **Short filter codes.** A `policy_area` filter with 12 values uses 1-letter codes (`h` = health, `e` = education) in the URL, expanded server-side. A short-code registry lives in `utility/url_codes.py`.
- **Hash-shortened URLs as fallback.** If the canonical URL exceeds 1,500 chars, persist the filter JSON to a `data/_url_aliases/<sha8>.json` file in the data branch and serve `?fs=<sha8>` instead. Aliases are immutable once written.

### 5.4 Stability across renames

A view rename or filter rename must not break a 2-year-old citation. Two mechanisms:

- **Permanent slugs.** `view_slug` in the contract is append-only; you can add a new slug but never delete an existing one. A renamed view declares both `view_slug` (new) and `view_slug_aliases: [...]` (old). The router accepts any.
- **Filter alias map.** `utility/page_contracts/_filter_aliases.yaml` maps old filter names to new ones, scoped per page. Old URLs are silently rewritten.

### 5.5 Privacy

URL state should never include personal browsing signal beyond what's intrinsic to the view. Specifically:
- No timestamps of the user's session.
- No "last selected" state that wasn't explicitly chosen by clicking a filter.
- No referrer-derived state.

The cite block is generated *from the URL bar*, not from session state — so the user can verify what they're sharing before they click copy.

---

## 6. Citation ID format

```
dailtracker:<view-slug>:<release-tag>[:<filter-fingerprint>]
```

### 6.1 Why not a DOI from day one

Real DOIs cost time and discipline. DataCite via Zenodo is free-as-in-money but each minted DOI is permanent — bad ones can't be retracted. We mint Zenodo DOIs *per data release* (§8), not per view. The view-level ID is ours.

### 6.2 Filter fingerprint

When a citation refers to a view with non-default filter state, append a fingerprint:

```python
canonical = json.dumps(filter_state, sort_keys=True, separators=(",", ":"))
fingerprint = base32(sha256(canonical))[:8].lower().rstrip("=")
```

Length 8 base32 chars = ~40 bits = collision risk negligible at our scale.

When the view has no filter state, omit the fingerprint segment entirely. `dailtracker:lobbying-summary:v2026.05.07` is the unfiltered view of the lobbying summary at that release.

### 6.3 Resolver

A small resolver `https://dailtracker.ie/cite/<id>` (or query-style `?cite=<id>`) takes a citation ID and 302-redirects to the canonical permalink. This means:
- A copy-pasted ID is itself clickable in some contexts.
- Renames and slug aliases are resolved server-side.
- We can change the URL structure later without breaking IDs.

The resolver reads from a small `data/_meta/cite_registry.json` written at release time, mapping IDs → URLs. Append-only.

### 6.4 What gets a citation ID

- Every page (page-level cite, no filter fingerprint or with current filters).
- Every gold mart on `/data` (dataset-level cite, no filter fingerprint).
- Every chart that has a "cite this view" button. v1 does *not* try to give every metric or table its own ID — only the chart-level container. Granular per-cell citation is out of scope.

---

## 7. Parquet hash strategy

### 7.1 Computation

```python
import hashlib
def sha256_file(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()
```

Computed once at the end of the pipeline run, in `utility/tools/write_run_manifest.py` (the file due on Day 1.2 of the existing plan). Hash + file size + dataset name go into the manifest.

### 7.2 What it covers and what it doesn't

The hash is over the parquet bytes. That means:
- ✅ Two readers downloading the same release will get the same hash.
- ✅ A row added or modified upstream produces a different hash.
- ❌ A non-content change (Parquet writer version, compression, metadata) also changes the hash. Pin the writer settings in the pipeline (compression="snappy", row_group_size=...) so this stays stable.
- ❌ The hash does *not* extend to the SQL view definition — only the materialised mart. If the view definition changes but the data is identical, hash stays the same.

This is fine for "did the dataset change between two cites" but not for "is the chart logic the same." For the latter, the manifest's `git_commit` field is the answer — and the cite block surfaces it for sceptical readers.

### 7.3 Display

In the cite block, hash is shown truncated:
```
sha256:5f4c8a1b…e3a2
```
Hovering or clicking expands to the full 64 chars. The clipboard always gets the full hash.

### 7.4 Verification helper

`/data` includes a one-line shell snippet for each mart:

```bash
sha256sum mart_lobbying_summary.parquet
# expected: 5f4c8a1b2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a
```

This is the kind of small touch that data-engineers-turned-journalists notice and trust.

---

## 8. Zenodo / DOI integration

### 8.1 What Zenodo gives us for free

- A persistent DOI per uploaded record.
- A landing page describing the record.
- A versioning relationship (each release is a new DOI under a "concept DOI" that always points to latest).
- An OAI-PMH endpoint, picked up by search infrastructure including Google Scholar.

Cost: zero. Setup: ~2 hours.

### 8.2 What we deposit

One Zenodo deposit *per data release* (`v2026.05.07`), not per view. The deposit contains:
- The full parquet bundle (`gold/parquet/*.parquet`).
- All per-mart manifests.
- A README listing every `view_slug` with a one-line description.
- The release-level CHANGELOG entry.

Each deposit gets a DOI like `10.5281/zenodo.12345678`. The concept DOI (always points to latest) is what news articles can cite if they want a "latest version" reference.

### 8.3 Automation

A `release.yml` GitHub Actions job, triggered on release tag push, uses Zenodo's REST API to:
1. Create a new version under the existing concept DOI.
2. Upload the parquet bundle.
3. Set metadata from the release notes + CHANGELOG.
4. Publish.
5. Write the resulting DOI back into `data/_meta/release_dois.json` so the cite block can include it.

### 8.4 Display in the cite block

When a release has a Zenodo DOI, the cite block includes it as a fifth line:

```
DOI: https://doi.org/10.5281/zenodo.12345678
```

The DOI is release-level, so two cites against the same release share a DOI but differ in citation ID + permalink. This is the right semantics — the DOI proves "the data is published"; the citation ID proves "the specific view".

### 8.5 What a journalist actually pastes

Most journalists will paste the plain-text or Markdown version. The DOI line is for academic readers and search-engine indexing. Both populations get what they want from one block.

---

## 9. UX — the cite block

### 9.1 Where the button goes

- **Page-level "cite this page" button** in every page header, next to the freshness badge.
- **Per-chart "cite this view" button** in the top-right of each chart container. Small icon button, tooltipped.
- **Cite block on `/data`** — one per mart, expandable.

The icon is `link` or `format_quote` (Material). Avoid emoji. The hit target is 24×24px minimum.

### 9.2 What the modal/popover shows

Click → a dialog with:
1. **Three tabs:** Plain | Markdown | BibTeX.
2. The composed block, monospace, selectable.
3. **Copy** button (primary) and **Download as `.bib`** secondary.
4. A small "What is this?" link to `/methodology#citation`.

The first time a user clicks, briefly show a one-sentence explainer: "This citation will reproduce the exact data shown above. The hash and version pin it in time." On subsequent clicks, no explainer.

### 9.3 What it does *not* do

- No "share to Twitter/Bluesky" buttons. Off-strategy and adds dependencies.
- No screenshot capture. Out of scope for v1.
- No "cite this row" or "cite this cell." Granularity stops at the chart.

### 9.4 Accessibility

- Modal is keyboard-navigable. Tab order: tabs → text area → copy button → close.
- The cite block content is in a `<pre>`-equivalent so screen readers don't munge whitespace.
- Copy button announces "Citation copied to clipboard" via aria-live.

---

## 10. UX — the /data page

### 10.1 Placement and naming

Add a `Data` page to [`utility/app.py`](../utility/app.py), positioned **second-to-last** in the nav (before a future "About"). Slug `/data`. Icon `:material/database:`.

It should *not* be hidden — newsroom developers will look for "Data" or "Download" in the nav. Burying it behind a footer link defeats the purpose.

### 10.2 Layout

A single page, three sections:

**Section 1 — Releases.** A table with one row per release: tag, date, total size, row count, link to Zenodo DOI, "Browse this release" link.

**Section 2 — Datasets in the latest release.** A card grid, one card per gold mart:

```
mart_lobbying_summary
One row per (member, policy_area, year). 8,423 rows · 1.8 MB · sha256:5f4c…e3a2
[Parquet] [CSV] [Schema] [Manifest] [Cite]
```

The five buttons:
- **Parquet** — direct download.
- **CSV** — generated on the fly (Streamlit + DuckDB COPY) for non-technical users.
- **Schema** — opens a modal showing the column-level schema from the manifest.
- **Manifest** — direct link to the JSON.
- **Cite** — same modal as §9.

**Section 3 — How to use this data.** Three short subsections:
1. "In a spreadsheet" — Datawrapper, Excel, Google Sheets paths.
2. "In Python" — three lines of `pl.read_parquet`.
3. "In R" — three lines of `arrow::read_parquet`.

A `wget` / `curl` block for everyone. Verification via `sha256sum`.

### 10.3 Permalinks within /data

Each mart has its own anchor: `/data#mart_lobbying_summary`. A direct link survives release upgrades — anchor is the mart slug, not the release tag.

A user who wants a *specific release* of a *specific mart* uses `?as_of=v2026.05.07#mart_lobbying_summary`.

### 10.4 What lives on the data branch vs main

- **Main:** the page code, the schema/manifest readers, the cite block component.
- **Data branch:** every release's parquet + manifest + alias files, plus `release_dois.json` and `cite_registry.json`.

Streamlit Cloud reads from data branch (per [v4 §3.3](dail_tracker_improvements_v4.md#33-github-actions-skeleton)). Direct download URLs point at GitHub raw on the data branch *or* the Zenodo DOI — pick Zenodo as the canonical, GitHub raw as the fallback.

### 10.5 Bandwidth

Streamlit Cloud bandwidth is generous but not infinite. Direct parquet downloads should *not* be served by Streamlit — too easy to DoS-by-accident. Three options, ranked:

1. **Zenodo direct download** (preferred — they have CDN, no cost to us).
2. **GitHub raw on data branch** (works, slightly clunky URLs).
3. **Cloudflare R2 with public read** if scale demands it (zero egress, ~$0.015/GB stored).

Pick (1) for v1; (3) only if a chart goes viral and Zenodo throttles.

---

## 11. Implementation phases

### Phase A — Foundations (~3 days)

A.1. **Extend manifest schema.** Add `release_tag`, `parquet_sha256`, `parquet_bytes`, `view_slug`, `schema[]` to the manifest. Update the strict pydantic model. Update `utility/tools/write_run_manifest.py` (the Day 1.2 file) to emit the new fields. ~4 hours.

A.2. **Pin parquet writer settings** so the hash is stable across runs. Compression, row group size, dictionary encoding all explicit. ~2 hours.

A.3. **Per-page contract: declare `view_slug`.** Walk every YAML in `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/`, add the field. ~1 hour.

A.4. **URL state encoding helper** at `utility/ui/url_state.py`. Two functions: `serialise(filters: dict) -> str` and `deserialise(query_params: dict) -> dict`. Canonical ordering, short codes. ~5 hours.

A.5. **Filter alias map.** Empty file `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/_filter_aliases.yaml`. Populated only when something is renamed. ~30 min.

### Phase B — Citation block component (~2 days)

B.1. **Citation ID generator** at `utility/ui/citation.py`. Functions: `make_citation_id(view_slug, release_tag, filters) -> str`, `compose_cite_block(citation_id, permalink, parquet_hash, retrieved_at, doi=None) -> dict[format, str]`. ~3 hours.

B.2. **Cite registry writer.** A pipeline post-step that walks contracts and writes `data/_meta/cite_registry.json`. ~2 hours.

B.3. **Cite modal component** at `utility/ui/cite_modal.py`. Three-tab Streamlit dialog with copy button. Test on one page first. ~5 hours.

B.4. **Wire cite button into page header** (one helper call per page). Start with `member_overview.py` and `lobbying_2.py`. ~3 hours.

B.5. **Per-chart cite buttons.** Wrap `st.altair_chart` / `st.plotly_chart` calls with a thin component that adds the cite icon. Top-right placement. ~5 hours.

### Phase C — /data page (~2 days)

C.1. **Page skeleton** at `utility/pages_code/data.py` — releases section, mart cards section, how-to section. ~5 hours.

C.2. **CSV-on-the-fly endpoint** — DuckDB `COPY (SELECT * FROM mart) TO STDOUT (FORMAT CSV)` streamed via `st.download_button`. ~2 hours.

C.3. **Schema modal** — reads `schema[]` from the manifest, renders a small table. ~2 hours.

C.4. **Wire into nav** — add the page to [`app.py`](../utility/app.py). ~30 min.

C.5. **Methodology section linked from cite blocks** — `methodology.md#citation` exists and explains the cite format. The methodology doc is on Day 19 of the existing plan; this only needs ~30 min of additional copy.

### Phase D — Zenodo integration (~1.5 days)

D.1. **Zenodo account + sandbox account.** Verify upload works manually with the sandbox before automating. ~2 hours.

D.2. **`release.yml` GitHub Actions workflow.** Triggered on tag push. Uses Zenodo REST API. Writes DOI back to `release_dois.json`. ~5 hours.

D.3. **Cite block reads DOI when available** — append the DOI line if `release_tag` has a DOI in `release_dois.json`. ~1 hour.

D.4. **First real release.** Tag `data-v2026.05.07`, watch the workflow, manually verify Zenodo deposit. ~2 hours.

### Phase E — Hardening (~1 day, optional)

E.1. **Hash-shortened URL aliases** for filter sets above the 1,500-char threshold. ~3 hours.

E.2. **Resolver page** at `/cite/<id>` that 302s to canonical URL. ~2 hours.

E.3. **CI test:** every contract in the pack has a `view_slug` and a non-empty manifest after a pipeline run. ~2 hours.

**Total: ~9–10 days of focused work for the v1 (Phases A–D).** Phase E is post-launch hardening.

---

## 12. Open decisions

These need answering before Phase A starts:

1. **Domain.** What's the public URL? `dailtracker.ie` is the assumed pitch but probably not registered. Citation IDs and permalinks both encode the host. Pick once; cite IDs *do not* include the host (`dailtracker:` not `dailtracker.ie:`) so a domain change is survivable.
2. **Release cadence.** Monthly? Per-source? Per-pipeline-run? The existing plan suggests monthly tagged releases ([§3.4](dail_tracker_improvements_v4.md#34-per-source-cadence-and-risk)). Confirm. Cadence determines how stale a typical citation will be.
3. **Concept DOI vs versioned DOI in cite blocks.** When a journalist cites a specific snapshot, should the displayed DOI be the versioned DOI (this exact release) or the concept DOI (always-latest)? Recommendation: versioned DOI as primary, concept DOI in the methodology page as "what to cite if you want the live dataset."
4. **What happens when a citation refers to a release whose data has been withdrawn for correctness?** Zenodo deposits cannot be unpublished, only superseded. The cite block on the dashboard should display a warning banner "this data was superseded on YYYY-MM-DD; see [link]" when an `as_of=` resolves to a superseded release. Behaviour for the `/cite/<id>` resolver: redirect to canonical URL but include `superseded=1` query param so the page renders the banner.
5. **Mart slug stability.** Several SQL views are at v1 names (`v_lobbying_summary`, etc.). If they get renamed during the v4 dim/fact refactor, do we want to *also* keep the old slug working? Yes — append-only slugs from day one. Mark the file rule explicitly in [`PIPELINE_VIEW_BOUNDARY.md`](../dail_tracker_bold_ui_contract_pack_v5/docs/PIPELINE_VIEW_BOUNDARY.md).
6. **CSV-on-the-fly vs pre-computed CSV.** Pre-computing every CSV inflates the data branch by ~2x. CSV-on-the-fly is slower (seconds for big marts) but cheaper. Recommendation: pre-compute only the three smallest, most-likely-to-be-shared marts; on-the-fly for everything else.
7. **Whether to expose the silver layer.** v1 says no — silver is for the pipeline, gold is for users. But journalists writing technical investigations will want it. Defer until first asked, then decide whether to expose by request or in `/data`.
8. **Author/attribution on the Zenodo deposit.** "Patrick Glynn" or a project name? Recommendation: project name as author, Patrick as contributor, until a fiscal sponsor is in place. Once an org backs the project, switch.

---

## 13. Integration with the 8-week plan

The existing [`SHORT_TERM_PLAN.md`](SHORT_TERM_PLAN.md) sequence needs three insertions and one re-order:

| Existing day | Add / change |
|---|---|
| Day 1.2 (manifest writer) | Extend manifest with `parquet_sha256`, `release_tag`, `view_slug`, `parquet_bytes`, `schema[]`. (~4h on top of existing.) |
| Day 4 (provenance helper) | Provenance helper *also* exposes the cite-block payload from the same manifest fields. ~2h on top. |
| Day 5 (wire helpers on 3 pages) | Page-level cite button is wired alongside the freshness badge from day one. ~2h on top. |
| **NEW: Day 17.5** (insert between rearch and versioned releases) | Phase A.4–A.5 + Phase B (URL encoding + cite block component + per-chart wiring). ~2 days. |
| Day 18 (versioned releases) | Implement Phase D (Zenodo) as part of the same day. ~+0.5 day. |
| **NEW: Day 18.5** (between Zenodo and methodology) | Phase C (`/data` page). ~2 days. |
| Day 19 (methodology doc) | Add `#citation` section to `methodology.md` explaining cite format and verification workflow. |
| Day 20 (journalist email) | The email *includes* a cite-block-formatted reference to the most damning chart. This is the single biggest reason to do this work before Day 20, not after. |

Net effect: the 8-week plan extends by ~3–4 days, and the journalist email on Day 20 gets dramatically more credible.

If schedule is tight, defer *only* Phase D (Zenodo). Phases A–C are the value; Zenodo is the polish on top. A cite block with a permalink + ID + hash but no DOI is still hugely useful. Add Zenodo in Phase 2 (post-Week 4) without breaking anything.

---

## 14. What this plan deliberately does not solve

- **Real-time alert subscriptions** ("notify me when TD X has a new lobbying contact"). Different feature, different audience.
- **Dataset diff tooling** ("what changed between v2026.04.07 and v2026.05.07?"). Highly desirable for journalists but a separate project — at minimum a couple of weeks. Mention in the methodology doc as future work.
- **Per-cell or per-row citation.** "Cite this number" is a different UX problem and needs an entirely different UI affordance. v2 territory.
- **Embeddable iframes.** Listed in the audit as an opportunity but is a separate feature requiring different security review (CSP, CORS, sandboxing). Defer to Phase 3.
- **Annotation / markup.** Letting journalists annotate a chart is its own product. Out of scope.
- **API surface.** A REST or GraphQL API on top of the marts is a different product. The parquet downloads + permalinks cover ~90% of the API use case for ~5% of the cost.

---

## Cross-references

- Audit and rationale: [`feedback.md`](../feedback.md)
- Roadmap context: [`dail_tracker_improvements_v4.md`](dail_tracker_improvements_v4.md) — esp. §3.5 (manifests), §3.3 (versioned releases), §13 (trust and methodology)
- 8-week sequence: [`SHORT_TERM_PLAN.md`](SHORT_TERM_PLAN.md) — Day 1.2, 4, 5, 17–20
- Pydantic boundary example: [`pipeline_sandbox/pydantic_manifest_example.py`](../pipeline_sandbox/pydantic_manifest_example.py)
- Data caveats (linked from cite blocks): [`DATA_LIMITATIONS.md`](DATA_LIMITATIONS.md)
- Existing query-param usage: [`utility/pages_code/member_overview.py:567`](../utility/pages_code/member_overview.py#L567)
