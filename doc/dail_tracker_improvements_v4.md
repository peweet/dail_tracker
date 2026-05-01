# dail_tracker — improvements roadmap (v4)

_Last revised: 2026-04-30. Supersedes `dail_tracker_improvements_v3.md`._

This is the live improvement roadmap. It is opinionated, dated, and reconciled with the project as it actually stands today.

**What this doc is.** A comprehensive list of improvements across the whole project — robustness, modelling, performance, UI, ops, distribution, trust, developer experience, and sustainability. It is meant to be useful as both an in-order to-do list and as occasional reading for thinking about where to push next.

**What this doc is not.** A catalogue of new datasets to add. That lives in `ENRICHMENTS.md` and is deliberately kept separate so this roadmap stays focused on the existing surface and how to make it solid.

---

## Table of contents

0. [Status snapshot](#0-status-snapshot-april-2026)
1. [Architectural principles still load-bearing](#1-architectural-principles-still-load-bearing)
2. [Honest readiness picture](#2-honest-readiness-picture)
3. [Auto-refresh — the biggest single unlock](#3-auto-refresh--the-biggest-single-unlock)
4. [Pipeline rearchitecture — public-safe ingestion](#4-pipeline-rearchitecture--public-safe-ingestion)
5. [Pipeline robustness](#5-pipeline-robustness)
6. [Tests, CI, dependency hygiene](#6-tests-ci-dependency-hygiene)
7. [Data modelling](#7-data-modelling)
8. [Performance](#8-performance)
9. [UI / UX maturity](#9-ui--ux-maturity)
10. [Observability and ops](#10-observability-and-ops)
11. [Security and licensing](#11-security-and-licensing)
12. [Distribution and citation](#12-distribution-and-citation)
13. [Trust and methodology](#13-trust-and-methodology)
14. [Developer experience](#14-developer-experience)
15. [Sustainability and bus factor](#15-sustainability-and-bus-factor)
16. [Hosting and cost](#16-hosting-and-cost)
17. [AI-assisted development discipline](#17-ai-assisted-development-discipline)
18. [Recommended 90-day sequence](#18-recommended-90-day-sequence)
19. [Reading list](#19-reading-list)
20. [What changed in this rev](#20-what-changed-in-this-rev)

---

## 0. Status snapshot (April 2026)

### What's shipped since earlier revisions

- **Pipeline orchestration is real.** `pipeline.py` runs end-to-end. Module-level execution has been removed. `pipeline_sandbox/` holds in-flight enrichments without polluting the main pipeline.
- **Medallion is implemented.** `data/bronze/`, `data/silver/`, `data/gold/parquet/` exist with stable outputs. `GOLD_LAYER_NOTES.md` documents the gold layer.
- **SQL-first analytical layer.** Around 30 registered views in `sql_views/` (attendance, payments, lobbying, votes, legislation, interests). DuckDB is the analytical engine.
- **Page contracts in place.** `dail_tracker_bold_ui_contract_pack_v5/` defines per-page YAML contracts, agents, and skills. The UI is being driven from contracts, not ad-hoc Python.
- **Streamlit is mostly thin.** Eight pages: attendance, member overview, votes, interests, payments, lobbying, legislation, committees. Heavy logic is in views.
- **Honest caveat documentation.** `DATA_LIMITATIONS.md` is unusually thorough and is the single biggest trust asset the project has.
- **A working bold-UI redesign loop.** Skills for `bold-redesign-page`, `civic-ui-review`, `shape`, `streamlit-frontend` exist and are being used.

### What's still missing — and matters

1. No public deployment. Nothing is live. There is no feedback loop with users.
2. No automated refresh. Every dataset is refreshed manually.
3. No CI. No GitHub Actions. No automated tests on push.
4. PDF parsers are unprotected by golden-file regression tests.
5. Lobbying ingestion is manual CSV — the most analytically valuable source has the most fragile path.
6. Fuzzy join keys are still primary in some paths; `match_method` and confidence are not first-class.
7. Provenance is documented in markdown but not surfaced in the UI from manifests.
8. Bus factor of one. No contributor onboarding doc; no scheduled refresh = no dead-man's-switch if attention lapses.

### What this rev is structured around

The asymmetry to internalise: **the analytical layer is materially closer to beta than the operational layer is.** The fastest readiness gains come from operationalising what already works, not from building new analytical surfaces. v4 is structured accordingly — robustness, ops, distribution, trust come first; new dataset ideas live in `ENRICHMENTS.md`.

---

## 1. Architectural principles still load-bearing

These have not changed and should not be relitigated. The corresponding skills and contracts already enforce most of them.

### 1.1 Thin Streamlit, SQL-heavy

> A Streamlit page should mostly contain `SELECT`, `filter`, `display`, `download`.

If a page is doing joins, name normalisation, deduplication, ranking, or metric definition, that logic belongs in SQL or the pipeline. The contract pack flags this with `business_logic_in_page: forbidden`; the `civic-ui-review` skill enforces it.

### 1.2 Page contracts are the AI-facing API

Each page is governed by a small YAML contract under `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/`. AI page generators read the contract, not the warehouse. Token cost stays manageable; metric definitions don't drift.

A good contract names: source view, grain, primary key, allowed filters, display columns, charts, export options, provenance fields. A bad contract starts encoding join logic — that's a smell, push it into SQL.

### 1.3 Provenance is data, not prose

Provenance should live in tables (`meta_pipeline_runs`, `meta_dataset_registry`, `meta_dataset_lineage`, `meta_source_registry`) and per-mart manifests (`*.manifest.json`). The UI reads from manifests; it does not hard-code freshness, run IDs, or caveats.

This is partially done. Manifests are not yet emitted for every mart (§3.5).

### 1.4 What stays where

| Layer | Owns |
|---|---|
| Pipeline (Python/Polars) | Source ingestion, normalisation, entity resolution, parser logic |
| SQL views (DuckDB) | Joins, aggregations, metric definitions, ranking, marts |
| YAML contracts | Page structure, filters, charts, columns, provenance fields |
| Streamlit | Layout, widget placement, presentation, tabs, expanders |
| Skills + agents | Contract enforcement, UI review, pipeline-view boundary |

### 1.5 The pipeline-sandbox rule

`pipeline_sandbox/` is for new Python/Polars enrichment work. It graduates into `pipeline.py` only after a fixture test, a manifest writer, and a registered SQL view exist. New SQL views go directly to `sql_views/`. The core `pipeline.py`, `enrich.py`, `normalise_join_key.py` are change-controlled.

---

## 2. Honest readiness picture

| Layer | Maturity (1–5) | Confidence | Notes |
|---|---|---|---|
| Vision and scope | 4 | High | Civic mission, well-bounded; current Dáil only. |
| Data sources and coverage | 3 | Medium | Lobbying manual; otherwise solid. |
| Pipeline robustness | 2 | Low | No fixture tests, no schedule, no quarantine flow. |
| Analytical layer (SQL) | 4 | High | View structure is sound; ~30 registered views. |
| UI / dashboard | 3 | Medium | Actively refactoring under contract pack v5. |
| Trust and provenance docs | 4 | High | DATA_LIMITATIONS.md is a real asset. |
| Tests / CI / Ops | 1 | Low | Partial tests, no CI, no deploy. |
| Distribution / citation | 1 | Low | No release artefacts, no permalinks, no API. |
| Sustainability / bus factor | 1 | Low | Single maintainer, no onboarding. |

The asymmetry: vision and analytics are 4/5; everything operational is 1–2/5. v4 is built around closing that gap.

---

## 3. Auto-refresh — the biggest single unlock

Auto-refresh is the single largest readiness lift available right now. It transforms a hand-cranked artefact into something that survives weeks of inattention. The mechanics matter, so this section is concrete.

### 3.1 What "auto-refresh" must mean here

Four kinds of source:

| Source kind | Examples | Auto-refresh feasibility |
|---|---|---|
| Public REST/JSON API | Oireachtas API (members, legislation, questions, votes, debates) | High — no auth, stable schemas |
| Public PDF, predictable URL | Attendance, payments, interests | Medium — URLs stable but layout drift risk |
| Public PDF, varying URL | Some interests/payments PDFs | Medium — needs `pdf_endpoint_check` to discover |
| Manual CSV | lobbying.ie | Low without scraping work |

Auto-refresh = on a schedule, regenerate silver/gold parquet from upstream, write back into the deployed app, and keep a structured record of what happened.

### 3.2 Reference architecture (cheap)

```text
GitHub repo (main)
  └─ pipeline code, sql_views, page contracts, Streamlit app

GitHub Actions (cron + workflow_dispatch)
  ├─ pulls API + PDFs
  ├─ runs pipeline.py
  ├─ runs DuckDB SQL views to materialise parquet marts
  ├─ writes/updates per-mart manifest.json
  ├─ writes meta_pipeline_runs row
  └─ commits regenerated parquet to a `data` branch

Streamlit Community Cloud
  ├─ tracks `main` for app code
  └─ tracks `data` branch for parquet (or pulls at start)

Hugging Face Datasets (optional, for size headroom)
  └─ mirrors the parquet for size-bound deploys
```

The key separation: **app code on `main`, data on `data` branch.** This keeps git history readable, makes data-only rebuilds cheap, and lets a bad refresh be rolled back without touching code.

### 3.3 GitHub Actions skeleton

`.github/workflows/refresh.yml`:

```yaml
name: Refresh data

on:
  schedule:
    # Mondays at 04:00 UTC. Tighten cadence per source under §3.4.
    - cron: '0 4 * * 1'
  workflow_dispatch:
    inputs:
      sources:
        description: 'Comma-separated source slugs (default: all)'
        required: false
        default: 'all'

permissions:
  contents: write

jobs:
  refresh:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - run: pip install -e ".[dev,db]"

      - name: Run pipeline (refresh-only)
        env:
          DAIL_REFRESH_SOURCES: ${{ github.event.inputs.sources || 'all' }}
        run: python pipeline.py --refresh

      - name: Write run manifest
        run: python -m utility.tools.write_run_manifest

      - name: Commit refreshed data to data branch
        run: |
          git config user.name "dail-tracker-bot"
          git config user.email "dail-tracker-bot@users.noreply.github.com"
          git fetch origin data:data || git checkout --orphan data
          git checkout data
          git checkout main -- data/ sql_views/
          git add data/gold/parquet/*.parquet \
                  data/silver/parquet/*.parquet \
                  data/gold/parquet/*.manifest.json
          if git diff --staged --quiet; then
            echo "No data changes."
          else
            git commit -m "chore(data): scheduled refresh $(date -u +%Y-%m-%dT%H:%MZ)"
            git push origin data
          fi
```

The two referenced things — `pipeline.py --refresh` and `utility/tools/write_run_manifest.py` — don't exist yet and are the first concrete tasks (§18).

### 3.4 Per-source cadence and risk

| Source | Cadence | Risk | Notes |
|---|---|---|---|
| Oireachtas API (members, legislation, questions, votes, debates) | Daily | Low | Add proper pagination loops where `limit=…` is still used. |
| Attendance PDFs | Weekly | Medium | New PDFs publish ~weekly while Dáil sits. Schedule after publication day. |
| Payments PDFs (PSA) | **Monthly** | Medium | Each PDF covers one data month with a 31–46 day publication lag (median 32). Run discovery ~35 days after end of data month, retry weekly on miss until 60 days. See [`pipeline_sandbox/payment_pdf_discovery_notes.md`](../pipeline_sandbox/payment_pdf_discovery_notes.md). |
| Interests PDFs | Annual | High | Layout drift between years; needs golden-file test before each annual run. |
| Lobbying.ie | Tri-annual + ad-hoc | High | Currently manual. Build a Playwright job that mimics the export form; commit CSVs to bronze. Treat as best-effort, not blocking. |

### 3.5 Manifests and freshness UI

Each mart writes a manifest beside its parquet:

```json
{
  "dataset_name": "mart_member_overview",
  "layer": "mart",
  "grain": "one row per member",
  "built_from": ["dim_member", "fact_attendance", "bridge_lobby_return_member"],
  "row_count": 174,
  "run_id": "2026_04_30_001",
  "git_commit": "c7a67de",
  "source_versions": {
    "oireachtas_api": "fetched 2026-04-30T04:12Z",
    "attendance_pdfs": "covers 2024-01..2026-04",
    "lobbying_ie": "manual export 2026-03-10"
  },
  "caveats": [
    "Lobbying counts include only returns where a member name resolved.",
    "Payments aggregate excludes quarantined rows; see payments_quarantined.parquet."
  ]
}
```

Every page renders a "Source and methodology" expander **populated from the manifest**, not hard-coded. ~30 lines of helper, reused across pages. This is the single biggest credibility upgrade for a journalist user.

### 3.6 Failure handling

A scheduled refresh that fails silently is worse than no refresh. The workflow must:

1. Fail loudly per-source, succeed in aggregate. A broken interests PDF parser must not block attendance refresh. Use try/except with a structured error log written into `data/_run_errors.json`.
2. Open a GitHub issue on failure (`gh issue create`).
3. Surface staleness in the UI. Freshness badge per page goes amber if a relevant source has not refreshed in N expected cycles.
4. Always emit a `meta_pipeline_runs` row including partial-failure status.

### 3.7 What auto-refresh does NOT solve

Schema drift. A new column appearing in the Oireachtas members API will not break the workflow but will silently fail to populate downstream tables. That is what schema validation (§6.5), row-count drift assertions (§5.3), and the quarantine flow (§5.5) are for. Add those at the silver write step before adding more sources.

---

## 4. Pipeline rearchitecture — public-safe ingestion

The pipeline today is an artisanal craft: the maintainer triggers refreshes, manually places PDFs in folders, and the deployed app (eventually) reads the resulting parquet. That works for one developer. It fails the moment the project is public, forked, or runs unattended for a week. This section is the shape-change required to operate publicly without becoming a problem for upstream services.

This section sits between §3 (auto-refresh, the *when*) and §5 (pipeline robustness, the *how-not-to-break*). It covers the *where* and *who*: where the scraping code runs, who/what identifies as the requester, and what guarantees the architecture provides upstream and downstream.

### 4.1 Why this needs to change before going public

Three risks compound:

1. **Fork amplification.** If ten people fork the repo and run the cron against `data.oireachtas.ie`, that's 10× the upstream load you cause, with no coordination or backoff. Government services notice patterns; a project that gets named in an upstream incident report does not recover its reputation. Mitigation has to be structural — the README warning is necessary but insufficient.
2. **Operator-as-critical-path.** Manual PDF drag-into-folder makes the maintainer a single point of failure. The DORA / Accelerate research is unambiguous: manual handoffs in delivery pipelines reduce reliability and increase recovery time. A civic-data project that requires the maintainer's attention to ingest is a project that will silently rot.
3. **No identifiable bot.** Anonymous scraping looks indistinguishable from abuse. Government services correctly block what they can't identify or contact.

These three things are not solved by §3 auto-refresh alone. Auto-refresh is *when*; this section is the *what runs where, and how it announces itself*.

### 4.2 Layer A — Publish, don't crawl (separate ingest from app)

The largest amplifier is downstream consumers re-doing upstream work. The fix is structural separation:

```text
canonical refresher          published artefact            consumers
(GitHub Actions)             (versioned parquet on         (Streamlit app, forks,
       │                      Releases / HF Datasets)       researchers, journalists)
       │                              ▲                            │
       ▼                              │                            ▼
upstream sources                      └────── pull artefact, never upstream
(data.oireachtas.ie,
lobbying.ie, etc.)
```

**Implications for the codebase:**

- **All upstream-fetching code lives in one place.** Currently the scraping logic is spread across `pdf_endpoint_check.py`, `members_api_service.py`, `attendance.py`, `payments.py`, etc. Consolidate into `pipeline/sources/<source_name>.py` with a uniform interface. The deployed Streamlit app imports zero modules from `pipeline/sources/`.
- **The Streamlit app reads only published artefacts.** Right now `utility/data_access/*` is the right pattern; enforce it as a rule. No code path in the app should hit upstream.
- **The README explicitly tells forkers to consume the artefact.** "If you want a fresh copy of the data, pull our published parquet release. Do not run the refresher against `data.oireachtas.ie` from your fork. If you have a use case the published artefacts don't support, open an issue."
- **One canonical refresh location.** The cron lives in this repo's GitHub Actions, not in any fork. Forks that enable Actions and don't disable the workflow are the highest-risk fork-amplification path; the README addresses this and the workflow itself can detect "running in non-canonical repo" and refuse to scrape upstream (only allow local fixture data).

This is mySociety's EveryPolitician model in practice: one canonical scrape, many downstream consumers, the artefact is the public surface.

### 4.3 Layer B — Web-citizenship hygiene

Every outbound request from the canonical refresher must:

- **Identify.** `User-Agent: dail-tracker-bot/<version> (+https://github.com/<you>/dail-extractor; mailto:<you>)`. RFC 9110 convention. Anonymous scrapers get blocked; identified, contactable bots get tolerated and often whitelisted.
- **Use conditional GET.** `If-Modified-Since` from the last seen `Last-Modified`, `If-None-Match` from the last seen `ETag`. Most government CDNs honour these. A 304 response means we transferred zero bytes of payload. This is the single biggest reduction in upstream load you can make without changing what you fetch.
- **Throttle with jitter.** Max 1 req/sec per host, 0–500ms random jitter, exponential backoff on 429/503. Use `tenacity` or equivalent. Document the throttle policy as data, not code, so upstream operators can read it.
- **Respect `robots.txt`.** `urllib.robotparser` once per session per host. If a path is excluded, do not fetch it — full stop, regardless of whether the data would be useful.
- **Time out aggressively.** A 30-second connect timeout and a 60-second read timeout. Hanging connections compound across a refresh and look like abuse.

This is the [IIPC web archiving best practices](https://netpreserve.org/web-archiving/) checklist. None of it is novel; the value is in not skipping any item.

### 4.4 Layer C — Upstream coordination

Before the project is publicly named:

- **Email Oireachtas Information Service.** "We're building a public-interest dataset that consumes data.oireachtas.ie. Here's the load profile: HEAD checks of N URLs daily, full PDF fetches of M URLs weekly with conditional GET. Here's our User-Agent. Here's the contact. Please tell us if our load profile becomes a problem."
- **Same for lobbying.ie.** Their team has dealt with academic/journalism projects before and has process for this.
- **Consider applying to be on the Oireachtas Open Data partner page** if such a thing exists. Being named upstream is the strongest possible legitimisation.

The cost is one email per source. The benefit is that when a refresh accidentally goes wrong (and it will), you have a contact who knows the project and won't reach for the IP block first.

### 4.5 Hash-based change detection for static assets

A second-order architecture choice. PDFs are large; re-parsing every refresh is wasteful and noisy. Instead:

```text
weekly refresh:
    for each known PDF URL:
        HEAD with If-Modified-Since
        if 304:                  # unchanged since last fetch
            log "unchanged"; continue
        if 200:
            GET; sha256(body)
            if hash matches stored hash:
                log "byte-identical"; continue
            else:
                store new hash and timestamp
                trigger re-parse
                emit "republished" event for downstream visibility
```

This pattern handles two cases at once:
- **Routine non-change.** Most PDFs don't change between refreshes. We don't pay parsing cost for them.
- **Silent re-publication.** Upstream republishes a corrected PDF with the same URL. Our hash differs from the stored hash, so we detect the correction and re-parse.

Treating PDFs as "fetch always, parse on hash-change" is the right operational shape. It's cheaper than re-parse-everything and stricter than parse-once-and-forget.

### 4.6 Pluggable scraper interface

Right now each source has its own ingestion code with no shared shape. As sources multiply (auto-refresh in §3, Iris Oifigiúil for judicial appointments in `ENRICHMENTS.md`, eventually SIPO and others), the lack of a shared interface becomes a maintenance problem.

The Open Civic Data project's `pupa` framework solved the same problem for ~50 US state legislatures. The pattern, in shape:

```text
class SourceScraper:
    name: str
    cadence: Cadence            # daily | weekly | quarterly | annual
    robots_check_url: str

    def discover(self) -> Iterator[AssetRef]:
        """Return URLs/identifiers of assets this source has, since last run."""

    def fetch(self, ref: AssetRef) -> RawAsset:
        """Conditional GET + hash check + timestamping."""

    def parse(self, asset: RawAsset) -> Iterator[Record]:
        """Parser-specific logic; output validates against pandera schema."""
```

This is *not* an immediate priority. It's where the architecture should converge as new sources are added. Adopting it for the existing sources first is overkill. Adopting it for the *next* new source is the right time.

### 4.7 Notification pattern (privacy-aware, low-maintenance)

The pipeline needs to tell the maintainer when something's wrong. The obvious answer — a transactional email service like SendGrid or Mailgun — is the wrong answer for a solo, indefinitely-maintained civic-data project. Three real costs:

1. **Account paper trail.** Signing up for any SaaS email provider ties a personal email to a third-party data broker. Free tier doesn't reduce the exposure. For a privacy-conscious project this is a meaningful cost with no offsetting benefit.
2. **Ongoing tuning.** Email noise leads to email apnea. Threshold tuning is real maintenance work; hobby projects that need maintenance to make notifications useful end up with neither working.
3. **Failure mode of the failure mode.** SaaS free tiers change, get rate-limited, or disappear entirely. The alerting infrastructure now has its own ops burden.

The right pattern for this project — and for almost every solo civic-data project I've seen converge — is a three-tier stack of free, low-config, no-account services.

#### Tier 1 — GitHub Issues as the primary alarm

The pipeline opens an issue when something fails. GitHub's existing per-user notification system handles delivery. No new SaaS, no API keys, no email templates.

```yaml
# Inside the refresh workflow, after a failed step
- name: Open issue on failure
  if: failure()
  uses: actions/github-script@v7
  with:
    script: |
      github.rest.issues.create({
        owner: context.repo.owner,
        repo: context.repo.repo,
        title: `[refresh-failed] run ${context.runId}`,
        body: `Logs: ${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`,
        labels: ['refresh-failed', 'autogen']
      })
```

Why this works for this project:

- Notifications flow through the maintainer's existing GitHub profile email (which can be set to `username@users.noreply.github.com` — zero net exposure).
- Issues self-document and can be labelled, batched, closed.
- Per-repo notification settings let you mute the firehose if a parser is repeatedly failing while you fix it.
- Same pattern used by `mysociety/parlparse`, the per-state `opencivicdata` scrapers, and most small-team civic-data projects.

This handles: refresh failed, parser regression detected, schema drift caught, row-count drift exceeded threshold, freshness SLO past fail threshold.

#### Tier 2 — Healthchecks.io as the dead-man's-switch

Tier 1 only fires if a run actually starts. If the cron itself stops firing (workflow disabled, repo dormant, GitHub Actions account issue), no failures get reported because no run happens. Healthchecks.io solves this.

- Free tier, **anonymous** (no signup required for the basic free tier — UUID-keyed ping URL is the auth).
- Pipeline does `curl -fsS -o /dev/null https://hc-ping.com/<uuid>` at the start and end of each run.
- If Healthchecks doesn't see a ping within the configured interval, *they* notify you (email, webhook, Discord, ntfy).
- "Expected interval" is set on Healthchecks's side, not in the repo, so adjusting cadence doesn't require a code change.

Privacy: use a throwaway email purely for delivery, or chain webhook → ntfy.sh for zero-account usage.

This handles the one failure mode Tier 1 can't catch: **the pipeline silently stopped running entirely.**

#### Tier 3 — ntfy.sh for optional push

For the "I want to know within minutes, not whenever I next check email" cases. Cost: zero, no account.

```bash
curl -d "Pipeline broke on run $RUN_ID" \
     -H "Tags: warning" \
     ntfy.sh/<random-uuid-only-you-know>
```

Install the ntfy app on your phone, subscribe to the topic, done. No email ever involved.

Privacy mode: the topic name is the only auth, so pick a UUID-grade random string and treat it as a soft secret. Self-hosting ntfy is also one Docker container if you want stronger guarantees, but the public ntfy.sh free tier with a random topic is fine for this scale.

Use ntfy for: weekly live-canary "system alive" beat (one push per week confirming the upstream-facing parts still work), critical-path errors you want to see immediately.

#### What each tier covers

| Failure mode | Tier 1 (GH Issues) | Tier 2 (Healthchecks) | Tier 3 (ntfy) |
|---|---|---|---|
| Pipeline run failed mid-run | ✓ | — | optional |
| Schema drift detected | ✓ | — | — |
| Parser regression (golden-file mismatch) | ✓ | — | — |
| Row-count drift > threshold | ✓ | — | — |
| Freshness SLO exceeded | ✓ | — | — |
| Cron stopped firing entirely | — | ✓ | — |
| Weekly canary "system alive" | — | — | ✓ |

Three components, all free, all anonymous-or-low-signup, no SaaS holding anyone's details.

#### Upstream-facing contact (different concern)

The User-Agent string in §4.3 advertises a contact email so upstream operators can reach the maintainer if our scraper misbehaves. That email is **public-facing by design** — it's the abuse-contact. Practical options:

- A `+` alias on a personal address: `you+dail-tracker@gmail.com`.
- A throwaway address used only for this purpose.
- A project-domain address if you ever buy a domain (overkill now).

Don't put a primary personal email there. The User-Agent contact is for upstream operators; the GitHub-Issues notifications are for you.

#### When (if ever) to upgrade

The SendGrid/Mailgun pattern is right when:
- You have teammates without GitHub access.
- You need rich HTML email digests.
- You're running at a scale where Healthchecks's free tier doesn't cover your monitoring needs.

None of those apply to a solo civic-data project. The three-tier stack above can run untouched for years. **Don't reach for SaaS email until you've outgrown the free stack — which for this project is unlikely.**

### 4.8 The TODO list

In implementation order. None of these is research-grade work; the value is in actually doing them, not in the cleverness.

#### Pre-flight (do before any of the below)
- [ ] Decide on the published artefact location: GitHub Releases vs HF Datasets vs Cloudflare R2. Default: GitHub Releases for now (simplest). HF Datasets if size becomes a problem.
- [ ] Decide on the bot identity string. Format: `dail-tracker-bot/0.1 (+https://github.com/<you>/dail-extractor; mailto:<you>)`. Commit it to a constant.
- [ ] Email Oireachtas Information Service and lobbying.ie. One-paragraph notice. Wait at least one cycle for replies before going public.

#### Layer A (publish-don't-crawl)
- [ ] Audit all imports in `utility/`. Confirm zero references to scraping/fetching code. Refactor any violators.
- [ ] Move all upstream-fetching code into `pipeline/sources/<source>.py`. One file per source. Common HTTP helper above.
- [ ] Add a `data` branch (or equivalent) and the GitHub Actions workflow from §3.3.
- [ ] Tag the first parquet release: `data-v2026.05.07` (or similar). Confirm the Streamlit Cloud deploy reads it.
- [ ] Update README with the explicit "do not run scrapers from forks" notice. Link to the published artefact.

#### Layer B (web citizenship)
- [ ] Single HTTP helper (`pipeline/sources/_http.py`) that all source modules use. It owns: User-Agent, conditional GET, throttle, jitter, robots.txt check, retry/backoff, timeouts.
- [ ] Replace direct `requests.get` calls in source modules with the helper. One PR per source.
- [ ] Log every request with: URL, method, status, content-length, last-modified, etag, run_id. Structured JSON.
- [ ] Add a "respected robots.txt" assertion to each source's logs — no source's first request goes out without a successful robots.txt parse.

#### Layer C (upstream coordination)
- [ ] Send the introductory emails (see Pre-flight).
- [ ] Document responses, contacts, and any agreed rate-limit allowances in `doc/source_licensing.md` (referenced in §11.3).
- [ ] Add upstream contact info to each source manifest. The provenance footer (§9.1) surfaces it.

#### Hash-based change detection
- [ ] Add `assets_hash_index.parquet` to bronze: one row per source-asset URL with last fetched timestamp, last hash, last status code.
- [ ] Update each source's fetch step to: HEAD with conditional GET → if 304, skip; if 200, GET, hash, compare, update.
- [ ] Emit "republished" events to the run log when a hash changes despite an unchanged URL.

#### Notifications (Tier 1 — GitHub Issues)
- [ ] Add the `if: failure()` step to each refresh workflow that opens an Issue with the run ID and logs URL.
- [ ] Add a `refresh-failed` label to the repo's label set.
- [ ] Confirm GitHub notification settings: per-user email is set to `<username>@users.noreply.github.com` if zero exposure is desired.
- [ ] Decide on per-failure-type labels (`schema-drift`, `parser-regression`, `freshness-slo`, `row-count-drift`) so issues self-classify.
- [ ] Auto-close issue when the next successful run for the same source completes (optional; can also be manual).

#### Notifications (Tier 2 — Healthchecks.io)
- [ ] Sign up for Healthchecks.io free tier (or use anonymous UUID-only mode).
- [ ] Generate one ping URL per source (payments, attendance, interests-dail, interests-seanad). One per source means each source has its own dead-man's-switch.
- [ ] Add `curl -fsS -o /dev/null https://hc-ping.com/<uuid>` to the start and end of each refresh workflow.
- [ ] Configure each ping URL's expected interval to match the source's cadence (monthly, weekly, annual).
- [ ] Configure Healthchecks notification destination (email, Discord webhook, ntfy, whatever the maintainer prefers).
- [ ] Store ping UUIDs as GitHub repo secrets (`HEALTHCHECK_PAYMENTS_UUID`, etc.).

#### Notifications (Tier 3 — ntfy.sh, optional)
- [ ] Pick a UUID-grade random topic name; treat it as a soft secret.
- [ ] Install ntfy app on phone, subscribe to topic.
- [ ] Add a `curl -d "<message>" ntfy.sh/<topic>` step to the live-canary workflow for the weekly "system alive" beat.
- [ ] Optionally route Tier 1 ERROR events through ntfy as well, for push-to-phone on critical failures.
- [ ] Store topic name as a GitHub repo secret (`NTFY_TOPIC`).

#### Upstream-facing contact (different from notifications)
- [ ] Decide on the public abuse-contact email used in the User-Agent (see §4.3). Use a `+` alias or throwaway, not a primary personal address.
- [ ] Confirm the chosen address is monitored — upstream operators occasionally use it.

#### Pluggable interface (deferred)
- [ ] Not now. Adopt when adding the next new source — apply it there first as a proof.

### 4.9 What this section deliberately does NOT do

- It does not pick a specific HTTP library. `requests` works. The shape matters; the library doesn't.
- It does not specify retry counts or throttle constants. Those are configuration, not architecture.
- It does not address authentication. None of our current sources require it.
- It does not address private data handling. There is none.
- It does not solve lobbying.ie's manual export problem. That requires a Playwright job (or eventual API access from the regulator); it sits in the auto-refresh per-source table in §3.4.

### Cross-references

- §3 covers *when* the refresher runs. §4 covers *how* it behaves while running.
- §11 (Security and licensing) overlaps on robots.txt and ToS. Treat §4.3 as the operational shape; §11 as the policy-level documentation. Don't duplicate; cross-reference.
- §12 (Distribution and citation) extends Layer A — versioned artefact releases are the citation surface.
- The §18 90-day sequence should fold these TODO items in.

---

## 5. Pipeline robustness

The pipeline runs but is unprotected. These items make it resilient to upstream change.

### 5.1 Schema validation at silver writes

Use pandera (or pydantic for less-tabular cases) at every silver-write step. Fail loudly when a column type drifts or a non-null column starts allowing nulls. Currently a drift would propagate silently into gold.

```python
from pandera.polars import DataFrameSchema, Column

ATTENDANCE_SILVER_SCHEMA = DataFrameSchema({
    "member_id": Column(str, nullable=False, unique=False),
    "sitting_date": Column("date", nullable=False),
    "present": Column(bool, nullable=False),
    "source_pdf": Column(str, nullable=False),
    "source_page": Column(int, nullable=False),
    "parser_version": Column(str, nullable=False),
})
ATTENDANCE_SILVER_SCHEMA.validate(df)
```

Schemas live next to the pipeline module that writes them, not in a central registry — that way drift causes a noisy local failure, not a global breakage.

### 5.2 Golden-file PDF regression tests

Pick one representative PDF per parser (attendance, payments, interests, sponsors). Commit it to `test/fixtures/`. Commit the expected silver output beside it. Run on every PR.

```python
def test_attendance_parser_2024_q1():
    out = parse_attendance(Path("test/fixtures/attendance_2024_q1.pdf"))
    expected = pl.read_parquet("test/fixtures/attendance_2024_q1.expected.parquet")
    pl.testing.assert_frame_equal(out, expected)
```

This is the single most valuable test in the project. It will catch parser regressions that no schema check can.

The full fixture-creation plan, per-source coverage strategy, and integration with CI cadences is in [`test/HANDS_OFF_TEST_PLAN.md`](../test/HANDS_OFF_TEST_PLAN.md) §4 (Phase B of the build-out checklist).

### 5.3 Row-count drift assertions

When a refresh produces ≥10% fewer rows than the prior run for any silver table, fail the run unless explicitly allowed. The threshold is tunable per source.

Stored beside each manifest:

```json
"row_count_history": [
  {"run_id": "...", "rows": 4221, "delta_pct": null},
  {"run_id": "...", "rows": 4334, "delta_pct": 2.7},
  {"run_id": "...", "rows": 3812, "delta_pct": -12.0}
]
```

That third row should hard-fail.

### 5.4 Source endpoint health checks

`pdf_endpoint_check.py` already exists. Extend it to check every endpoint the pipeline touches and run it as the first step of every refresh. A 404 on a known-good URL should not silently propagate to "0 rows extracted".

### 5.5 Quarantine flow for bad rows

Already partially exists for payments (`payments_quarantined.parquet`). Generalise:

- Every silver writer accepts a `quarantine` callback.
- Rows that fail validation go to `data/silver/_quarantine/<source>_<run_id>.parquet` with the failed-rule annotation.
- A nightly summary opens an issue if any source has more than N quarantined rows in a run.
- Quarantined rows never silently disappear.

### 5.6 Parser idempotency

Every parser should produce identical output for identical input. This is true today for most modules but is not asserted. A simple test:

```python
def test_payments_parser_idempotent():
    out_a = parse_payments(FIXTURE_PDF)
    out_b = parse_payments(FIXTURE_PDF)
    pl.testing.assert_frame_equal(out_a, out_b)
```

Idempotency is the precondition for safely re-running refreshes.

### 5.7 Manifest discipline

Every gold mart writes a manifest at build time. A page that loads a mart without a manifest should warn loudly, not silently render with stale provenance. This is enforceable via a small helper that the page contract loader calls.

---

## 6. Tests, CI, dependency hygiene

These are small individually and large in aggregate.

### 6.1 Minimum viable CI

`.github/workflows/ci.yml`:

- `ruff check .`
- `ruff format --check .`
- `pytest test/`
- Page-import smoke: import every module under `utility/pages_code/` and ensure no top-level error.
- Schema diff: re-read each gold parquet and compare column types against a committed `schemas/*.json`.

Half a day to set up. Pays back forever.

### 6.2 Test layering

Three layers, each with a clear role:

- **Unit.** Pure functions: name normalisation, fuzzy-key generation, date parsing. Fast, no I/O.
- **Fixture.** Parser tests against committed PDF/CSV fixtures (§5.2).
- **Smoke.** Import every page; load every gold mart; run every SQL view against the committed parquet.

Don't have integration tests pulling live data — those belong in the scheduled refresh, not on every PR.

### 6.3 Pin and lock dependencies

`pyproject.toml` should have version specifiers. A lockfile (`uv.lock` or `requirements.txt` from `pip-compile`) should be committed. Run `pip-audit` weekly.

This is also the prerequisite for reproducible Streamlit Community Cloud deploys.

### 6.4 Pre-commit hooks

Minimum:

- ruff
- ruff-format
- detect-secrets (or an equivalent — accidental secret commits are easy)
- yamllint for page contracts

`pre-commit run --all-files` in CI catches what local hooks miss.

### 6.5 Type hints + pydantic

Add type hints to every public function. Use pydantic for config schemas, page contracts, and source manifests. This pays off in editor support and in catching contract drift at load time, not at use time.

### 6.6 Page-import smoke test

Every page in `utility/pages_code/` should import cleanly without any data being present. The contract loader, the page function definition, and the helpers should all stand up. This catches the most common Streamlit failure mode: a missing import or typo that only surfaces when the page is opened.

### 6.7 The hands-off test plan

The above are minimum CI requirements. The extended test plan for unattended operation — including discovery probe tests, golden-file fixtures per parser, source-side schema validation, row-count drift detection, freshness SLO checks, end-to-end smoke tests, and the Tier 1/2/3 notification scheme from §4.7 — is documented in [`test/HANDS_OFF_TEST_PLAN.md`](../test/HANDS_OFF_TEST_PLAN.md).

That doc has its own 8-phase build-out checklist (Phase A through Phase H) covering every test class needed for the pipeline to run untouched for 12+ months. It complements `test/TEST_SUITE.md` (which covers existing Pandera schema validation) rather than replacing it.

The high-level sequence:
- **Phase A** — discovery probe tests (offline construction, fixture-based index parsing, mocked-HTTP orchestration, weekly live canary)
- **Phase B** — golden-file parser tests for payments, attendance, interests
- **Phase C** — source-side JSON schema validation for the Oireachtas API
- **Phase D** — row-count drift assertions in silver writers
- **Phase E** — freshness SLOs with per-source thresholds
- **Phase F** — end-to-end smoke against committed fixture pipeline
- **Phase G** — notification wiring (GitHub Issues + Healthchecks.io + ntfy.sh per §4.7)
- **Phase H** — silence and tuning after 4 weeks of operation

---

## 7. Data modelling

The current model is medallion + ad-hoc gold marts. There is room for it to become more deliberate without becoming a dbt project.

### 7.1 Move toward dim/fact/bridge

Today there is a mix of "wide gold marts" (e.g. `enriched_td_attendance`) and "fact-shaped views" (e.g. `vote_member_detail`). The wide marts are convenient for one-off pages but become liabilities when a column needs to change.

Target shape:

- `dim_member`, `dim_party`, `dim_constituency`, `dim_government` — dimensions.
- `fact_attendance`, `fact_vote`, `fact_payment`, `fact_question`, `fact_bill_sponsorship` — facts at their natural grain.
- `bridge_lobby_return_member`, `bridge_member_committee` — many-to-many.
- `mart_*` views built on top of facts/dimensions for each page.

The migration is incremental — pick one wide mart per quarter and split it.

### 7.2 Replace fuzzy joins where canonical IDs exist

The Oireachtas API exposes a stable `pId` for each member. Use it as the primary key in `dim_member`. The sorted-character fuzzy key from `normalise_join_key.py` becomes a *fallback* match method, not the primary key, with confidence flag.

### 7.3 Match confidence as first-class

Every join that depends on name resolution should carry:

- `match_method`: `pid_exact` | `name_exact` | `fuzzy_sorted_char` | `manual`
- `match_confidence`: `high` | `medium` | `low`
- `match_evidence`: optional pointer to manual review note

Pages that count rows can filter by confidence. `low` matches go in a "review queue" view, not the main page.

### 7.4 Quarantine tables per source

§5.5 covers this in pipeline terms. The data-modelling consequence: every source has a paired `_quarantine` table that the page can expose under "data quality issues".

### 7.5 Slowly changing dimensions for member metadata

Members change parties, constituencies, and roles mid-Dáil. The current model implicitly snapshots latest state. For longitudinal analysis (especially once SIPO donations and historical voting are added), `dim_member_history` with valid-from / valid-to is the right shape. Add it before adding cross-cycle datasets, not after.

### 7.6 Surrogate keys vs natural keys

Use natural keys where they're stable (Oireachtas `pId`, lobbying.ie `primary_key`). Use surrogates only where natural keys are absent or unstable (judges, donors, charities). Document the choice per dimension in `meta_dataset_registry`.

---

## 8. Performance

Streamlit Community Cloud has constrained resources. This matters more than it should.

### 8.1 Push joins into DuckDB

Already mostly done. The remaining wide-mart pages (member overview, attendance overview) still do some joins in Python — finish moving them into views.

### 8.2 Pre-aggregate where it pays

If a chart is the same chart for every visitor, build it as a pre-aggregated view. The page filters columns; it does not recompute aggregations.

`payments_yearly_evolution.sql` is the right shape. `current_dail_vote_history.parquet` is too wide and gets re-aggregated by the votes page — that's a candidate for pre-aggregation.

### 8.3 Streamlit caching discipline

Two rules:

- `st.cache_resource` for the DuckDB connection. One per app.
- `st.cache_data(ttl=…)` for query results. TTL set to "as long as the data branch is unchanged" — typically 1 hour is fine.

Cached return values must be picklable. Returning a polars DataFrame is fine; returning a closure is not.

### 8.4 Parquet typing

Specify dtypes at write time. `int64` for counts, `int32` for years, `categorical` for low-cardinality columns (party, court, chamber). Compressed with zstd by default. This makes Streamlit page loads visibly faster and saves a meaningful chunk of repo size.

### 8.5 Limit page-time work

Rule of thumb: a page should do less than 500 ms of work between cached query and render. Anything more belongs in the SQL view.

---

## 9. UI / UX maturity

The contract pack and bold-redesign skill have done the architectural work; the remaining items are concrete user-facing fixes.

### 9.1 Provenance footer on every page (auto)

Helper: `render_provenance(manifest_path)`. Reads the manifest, renders an expander with: source mart, grain, last refresh, run ID, git commit, source versions, caveats. Wired into every page's last 3 lines.

### 9.2 Freshness badge per page

Top of every page: a small pill showing "Refreshed N days ago" with colour coding (green < 7d, amber 7–30d, red > 30d for sources expected to refresh more often than that).

### 9.3 Mobile responsiveness within Streamlit limits

Streamlit's mobile story is acceptable for narrow tables and metrics, painful for wide tables and side-by-side columns. Decisions:

- Wide tables: collapse to card layout under N pixels (existing card pattern in `shared_css.py`).
- Side-by-side columns: stack on narrow screens (Streamlit default is okay).
- Charts: explicit min-height; let Streamlit handle width.

This is a 2–3 day pass over all pages, not a structural change.

### 9.4 Accessibility audit

Run `axe-core` against the deployed app. The Streamlit baseline is decent but card patterns and custom CSS can break contrast and keyboard navigation. Track findings in an issue, fix highest-impact items first.

### 9.5 Page-mart-per-page rule

Every page reads from one named mart, ideally. Where a page reads two, that's a candidate for a unifying view. Where it reads three or more, the page is doing modelling work that belongs in SQL.

### 9.6 Cross-page navigation

A TD's name on the lobbying page should link to that TD on the member overview page. Today it doesn't. Cross-page navigation is the difference between "a dashboard" and "an explorer". Implementation: query-param-driven page state; helper for "this entity's profile URL".

### 9.7 Search across entities

A single global search box: TD names, lobbying organisations, bills, committees. Implemented as a small in-memory index built at app startup; click result → deep link to relevant page filtered to that entity.

### 9.8 Onboarding for first-time visitors

The first visit currently lands on the attendance page. That's not the strongest entry point. Either:

- A landing page that explains what the project is and links to two or three "good places to start" queries; or
- The member overview page as default with a short explainer banner.

### 9.9 Empty states and zero-result handling

Already partly done via `empty_state` helpers. Audit every page for: zero filter results, all-data-quarantined, source-currently-refreshing. Each should explain what happened and suggest what to do.

---

## 10. Observability and ops

Once the refresh is automated, the question becomes "is it healthy?" rather than "did I run it?".

### 10.1 Run summaries

Every refresh emits a single JSON document:

```json
{
  "run_id": "2026_04_30_001",
  "started_at": "2026-04-30T04:00:00Z",
  "finished_at": "2026-04-30T04:14:21Z",
  "git_commit": "c7a67de",
  "sources": {
    "oireachtas_api": {"status": "ok", "rows_added": 412},
    "attendance_pdfs": {"status": "ok", "rows_added": 0, "no_new_pdf": true},
    "lobbying_ie": {"status": "skipped", "reason": "manual"}
  },
  "errors": [],
  "warnings": ["lobbying_ie not refreshed in 51 days"]
}
```

Committed under `data/_run_summaries/run_*.json`. The UI surfaces the latest one on a "Pipeline status" page.

### 10.2 Structured logs with run_id

Every log line carries the `run_id`. Logs go to stdout (so GitHub Actions captures them) and to `data/_logs/run_*.jsonl`.

### 10.3 Error issue auto-creation

When a refresh has any error, the workflow opens a GitHub issue with the run summary attached. Triage from the issue.

This is **Tier 1** of the notification stack defined in §4.7. The full pattern (GitHub Issues + Healthchecks.io for dead-man's-switch + optional ntfy.sh for push) is described there. This subsection is the operational implementation detail; §4.7 is the policy.

### 10.4 Data freshness SLOs

Per source, a target cadence and a stale-warning threshold:

| Source | Target | Warn | Fail |
|---|---|---|---|
| Oireachtas API | Daily | 3 days | 7 days |
| Attendance PDFs | Weekly | 14 days | 30 days |
| Payments PDFs | Quarterly | 100 days | 180 days |
| Interests PDFs | Annual | 365 days | 540 days |
| Lobbying.ie | Tri-annual | 130 days | 200 days |

The freshness badge (§9.2) and the latest run summary use these thresholds.

### 10.5 A simple "Pipeline status" page

A dedicated Streamlit page showing: last run, per-source freshness, any open issues from the auto-creator, the last 30 days of runs as a sparkline. Targeted at the maintainer, but useful for journalists too — "is the data current as of when I'm writing my piece?".

---

## 11. Security and licensing

A civic-data project lives or dies on whether its handling of public data is defensible. None of these are heavy items; missing them is the risk.

### 11.1 Env vars and secrets

Move all environment-specific config to environment variables. Commit a `.env.example`. No secrets in the repo, no API keys in code.

### 11.2 Dependency scanning

`pip-audit` + Dependabot. Both run weekly.

### 11.3 Source licensing per dataset

`doc/source_licensing.md`: one row per source, with: licence type, attribution required, redistribution allowed, link to source's terms. The provenance footer (§9.1) reads from this.

### 11.4 GDPR-light considerations

The project handles public-record data about identified individuals (TDs). That is not a GDPR safe-harbour automatically; it relies on the public-interest journalism / democratic-accountability lawful bases. Document the position in `doc/data_protection_position.md`. Include:

- categories of personal data processed,
- lawful basis,
- data minimisation steps (e.g. addresses redacted from PDFs),
- subject rights handling,
- retention.

This is one document, not a project. A version of it should exist before the alpha is shared with a single user.

### 11.5 Robots.txt and ToS compliance

Each scrape job (lobbying.ie, Iris Oifigiúil, courts.ie) needs a documented check against the source's robots.txt and terms of service. The check goes into the source's manifest. If a source's terms exclude automated retrieval, that source does not get scraped — full stop.

### 11.6 Rate-limit and identify the bot

Every outbound request from the refresh sends a `User-Agent: dail-tracker-bot (https://github.com/...)` header and respects per-source rate limits. Don't be the project that gets a public records site to add a captcha.

---

## 12. Distribution and citation

Right now the project is a private dashboard. Distribution is what makes it useful to someone who isn't the maintainer.

### 12.1 Versioned data releases

Every Monday's refresh tags a release: `data-v2026.04.30`. The release contains:

- All gold parquets.
- All manifests.
- The run summary.
- A `RELEASE_NOTES.md` derived from `meta_pipeline_runs`.

This is what makes citations possible: "data as of release v2026.04.30" is a stable claim.

### 12.2 Permalinks per page state

Page state (filter values, sort order) lives in URL query params. A user can bookmark, share, and cite a specific view. Streamlit's `st.query_params` API handles this. Page contracts should declare which filters are URL-bindable.

### 12.3 Open data exposure

Three increasing levels:

1. **Parquet downloads.** Every gold mart is downloadable from the page footer. Already supported by Streamlit's CSV button; switch to parquet for typed downloads.
2. **DuckDB-WASM in the browser.** The whole gold dataset is small enough to ship to the browser. This unlocks "click here to query the data yourself" with no server.
3. **A read-only HTTP API.** Optional. Only worth doing if a user actually asks for it.

Levels 1 and 2 are cheap and high-leverage. Level 3 is not until someone needs it.

### 12.4 RSS / Atom for new events

A feed per dataset:

- New lobbying returns this week.
- New parliamentary questions this week.
- New attendance data published.
- (Once D.1 graduates) New judicial appointments.

Journalists who follow Irish politics can subscribe and skim. This is one of the highest leverage-to-effort ratios available.

### 12.5 Citation guidance

`doc/citation.md`: how to cite the project, including release version and access date. Crucial for academic and journalistic use; nobody cites what doesn't tell them how.

---

## 13. Trust and methodology

`DATA_LIMITATIONS.md` is engineer-quality. The trust gap is everything *between* that doc and the page.

### 13.1 Journalist-readable methodology

`doc/methodology.md`: one page per dataset. For each:

- what the numbers mean,
- what they don't mean,
- known caveats,
- worked example of a single record from source to chart.

Aimed at a journalist who has 10 minutes before filing.

### 13.2 Per-page source citations

Every chart and every table caption ends with a one-line source citation, drawn from the manifest. Currently most pages have an expander; that's good but not enough. The citation should be visible without expanding.

### 13.3 Caveat banners where data is partial

Where `DATA_LIMITATIONS.md` flags a known issue (e.g. office-holders' interests in §2.1, lobbying collective targets in §7.7), the relevant page shows a small inline banner. Not an expander. Not a footnote. A visible banner.

### 13.4 Update history per dataset

Each gold mart's page shows a small "data updates" log: "2026-04-30: refreshed; 2026-04-23: refreshed; 2026-04-16: parser fix for new attendance PDF layout". Built from `meta_pipeline_runs`. Three entries shown, link to a full history.

### 13.5 Public changelog

`CHANGELOG.md` at the repo root, kept in keepachangelog.com style. Every release entry. Every parser fix. Every new dataset. The bar for entry: "would a user notice?".

### 13.6 Methodology review by an external reader

Before the alpha goes to a journalist, hand `methodology.md` and the DATA_LIMITATIONS doc to one independent reader who hasn't worked on the project. Their first 10 questions are gold.

---

## 14. Developer experience

The fastest way to make progress sustainable is to lower the cost of every change.

### 14.1 One-command bootstrap

`make bootstrap` (or `just bootstrap`):

1. Creates venv.
2. Installs dependencies (locked).
3. Pulls a small fixture-only data bundle.
4. Runs the smoke test suite.

A new contributor (or future-you after a break) is productive in five minutes, not five hours.

### 14.2 Contribution guide

`CONTRIBUTING.md`: how to set up, where to put new code (sandbox vs core), how to run tests, how to run the dashboard locally, how to add a page contract. Short.

### 14.3 Module size discipline

The largest current modules (`pipeline.py`, `enrich.py`) are doing a lot. Target: no module over 600 lines. Split by responsibility, not by size. The contract pack already imposes some of this; carry it through the rest of the codebase.

### 14.4 Tests as documentation

A new contributor reading `test/` should be able to learn what each parser is supposed to do. This is a writing-style thing: test names describe behaviour, fixtures are realistic, assertions are specific.

### 14.5 Smaller, scoped PRs

Even as a solo project, PRs are better than direct-to-main commits because they create a review surface for tomorrow-you. CI runs on PRs. A revert is a one-click action.

### 14.6 Reduce constants/mapping file sprawl

`select_drop_rename_cols_mappings.py` and friends are convenient but have grown. Split by domain (`mappings/attendance.py`, `mappings/payments.py`, ...). Smaller modules are AI-friendly: smaller context, fewer accidental edits.

---

## 15. Sustainability and bus factor

Solo civic-data projects rot when the maintainer steps back. These items stretch the half-life.

### 15.1 Tribal knowledge capture

The PDF parsers in particular embed knowledge about layout quirks ("payments PDFs since 2022 wrap the description column at 67 chars"). That knowledge currently lives in commit messages and the maintainer's head. Capture it in module docstrings and the methodology doc.

### 15.2 Refresh calendar doc

`doc/refresh_calendar.md`: when each source refreshes, what the typical lag is, who at the source to contact if it breaks. One row per source, one paragraph each.

### 15.3 First-contributor experience

Someone clones the repo cold. Can they:

- Run the test suite? (yes after §14.1)
- Run the dashboard? (yes if `data` branch is fetchable)
- Add a small feature? (yes if §14.2 exists and contracts are explained)
- Refresh data locally? (yes after §18 lands `--refresh`)

If any answer is "no", that's the next sustainability item.

### 15.4 Handover note

`doc/handover.md`: the document a new maintainer reads if you stopped tomorrow. Includes:

- secrets and access locations,
- the GitHub Actions schedule,
- known fragile parsers,
- open issues by priority,
- the "start here" sequence for the first day.

You will never read this; it exists for someone else.

### 15.5 Monthly rebuild from clean state

Once a month, blow away local data, clone fresh, run `make bootstrap` and `make refresh`. Anything that breaks is a sustainability bug. This is the early-warning system for "the project still works".

### 15.6 Funding / grants

A small civic-data project in Ireland has access to: Enterprise Ireland innovation vouchers, Open Knowledge Ireland community, certain academic micro-grants (TCD/UCD/DCU politics depts), and Journalism Funds (small). Not life-changing money, but enough to fund hosting + occasional contract help. Not urgent; flag for when the project is public.

---

## 16. Hosting and cost

Mostly unchanged from earlier revisions; included for completeness.

### 16.1 Recommended stack

1. **Streamlit Community Cloud** for the app. Free, GitHub-connected, auto-rebuild on push.
2. **GitHub Actions cron** for the refresh (§3).
3. **Hugging Face Datasets** as an optional secondary publication target if parquet sizes outgrow Community Cloud's comfort.

### 16.2 What not to do yet

- No custom React frontend.
- No backend API service unless a real user asks.
- No live joins in Streamlit.
- No continuous AI regeneration of metric logic.
- No heavyweight orchestrator (Dagster, Prefect) — GitHub Actions is plenty for this scale.

### 16.3 Cost model

At current scale, total monthly cost is plausibly £0–10. Mostly free tiers. The cost trap is "one paid dependency at a time" — keep the bar high.

---

## 17. AI-assisted development discipline

The contract pack and skills make AI-assisted development viable for this project. The discipline is what keeps it that way.

### 17.1 Skills as enforcement

The skills (`bold-redesign-page`, `civic-ui-review`, `pipeline-view`) are not optional — they're the contract enforcement layer. New pages should always go through them. Drift creeps in when shortcuts are taken under deadline; the skills are designed for "use even when in a hurry".

### 17.2 Contract pack as token discipline

An AI generating a page should read the page contract and the column dictionary, not the full repo. If a generation prompt includes more than ~30k tokens of context, the prompt is wrong, not the AI. The contract pack v5 is sized to keep prompts small.

### 17.3 What AI must not own

- Source joins.
- Fuzzy matching logic.
- Metric definitions.
- Provenance logic.
- Data-grain decisions.
- Anything in `pipeline.py` or `enrich.py` core.

These are change-controlled. AI can suggest, the maintainer commits.

### 17.4 Diff review discipline

Every AI-generated change is reviewed as a diff before merge. Even when the AI seems confident. Especially when the AI seems confident.

### 17.5 Memory hygiene

Save user/feedback memories that capture *why* a decision was made (not just *what*). When a memory becomes stale (pattern changed, file moved), update or remove it — don't just ignore it.

---

## 18. Recommended 90-day sequence

Concrete, in priority order. Each item is small enough to ship in a sitting. Updated to reflect the consolidated work in `pipeline_sandbox/` and `test/HANDS_OFF_TEST_PLAN.md`.

### Weeks 1–2 — operational baseline + discovery probe

1. Add `pipeline.py --refresh` flag and the GitHub Actions workflow in §3.3.
2. Stand up the `data` branch and a Streamlit Community Cloud deploy from `main` + `data`.
3. Add the first per-mart manifest writer (`utility/tools/write_run_manifest.py`).
4. Add `render_provenance(manifest_path)` and wire it on three pages.
5. Add the freshness badge helper (§9.2) and wire it on the same three pages.
6. **Build out [`pipeline_sandbox/payment_pdf_url_probe.py`](../pipeline_sandbox/payment_pdf_url_probe.py)** to a working state. Validation: probe returns the March 2026 payment PDF URL. Reference: [`pipeline_sandbox/payment_pdf_discovery_notes.md`](../pipeline_sandbox/payment_pdf_discovery_notes.md).

### Weeks 3–4 — protect what exists (HANDS_OFF_TEST_PLAN Phases A–B)

7. Add CI: ruff + ruff-format + pytest + page-import smoke + schema diff (§6.1).
8. **HANDS_OFF Phase A**: discovery probe tests — offline construction tests, fixture-based index parsing, mocked-HTTP orchestration, `@pytest.mark.live` weekly canary.
9. **HANDS_OFF Phase B**: golden-file PDF tests for payments, attendance, interests parsers (§5.2).
10. Add row-count drift assertions in silver writers (§5.3, HANDS_OFF Phase D).
11. Pin dependencies and commit a lockfile (§6.3).
12. Vote pagination fix per [`pipeline_sandbox/votes_pagination_plan.md`](../pipeline_sandbox/votes_pagination_plan.md). Phase 0 confirmed truncation; proceed through Phases 1–3.

### Weeks 5–6 — distribution, trust, and remaining test phases

13. Versioned data releases (§12.1).
14. `methodology.md` first draft (§13.1).
15. Per-page caveat banners where DATA_LIMITATIONS flags a gap (§13.3).
16. `CHANGELOG.md` started (§13.5).
17. RSS feed for new events (§12.4) — at least one feed.
18. **HANDS_OFF Phase C**: source-side JSON schema validation for the Oireachtas API.
19. **HANDS_OFF Phase E**: freshness SLOs with per-source thresholds.

### Weeks 7–8 — UI, ops, and notification stack

20. Cross-page navigation (§9.6).
21. Global search (§9.7).
22. Pipeline status page (§10.5).
23. Run summaries committed per refresh (§10.1).
24. **§4.7 Tier 1 notifications**: GitHub Issues auto-creation on refresh failure (§10.3, HANDS_OFF Phase G Tier 1).
25. **§4.7 Tier 2 notifications**: Healthchecks.io ping per source (HANDS_OFF Phase G Tier 2).
26. **§4.7 Tier 3 (optional)**: ntfy.sh weekly canary push (HANDS_OFF Phase G Tier 3).

### Weeks 9–12 — first real user, and the remaining backstops

27. Hand the alpha to one named journalist or researcher.
28. Whatever they ask for first becomes the next priority (likely SIPO donations from `ENRICHMENTS.md` §A.1, or judicial appointments from §D.1).
29. Iterate on whatever broke under their use.
30. **HANDS_OFF Phase F**: end-to-end smoke test against committed bronze fixture pipeline.
31. Refresh calendar doc and handover note (§15.2, §15.4).
32. **HANDS_OFF Phase H**: silence and tuning — review email frequency after 4 weeks of operation, adjust thresholds.

### Phase 2 (months 4–6, if Phase 1 lands cleanly)

33. Extend the discovery probe to attendance and interests sources (per [`pipeline_sandbox/payment_pdf_discovery_notes.md`](../pipeline_sandbox/payment_pdf_discovery_notes.md) Phase 2). This is also when the v4 §4.6 pluggable scraper interface is worth introducing — three implementations is enough to validate the abstraction.
34. Add one new dataset from `ENRICHMENTS.md` (most likely SIPO donations §A.1 or judicial appointments §D.1).

This sequence assumes evening-and-weekend pacing. None of it is research-grade work; it's all operationalising what's already designed in v4 + sandbox + test plan.

### Why this order

- **Discovery probe (week 1–2)** ships before tests because the test fixtures need real probe output to validate against.
- **Golden-file tests (week 3–4)** ship before any further pipeline changes because they're the regression net for everything that follows.
- **Notifications (week 7–8)** ship after observability (manifest + freshness badge + status page) because notifications without context are unhelpful — you need the run summaries to populate issue bodies.
- **First user (weeks 9–12)** comes after the system is genuinely hands-off, not before. A journalist using a fragile system creates support load, not feedback.

---

## 19. Reading list

Streamlit:

- Streamlit Community Cloud — https://docs.streamlit.io/deploy/streamlit-community-cloud
- Multipage apps — https://docs.streamlit.io/develop/concepts/multipage-apps/overview
- Caching — https://docs.streamlit.io/develop/concepts/architecture/caching
- `st.cache_data` — https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data
- Query params — https://docs.streamlit.io/develop/api-reference/caching-and-state/st.query_params

DuckDB / Parquet:

- DuckDB Parquet overview — https://duckdb.org/docs/current/data/parquet/overview.html
- DuckDB Python API — https://duckdb.org/docs/current/clients/python/overview.html
- DuckDB metadata functions — https://duckdb.org/docs/current/sql/meta/duckdb_table_functions.html
- DuckDB-WASM — https://duckdb.org/docs/api/wasm/overview
- MotherDuck — *Why Semantic Layers Matter* — https://motherduck.com/blog/semantic-layer-duckdb-tutorial/

CI / testing:

- pytest getting started — https://docs.pytest.org/en/stable/getting-started.html
- GitHub Actions Python build/test — https://docs.github.com/en/actions/tutorials/build-and-test-code/python
- pandera schemas — https://pandera.readthedocs.io/

Schema / data quality:

- pandera — https://pandera.readthedocs.io/
- Great Expectations (heavier alternative) — https://greatexpectations.io/

Hosting / distribution:

- Streamlit Community Cloud — https://docs.streamlit.io/deploy/streamlit-community-cloud
- Hugging Face Spaces — https://huggingface.co/docs/hub/spaces-overview
- Hugging Face Datasets — https://huggingface.co/docs/hub/datasets-overview
- Keep a Changelog — https://keepachangelog.com/

Notifications and monitoring (per §4.7):

- Healthchecks.io — https://healthchecks.io/
- ntfy.sh — https://ntfy.sh/
- IIPC web archiving best practices — https://netpreserve.org/web-archiving/
- `dawidd6/action-send-mail` (only if §4.7 Tier 1 outgrown) — https://github.com/dawidd6/action-send-mail

Civic-data inspiration:

- TheyWorkForYou — https://github.com/mysociety/theyworkforyou
- HowTheyVote.eu — https://howtheyvote.eu/
- OpenKamer (Dutch parliamentary scraper) — https://github.com/openkamer/openkamer

For dataset enrichment ideas, see `ENRICHMENTS.md`.

---

## 20. What changed in this rev

### Updates 2026-05-01 (consolidating sandbox + test plan work)

- **§4.7 added** — *Notification pattern (privacy-aware, low-maintenance)*. Three-tier stack: GitHub Issues + GitHub-native email (Tier 1) + Healthchecks.io anonymous tier (Tier 2) + ntfy.sh with random topic (Tier 3). Explicitly rejects the SaaS-email pattern (SendGrid/Mailgun) for solo civic-data projects on privacy-and-maintenance grounds. Includes a coverage matrix mapping each failure mode to which tier catches it.
- **§4.8 TODO list expanded** with notification-tier setup checklists (Tier 1, Tier 2, Tier 3) and an "upstream-facing contact" subsection clarifying the User-Agent contact email is a separate concern from internal alerting.
- **§5.2 links** [`test/HANDS_OFF_TEST_PLAN.md`](../test/HANDS_OFF_TEST_PLAN.md) §4 / Phase B for the full fixture-creation strategy.
- **§6.7 added** — pointer to `test/HANDS_OFF_TEST_PLAN.md`'s 8-phase build-out (A discovery probe tests → H silence/tuning) as the extended test plan for hands-off operation.
- **§10.3 cross-references** §4.7 (notification policy lives there; §10.3 is operational implementation detail).
- **§18 90-day sequence rewritten** to integrate (a) the discovery probe build from `pipeline_sandbox/payment_pdf_url_probe.py`, (b) the votes pagination fix from `pipeline_sandbox/votes_pagination_plan.md` (Phase 0 truncation confirmed), (c) the §4.7 three-tier notification stack, and (d) the HANDS_OFF Phase A–H test plan. Added "Phase 2 (months 4–6)" for extending the discovery probe to attendance + interests, and a "Why this order" rationale block.
- **§19 reading list** gained a "Notifications and monitoring" section: Healthchecks.io, ntfy.sh, IIPC web archiving best practices, `dawidd6/action-send-mail` (only if Tier 1 is outgrown).
- **§3.4 cadence table** updated: payments PDF cadence corrected from "Quarterly" to "Monthly" (the original entry was just wrong; PDFs publish per-data-month with a 31–46 day lag). Links to `pipeline_sandbox/payment_pdf_discovery_notes.md`.
- **All subsection numbering 5.1–17.5 fixed** — many headers retained the pre-renumbering numbers from earlier in the rev. Now consistent throughout.

Companion files written/updated this rev (live outside v4 but referenced from it):

- [`pipeline_sandbox/payment_pdf_url_probe.py`](../pipeline_sandbox/payment_pdf_url_probe.py) — parlparse-style URL discovery probe, construction + index-fallback + diagnostic-failure tiers.
- [`pipeline_sandbox/payment_pdf_discovery_notes.md`](../pipeline_sandbox/payment_pdf_discovery_notes.md) — per-source discovery patterns (payments, attendance, interests), backfill viability, cadence.
- [`pipeline_sandbox/votes_pagination_plan.md`](../pipeline_sandbox/votes_pagination_plan.md) — phased rollout, Phase 0 (truncation diagnostic) confirmed.
- [`pipeline_sandbox/learnings_from_civic_data_projects.md`](../pipeline_sandbox/learnings_from_civic_data_projects.md) — case studies from parlparse, ProPublica, Open Civic Data, IIPC, EveryPolitician.
- [`test/HANDS_OFF_TEST_PLAN.md`](../test/HANDS_OFF_TEST_PLAN.md) — extended test plan complementing `test/TEST_SUITE.md`. 8-phase build-out, severity-based notification scheme, CI integration cadences, anti-patterns.

### vs v3 (original v4 rewrite)

- Renamed and rewritten from scratch as v4.
- Removed the dataset-enrichment catalogue entirely; that now lives in `ENRICHMENTS.md`. v4 stays focused on the existing surface.
- Reorganised into 20 discrete sections covering the full project surface — robustness, modelling, performance, UI, ops, security, distribution, trust, DX, sustainability — not just the original "operating model + page contracts" axis.
- Status snapshot updated to April 2026: contract pack v5, ~30 SQL views, 8 pages, skills system, sandbox pattern.
- §3 auto-refresh kept and refined with concrete YAML.
- §4 pipeline rearchitecture is new in this rev — public-safe ingestion: publish-don't-crawl, web-citizenship, upstream coordination, hash-based change detection, pluggable scraper interface.
- §5–6 pipeline robustness expanded with schema validation, golden files, drift assertions, quarantine flow.
- §7 data modelling adds explicit dim/fact/bridge target, match-confidence as first-class, SCDs.
- §9 UI maturity is new — covers cross-page nav, search, mobile, accessibility, onboarding.
- §10 observability is new — run summaries, freshness SLOs, status page.
- §11 security and licensing is new — explicit ToS, GDPR-light, robots.txt.
- §12 distribution is new — versioned releases, permalinks, RSS, DuckDB-WASM.
- §13 trust expanded — methodology doc, caveat banners, public changelog.
- §14–15 DX and sustainability are new — bootstrap, contributor experience, handover, monthly clean-rebuild.
- §17 AI discipline made explicit — skills as enforcement, contract pack as token discipline, what AI must not own.
- §18 90-day sequence rewritten to be evening-pace realistic, not multi-quarter.

vs prior attempts at this kind of doc:

- Stops trying to be a textbook. The architectural philosophy is in §1; everything else is concrete improvements to the existing project.
- Separates data enrichment ideas (`ENRICHMENTS.md`) from project improvements (this doc). Each is more useful when not interleaved with the other.
- Acknowledges that the analytical layer is materially closer to beta than the operational layer is, and structures priorities accordingly.
