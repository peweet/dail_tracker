# PDF auto-discovery — notes

_Sandbox notes for the auto-discovery feature being prototyped in [`payment_pdf_url_probe.py`](payment_pdf_url_probe.py). This was scoped to payments first; the same shape extends to attendance and member interests, and is the seed of the pluggable scraper interface in v4 §4.6._

## Goal

Replace the manual "drag PDF into folder" step with automated discovery of new Oireachtas-published PDFs across **three sources**:

1. **Parliamentary Standard Allowance payments** (monthly per data month)
2. **Deputies' verification of attendance** (irregular, multi-month spans)
3. **Register of Members' Interests** (annual, both Dáil and Seanad)

**Success criterion (Phase 1):** the probe correctly identifies the URL for the March 2026 payments PDF without any web search and without hard-coded URL strings.

If March payments works, the architecture is validated and we extend to attendance + interests with per-source pattern overrides.

---

## The shared strategy (applies to all three sources)

In priority order:

### 1. Index-first

Fetch the topic-filtered publications index for the source:

```
https://www.oireachtas.ie/en/publications/?topic[]=<topic-slug>&resultsPerPage=50
```

Where `<topic-slug>` differs per source (table below). If a publication entry on the first page matches the target month/year/period, extract the link. Done in 1 request.

This is the cheapest path **and** the only path that handles slug variance — which matters a lot for attendance and interests, less so for payments.

Caveat: when probed during URL verification, the index page returned a 403 to automated requests. Likely WAF / bot rule that needs `User-Agent` set (v4 §4.3) or a session warm-up. Confirm browser-vs-code parity early.

### 2. HEAD-spread fallback

If the index is unreachable or doesn't yet list the new PDF, fall back to HEAD-checking ~8 candidate URLs constructed from the source's lag pattern. **HEAD, not GET. Sequential with early exit. Both folder variants where applicable.** See per-source tables below for the candidate dates.

### 3. Wider lag-window fallback

If Tier 1 misses, try the wider window (typically full +25 to +60 days from the data-period-end). Worst-case adds ~30 HEAD requests; only triggers when the publication is unusually early or late.

### 4. Fail loudly with diagnostics

- **Within expected lag window:** log "not yet published", exit cleanly.
- **Past expected lag window:** log "URL pattern probably broke", open a GitHub issue.

---

## Per-source patterns

### A. Payments (Parliamentary Standard Allowance)

```text
Base path     : data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/{pub_year}/
Filename slug : {pub_date}_parliamentary-standard-allowance-payments-to-deputies-
                for-{month}-{data_year}_en.pdf
Index topic   : parliamentary-allowances
Folder variants: psa/  (current), caighdeanOifigiul/ (used for payment_nov_td_2025)

Publication cadence: monthly per data month
Pub-date lag distribution (data-month-end → publication):
  Jan 2026 → 34 days
  Feb 2026 → 32 days
  Dec 2025 → 46 days   (outlier)
  Feb 2024 → 32 days
  Mar 2024 → 31 days
Median lag : 32 days
Tier 1 spread : +30 to +36 days
Tier 2 spread : +25 to +29 and +37 to +60
```

### B. Attendance (deputies' verification of attendance for TAA)

```text
Base path     : data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/{pub_year}/
Filename slug : {pub_date}_deputies-verification-of-attendance-for-the-payment-of-taa-
                {date_range}_en.pdf
Index topic   : record-of-attendance

Publication cadence: irregular — annual full-year PDFs PLUS occasional "gap"
                     PDFs covering periods between annual cycles.

Observed date_range slugs (see pdf_endpoint_check.py):
  pdf_2023      : 01-january-2023-to-31-december-2023
  pdf_2024      : 01-january-2024-to-08-november-2024
  pdf_2024_gap  : 29-november-2024-to-31-december-2024
  pdf_2025_gap  : 01-january-2025-to-31-january-2025
  pdf_2025      : 01-february-2025-to-30-december-2025
  pdf_2026      : 01-january-2026-to-28-february-2026

Pub-date pattern: announces shortly after the period it covers, with a small
                  lag. Recent examples cluster in Feb–Apr.
```

**Important:** the `{date_range}` slug is **not predictable from a simple input**. The boundary dates depend on Dáil sittings, dissolution, election cycles, and editorial decisions at the Oireachtas. Construction-only is much weaker for attendance than for payments.

**Implication for the discovery probe:**
- Index-first becomes essentially mandatory for attendance. The probe should not try to construct attendance URLs from a single date input the way it can for payments.
- HEAD-spread can still work if you constrain to the *publication date* and use a wildcard-style match against the listing of files in the folder (some web servers expose folder listings; Oireachtas may or may not).
- Default pattern: scrape the topic-filtered index every time, expect new entries when they appear.

### C. Register of Members' Interests (annual, Dáil and Seanad)

```text
Base path     : data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/
                {chamber}/{pub_year}/
Index topic   : register-of-members-interests  (verify this slug — not yet checked)
Chambers      : dail, seanad

Filename slug pre-2022 :
  {pub_date}_register-of-members-interests-{chamber}-eireann_en.pdf

Filename slug 2022+    :
  {pub_date}_register-of-member-s-interests-{chamber}-eireann-{data_year}_en.pdf
  (note the apostrophe in "member-s" and the year suffix)

Publication cadence: annual, published in February of {data_year + 1}
Pub-date examples (Dáil):
  data 2020 → 2021-02-25
  data 2021 → 2022-02-16
  data 2022 → 2023-02-22
  data 2023 → 2024-02-21
  data 2024 → 2025-02-27
  data 2025 → 2026-02-25

Pub-date examples (Seanad, slightly later):
  data 2020 → 2021-03-16
  data 2021 → 2022-02-25
  data 2025 → 2026-03-10

Lag from data-year-end (Dec 31) to publication: ~50–60 days
Tier 1 spread : Feb 16 – Feb 28 (Dáil), Feb 25 – Mar 16 (Seanad)
```

**Important:** the slug changed once, between 2021 and 2022, with two changes simultaneously: `members` → `member-s` (apostrophe added) and a `-{data_year}` suffix added. **Construction across a slug-change boundary fails silently.** This is a concrete example of why index-first is the right primary strategy.

**Implication:** for interests, the discovery probe should default to index-first. HEAD-spread is a fallback that needs to know which slug variant to try based on data year.

---

## Backfill — can this scrape older examples?

The user's question: can the same probe also fetch historical PDFs not yet in the codebase? Yes for recent backfill, no for deep historical, with the boundary at roughly **2018**. Three reasons:

### 2.1 The publications index 200-page cap (10,000 results)

Topic-filtered listings are paginated, with a hard cap of 200 pages × 50 results = 10,000 entries. For payments alone, that's ~830 publication-years before the cap bites — fine for any plausible historical span. For combined topics or the global index, the cap is reachable.

**For backfill via index:** filter by topic, paginate to the deepest available. For payments and interests, this should reach the 2010s without hitting the cap.

### 2.2 URL slug pattern changes deeper into history

Already observed:
- Interests slug changed once (2021 → 2022).
- Payments folder changed once (`psa` → `caighdeanOifigiul`, briefly).

Likely additional changes pre-2018 that I don't have evidence of from the current codebase. Construction-only fails across these; index-first works.

### 2.3 The 2020 layout boundary (parser-side, not URL-side)

This is from `DATA_LIMITATIONS.md` §1.3:

> "PDF-derived data should be treated as reliable only from around 2020 onward, unless a specific parser has been written and validated for an older source layout."

So even if the discovery probe finds older PDFs, the **parser** will produce poor or wrong output for pre-2020 PDFs. Backfill via discovery isn't the bottleneck — backfill via parsing is.

### Practical viability summary

| Period | URL discovery | Parser viability | Net usable backfill? |
|---|---|---|---|
| 2020 → today | Yes (probe handles) | Yes (current parsers) | **Yes — high priority** |
| 2018 → 2019 | Likely yes via index | Layout differs; parser fixes needed | Yes with parser work |
| Pre-2018 | Probably yes via index | Substantially different layouts | Not without major parser work |
| Pre-2010 | URL pattern may not exist | N/A | No |

**Recommendation:** in the same probe, add a `--backfill` mode that paginates through the topic-filtered index and downloads anything not already in `data/bronze/`. This is one new code path and gives free recent-historical coverage. Don't extend parsers to pre-2020 without a separate evaluation; that's a different project (and `DATA_LIMITATIONS.md` already documents why).

---

## Cadence (per source)

| Source | Probe cadence | Reason |
|---|---|---|
| Payments | **Monthly** ~35 days after end of data month, retry weekly on miss until 60 days | Aligns to monthly publication lag; weekly retry handles outliers like Dec 2025's 46-day lag |
| Attendance | **Weekly** during Dáil terms; **monthly** during recess | Irregular publication; weekly probes during sittings catch new PDFs within 7 days |
| Interests (Dáil) | **Once a week through February** of year+1, then monthly | Annual publication clusters Feb 16–28 |
| Interests (Seanad) | **Once a week Feb 25 – Mar 20** of year+1 | Slightly later than Dáil |

GitHub Actions schedule expressions (illustrative):

```yaml
schedule:
  # Payments: 5th of each month, hits previous-previous-month's data PDF
  - cron: '0 4 5 * *'

  # Attendance: every Monday during Dáil terms (Sept–Dec, Jan–Apr, May–Jul)
  # Actions doesn't have a "during Dáil terms" filter — schedule weekly
  # year-round and let the index-first strategy no-op when nothing is new.
  - cron: '0 4 * * 1'

  # Interests: every Wednesday during Feb–Mar
  - cron: '0 4 * 2,3 3'
```

The "no-op when nothing new" pattern is fine and free — index-first probe + 304/empty index = ~1 KB of network traffic per probe.

---

## Why this is converging on v4 §4.6 (pluggable scraper interface)

The pattern across the three PDF sources is the same shape:

```text
class PdfSourceScraper:
    name: str                     # "payments" / "attendance" / "interests-dail" / "interests-seanad"
    index_topic: str              # topic filter slug for publications index
    base_path_template: str       # data.oireachtas.ie URL prefix
    filename_slug_builder: Fn     # given (data_year, data_month_or_period), construct slug
    lag_distribution: dict        # min/max/median days from data-period-end to pub
    cadence: Cadence

    def discover_via_index(self) -> Iterator[AssetRef]: ...
    def construct_candidates(self, data_period) -> Iterator[CandidateUrl]: ...
    def fetch(self, ref) -> RawAsset: ...
```

This is the `pupa` interface from Open Civic Data, parameterised for Oireachtas PDFs. **Don't refactor for this until the next source is added.** New code is easier to fit to a new pattern than old code is to retrofit. But the moment we add SIPO donations, judicial appointments, or any other PDF source from `ENRICHMENTS.md`, this is the shape to lift it into.

For now: keep the probe as one file, parameterised by source-specific tables. When the second source (attendance) lands, refactor into the interface above. Three implementations (payments, attendance, interests) is enough to validate the abstraction; one is just a function with a config arg pretending to be an abstraction.

---

## What this won't catch (across all sources)

- **Hand-published PDFs at non-standard URLs.** If Oireachtas publishes a one-off attendance correction in a press release rather than the standard folder, neither index nor construction will find it. Acceptable; flag in `DATA_LIMITATIONS.md`.
- **Withdrawn PDFs.** If a PDF is removed and replaced, the discovery probe will see the new one. The hash-based change detection from v4 §4.5 surfaces this as a "republished" event.
- **Non-PDF source changes.** Some Oireachtas publications are HTML-only or have linked supplementary materials. Out of scope here; this feature is PDF-specific.

---

## Build-out checklist

In implementation order. Phase 1 = payments only; Phase 2 = extend to attendance and interests; Phase 3 = backfill mode.

### Phase 1 — payments
- [ ] Resolve the index-page 403 (User-Agent? cookie? upstream contact?). Without this, Strategy 1 can't run from CI.
- [ ] Implement Strategy 2 (HEAD-spread) in the existing `payment_pdf_url_probe.py`. Tier 1 is already there; verify it works against live `data.oireachtas.ie`.
- [ ] Add Strategy 3 (wider window) as a fallback after Tier 1 misses.
- [ ] Add the diagnostic distinction in Strategy 4 (within-window vs past-window failures).
- [ ] Add structured logging so failures are debuggable from CI logs alone.
- [ ] Wire conditional GET (`If-Modified-Since`) into the HEAD requests.
- [ ] Add a fixture test: given a mocked publications-index HTML and mocked HEAD responses, the probe returns the right URL.
- [ ] **Validation: probe returns the March 2026 URL.** This is the green light.

### Phase 2 — attendance and interests (after Phase 1 lands)
- [ ] Confirm topic-slug strings for `record-of-attendance` and `register-of-members-interests` (the latter is a guess — verify on the index page).
- [ ] Refactor probe into the v4 §4.6 pluggable shape with three source configs.
- [ ] Special-case attendance: index-first only, no construction (date-range slug is unpredictable).
- [ ] Special-case interests: handle the 2021/2022 slug-change boundary with a per-year slug-variant lookup.
- [ ] Add per-source fixture tests.

### Phase 3 — backfill (after Phases 1 and 2 land)
- [ ] Add a `--backfill` mode that paginates through the topic-filtered index, lists every PDF, and downloads any not already in `data/bronze/<source>/`.
- [ ] Cap backfill depth to a configurable year (default 2020 to align with parser viability).
- [ ] Add a "discovered but parser-unsupported" quarantine state for older PDFs the probe finds but parsers can't handle yet — better to know they exist than silently ignore them.

---

## Test against March 2026 — Phase 1 validation

The probe should return:

```
https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2026/2026-04-30_parliamentary-standard-allowance-payments-to-deputies-for-march-2026_en.pdf
```

When it does, log it, and we know Phase 1 is sound. If the probe finds it via index (Strategy 1), even better — that proves the cheapest path works. If it finds it via HEAD-spread (Strategy 2), that proves the fallback works and tells us the index isn't accessible from code (which is information we need anyway).

After that: Phase 2 — extending to attendance and interests — is the obvious next step.
