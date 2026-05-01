# Learnings from civic-data projects that apply to dail_tracker

_Reading list with explicit parallels. Not a roadmap; not a "do all of this". The point is to know what's been figured out elsewhere so we don't reinvent it._

This sits in `pipeline_sandbox/` because it's reference material — read it once, lift specific patterns, then it's done. It supports the architectural choices made in v4 §4 (rearchitecture), §6 (modelling), and the discussion around vote pagination and PDF discovery.

---

## 1. mySociety — `parlparse` (UK Parliament scraping)

**Repo:** https://github.com/mysociety/parlparse
**Key files to read:**
- `README.md` — overall architecture
- `pyscraper/` — the actual scrapers
- `members/people.json` — the slowly-changing-dimension model

### What's worth reading

`people.json` is the answer to a problem dail_tracker hasn't formally solved yet: how do you track the same person across role changes, party changes, by-elections, and resignations without losing history?

Their model (paraphrased):

```text
{
  "id": "uk.org.publicwhip/person/<n>",
  "memberships": [
    {"start_date": "...", "end_date": "...", "role": "MP",
     "constituency": "...", "party": "..."},
    {"start_date": "...", "end_date": "...", "role": "MP",
     "constituency": "...", "party": "..."},   # different cycle
    ...
  ],
  "names": [{"given_name": "...", "family_name": "..."}],
  "other_names": [...]
}
```

The discipline: **one stable person ID; a history of memberships, each with valid dates and the role/party/constituency at that time.** That's a slowly-changing-dimension type-2 model expressed in JSON.

### Direct application to dail_tracker

v4 §6.5 calls for `dim_member_history` with valid-from / valid-to. `people.json` is the working example to copy the *shape* from. Don't copy the file format — that's an artefact of their 2000s-era stack — but the data model maps cleanly to a parquet table:

```text
dim_member            -- one row per person, stable ID
  member_id            VARCHAR PK    -- Oireachtas pId where stable, surrogate otherwise
  display_name         VARCHAR
  …

dim_member_membership  -- one row per (member, role, period)
  member_id            VARCHAR FK
  role_type            VARCHAR       -- td | senator | minister | ag | …
  party_id             VARCHAR FK    -- nullable for non-party roles
  constituency_id      VARCHAR FK
  valid_from           DATE
  valid_to             DATE          -- nullable = currently held
  source               VARCHAR
```

This is what fixes the "what happens when a TD switches parties mid-term?" problem the current model handles implicitly via "latest state wins".

### What not to copy

The PHP-era code, the XML-everywhere format choices, the bespoke runner glue. The data model is great; the implementation is two decades old.

### Other patterns worth lifting from `pyscraper/`

- **Construction-with-index-fallback** for HTML scraping. They try known URL templates first, fall back to scraping the index when templates miss. Mirrored in `payment_pdf_url_probe.py` in this directory.
- **Per-source files with a uniform shape.** Each source has its own module under `pyscraper/`; common behaviour (HTTP, retries, identity) lives in shared helpers above. v4 §4.2 / §4.6 endorses the same structure.
- **Re-fetching as the default.** They don't trust prior outputs as canonical. Daily re-runs detect upstream amendments. v4 §4.5 (hash-based change detection) operationalises this.

### Caveat

`parlparse` is one repo serving multiple downstream projects (TheyWorkForYou, PublicWhip, EveryPolitician). Some of its complexity exists because of that multi-tenancy. dail_tracker doesn't need that; lift the patterns, not the architecture surface area.

---

## 2. ProPublica — Congress API and Sunlight Foundation transition

**Posts to read:**
- *Congress API Update*: https://www.propublica.org/nerds/congress-api-update
- *Bill Subjects, Personal Explanations and Sunsetting Sunlight*: https://www.propublica.org/nerds/congress-api-bill-subjects-personal-explanations-and-sunsetting-sunlight

### What's worth reading

ProPublica took over the Sunlight Foundation's Congress API when Sunlight wound down. The blog posts describe what they kept, what they retired, and crucially: **they retired older scrapers when upstream offered a structured API.**

Two specific decisions worth noting:

1. **When upstream gets structured, retire the scraper.** When `congress.gov` started exposing an API for a dataset that previously only existed as scraped HTML, ProPublica deprecated the scraper rather than maintaining both. Scraped sources are technical debt; structured sources are assets.
2. **Run both paths in parallel during transition.** They didn't switch overnight. They documented both, ran them side-by-side during the cutover, and gave consumers explicit version pinning.

### Direct application to dail_tracker

The Oireachtas API covers members, legislation, debates, and votes. PDFs cover attendance, payments, and interests. **If/when the Oireachtas adds API endpoints for any of the PDF-derived sources, retire the PDF parser.** Don't keep both running; that's just maintenance debt.

The transition pain is real and ProPublica's posts describe the specific failure modes:

- **Schema differences.** The new API doesn't always have the same fields as the scraper's output.
- **ID reconciliation.** A member ID in the API may not match the ID surfaced from a scraped table; need a manual reconciliation table for the cross-over window.
- **Double-counting during cutover.** Both pipelines emitting records for the same period; consumers see inflated counts.
- **Versioned cutover.** They tagged the API release explicitly so downstream consumers could pin and cut over on their own schedule.

### Pattern to put in the back pocket

If a structured Oireachtas equivalent appears for any current PDF source, the migration order is:

1. Add the new ingestion path under `pipeline_sandbox/`. Don't touch the existing one.
2. Run both paths into separate silver tables for one or two refresh cycles.
3. Side-by-side compare outputs; document discrepancies in a `migration_<source>.md`.
4. Tag a versioned data release where the new path becomes canonical and the old one is marked deprecated-but-still-emitting.
5. After two more refresh cycles with no consumer complaints, remove the old path.

This is exactly what the `pipeline_sandbox/votes_pagination_plan.md` follows — it's the same pattern reduced to one bug fix.

---

## 3. Open Civic Data — `pupa` scraper framework

**Repo:** https://github.com/opencivicdata
**Specifically:** `pupa` (the scraper framework) and the per-state scrapers under the `opencivicdata` organisation.

### What's worth reading

Open Civic Data normalises ~50 US state legislatures into one schema. Different state websites have wildly different formats — some have APIs, some don't, some publish XML, some PDFs, some random HTML — but the project funnels everything into a single schema:

```text
Person, Organization, Bill, VoteEvent, Membership, Post, …
```

Each state has a scraper class with a uniform interface:

```python
class StateLegislatorScraper:
    def scrape(self) -> Iterator[Record]:
        ...
```

A central runner orchestrates them with retries, logging, and a structured importer. The scraper's job is to emit Records; the runner's job is to handle everything operational.

### Direct application to dail_tracker

dail_tracker is one jurisdiction, not 50. So this isn't an immediate priority. But two patterns are directly applicable:

**A. Schema as contract.** Open Civic Data's schema is the *checklist* for what fields a sensible `dim_member`, `fact_vote`, `fact_bill_sponsorship` should have. We don't need to adopt their schema literally. We need to use it as the "have we forgotten any obvious column?" check when designing v4 §6.1's dim/fact tables.

**B. Pluggable scraper interface.** v4 §4.6 talks about adopting this *when adding the next new source*. `pupa`'s interface is the working example:

```python
class SourceScraper:
    name: str
    cadence: Cadence
    def discover(self) -> Iterator[AssetRef]:
        ...
    def fetch(self, ref: AssetRef) -> RawAsset:
        ...
    def parse(self, asset: RawAsset) -> Iterator[Record]:
        ...
```

Adopting it for the existing four sources at once is overkill. Adopting it for the next source we add (likely judicial appointments per `ENRICHMENTS.md` D.1, or SIPO donations per A.1) is the right time. New code is easier to fit to a new pattern than old code is to retrofit.

### What's specific to Open Civic Data and not to us

Their schema includes fields for things we don't have in Ireland (state-specific committees, district-vs-county distinctions, US-specific bill types). Don't blindly copy fields. The discipline of "schema first, then scraper" is the takeaway.

---

## 4. IIPC — web archiving best practices

**URL:** https://netpreserve.org/web-archiving/

### What's worth reading

The International Internet Preservation Consortium maintains the canonical "how to be a polite web crawler" guidance. It's a checklist, not a research paper. Read it once, internalise it, drop it into all scraping code.

The relevant items for dail_tracker:

- **Identify yourself.** User-Agent with project name, repo URL, contact email. Anonymous bots get blocked; identified ones get tolerated.
- **Conditional GET.** `If-Modified-Since` / `If-None-Match`. Bandwidth saved on both sides.
- **Throttling and concurrency.** Max concurrent requests per host; sleep between requests; jitter so requests don't bunch.
- **`robots.txt`.** Always check; respect; document any deviation.
- **Politeness during retries.** Exponential backoff with jitter, not constant retry; circuit breaker on repeated 5xx.
- **Documenting the crawl.** Crawl logs as data — source URL, fetch time, response code, content hash.

### Direct application to dail_tracker

v4 §4.3 is the policy version of this checklist. The implementation lives (or will live) in `pipeline/sources/_http.py` as a single shared HTTP helper that all source modules use. Don't write a new helper per source; that path leads to inconsistent behaviour and silent ToS violations.

### Where the checklist deliberately leaves gaps

It doesn't say *what* throttle constants to use; that depends on the upstream. For `data.oireachtas.ie`, 1 req/sec with 0–500 ms jitter is generous and conservative. For `lobbying.ie` (smaller, less robust), drop to 1 req/2 sec.

---

## 5. mySociety — EveryPolitician (defunct but conceptually relevant)

**URL (archived):** http://everypolitician.org/
**GitHub:** https://github.com/everypolitician

### What's worth reading

EveryPolitician was a project to maintain a single, normalised dataset of every elected politician in every legislature, fed by per-country scrapers. It eventually wound down due to maintenance burden, but the architectural pattern is still instructive.

Specific patterns worth understanding:

**A. Single canonical dataset, many consumers.** They published normalised JSON; downstream projects (TheyWorkForYou, others) consumed the JSON, not the upstream scrapes. This is exactly v4 §4.2's "publish, don't crawl" pattern at multi-jurisdiction scale.

**B. Wikidata as cross-reference layer.** They used Wikidata Q-IDs as the cross-jurisdictional join key. dail_tracker should do the same for `dim_member`: store Wikidata Q-ID where known, treat it as a soft key (not authoritative; it's community-edited), use it for cross-references to other datasets that also use Wikidata.

**C. Why it wound down.** The maintenance burden of ~200 scrapers across ~200 legislatures was unsustainable for a small team. Lesson: scope ruthlessly. dail_tracker's "current Dáil and Seanad only" scope is correct; widening it to historical, local council, or regional would multiply maintenance cost without proportional value increase.

### What this tells us about scope

The temptation when a project starts working will be to expand jurisdiction or time-range. Resist it. EveryPolitician died of scope creep. dail_tracker's scope discipline (current Dáil, post-2020 PDFs, no historical backfill of pre-2020 scraped data) is a feature, not a limitation.

---

## 6. ProPublica's nonprofit explorer — get_the_990

**Repo:** https://github.com/billfitzgerald/get_the_990
**Source it scrapes:** https://projects.propublica.org/nonprofits/

### What's worth reading

A small, focused scraper for IRS Form 990 data via ProPublica's nonprofit explorer. Notable for being *small* and *single-purpose* — not over-engineered.

### Why it's relevant

Sometimes a third-party project has already done the upstream parsing work. For some of the enrichment targets in `ENRICHMENTS.md` (e.g., tax/charity/regulatory data), there may be an existing scraper or aggregator that's ahead of doing it ourselves. Worth checking before building from scratch.

For dail_tracker specifically: nothing in this list maps directly. But the *habit* of "before scraping a source, check whether someone else already did it" is the lesson.

---

## What none of these projects solve well

Worth knowing what we're on our own with:

- **PDF parser drift detection.** Every project listed treats PDF parsing as "write it, hope for the best, fix when it breaks". Nobody has cracked auto-detection. v4 §5.2 (golden-file fixture tests) is state-of-the-art; we're not behind anyone.
- **Cross-source name resolution at scale.** Open Civic Data has `Person.name + other_names + family_name` etc., but matching across sources is still mostly manual or fuzzy. Our normalised join key (`normalise_join_key.py`) isn't worse than theirs; it's just shared territory of "we all have this problem".
- **Lobbying register integration.** Almost no jurisdiction's transparency project ingests their lobbying register cleanly. We'll be ahead of the field if we do this well; there's no exemplar to copy. The closest is the EU Transparency Register integration in some Brussels-focused projects, but that's its own beast.

---

## Reading order if time-boxed

If you have an hour, in this order:

1. **`parlparse` README + `members/people.json` structure** — most direct application to v4 §6.5 modelling decisions. (15 min)
2. **ProPublica's "Sunsetting Sunlight" post** — the transition pattern, lift the side-by-side cutover for the votes pagination work. (10 min)
3. **`pupa`'s scraper interface** — the source-pluggability pattern for v4 §4.6. (15 min)
4. **IIPC checklist** — internalise once, apply forever. (10 min)
5. **EveryPolitician postmortem (whatever you can find about why it wound down)** — the scope-discipline lesson. (10 min)

That's enough background to not relitigate decisions other projects already settled.
