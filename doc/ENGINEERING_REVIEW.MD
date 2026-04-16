# ENGINEERING_REVIEW.md

A senior-engineer critique of the codebase, plus experimental ideas for data intersections and alternative destinations for the work.

The feedback below is diagnostic — the intent is to name gaps clearly so you can decide how to address them. A few items are phrased as questions where the question itself is more useful than a prescribed answer. Everything here is offered in the spirit of "you have good work; here is what would make it land harder".

Each section ends with learning links relevant to the topic. These are resources I've seen repeatedly used by working engineers — not a comprehensive list, just good starting points.

---

## Table of Contents

1. [Overall Assessment](#1-overall-assessment)
2. [Code Organisation](#2-code-organisation)
3. [Data Flow and State](#3-data-flow-and-state)
4. [Error Handling and Observability](#4-error-handling-and-observability)
5. [Testing Posture](#5-testing-posture)
6. [Join Strategy](#6-join-strategy)
7. [Configuration and Portability](#7-configuration-and-portability)
8. [API Client Design](#8-api-client-design)
9. [Lobbying Pipeline — Specific Feedback](#9-lobbying-pipeline--specific-feedback)
10. [Questions Worth Sitting With](#10-questions-worth-sitting-with)
11. [Experimental — Data Intersections and Adjacent Destinations](#11-experimental--data-intersections-and-adjacent-destinations)

---

## 1. Overall Assessment

The domain work is solid. You have correctly identified that the civic value of this dataset is in the joins — attendance alone is boring, lobbying alone has been done, but attendance × lobbying × member interests is genuinely novel for Ireland. The choice of medallion architecture, DuckDB for analytics, and sitting-politicians-only scope are all defensible decisions made for clear reasons.

The gap between "this works on my machine" and "a team could take this over" is the gap most juniors and mid-levels do not close until someone makes them. That gap is where the feedback in this document lives. It is not a list of things wrong with the code so much as a list of things that will matter when someone else runs it, when the data changes shape upstream, or when you come back to it in six months and try to reason about why something is producing the numbers it's producing.

The honest short version: the code does its job. The code does not yet defend itself against the world changing around it. Closing that gap is the difference between a good project and a portfolio piece that survives contact with an interviewer.

---

## 2. Code Organisation

**Flat top-level script directory.** Every script lives at the repo root — `attendance.py`, `payments.py`, `enrich.py`, `lobbying_processing.py`, `oireachtas_api_service.py`. At 10+ files this is starting to strain. A reader arriving cold cannot tell from the file tree which scripts are ingestion, which are transformation, which are enrichment, or what the execution order is.

A package structure (`dail_tracker/ingest/`, `dail_tracker/transform/`, `dail_tracker/enrich/`) would make the responsibilities visible at a glance. The `__main__` guards you added are the prerequisite for this move — now the imports can be surfaced cleanly.

**`utility/` is a dumping ground.** Directories named `utility/`, `helpers/`, or `common/` are almost always accreted landfills rather than designed abstractions. A directory named for what it *does* (`dail_tracker/schema/`, `dail_tracker/api_client/`) tells a reader more than one named for where it goes.

**Column-config file is doing real work in the wrong place.** `utility/select_drop_rename_cols_mappings.py` holds domain knowledge — which bill fields matter, which member fields are noise. That's schema. Treating it as a utility undersells it and makes it hard to find.

**Learning:**

- "Python Application Layouts: A Reference" — https://realpython.com/python-application-layouts/
- "Hypermodern Python" (Claudio Jolowicz, series) — https://cjolowicz.github.io/posts/hypermodern-python-01-setup/ — still the best modern guide to Python project structure
- "The Hitchhiker's Guide to Python — Structuring Your Project" — https://docs.python-guide.org/writing/structure/

---

## 3. Data Flow and State

**Scripts communicate through the file system.** Script A writes CSV, Script B reads CSV. This works, but it means every stage's contract with the next is implicit — encoded in column names in a file rather than in a function signature or a schema.

Ask yourself: if `attendance.py` renames a column from `sitting_days_attendance` to `sitting_attendance`, what happens? (It silently produces NULLs in `enrich.py`.) What breaks loudly? (Nothing, until someone notices the numbers look off.)

**No manifest of what has run.** If the pipeline dies halfway — say fada-URL fails during the legislation fetch and kills 4 TDs' data — the next run starts from scratch. There is no record of "these 170 TDs are fetched, these 4 need retry". Every re-run is a full re-fetch. This is cheap at current scale (~200 API calls), expensive at 10x scale, and impossible at 100x.

What does it cost, in your current setup, to re-run only the failed TDs from yesterday's pipeline? (Right now, more than it should.)

**Idempotency is partial.** Rerunning the pipeline produces the same output *if and only if* upstream hasn't changed. Once upstream changes, there's no way to tell without diffing CSVs manually. A checksum or row-count-per-stage in a metadata file would surface drift automatically.

**Learning:**

- "Building a Reproducible Data Pipeline" (dbt blog) — https://www.getdbt.com/blog/what-is-reproducible-data
- Martin Kleppmann, *Designing Data-Intensive Applications*, chapter 10 ("Batch Processing") — describes the idempotency / manifest pattern for batch systems
- Pandera for schema contracts — https://pandera.readthedocs.io/
- Great Expectations (heavier alternative) — https://greatexpectations.io/

---

## 4. Error Handling and Observability

**Logging is in one file.** `oireachtas_api_service.py` has `logging` set up correctly with a file handler. The other five or six scripts still use `print()`. This means when something goes wrong in `enrich.py` or `lobbying_processing.py`, the diagnostic trail stops at the terminal and doesn't land in `pipeline.log`.

Ask yourself: if this pipeline ran on a schedule overnight and the morning output is empty, where do you look? If your answer includes "re-run with print statements and watch", the logging isn't doing its job yet.

**`try/except` blocks tend to swallow quietly.** The fada-URL failure is the canonical example — the exception is logged, but the TD is then dropped from results with no downstream signal. A count of "how many TDs were expected vs how many arrived" at the end of each stage would surface this without you having to grep the log.

**No metrics, just logs.** Metrics are aggregates — "this stage produced 170 rows, expected 174, delta 4". Logs are events — "TD X failed with reason Y". Both are useful; you have one. A `stage_metrics.json` or a row in a DuckDB `pipeline_runs` table after each stage is cheap to add and enormously useful for catching drift.

**What "good" looks like here.** A senior engineer's pipeline run produces: (a) a log of every meaningful event with a level, (b) a metrics record with counts and timings per stage, (c) an alert — even just a non-zero exit code — when counts are outside expected bounds. You have (a) partially. (b) and (c) are absent.

**Learning:**

- "The Twelve-Factor App" — https://12factor.net/ — the section on logs-as-event-streams is foundational
- Python logging HOWTO — https://docs.python.org/3/howto/logging.html — the official docs are genuinely good
- "Distributed Systems Observability" (Cindy Sridharan, free online) — https://www.oreilly.com/library/view/distributed-systems-observability/9781492033431/ — the logs-metrics-traces framing is interview-useful
- Google SRE Book, Chapter 4 (SLOs) — https://sre.google/sre-book/service-level-objectives/ — overkill for this project but the concepts of "what should I alert on" are right

---

## 5. Testing Posture

**The `test/` directory is a placeholder.** Any serious project, especially one that advertises itself as a data pipeline, needs a test suite before it needs new features. You have named the gap in REQUIREMENTS.md — good. The question now is whether you fix the next bug with or without also writing the test that would have caught it.

My recommendation: next bug you fix, write the test first. It's slower for that one bug. It's faster for every future bug because the test catches regressions automatically. This is the habit that compounds.

**Test types missing:**

- *Unit tests* — Does `normalise_join_key("Ó Súilleabháin")` equal `normalise_join_key("O'Sullivan")`? This is a pure function. It has no excuses.
- *Data quality tests* — Does the enriched output have 174 distinct TDs? Does no TD have attendance > 103 days? These are assertions, not unit tests — different tools (Pandera, Great Expectations, or just plain `assert` in a pytest).
- *Integration tests* — Does a mini pipeline run (one PDF, 10 TDs, one scenario) produce the expected output shape? This catches the "I renamed a column and everything broke" failure mode.

**What the absence of tests communicates to an interviewer.** A take-home review in a senior interview will check test coverage. Not because the tests themselves matter, but because writing tests is evidence of having internalised that code is read more than it's written and that data changes shape over time. You will be asked "how did you verify this worked?" and "what's your regression strategy?". Having an answer that isn't "I ran it and looked at the output" is the point.

**Learning:**

- pytest docs — https://docs.pytest.org/en/stable/
- "Test-Driven Development with Python" (Harry Percival, free online) — https://www.obeythetestinggoat.com/
- "Property-Based Testing in Python" (Hypothesis) — https://hypothesis.works/articles/what-is-property-based-testing/ — useful for fuzzy-join key testing, could find anagram collisions automatically
- Pandera tutorial — https://pandera.readthedocs.io/en/stable/tutorials/tutorial_1.html

---

## 6. Join Strategy

**Fuzzy join key is pragmatic, not robust.** The sorted-character key handles the Irish-name case well, but it has known failure modes (§6 of DATA_LIMITATIONS.md captures these honestly). What it does not have is a way to *know* when it has failed.

The specific question worth sitting with: if two different TDs have names that are anagrams of each other, how would your pipeline signal that right now? (It wouldn't — one would silently overwrite the other.)

**Consider a fallback rather than a replacement.** The stable identifier is the Oireachtas member URI (`unique_member_code`). For API-sourced data you have this. For PDF-sourced data you don't. A two-stage join strategy — exact match on stable ID where possible, fuzzy match where not, with the fuzzy matches logged and count-checked — is how this is normally done in production. `rapidfuzz` gives you a confidence score per match, which means you can threshold: high-confidence matches auto-apply, low-confidence matches land in a review queue.

**The normalisation logic is duplicated.** It lives in `normalise_join_key.py` and again in `payments.py`. If one of them ever diverges — even by a whitespace strip — the same TD will produce two different keys from two different sources and the join will fail silently. Ask yourself: how would you discover that divergence? (You wouldn't, until a specific TD started appearing twice or not at all.)

**Learning:**

- `rapidfuzz` — https://rapidfuzz.github.io/RapidFuzz/ — the modern fast fuzzy matching library
- "Probabilistic Record Linkage" (Splink docs) — https://moj-analytical-services.github.io/splink/ — UK Ministry of Justice's record linkage library, genuinely the best open-source tool for this problem
- "The Fellegi-Sunter Model" — https://en.wikipedia.org/wiki/Record_linkage#Probabilistic_record_linkage — foundational paper summary; worth understanding even if you never implement it
- Dedupe.io blog — https://dedupe.io/blog/ — practical entity resolution writeups

---

## 7. Configuration and Portability

**Hardcoded Windows paths in five places.** You know this. It's in the bugs list. But it's worth naming *why* this is symptomatic rather than just a bug: each hardcoded path is an encoding of the assumption that the code runs on your specific machine. The question to ask about each is not "how do I fix this" but "where else am I making that kind of assumption?". Probably: working directory, tool paths, the assumption that PDFs download before the script runs, the assumption that lobbying CSVs are manually placed.

**`doc/config.py` exists and is not used.** You have done the work of defining the paths centrally. You haven't done the work of *enforcing* that they are used. Until every script imports from `config.py`, the central config is aspirational.

**No environment separation.** Dev / staging / prod don't exist as concepts in the codebase. For a civic side project this is fine. For anything that ingests changing upstream data and feeds a dashboard people read, the concept of "I tested this against a pinned snapshot before pointing it at live data" starts to matter. Pinning to a dated snapshot of the lobbying CSV in tests, rather than reading whatever is in `lobbyist/raw/` today, is the minimum version of this.

**Learning:**

- `pydantic-settings` — https://docs.pydantic.dev/latest/concepts/pydantic_settings/ — for environment-driven config with validation
- `python-dotenv` — https://github.com/theskumar/python-dotenv — simpler, for when pydantic-settings is overkill
- "The Twelve-Factor App — Config" — https://12factor.net/config — the canonical write-up on why config belongs in environment, not code

---

## 8. API Client Design

**Three scripts, one pattern.** `member_api_request.py` (now consolidated), `bills_by_td.py` (consolidated), `question_api.py` (consolidated) all follow the same shape: build URL, call API, parse JSON, handle errors, write JSON. The consolidation into `oireachtas_api_service.py` is progress. It is not yet an abstraction.

A single `OireachtasClient` class with one `get(endpoint, params)` method, backed by a `requests.Session` with a `Retry` adapter, would collapse what's currently three scenario branches into method calls that share error handling and retry logic.

The question worth sitting with: if the Oireachtas API starts returning 429 rate-limit responses next week, how many places in your code need to change? (Right now: one, but not the place you'd expect — the retry logic needs to live *inside* the fetch, not per-scenario.)

**`urllib.request` and `requests` both used.** The main code uses `urllib.request` for the actual fetch and `requests` for nothing currently but it's imported. This is unusual — mixing HTTP libraries in one module. `requests` handles fada encoding correctly out of the box; `urllib.request` does not. Standardising on `requests` would have made the fada bug impossible rather than merely noticed-and-worked-around.

**Concurrency is uncontrolled.** `ThreadPoolExecutor(max_workers=5)` fires five concurrent requests with no rate limiting, no backoff, and no circuit breaker. For a civic-data API that probably has lenient limits, this is fine. For a commercial API it would get you blocked. The pattern matters regardless: "what happens when the API is slow?" is a reasonable interview question and the answer "5 threads sit on open connections until timeout" is not the answer you want.

**Learning:**

- `requests` Session and HTTPAdapter — https://docs.python-requests.org/en/latest/user/advanced/#session-objects
- `urllib3` Retry — https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.Retry
- `httpx` — https://www.python-httpx.org/ — the modern successor to `requests`, supports async
- "Rate limiting strategies and techniques" (Cloudflare) — https://blog.cloudflare.com/counting-things-a-lot-of-different-things/ — the other side of the same problem, useful perspective

---

## 9. Lobbying Pipeline — Specific Feedback

The lobbying pipeline is the strongest piece of domain work and also the weakest piece of engineering. That's typical — you've been exploring the problem rather than consolidating the solution, and the code shows it.

**The two bugs on lines 86 and 119 are symptomatic.** Both are reference mistakes — using the wrong variable name, forgetting a join already happened. These are the kind of bugs that stop existing the moment functions become small and pure. If `explode_dpo_targets(df)` took a dataframe and returned a dataframe, there would be no ambiguity about which `df` it's operating on. The procedural style of the current script invites these errors.

Ask yourself: what's the smallest refactor that makes line 86 impossible to get wrong? (Hint: it's not a comment.)

**No test would currently catch a regression here.** The DPO-explode bug produced wrong counts in `most_lobbied_politicians.csv`. How would you know if it appeared again? Manual inspection of the output CSV. A test that asserts "sum of per-TD counts equals total DPO contacts in the source" would catch this class of bug mechanically.

**The conflict-of-interest idea is the highest-value work in the project.** It is also the one most likely to produce embarrassing output if shipped carelessly. A match on the word "health" between a member's declared healthcare shareholding and a hospital group's lobbying activity is trivially easy to produce and wrong half the time. The right approach: keyword match is the retrieval, human review is the precision filter. Build the pipeline to produce candidate matches; don't publish the results without reviewing them.

The question worth sitting with here is not engineering, it's product: what's the *minimum* claim you'd make about a matched pair, and what evidence would you need to make it? ("TD X declared shares in sector Y; organisation Z operating in sector Y lobbied TD X on date D" is a factual claim. "TD X has a conflict of interest" is an editorial claim. The first is defensible from the data; the second requires journalism.)

**Learning:**

- Scikit-learn TF-IDF docs — https://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting — the right baseline for keyword matching
- "Information Retrieval" (Manning, Raghavan, Schütze, free online) — https://nlp.stanford.edu/IR-book/ — chapters 6 and 7 on scoring and term weighting
- ICIJ's Offshore Leaks methodology — https://www.icij.org/investigations/panama-papers/pages/panama-papers-about-the-investigation/ — how real investigative data journalism validates matches before publishing
- "Functional Programming in Python" (David Mertz, free O'Reilly) — https://www.oreilly.com/library/view/functional-programming-in/9781492048633/ — makes the "small pure functions" argument concretely

---

## 10. Questions Worth Sitting With

These are deliberately phrased as questions rather than recommendations. The question is the useful object — the answer will depend on context I don't have.

- What's the blast radius of an upstream schema change? If the Oireachtas API adds a new field to the member response tomorrow, does your pipeline still run? If they rename one, does it fail loudly or silently?
- What's the refresh cadence — and does the code enforce it? Lobbying returns drop quarterly. Attendance PDFs appear weekly. If someone runs the pipeline today vs tomorrow, what changes?
- Where does the data's accuracy decline over time? Names change. TDs resign. Parties rebrand. Which of your outputs become wrong after six months with no code changes?
- If you hire a collaborator tomorrow, what's the first thing they'd need to know that isn't in the repo? Write that down.
- What's the narrowest public commitment you could make about data quality? "I guarantee every row in the enriched CSV corresponds to a sitting TD" is narrow and defensible. "I guarantee attendance numbers are correct" is wide and indefensible. The narrow version is the one you can build tests for.
- Is the current architecture optimised for adding new data sources, or for changing existing ones? Your last 30 commits can answer this empirically.

---

## 11. Experimental — Data Intersections and Adjacent Destinations

This section is more speculative than the rest. The intent is to surface overlap ideas and "where else could this live" possibilities, not to recommend all of them. Pick what resonates.

### 11.1 Intersections worth pursuing

**Attendance × Committees × Ministerial office → the "invisible worker" cohort.** TDs with low plenary attendance AND no committee assignments AND no ministerial office. This is the cohort a journalist would want to ask about. The same query in reverse (high attendance + multiple committees + ministerial office) surfaces the most-loaded MPs — useful for a "do they have time for all this?" story.

**Lobbying × Member Interests × Questions → influence chain.** For each TD, for each lobbying subject, check whether they asked a parliamentary question on the same subject in the 90 days following the lobbying contact. The null result is informative (lobbying doesn't predict PQs) and the positive result is publishable (lobbying correlates with PQs).

**Payments × Attendance.** The parliamentary standard allowance is paid regardless of attendance. A TD receiving €X in payments while missing half of sittings is a story about the system, not about the individual. The framing matters — it's a structural observation, not an accusation.

**Bills × Debates × Votes.** Once divisions and debates are ingested: for each bill, which TDs spoke on it, which TDs voted on it, which TDs were absent. The cross-tab "spoke against but voted for" flags rebels (or whips being whipped).

**Constituency demographics × Attendance × Bill topic.** Do TDs from high-unemployment constituencies sponsor more labour/welfare bills? Do rural TDs attend more plenary sittings because they need the travel claim? These are testable hypotheses rather than narratives.

**Bill duration × Government vs Private Member.** How long does a Government bill take from introduction to enacted, compared to a Private Member's bill? The median gap is almost certainly large. The interesting finding is the outlier PMs that passed fast — what did they have in common?

**Bills × Lobbying → "lobbied bills".** For each enacted bill, which lobbying organisations were active in the relevant sector during the bill's committee stage? This is the highest-watermark analysis — a clear methodology, a clear output, and inherently interesting. Also inherently easy to get wrong, so needs the same careful framing as §9.

**Register of Interests × Committee Assignment.** TDs whose declared financial interests align with the sector their committee oversees. A TD on the Finance committee who declares bank shareholdings. A TD on the Housing committee who declares property holdings. These are not allegations; they are questions worth surfacing.

**Temporal pattern: "quiet week" detection.** Attendance dates form a time series. For each TD, identify weeks with sharply lower-than-their-baseline attendance. Cross-reference with illness announcements, ministerial reshuffles, resignation events. The bulk of quiet weeks will be mundane; the ones that aren't are story leads.

### 11.2 Where else this could live

**An RSS feed / weekly email.** The dataset as-is is a static artefact. The same data as a weekly digest — "this week in the Dáil: X bills introduced, Y parties lobbied, Z TDs missed the most sittings" — is a product. Buttondown or Substack, 100 subscribers, and a weekly publication cadence would turn this from a portfolio piece into a civic resource. The Irish civic-tech space does not have this yet.

**A CSV dataset on Kaggle or HuggingFace.** Published dataset with versioned releases. Downloads are a signal of independent usefulness. Academics writing about Irish politics would cite it. This is low-cost and high-legitimacy.

**An API.** FastAPI + DuckDB gives you a JSON API over the gold layer in a day. `GET /td/{id}` returns a TD's full profile. `GET /lobbying?since=2024-01-01` returns lobbying contacts. This turns the project from "a repo" into "a service" — and a service with an API is the kind of thing that invites other people to build on top.

**A visualisation commissioned by a news outlet.** Noteworthy, thejournal.ie, RTÉ's data desk, the Irish Times. One-off interactive visualisations with a strong editorial angle. This is how civic data work actually reaches people. A single "I built the choropleth that appeared in the Irish Times on X date" carries more signalling value than any number of GitHub stars.

**A research paper.** Political science departments run empirical projects on parliamentary behaviour. A well-prepared dataset with a clear methodology section is publishable in a junior-author position. UCD, Trinity, DCU, Queen's Belfast all have people in this space.

**An undergraduate thesis contribution.** If you're ever near a political science, journalism, or data science programme that wants a capstone project, this is a ready-made foundation. You could offer supervision and co-authorship.

**A training dataset for small LLMs.** The flattened bill records, question records, and debate records (when ingested) constitute a domain-specific corpus. Fine-tuning a small LLM (Llama 3 8B, Mistral 7B) on "Irish parliamentary language" is an experimental piece of AI work that nobody else has the data to try.

**A competition entry.** Kaggle occasionally runs civic data competitions. Data for Democracy, Open Knowledge Ireland, and the Tetra Tech Civic Data Challenge run similar. A polished pipeline is a head start on any of these.

**An Irish "OpenStates".** The US has OpenStates.org, a single tool that normalises state-legislature data across 50 states. Ireland has one parliament, so the direct analogue doesn't scale — but "comparative European parliaments" would. Adding the UK (parallelparliament.co.uk), the Netherlands (openkamer.org), and a few more would turn this into an EU-comparative tool. That's multi-year work, not a weekend, but the scoping is worth thinking about.

### 11.3 Ideas to deliberately *not* pursue

**Sentiment analysis of debates.** Sounds great, delivers badly. Parliamentary speech is adversarial by design — sentiment classifiers trained on product reviews produce garbage on it. If you do this, you need domain-fine-tuned models or you produce cargo-cult charts. High effort, low trust-in-output.

**Predicting election outcomes.** Tempting, especially if you add polling data. Doable with enough care. Wrong reasons to do it: it's flashy. Right reasons to do it: you have a specific hypothesis about Irish electoral behaviour that needs this data to test. Without a hypothesis, it's a horoscope generator.

**A "dashboard of everything".** Kitchen-sink dashboards convey less than focused ones. The impulse to surface every metric the data can compute produces a page nobody reads. Better to pick three strong questions and answer them well.

**Generative AI explanation of voting records.** "Ask an AI why this TD voted this way" — lets the user feel informed without being informed. Hallucinations in political claims are not a niche risk; they are the risk. Don't.

---

## Final Note

The hardest feedback to give on a project like this is "it's good, and here's what would make it cross the line from good to striking". Most of what's above is of that shape. None of it is suggesting you rebuild from scratch or that the current work is wrong. The current work is solid; there are ways to make it land harder. Pick a few.

The single most important piece of feedback, if I had to compress the whole document to one sentence: *the work you've done is undersold by its current presentation, and the gap is closed more by one deployed dashboard and one written finding than by any amount of additional code.*