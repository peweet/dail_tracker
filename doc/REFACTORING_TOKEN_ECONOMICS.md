# Refactoring for Token Economics

Source: Giles Edwards-Alexander, "The Economic Benefit of Refactoring", martinfowler.com
Exploring Gen AI series, 30 July 2026
(https://martinfowler.com/articles/exploring-gen-ai/refactoring-economic-benefit.html).
All figures in §1–§2 are [Reported — single authoritative source, not reproduced here].
The application of the findings to this repo (§4) is this document's own analysis.

## 1. The experiment

The author's 150 kLoC agent-written Rust application grew a 17,155-line single-file data
access layer. He refactored it in fifteen measured steps and, after every step, had a fresh
sub-agent execute the identical feature-change prompt (add an `ItemWatchStore` trait with
three methods, implemented for both the fake and the real store), recording input tokens,
output tokens, and wall time, then discarding the change. Because agents carry no memory
between sessions, the same change replayed by a fresh agent is a controlled experiment — a
human engineer would learn the codebase and contaminate the measurement.

Headline result: input tokens for the same change fell from 159,564 to 27,360 — an 83%
reduction — while output tokens stayed flat (1,705 baseline → 2,113 final, noise-dominated).
Total lines of code in the layer stayed nearly constant (17,155 → 16,608 across 19 files).
The saving repeats on every future change that touches the layer.

## 2. Durable learnings

1. **The saving is in reading, not writing.** Output tokens (the code produced) did not
   move; input tokens (the code the agent had to read to orient itself) collapsed. Refactoring
   for agents optimises the read path.
2. **Largest-file size is the leading metric, not total LoC.** Total layer LoC was flat
   throughout. Input tokens stayed flat until the largest single file began to shrink
   (17,155 → 9,269 lines: tokens 159k → 104k), then fell off a cliff at the final split
   (largest file 7,225 → 3,695: tokens 107k → 27k).
3. **The payoff is non-linear and back-loaded.** Twelve of fifteen steps produced almost no
   measured saving. They were not wasted: the local deduplication steps exposed the repeating
   core that made the final domain-aligned split possible. Do not judge a refactoring
   sequence by mid-sequence measurements.
4. **Splits must be domain-aligned, or they don't pay.** The author's stated mechanism: the
   agent banks the saving only if it can identify the smallest subset of files to read.
   Randomly cutting a big file into small files forces the agent to read many files hunting
   for the relevant code. The Rust split grouped 17 traits into four domain files
   (planning / content / people / system) and store impls into one file per domain.
5. **A stable interface is the precondition.** The 17 kLoC file was a good target because it
   was a self-contained module with a clear boundary to preserve. The refactoring prompt's
   hard constraint was "without changing the interface at all".
6. **Agents neither choose nor apply refactorings well (as of the article's writing).**
   Claude could not look at code and select applicable refactorings unprompted — a harness
   with an explicit "refactoring step" never improved the file. It skipped the single most
   valuable step (the store split) on the first pass. Mechanically, it applied edits via
   grep/sed Python scripts that got confused by indentation. Human-directed plan, one
   Fowler-named refactoring per step, each step individually tested.
7. **Measure the cost of the refactoring itself.** The author failed to and could only bound
   it: ≤5M tokens (including designing the experiment twice over). Against a 132k-token
   saving per change, the naive breakeven is ≈38 same-sized changes — before counting the
   flat-rate reality of subscription pricing, faster wall time (342 s → 454 s was noise, but
   orientation reads dominate latency), and healthier context windows on complex tasks.
8. **Housekeeping masquerades as model slowness.** Test execution degraded during the
   experiment; the cause was a bloated cargo build cache, not the agent or the hotel WiFi.
   Check the boring causes before blaming the interesting ones.
9. **Live token accounting is unreliable**; the experiment approximated tokens as
   characters-read ÷ 4 and had the sub-agent self-report files read and response size in a
   JSON footer. Crude, but consistent across steps, which is all a controlled comparison needs.

## 3. Design patterns

### P1 — Cost-of-Change Benchmark
*You want to know whether a refactoring (or any structure/tooling change) pays for itself
in agent-time.* Freeze one representative change as a single self-contained prompt. Execute
it in a fresh sub-agent against the current tree; have the agent report files read with
character counts and response size; approximate tokens as chars/4; discard the change.
Re-run after each intervention. The fresh agent's amnesia is what makes the comparison
valid. Consequence: the benchmark is only as representative as the frozen change — a small
additive change measures orientation cost, not design quality.

### P2 — Refactor-for-Reading
*A hot file has grown past the point where an agent can orient cheaply.* Optimise the read
path: the target metric is input tokens per change (proxy: largest-file LoC on the paths
agents actually touch), not total LoC, not elegance. Prioritise by
change-frequency × file-size, because the saving is banked per future change.

### P3 — Dedup, then Split (in that order)
*How to actually execute P2.* First pass: local, in-file refactorings — Extract Function,
Extract Class, Replace Inline Code with Function Call, builders — to compress boilerplate
and expose the repeating core. Second pass: Move Function into a module directory, split
along the domain seams the first pass revealed, one domain per file, mod/`__init__` as a
pure re-export manifest so the import surface is unchanged. Co-locate tests with the module
they exercise. Splitting first (or only splitting) fails: the seams aren't visible yet, and
random splits make reading worse.

### P4 — Interface-Preserving Boundary
*Choosing the target.* Refactor behind a boundary that external callers already treat as an
interface, and hold that interface fixed for the whole sequence. If a module has no clear
boundary, establishing one is the prerequisite refactoring, not step 3.

### P5 — Human-Directed, Step-Tested Plan
*Executing with agents.* A human (or a deliberately-prompted planning pass citing a named
catalogue — the article used Fowler's *Refactoring* 2nd ed. by section number) selects the
refactorings and their order. Each step is separately applied, separately tested, and
reversible. Do not expect an agent to spot the opportunity, and audit for silently skipped
steps — the most valuable one is the likeliest casualty because it is the biggest.

## 4. Application to this repo

This repo already attacks orientation cost from the retrieval side: fact_cards /
`describe_dataset`, `search_project`, `outline`, SECTION MAP headers in files >1,500 lines,
the flood_warn hook, tiered memory, and `token_ledger.py`. The article's result says
retrieval aids are necessary but not sufficient — SECTION MAP headers help an agent find a
span, but the measured saving only arrived when the largest file physically shrank and the
agent could read a small, correctly-named file instead of a span of a huge one.

What the article adds, concretely:

- **A measurement we can run.** P1 needs only what's already installed (agent SDK evals,
  `token_ledger.py`). A per-candidate frozen change + fresh-agent replay gives this repo its
  own before/after numbers instead of trusting the article's.
- **A ratchet metric.** `check_conventions.py` already ratchets extractor/page conventions;
  a largest-file ratchet (no tracked source file above its current line count; hot files
  must shrink) is the same mechanism pointed at learning #2.
- **A candidate register.** Hot-file candidates, chosen by churn × size with a preserved
  interface, are listed in the companion candidate plan (see `doc/REFACTORING_CANDIDATES.md`).

The existing convention ratchet (extractors → `services/http_engine`, `coverage_io`,
`parquet_io`, `extract_runner`; pages → `ui/format.py` + `@dt_page`) is precisely P3's first
pass, already in flight. The article supplies the missing second pass and the measurement
that tells us whether either pass is paying.
