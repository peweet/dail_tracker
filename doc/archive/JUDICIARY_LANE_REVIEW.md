# Judiciary / Courts Lane — Review

**Scope:** the Judiciary/Courts lane only of `doc/dail_tracker_local_housing_procurement_judiciary_plan.md`
(sec 4, sec 6 judiciary contract, sec 7 judiciary+CPO views, sec 8 judiciary tests,
sec 9 judiciary acceptance, Sprint-7 CPO probe). Privacy is the dominant axis.
**Date:** 2026-06-05. **Verdict in one line:** architecture is sound and the plan is
broadly accurate, but the published gold `cases` layer **demonstrably leaks full
natural-person names today**, the only privacy guard is a single `assert` that checks a
column *name* (not content), and there are **zero** privacy tests — so the page must be
**beta-hidden / HOLD from public exposure** until golden anonymisation tests pass.

---

## Claims Ledger

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|
| App routes Courts & Judiciary | sec 4 / sec 12: `Courts & Judiciary` at `rankings-judiciary` | `utility/app.py:7` import, `:125-130` `st.Page(judiciary_page, title="Courts & Judiciary", url_path="rankings-judiciary")` | confirmed |
| Legal-diary is NOT a pipeline.py chain (separate poller) | sec 4 J3: "If not in pipeline.py … mark as separately scheduled poller" | No `judiciary`/`legal_diary` token in `pipeline.py` (grep: no matches). Driven by Win Task Scheduler: `tools/run_legal_diary_daily.ps1:25-32`, registered by `tools/register_legal_diary_task.ps1` | confirmed — separate poller, not a chain |
| Poller exists | sec 4 arch: `pdf_infra/legal_diary_poller.py` | `pdf_infra/legal_diary_poller.py:1` (doc'd "legal_diary_poller.py"); plan named it `legal_diary_poller.py` correctly | confirmed |
| Extractor produces 3 privacy tiers | sec 4: schedule / counts / anonymised cases | `extractors/legal_diary_extract.py:312-336` (Tier A sched, Tier B counts, Tier C anonymised cases) | confirmed |
| Three gold parquets | sec 4: `judicial_legal_diary_{schedule,counts,cases}.parquet` | written `legal_diary_extract.py:315,320,336`; all three exist on disk + committed | confirmed |
| Three SQL views | sec 4 / sec 7: `v_judiciary_legal_diary_{schedule,counts,cases}` | `sql_views/judiciary_legal_diary_{schedule,counts,cases}.sql` (`:11,:8,:16` resp.) | confirmed |
| Core query + data-access + page | sec 4 | `dail_tracker_core/queries/judiciary.py:35-47`; `utility/data_access/judiciary_data.py:29-43`; `utility/pages_code/judiciary.py:283` | confirmed |
| Privacy uses `assert` (must replace) | sec 4 J1: replace `assert "raw_case" not in cases_df.columns` with runtime exception | `extractors/legal_diary_extract.py:339` is *exactly* that assert — and it is the **only** privacy guard in the writer | confirmed (the plan's bad-example is real code) |
| In-camera categories dropped | sec 4 / sec 6: minors/family/wards/childcare/asylum dropped | `legal_diary_extract.py:130-134` `PROTECTED_KEYS`, `:327` `.filter(~pl.col("protected"))`; coverage shows 230 dropped (asylum 167, minor 43, family 18, child&family 2) `data/_meta/judicial_legal_diary_coverage.json:26-33` | confirmed (keyword-based) |
| Natural persons reduced to initials | sec 6: "No raw party names" | `anonymise()` `:197-207`, `_initials()` `:183-190` — **but leaks, see Devil's Advocate** | partially wrong — heuristic, not robust |
| `raw_case` never in gold cases | sec 6 | gold cols are exactly the 10-col contract; `raw_case` absent (probe: `contract_ok=True`, `forbidden_present=[]`) | confirmed |
| Golden privacy tests exist | sec 4 J2 / sec 8: `test/test_judiciary_privacy.py` | **no such file** (glob `test/**/*judiciary*` + `*legal_diary*` → none) | wrong — the test the plan asks for does not exist yet |
| Raw daily docx git-ignored | poller docstring `:20-22` | `.gitignore:352-358` explicit block + `git check-ignore data/bronze/legal_diary/` → ignored | confirmed |
| Raw sandbox audit parquet not committed | extractor `:25` "NEVER promoted to gold" | `git check-ignore` ignores it (blanket `*.parquet` `.gitignore:2`); `git ls-files` → untracked | confirmed |
| Gold cases parquet IS committed/shipped | (implicit) | `git ls-files` lists `data/gold/parquet/judicial_legal_diary_cases.parquet`; re-included `.gitignore:295-297` | confirmed — **raises the anonymisation bar: it ships to Cloud** |
| CPO belongs to Infra/LA, not Judiciary | sec 7 / Sprint-7: "Do not merge CPO into Judiciary" | no CPO extractor/view exists at all (no `*cpo*` files) — CPO is scoped-only | confirmed (and currently moot) |

---

## Architectural Assessment

The lane is **well-architected and matches the repo's house style**:

- **Clean tiering by privacy risk.** Tier A (schedule: public officials only) / Tier B
  (counts: aggregate density) / Tier C (anonymised cases) is the right decomposition.
  Tier A/B carry no party data *by construction* and are genuinely safe
  (`legal_diary_extract.py:312-320`). The risk is concentrated entirely in Tier C.
- **Correct separation from pipeline.py.** A forward-accumulating, source-of-one-day
  feed (Domino/.nsf, no historical URL — poller docstring `:1-18`) does not belong in
  the batch pipeline. The Windows-scheduled `run_legal_diary_daily.ps1` runs poller →
  extractor only on poll exit 0, with idempotent archiving (`:136-145`) and a "don't
  clobber gold on 0-parse drift" guard (`legal_diary_extract.py:307-310`). This is the
  *better* design than the plan's fallback "add a `judiciary` chain" (sec 4 J3) — keep it
  out of pipeline.py.
- **Layer discipline holds.** `queries/judiciary.py` is retrieval-only `SELECT *`;
  `data_access/judiciary_data.py` is a thin cached unwrap to `.data`; faceting/grouping
  happen in the page. Consistent with the rest of the app.
- **Parquet writes compliant.** `PARQUET_KW` = zstd/3/statistics on all three writers
  (`:72,315,320,324,336`).
- **Provenance is real.** Every Tier C row carries `source`, `source_url`,
  `source_sha256` (`:331-334`); the page surfaces the digest in the footer
  (`judiciary.py:362-369`).

**Where the architecture is weak (all in the privacy contract, not the plumbing):**

1. **The privacy invariant is an `assert`, and it's the wrong invariant.**
   `legal_diary_extract.py:339` asserts only that the *column name* `raw_case` is absent.
   It (a) is stripped under `python -O` (project invariant explicitly forbids asserts for
   privacy — REVIEW_CONTEXT §2), and (b) **never checks the anonymised content** — a
   column called `case_anonymised` full of real names passes this assert trivially. The
   plan's J1 fix (raise `PrivacyInvariantError`) is necessary but **insufficient**: it
   only hardens the same too-weak check.
2. **Anonymisation is a regex/keyword heuristic with no test and no reconciliation.**
   `_initials` / `_is_org` / `strip_refs` are best-effort string surgery. There is no
   golden test (the file the plan names in J2/sec 8 does not exist) and no post-write
   content assertion. Heuristic + un-tested + committed-to-Cloud = the worst combination
   for a Critical-severity privacy surface.
3. **The drop-list is also keyword-only.** `PROTECTED_KEYS` (`:130-134`) is a hand list;
   an in-camera matter phrased without one of those tokens is *kept and anonymised*
   rather than dropped. No test guards completeness.

---

## Devil's Advocate

I ran read-only probes against the **already-published** gold cases parquet
(`pipeline_sandbox/probe_review_jud_*.py`). Findings (party names redacted here; raw
strings deliberately not reproduced in this doc):

- **The cases layer leaks full natural-person names into committed gold — today.**
  Quantified: **8 of 602 rows** expose at least one real personal name in clear. Two
  structural bugs in `anonymise()` (`:197-207`):
  - **(A) First-`v`-only split (`maxsplit=1`, `:201`).** Listings of the form
    `… CONSOLIDATED WITH <Full Name> -v- <Company> PLC` are split once; the *second*
    `v` clause is never anonymised, so a full plaintiff name survives. (1 row.)
  - **(B) Whole-side org test (`_is_org`, `:193`).** A multi-party side like
    `<Name1> and <Name2> and <Name3> and <X> County Council` contains the org token
    `council`, so the **entire side is kept verbatim**, exposing the named individuals
    riding alongside the public body. (7 rows; e.g. three-person plaintiff chains, and
    multi-defendant `and …` lists.) This is the dominant leak path and will scale with
    every new day captured.
  These are not edge curios — they are in the **Cloud-shipped** parquet. Severity:
  Critical (matches risk-register row "Judiciary exposes personal case details").
- **Re-identification via court+date+list even when initials are clean.** The Legal
  Diary is itself public for the current day. A user who downloads our archive of
  *past* days gets (court, courtroom, judge, list_type, time, initials, category) — a
  quasi-identifier set. For a niche list (e.g. one Commercial matter before a named
  judge on a given date) the initials + list + court can be matched back to the live
  diary or to news coverage. Initials-only does **not** defeat this; the mitigation is
  the in-camera drop (good) plus *never presenting a followable per-party register*
  (the page does nest under judge/list, `judiciary.py:261-279` — correct), but the
  raw per-row gold still enables it for anyone with the parquet.
- **"Listed sessions" still implies judge performance.** The plan (sec 4 J4) and page
  copy avoid "busiest judge", but section ② is literally titled **"Busiest lists
  today"** with a ranked proportion bar and the *judge's name* in the subtitle
  (`judiciary.py:204-219`). A reader will read "busiest" as a workload/performance
  signal attributable to a named judge. This is borderline against "no inference in UI"
  and the plan's own "Avoid: busiest judge". Recommend retitling to neutral
  *"Most active lists"* and/or suppressing the judge name in the busiest-list subtitle.
- **`raw_case` reaching gold — does NOT happen** (contract verified, `forbidden_present=[]`),
  but the *equivalent* leak (un-anonymised names inside `case_anonymised`) happens, which
  the contract check cannot catch. The contract is testing the wrong thing.
- **CPO leaking addresses/persons — not applicable yet** (no CPO code exists). The
  Sprint-7 instinct to keep CPO out of public UI and out of Judiciary is correct; a CPO
  fact would carry landowner names + property addresses (acute PII) and belongs in an
  Infra/LA context behind the same kind of (currently-missing) golden privacy tests.
- **Civic value vs privacy/legal risk.** Tier A/B (who sits where, how busy each list is)
  is high civic value and low risk — that alone is a defensible public page. Tier C's
  marginal value (anonymised case texture) is **not worth the Critical leak risk** in its
  current heuristic form. The honest trade is: ship A/B, gate C.

---

## Data Quality & Enrichments

- **Coverage is thin but honest.** 2 days captured (2026-06-04/05), 135 sessions, 832
  case lines parsed, 230 dropped protected, 602 kept (coverage JSON). Drop reasons are
  dominated by asylum (167) and minors (43) — the high-risk categories are being caught.
- **Category classifier is coarse.** `category_of` (`:151-159`) is keyword precedence
  (prosecutor→criminal, state→public-law, org→commercial, else civil). Fine for a chip,
  but it is render-time-counted in the page (`judiciary.py:232` `value_counts()` with a
  `logic_firewall: display_only` marker) — acceptable per the gold-layer display-only
  precedent, but a counts-by-category rollup would be better owned by a view if it ever
  drives anything but a chip.
- **`strip_refs` is solid** for the case-reference families it targets (`:163-175`), and
  the probe found **no** statutory-reference or protected-keyword leakage in gold text.
  The residual risk is purely the *party-name* paths (A) and (B) above.
- **Enrichment worth deferring:** linking listed matters to judgments / courts.ie case
  pages would re-introduce party identifiers — explicitly do not.

---

## Build / Defer / Reject

| item | verdict | value / effort | reason |
|---|---|---|---|
| Tier A schedule page (today on the bench) | **BUILD / keep public** | high / done | Officials in public function, zero party data; strongest civic content |
| Tier B counts (retitled "Most active lists", judge name softened) | **BUILD w/ copy fix** | med / S | Aggregate density is safe; current "Busiest … <judge>" framing risks performance read |
| Tier C anonymised cases — public exposure NOW | **HOLD (beta-hide)** | — | Leaks 8+ real names into committed gold today; not test-verifiable |
| Replace `assert` (`:339`) with `raise PrivacyInvariantError` | **BUILD (P0)** | high / S | Project invariant; stripped under -O |
| **Content** privacy assertion in writer (post-anonymise scan) | **BUILD (P0)** | high / M | The column-name check is the wrong invariant; must scan `case_anonymised` for residual full names / second-`v` / org-side individuals and FAIL the write |
| `test/test_judiciary_privacy.py` golden tests | **BUILD (P0, blocks public C)** | high / M | sec 8 test; currently absent; see exact cases below |
| Fix anonymise() multi-party + second-`v` (bugs A/B) | **BUILD (P0)** | high / M | Root cause of the leaks; split on ALL `v`, anonymise per-`and`-chunk not per-side |
| Add `judiciary` chain to pipeline.py (plan J3) | **REJECT** | — | Forward-accumulating poller feed; the scheduled-task design is correct. Just document it as a deliberate non-chain |
| Merge CPO into Judiciary | **REJECT** | — | CPO = Infra/LA context; landowner PII; plan already says so |
| CPO scheme-level probe (Sprint-7) | **DEFER** | med / — | Probe-only, address/person leak guard first; not a Judiciary concern |

**Exact golden privacy tests that must precede any public Tier C exposure**
(`test/test_judiciary_privacy.py`, run against a fixture day + the live gold parquet):

1. gold cases columns == the 10-col contract; assert `raw_case`/`party`/`solicitor`
   columns absent (keep the current check, but as `raise`, not `assert`).
2. **No residual full name in `case_anonymised`:** every `v`-delimited side that is not
   org-classed must match an initials shape (`^[A-Z](\.[A-Z])*\.?( & Ors)?$`); fail on any
   TitleCase multi-letter token in a non-org side.
3. **Multi-`v` coverage:** any row with ≥2 `v`/`-v-` tokens must have *every* segment
   anonymised (regression for bug A).
4. **Org-side mixed-party:** for any side containing an org keyword AND ` and `, each
   `and`-chunk that is not itself org-classed must be initials (regression for bug B).
5. Protected drop completeness: a fixture seeded with each `PROTECTED_KEY` phrase yields
   0 kept rows for those lines; assert `coverage.drop_reasons` non-empty.
6. Provenance present: `source`, `source_url`, `source_sha256` non-null on every row.
7. Page-level: `judiciary_page` renders Tier C only through judge/list grouping (no flat
   followable register) and shows the source-unavailable state when the view is empty.

---

## Bottom Line

The Judiciary lane is **architecturally correct and the plan's claims are accurate** —
the tiering, the separate-poller design, the provenance, the in-camera drop, and the
layer discipline are all genuinely good, and the plan's J1 instinct (kill the assert) is
right. But the plan **understates the severity**: the published, Cloud-shipped gold
`cases` parquet **leaks full natural-person names today** (8+ rows, two reproducible
anonymiser bugs — first-`v`-only split and whole-side org classification), the sole
privacy guard is a `-O`-strippable `assert` that checks a column *name* rather than
content, and the golden privacy tests the plan itself calls for **do not exist**. Tier A
(schedule) and a re-titled Tier B (counts) are safe and should stay public; **Tier C must
be beta-hidden / HELD from public exposure** until the multi-party anonymiser bugs are
fixed, the assert is replaced by a content-scanning runtime `PrivacyInvariantError`, and
`test/test_judiciary_privacy.py` (cases 1–7 above) passes. Keep CPO out of Judiciary
entirely — it is an Infra/LA matter with its own acute PII to gate.
