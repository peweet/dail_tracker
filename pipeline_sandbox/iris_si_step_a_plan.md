# Iris SI authorship — Step A enrichment plan

Pairs with [iris_oifigiuil_etl_polars.py](iris_oifigiuil_etl_polars.py).
Prerequisite for any UI page on SI authorship. No graduation work in this
step — Step A makes the existing taxonomy table fit to count from.

## 1. The gap

The existing Iris parser produces
[out/iris_si_taxonomy.csv](../out/iris_si_taxonomy.csv) — 6,892 rows, 38
columns, covering S.I. notices from 1986→2026 (effectively 2016→present).
The shape is right. Four data-quality issues block the planned
"who governs by SI" page:

| # | Issue | Page impact |
|---|---|---|
| 1 | `si_responsible_actor` is non-empty on **2,569 / 6,892 (37 %)** | Hero leaderboard would be 63 % "Unknown" |
| 2 | 186 distinct actor strings, no department dimension | Counts split across reshuffles and verbose phrasings |
| 3 | **770 duplicate `(si_year, si_number)`** (12.6 %) | Volume inflated ~13 % |
| 4 | Title field carries body-text bleed and `None` | Card display unreadable |

Pass-2 lives entirely in `pipeline_sandbox/`. Per
[project_pipeline_sandbox_rule.md](../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_pipeline_sandbox_rule.md):
no edits to `pipeline.py` / `enrich.py` / `normalise_join_key.py`. SQL views
and the page itself wait for Step B (graduation).

## 1a. Why fix in a sibling enrichment script, not in the parser

The PDF parse takes ~minutes per refresh and is mostly stable. The four
fixes above are pure post-processing on the existing CSV — regex on
already-extracted `raw_text`, a small reference table join, and a dedup.
A sibling script lets us iterate in seconds, A/B against the published
taxonomy, and unblock the page without re-running PyMuPDF.

The actor-extraction regex *could* be folded back into
[iris_oifigiuil_etl_polars.py:1163-1173](iris_oifigiuil_etl_polars.py#L1163-L1173)
once the new patterns prove out. Defer that fold-back to graduation.

## 1b. Empirical impact on the empty-actor rows

Inspecting 8 representative empty-`si_responsible_actor` rows in the
current CSV gives four recoverable patterns and one true-null:

| Pattern | Example raw_text fragment | Recovery |
|---|---|---|
| **Comma-bearing role** | `the decision of the Minister for Housing, Planning and Local Government to transfer …` (S.I. 573/2018) | Current regex `Minister for [^,\n]+` stops at the comma; widen to allow internal commas before `to/by/in/has/of/under` |
| **EU/"These Regulations" body** | `These Regulations implement elements of EU Regulation 1257/2013 …` (S.I. 13/2019) | No explicit actor in body. Fallback: title prefix `EUROPEAN UNION (…)` ⇒ default actor `The Minister for the Environment` only when the policy domain is `environment_climate`; otherwise leave null and flag |
| **Court rules committee** | `The Circuit Court Rules Committee, with the concurrence of the Minister for Justice …` (S.I. 19/2019) | Add explicit anchors for `Circuit Court Rules Committee`, `Superior Courts Rules Committee`, `District Court Rules Committee`. These are SI-issuers in their own right |
| **Named-minister leakage** | `Eamon Ryan, Minister for the Environment` (already captured but as a person) | Post-process: when matched value contains a `,`, take only the substring after the last comma if it starts with `Minister for` / `Minister of State` |
| **True null** | `S.I. No. 569 of 2018.` (single line — extraction failed at the parser) | Leave null; `extraction_confidence=0.32` already quarantines via title-null. No recovery without re-OCR |

Expected fill rate after pass-2: **>85 %** based on the 8-row sample. Step
A's success criterion is exactly this number measured against the full
6,892 rows (see §5).

## 1c. Department dimension is missing entirely

Top-20 raw `si_responsible_actor` strings (from the current CSV) collapse
to ~12 durable departments once normalised:

```
The Minister for Finance                  728  →  Finance
The Minister for Housing                  221  →  Housing, Local Government & Heritage
The Minister for Enterprise               138  →  Enterprise, Trade & Employment
The Minister for Transport                126  →  Transport
The Taoiseach                              88  →  Taoiseach
The Minister for Business                  87  →  Enterprise, Trade & Employment   ← reshuffle merge
The Minister for Agriculture               83  →  Agriculture, Food & Marine
The Minister for the Environment           83  →  Environment, Climate & Comms
The Minister for Justice                   73  →  Justice
The Minister for Children                  56  →  Children, Equality, Disability …
The Minister for Public Expenditure and …  56  →  Public Expenditure & Reform
The Minister for Jobs                      52  →  Enterprise, Trade & Employment   ← reshuffle merge
```

`The Minister for Justice` and `The Minister for Justice and Equality`
are the same department under different titles. `The Minister for
Business` / `Jobs` / `Enterprise` are all today's Department of
Enterprise. Without a mapping, the leaderboard counts are split.

## 2. Pass-2 actor extraction (regex tweaks)

Implementation: a new sandbox file
`pipeline_sandbox/iris_si_authorship_enrich.py` that reads the existing
CSV. The regex below is the additive set — runs only on rows where
`si_responsible_actor` is null/empty.

```python
# Comma-tolerant role pattern. Anchor on Minister-for and stop at the
# first verb-phrase rather than at the first comma.
ACTOR_ROLE_TOLERANT = (
    r"(?:The )?Minister (?:for|of State at the Department of) "
    r"[A-Za-z][A-Za-z &,'\-]+?"
    r"(?=\s+(?:to|by|in exercise|has made|of |under |with the))"
)

# Court rules committees — issue SIs under their own statutory authority.
COURT_RULES_COMMITTEES = [
    "Superior Courts Rules Committee",
    "Circuit Court Rules Committee",
    "District Court Rules Committee",
    "Court of Appeal Rules Committee",
]

# Named-minister cleaner — strip personal names, keep role only.
def role_only(s: str) -> str:
    # "Eamon Ryan, Minister for the Environment" -> "Minister for the Environment"
    # "BRIEN, Minister for Housing"               -> "Minister for Housing"
    parts = s.rsplit(",", 1)
    tail = parts[-1].strip()
    return tail if tail.lower().startswith(("minister for", "minister of state")) else s
```

Apply in this order:

1. `role_only` cleanup on **all** existing actor strings (not just empty
   rows) — fixes the 22 `Eamon Ryan, …` / `BRIEN, Minister for …`
   leakages already in the CSV.
2. For rows still empty: run `ACTOR_ROLE_TOLERANT` on `raw_text`, take
   the first match.
3. For rows still empty: scan for any `COURT_RULES_COMMITTEES` literal
   in `raw_text`.
4. For rows still empty: leave null. Do not synthesise an actor from the
   policy domain — the brief is to report what we know, not infer.

Verbose-tail trim — drop trailing fragments like `has made the above
Statutory Instrument`, `in exercise of the powers conferred`, `entitled
as above`. These appear in 35+ distinct strings today and explode the
distinct-string count from ~30 real roles to 186.

```python
TAIL_FRAGMENTS = re.compile(
    r"\s*(?:has made.*$|in exercise.*$|entitled as above.*$"
    r"|approves .*$|made (?:the |an? )?(?:Order|Regulations).*$)",
    re.IGNORECASE,
)
```

## 3. Role → department mapping table

New reference file: `pipeline_sandbox/reference/minister_role_to_department.csv`.
Hand-curated, ~40 rows. **Durable department label** ignoring reshuffle
dates — accurate enough for a leaderboard, simpler than a
date-range-keyed map. Reshuffle-aware mapping is a Step B concern.

Schema:

```csv
role_canonical,department,department_short,notes
Minister for Finance,Department of Finance,Finance,
Minister for Housing,Department of Housing Local Government and Heritage,Housing,Includes "Housing Planning and Local Government" pre-2020 rename
Minister for the Housing,Department of Housing Local Government and Heritage,Housing,Typo variant in source
Minister for Justice,Department of Justice,Justice,
Minister for Justice and Equality,Department of Justice,Justice,Pre-2020 title
Minister for Enterprise,Department of Enterprise Trade and Employment,Enterprise,
Minister for Business,Department of Enterprise Trade and Employment,Enterprise,Pre-2020 title
Minister for Jobs,Department of Enterprise Trade and Employment,Enterprise,Pre-2017 title
Minister for the Environment,Department of the Environment Climate and Communications,Environment,
Minister for Climate,Department of the Environment Climate and Communications,Environment,
Minister for Communications,Department of the Environment Climate and Communications,Environment,
Minister for Transport,Department of Transport,Transport,
Minister for Agriculture,Department of Agriculture Food and the Marine,Agriculture,
Minister for Health,Department of Health,Health,
Minister for Children,Department of Children Equality Disability Integration and Youth,Children,
Minister for Children and Youth Affairs,Department of Children Equality Disability Integration and Youth,Children,Pre-2020 title
Minister for Education,Department of Education,Education,
Minister for Education and Skills,Department of Education,Education,Pre-2020 split
Minister for Further and Higher Education,Department of Further and Higher Education Research Innovation and Science,Higher Education,
Minister for Public Expenditure,Department of Public Expenditure NDP Delivery and Reform,Public Expenditure,
Minister for Public Expenditure and Reform,Department of Public Expenditure NDP Delivery and Reform,Public Expenditure,
Minister for Foreign Affairs,Department of Foreign Affairs,Foreign Affairs,
Minister for Foreign Affairs and Trade,Department of Foreign Affairs,Foreign Affairs,
Minister for Defence,Department of Defence,Defence,
Minister for Tourism,Department of Tourism Culture Arts Gaeltacht Sport and Media,Tourism & Culture,
Minister for Culture,Department of Tourism Culture Arts Gaeltacht Sport and Media,Tourism & Culture,
Minister for Arts,Department of Tourism Culture Arts Gaeltacht Sport and Media,Tourism & Culture,
Minister for Social Protection,Department of Social Protection,Social Protection,
Minister for Employment Affairs and Social Protection,Department of Social Protection,Social Protection,
Minister for Rural and Community Development,Department of Rural and Community Development,Rural & Community,
Minister of State at the Department of Enterprise,Department of Enterprise Trade and Employment,Enterprise,
Minister of State at the Department of Housing,Department of Housing Local Government and Heritage,Housing,
Minister of State at the Department of Transport,Department of Transport,Transport,
Minister of State at the Department of the Taoiseach,Department of the Taoiseach,Taoiseach,
The Taoiseach,Department of the Taoiseach,Taoiseach,
The Government,Government (collective),Government,
The Commission for Communications Regulation,ComReg (independent),ComReg,Independent regulator — non-departmental
Superior Courts Rules Committee,Courts Service (rules committee),Courts,Independent — non-departmental
Circuit Court Rules Committee,Courts Service (rules committee),Courts,Independent — non-departmental
District Court Rules Committee,Courts Service (rules committee),Courts,Independent — non-departmental
```

The "non-departmental" rows matter editorially: they should not roll up
into a ministry on the leaderboard. The page will render them in a
separate band below the ministerial leaderboard.

Join: left-join on `role_canonical` after the verbose-tail trim. Rows
with no match are reported (see §5) and either added to the table or
left as `Unknown`.

## 4. Dedup and title cleanup

**Dedup.** 6,892 rows / 6,122 unique `(si_year, si_number)` ⇒ 770
duplicates. Cause: same SI appears in multiple Iris issues (correction
notices, re-publications). Keep the highest-confidence row per
`(si_year, si_number)`:

```python
deduped = (
    enriched
    .sort(["si_year", "si_number", "si_taxonomy_confidence", "extraction_confidence"],
          descending=[False, False, True, True])
    .unique(subset=["si_year", "si_number"], keep="first")
)
```

Verify: post-dedup row count must equal 6,122. Anything else means the
sort key didn't isolate one row per group.

**Title cleanup.** Two patterns in the existing CSV:

- Body bleed: `... ORDER 2018. This Statutory Instrument gives effect to the …`
  ⇒ truncate at the first `.` followed by ` This `, ` These `, or two
  spaces.
- Bare placeholder: `None` or `No. 19` ⇒ replace with the stripped
  enabling-Act label from `si_parent_legislation` if available, else
  flag `(Untitled)`.

Apply both after dedup so the highest-confidence row's title is the one
we clean.

## 5. Verification before pitching to Step B

Sibling validator: `pipeline_sandbox/iris_si_authorship_validate.py`
(mirrors [legislation_unscoped_validate.py](legislation_unscoped_validate.py)).
Reports the following counts; **all four numeric checks must pass**
before Step B is worth opening:

1. **Fill rate.** `department` non-null on >= 85 % of rows.
2. **Distinct departments.** Between 12 and 25 (any more = mapping
   leaks; any fewer = over-collapsing).
3. **Year-total parity.** Annual SI counts within ±2 % of the
   `iris_pdf_audit.csv` masthead counts for 2016 onwards.
4. **Dedup correctness.** Post-dedup row count == unique
   `(si_year, si_number)` count from the input.

Plus three editorial spot-checks (manual, log them in this file as a
ticked list when run):

- [ ] Finance is in the top 3 departments by SI count, all years.
- [ ] Agriculture is in the top 5 (driven by EU compliance / fisheries
      orders).
- [ ] No department named for a single minister's personal name.

If any of the four numeric checks fail, the failure mode (which check,
which years, which roles) goes into a follow-up section in this file
before retrying.

## 6. Out of scope (Step B — graduation, and Step C — page work)

Step A produces a clean sandbox CSV and nothing else. The following are
explicitly deferred:

- **Graduation to silver/gold parquet.** No `data/silver/` writes. No
  edits to `pipeline.py` or `enrich.py`.
- **SQL views** (`v_si_index`, `v_si_department_summary`,
  `v_si_department_quarter`). All wait on Step B.
- **`v_si_enabling_act_link` view** for the skeleton-legislation page —
  needs `si_parent_legislation` cleanup (separate effort).
- **Member-link join** (`v_si_member_link` — minister × tenure date).
  Needs `member_registry` integration; Step C territory.
- **Reshuffle-date-aware department attribution.** The map in §3 is
  durable-department-only. If a future page needs minister-of-the-day
  attribution, extend the reference table to a date-range schema.
- **Pre-2015 SIs.** 200 rows total, almost all retroactive references
  inside post-2015 SIs ("amends S.I. 142 of 1986"), not real 1986
  parses. Either suppress with a year floor or audit before counting.
- **Sunset-clause tracking.** Iris notices don't carry expiry; needs
  parsing inside `irishstatutebook.ie` HTML. Separate effort.
- **Topic clustering** beyond the existing `si_policy_domain_primary` —
  that field is already serviceable for Step B.

## Files this plan creates

```
pipeline_sandbox/
├── iris_si_authorship_enrich.py        NEW — pass-2 + dedup + title clean
├── iris_si_authorship_validate.py      NEW — four numeric checks, mirrors
│                                              legislation_unscoped_validate.py
├── reference/
│   └── minister_role_to_department.csv NEW — hand-curated, ~40 rows
└── iris_si_step_a_plan.md              THIS DOC

pipeline_sandbox/out/
└── iris_si_authorship.csv              OUTPUT of enrich.py — input to Step B
```

The enrich + validate scripts together should fit in <300 lines. Polars
only, per [project_polars_vs_pandas_split.md](../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_polars_vs_pandas_split.md).
