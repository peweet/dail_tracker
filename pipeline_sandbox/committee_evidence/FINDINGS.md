# Committee-Evidence Probe — Findings & Go/No-Go (Phase 0)

_Hand-written verdict (COVERAGE.md is auto-regenerated each run; this file is not). Sample:
15 most-recent meetings each for PAC + Joint Committee on Housing, since 2024-06-01._

## Verdict: GO (with eyes open)

Committee witnesses are overwhelmingly **real, identifiable organisations** — the question was
never whether we can find them, but whether they connect to data we hold. They do, and the
bottleneck is **name-matching precision, not coverage.**

### The numbers (and why the raw ones understate)
| | PAC | Housing |
|---|---|---|
| Witness orgs extracted | 37 | 28 |
| Exact normalised match | 14% | 21% |
| + token-subset ("likely") | 27% | 21% |
| Flagged-public-body ceiling | 43% | 36% |

The flagged ceiling (36–43%) is itself an **under**count of the truly matchable share, because the
public-body flag only fires on a keyword (Department/Agency/Authority/…). It misses named hospitals
(Beaumont, Mater), funds, and keyword-less agencies that we *do* hold. Realistic post-alias-map
matchable share is materially higher.

### What checked out by hand (zero false positives in sample)
- `Housing Agency` → payments payee ✓
- `Dublin City Council` → payments payee + councils roster ✓
- `An Coimisiún Pleanála` → payments payee ✓
- `Office of Public Works` → payments payee + publisher ✓
- `National Association of Regional Game Councils` → **lobbying register** ✓ (a genuine register cross-ref)

### The CCMA thread, vindicated
`County and City Management Association` / `CCMA` and `Construction Industry Federation` appear as
**recurring real witnesses that are invisible in our money data** (CCMA correctly matched nothing).
That invisibility is exactly the accountability gap the feature would surface — off-register bodies
shaping policy through committee evidence.

## Correctness check
- **Committee-identity reconciliation: 0 mismatches** (API `committeeCode` vs AKN FRBR path agreed on
  every meeting). The two-bodies conflation risk (CCMA internal committee vs Oireachtas committee) is
  controlled: we only record "org X gave evidence to Oireachtas committee Y on date Z", sourced to
  parliament's own API + path.

## Enumeration finding (resolved the plan's open risk)
- There is **no committee-submissions API endpoint**, and the `data.oireachtas.ie` S3 directory listing
  is denied. Submission PDFs are therefore **not enumerable**.
- BUT committee **meetings** are fully enumerable via `/v1/debates?chamber_type=committee`, and the
  witness-ORG signal is in the **transcript** (section headings + the Cathaoirleach's opening welcome),
  which is API-enumerable and self-contained. Submission PDFs remain a later per-meeting enrichment
  (fetchable by known URL once a meeting is identified), not the enumeration backbone.

## Phase-1 work-list (what turns ~20% exact into a strong feature)
1. **Org-identity alias map** for official body names — the diary feature's `ACRONYMS`/alias pattern,
   reused. Maps `Department of Housing` → its long publisher name, folds agency variants. This is where
   most of the gap closes.
2. **Use the FULL lobbying register**, not `top_lobbyist_organisations` (CIF was missed only because the
   probe used the top-N parquet; CIF is on the full register).
3. **Tighten extraction**: welcome-pattern over-captures trailing clutter ("… at 10", "… immediately on
   completion of the earlier business"); some pure-topic headings ("Market Cap Fund", "Hospital
   Insourcing Funding Arrangements") leak through. Cut at name boundaries; drop topic-only headings.
4. **Add the payer/publisher lens deliberately** for PAC (witness = Accounting Officer of a Vote →
   department = publisher), and cross-ref the C&AG Appropriation Accounts
   (project_cag_appropriation_accounts_2026_06_21) for the audited-outturn angle.

## Decision for the user
Exact-match floor ~14–21%, realistic post-alias-map ceiling well above the flagged 36–43%, residual is
genuine advocacy bodies (the actual story) plus filterable topic noise. Recommend proceeding to **Phase 1
on PAC + Housing**, building the alias map first, before any widening to Tier-A committees.
